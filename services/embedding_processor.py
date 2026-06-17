"""
Batch-процессор для обработки чанков из PostgreSQL.
Читает чанки пачками, вычисляет эмбеддинги, ищет дубликаты в Qdrant и вставляет уникальные записи.
Поддерживает параллельную обработку на CPU с отслеживанием прогресса.
"""
import asyncio
import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
from concurrent.futures import ProcessPoolExecutor

from fastapi import BackgroundTasks

import asyncpg
from tqdm import tqdm
import psutil

from config import settings
from services.database import database
from services.qdrant_service import qdrant_service
from services.task_manager import task_storage
from services.llm_service import llm_service
from models.schemas import (
    TaskStatus,
    TaskType,
    VectorSearchResponse,
    DuplicateResult,
    BatchProcessResponse,
    VectorStatsResponse,
    Source,
)
import uuid

logger = logging.getLogger(__name__)


def log_system_resources(prefix: str = ""):
    """Логирование использования памяти, CPU и диска."""
    try:
        mem = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        cpu_percent = psutil.cpu_percent(interval=0.1)
        logger.info(
            f"{prefix} MEM: {mem.used / 1024**3:.2f}GB/{mem.total / 1024**3:.2f}GB "
            f"({mem.percent}%), DISK: {disk.used / 1024**3:.2f}GB/{disk.total / 1024**3:.2f}GB "
            f"({disk.percent}%), CPU: {cpu_percent}%"
        )
    except Exception as e:
        logger.warning(f"Не удалось получить системные метрики: {e}")


class EmbeddingProcessor:
    """Процессор для пакетной обработки чанков."""

    def __init__(self, max_workers: Optional[int] = None):
        """
        Инициализация процессора.
        
        Args:
            max_workers: Количество воркеров для параллельной обработки
        """
        self.max_workers = max_workers or settings.EMBEDDING_MAX_WORKERS
        self.batch_size = settings.BATCH_SIZE
        self.similarity_threshold = settings.SIMILARITY_THRESHOLD
        self.llm_max_candidates = settings.LLM_MAX_CANDIDATES
        
        # Executor для параллельной обработки
        self.executor: Optional[ProcessPoolExecutor] = None
        
        self.stats = {
            "total_chunks": 0,
            "processed_chunks": 0,
            "inserted_chunks": 0,
            "duplicate_chunks": 0,
            "failed_chunks": 0,
            "start_time": None,
            "end_time": None,
            "processing_time": 0.0
        }
        
        logger.info(
            f"EmbeddingProcessor инициализирован: "
            f"max_workers={self.max_workers}, "
            f"batch_size={self.batch_size}"
        )

    async def initialize(self):
        """Инициализация необходимых ресурсов."""
        await qdrant_service.ensure_collection()
        
        self.executor = ProcessPoolExecutor(max_workers=self.max_workers)
        
        logger.info("EmbeddingProcessor инициализирован")

    async def get_chunks_batch(
        self,
        offset: int,
        limit: int,
        processed_after: Optional[datetime] = None,
        processed_before: Optional[datetime] = None,
        source: List[Source] = None
    ) -> List[Dict[str, Any]]:
        """
        Получает батч чанков из PostgreSQL.
        
        Args:
            offset: Смещение для пагинации
            limit: Количество чанков для получения
            processed_after: Фильтр по дате создания после (только новые чанки)
            processed_before: Фильтр по дате создания до (только старые чанки)
            source: Фильтр по источнику
        
        Returns:
            Список чанков с полями: chunk_id, doc_id, chunk_idx, chunk_text
        """
        query = """
            SELECT 
                c.chunk_id,
                c.doc_id,
                c.chunk_idx,
                c.chunk_text,
                d.public_dttm,
                to_char(d.public_dttm, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS public_dttm,
                d.doc_src
            FROM news.chunks c
            LEFT JOIN news.documents d using(doc_id)
            WHERE 1=1
            """
        
        params = []
        param_counter = 1
        
        if processed_after:
            query += f" AND d.public_dttm >= cast(${param_counter} AS timestamptz)"
            params.append(processed_after)
            param_counter += 1

        if processed_before:
            query += f" AND d.public_dttm <= cast(${param_counter} AS timestamptz)"
            params.append(processed_before)
            param_counter += 1
        
        if source:
            query += f" AND d.doc_src = ${param_counter}"
            params.append(source)
            param_counter += 1
        
        query += f" ORDER BY c.chunk_id LIMIT ${param_counter} OFFSET ${param_counter + 1}"
        params.extend([limit, offset])
        
        try:
            async with database.get_connection() as conn:
                rows = await conn.fetch(query, *params)
                
                chunks = []
                for row in rows:
                    chunks.append({
                        "chunk_id": row["chunk_id"],
                        "doc_id": row["doc_id"],
                        "chunk_idx": row["chunk_idx"],
                        "chunk_text": row["chunk_text"],
                        "public_dttm": row["public_dttm"],
                        "doc_src": row["doc_src"]
                    })
                
                logger.debug(f"Получено {len(chunks)} чанков (offset={offset}, limit={limit})")
                return chunks
            
        except Exception as e:
            logger.error(f"Ошибка при получении чанков: {e}")
            return []

    async def process_chunks_batch(
        self,
        chunks: List[Dict[str, Any]]
    ) -> Tuple[int, int, int]:
        """
        Обрабатывает батч чанков.
        
        Args:
            chunks: Список чанков для обработки
        
        Returns:
            Кортеж (обработано, вставлено, дубликатов)
        """
        if not chunks:
            return 0, 0, 0

        try:
            inserted, duplicates = await qdrant_service.batch_insert_chunks(chunks)
            
            processed = len(chunks)
            return processed, inserted, duplicates
            
        except Exception as e:
            logger.error(f"Ошибка при обработке батча: {e}")
            return len(chunks), 0, 0

    async def process_all_chunks(
        self,
        processed_after: Optional[datetime] = None,
        processed_before: Optional[datetime] = None,
        source: List[Source] = None,
        show_progress: bool = True,
        task_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Обрабатывает все чанки из PostgreSQL.

        Args:
            processed_after: Фильтр по дате создания после
            processed_before: Фильтр по дате создания до
            source: Фильтр по источнику
            show_progress: Показывать ли прогресс-бар
            task_id: Идентификатор задачи в БД для отслеживания прогресса (опционально)

        Returns:
            Словарь со статистикой обработки
        """
        await self.initialize()
        
        total_chunks = await self.get_total_chunks_count(processed_after, processed_before, source)
        
        self.stats["total_chunks"] = total_chunks
        self.stats["start_time"] = datetime.now()
        
        logger.info(f"Начало обработки {total_chunks} чанков")
        log_system_resources("START")
        
        if task_id:
            try:
                await task_storage.update_task(
                    task_id=task_id,
                    status=TaskStatus.RUNNING,
                    started_at=datetime.now()
                )
                logger.info(f"Задача {task_id} переведена в статус RUNNING")
            except Exception as e:
                logger.error(f"Не удалось обновить задачу {task_id}: {e}")
        
        pbar = tqdm(
            total=total_chunks,
            desc="Обработка чанков",
            unit="chunk",
            disable=not show_progress
        ) if show_progress else None
        
        offset = 0
        batch_tasks = []
        
        while offset < total_chunks:
            chunks = await self.get_chunks_batch(
                offset=offset,
                limit=self.batch_size,
                processed_after=processed_after,
                processed_before=processed_before,
                source=source
            )
            
            if not chunks:
                break
            
            task = asyncio.create_task(self.process_chunks_batch(chunks))
            batch_tasks.append(task)
            
            offset += len(chunks)
            
            # Ограничиваем количество параллельных задач
            if len(batch_tasks) >= self.max_workers * 2:
                # Ждем завершения некоторых задач
                done, pending = await asyncio.wait(
                    batch_tasks, 
                    return_when=asyncio.FIRST_COMPLETED
                )
                
                for done_task in done:
                    try:
                        processed, inserted, duplicates = done_task.result()
                        self.stats["processed_chunks"] += processed
                        self.stats["inserted_chunks"] += inserted
                        self.stats["duplicate_chunks"] += duplicates
                        
                        log_system_resources(f"BATCH processed={processed}, inserted={inserted}, duplicates={duplicates}")
                        
                        if task_id:
                            try:
                                await task_storage.update_task(
                                    task_id=task_id,
                                    result=self.stats.copy()
                                )
                            except Exception as e:
                                logger.warning(f"Не удалось обновить прогресс задачи {task_id}: {e}")
                        
                        if pbar:
                            pbar.update(processed)
                    except Exception as e:
                        logger.error(f"Ошибка в задаче обработки: {e}")
                        self.stats["failed_chunks"] += len(chunks)
                
                # Убираем завершенные задачи
                batch_tasks = list(pending)
        
        # Ждем завершения оставшихся задач
        if batch_tasks:
            results = await asyncio.gather(*batch_tasks, return_exceptions=True)
            
            for result in results:
                if isinstance(result, Exception):
                    logger.error(f"Ошибка в задаче обработки: {result}")
                    self.stats["failed_chunks"] += self.batch_size
                else:
                    processed, inserted, duplicates = result
                    self.stats["processed_chunks"] += processed
                    self.stats["inserted_chunks"] += inserted
                    self.stats["duplicate_chunks"] += duplicates
                    
                    if pbar:
                        pbar.update(processed)
        
        if pbar:
            pbar.close()
        
        self.stats["end_time"] = datetime.now()
        self.stats["processing_time"] = (
            self.stats["end_time"] - self.stats["start_time"]
        ).total_seconds()

        self.stats["end_time"] = self.stats["end_time"].isoformat()
        self.stats["start_time"] = self.stats["start_time"].isoformat()
        
        logger.info(
            f"Обработка завершена: "
            f"обработано={self.stats['processed_chunks']}, "
            f"вставлено={self.stats['inserted_chunks']}, "
            f"дубликатов={self.stats['duplicate_chunks']}, "
            f"ошибок={self.stats['failed_chunks']}, "
            f"время={self.stats['processing_time']:.2f}с"
        )
        
        # Обновляем задачу как завершенную, если task_id передан
        '''if task_id:
            try:
                await task_storage.update_task(
                    task_id=task_id,
                    status=TaskStatus.COMPLETED,
                    finished_at=datetime.now(),
                    result=self.stats.copy()
                )
                logger.info(f"Задача {task_id} переведена в статус COMPLETED")
            except Exception as e:
                logger.error(f"Не удалось обновить задачу {task_id} как завершенную: {e}")'''
        
        log_system_resources("FINISH")
        return self.stats.copy()

    async def get_total_chunks_count(
        self,
        processed_after: Optional[datetime] = None,
        processed_before: Optional[datetime] = None,
        source: List[Source] = None
    ) -> int:
        """
        Возвращает общее количество чанков в PostgreSQL.
        
        Args:
            processed_after: Фильтр по дате создания после
            processed_before: Фильтр по дате создания до
            source: Фильтр по источнику
        
        Returns:
            Количество чанков
        """
        query = """
            SELECT COUNT(*) AS count 
            FROM news.chunks c
            LEFT JOIN news.documents d using(doc_id)
            WHERE 1=1
            """
        
        params = []
        param_counter = 1
        
        if processed_after:
            query += f" AND d.public_dttm >= cast(${param_counter} AS timestamptz)"
            params.append(processed_after)
            param_counter += 1

        if processed_before:
            query += f" AND d.public_dttm <= cast(${param_counter} AS timestamptz)"
            params.append(processed_before)
            param_counter += 1
        
        if source:
            placeholders = ', '.join([f'${i}' for i in range(param_counter, param_counter + len(source))])
            query += f" AND d.doc_src IN ({placeholders})"
            params.append(source)

        try:
            async with database.get_connection() as conn:
                result = await conn.fetchval(query, *params)
                
                logger.info(f"Всего чанков в БД: {result}")
                return result
            
        except Exception as e:
            logger.error(f"Ошибка при подсчете чанков: {e}")
            return 0

    async def search_duplicates(
        self,
        text: str,
        top_k: Optional[int] = None,
        similarity_threshold: Optional[float] = None,
        filter_date: Optional[datetime] = None,
        use_rescore: bool = True,
        use_reranker: bool = True,
        use_llm: bool = False,
    ) -> VectorSearchResponse:
        """
        Поиск дубликатов для входного текста.
        
        Выполняет двухэтапный поиск:
        1. Primary search по косинусному сходству в Qdrant
        2. Reranking с помощью reranker модели (если включено)
        3. LLM анализ дубликатов (если включен use_llm)
        
        Возвращает найденные чанки с оценками схожести и анализ LLM.
        """
        start_time = datetime.now()
        logger.info(f"Начало поиска дубликатов")
        
        results = await qdrant_service.search_duplicates_by_text(
            text=text,
            top_k=top_k,
            similarity_threshold=similarity_threshold,
            filter_date=filter_date,
            use_rescore=use_rescore,
            use_reranker=use_reranker,
        )
        
        duplicate_results = []
        duplicates_found = 0
        
        for result in results:
            payload = result.get("payload", {})
            is_duplicate = result["score"] >= (similarity_threshold or settings.SIMILARITY_THRESHOLD)
            if is_duplicate:
                duplicates_found += 1
            duplicate_results.append(DuplicateResult(
                chunk_id=payload.get("chunk_id", 0),
                doc_id=payload.get("doc_id", 0),
                doc_src=payload.get("source", ""),
                doc_public_dttm=payload.get("public_dttm", None),
                similarity_score=result["score"],
                reranker_score=result.get("reranker_score"),
                chunk_text=payload.get("chunk_text", ""),
                is_duplicate=is_duplicate
            ))
        
        processing_time_ms = (datetime.now() - start_time).total_seconds() * 1000
        
        # LLM анализ (если включен)
        llm_analysis = None
        llm_processing_time_ms = None
        llm_max_candidates = self.llm_max_candidates
        
        if use_llm and duplicate_results:
            llm_start_time = datetime.now()
            
            # Ограничиваем количество кандидатов
            candidates_for_llm = duplicate_results[:llm_max_candidates]
            
            logger.info(
                f"Запуск LLM анализа для {len(candidates_for_llm)} кандидатов "
                f"(из {len(duplicate_results)} найденных)"
            )
            
            llm_analysis = await llm_service.analyze_duplicates(
                query_text=text,
                filter_date=filter_date if filter_date else None,
                candidates=candidates_for_llm,
            )
            
            llm_processing_time_ms = (datetime.now() - llm_start_time).total_seconds() * 1000
            logger.info(f"LLM анализ завершен за {llm_processing_time_ms:.2f}мс")
        
        return VectorSearchResponse(
            query_text=text,
            results=duplicate_results,
            processing_time_ms=processing_time_ms,
            total_found=len(results),
            duplicates_found=duplicates_found,
            llm_analysis=llm_analysis,
            llm_processing_time_ms=llm_processing_time_ms,
        )
    
    async def get_vector_stats(self) -> VectorStatsResponse:
        """
        Возвращает статистику по векторной БД.
        """
        collection_info = await qdrant_service.get_collection_info()
        
        warnings_value = collection_info.get("warnings", "None")
        if isinstance(warnings_value, list):
            warnings_value = ", ".join(warnings_value) if warnings_value else "None"
        elif not warnings_value:
            warnings_value = "None"
        
        return VectorStatsResponse(
            collection_name=settings.QDRANT_COLLECTION_NAME,
            vectors_count=collection_info.get("vectors_count", 0),
            vector_dimension=settings.VECTOR_DIMENSION,
            similarity_threshold=settings.SIMILARITY_THRESHOLD,
            top_k=settings.TOP_K,
            embedding_model=settings.EMBEDDING_MODEL_NAME,
            reranker_model=settings.RERANKER_MODEL_NAME,
            distance_metric=settings.DISTANCE,
            qdrant_status=collection_info.get("status", "unknown"),
            indexed_vectors_count=collection_info.get("indexed_vectors_count", 0),
            indexed_vectors_size=collection_info.get("indexed_vectors_size", 0),
            segments_count=collection_info.get("segments_count", 0),
            warnings=warnings_value,
            vectors_config=collection_info.get("vectors_config", {}),
            optimizers_config=collection_info.get("optimizers_config", {}),
            hnsw_config=collection_info.get("hnsw_config", {}),
            quantization_config=collection_info.get("quantization_config", {}),
            on_disk=collection_info.get("on_disk", False)
        )

    async def health_check(self) -> Dict[str, Any]:
        """
        Проверка здоровья векторных сервисов.
        
        Проверяет подключение к Qdrant и доступность моделей эмбеддингов.
        """
        health_status = await qdrant_service.health_check()
        health_status["timestamp"] = datetime.now().isoformat()
        
        return health_status

    async def get_batch_status(self, task_id: uuid.UUID) -> BatchProcessResponse:
        """
        Получение статуса batch-задачи.
        """
        task = await task_storage.get_task(task_id)
        if not task:
            raise ValueError(f"Задача {task_id} не найдена")
        return await self._convert_to_batch_response(task)

    async def list_vector_tasks(
        self,
        limit: int = 20,
        offset: int = 0,
        status: Optional[TaskStatus] = None
    ) -> List[BatchProcessResponse]:
        """
        Возвращает список векторных задач с пагинацией.
        """
        tasks = await task_storage.list_tasks(
            limit=min(limit, 20),
            offset=offset,
            status=status,
            task_type=TaskType.EMBED
        )
        responses = []
        for task in tasks:
            response = await self._convert_to_batch_response(task)
            responses.append(response)
        return responses

    async def _convert_to_batch_response(
            self, 
            task: Dict
        ) -> BatchProcessResponse:
        """
        Преобразует задачу из БД в формат, совместимый с BatchProcessResponse.
        """
        parameters = task.get("parameters", {})
        result = task.get("result", {})
        
        if result and isinstance(result, dict):
            processed_chunks = result.get("processed_chunks", 0)
            inserted_chunks = result.get("inserted_chunks", 0)
            duplicate_chunks = result.get("duplicate_chunks", 0)
            failed_chunks = result.get("failed_chunks", 0)
            processing_time = result.get("processing_time", 0.0)
        
        processing_speed = 0.0
        if processing_time > 0:
            processing_speed = processed_chunks / processing_time
        
        estimated_completion_time = None
        if task["status"] == TaskStatus.RUNNING and processing_speed > 0:
            total_chunks = parameters.get("total_chunks", 0)
            if total_chunks > processed_chunks:
                remaining = total_chunks - processed_chunks
                remaining_seconds = remaining / processing_speed
                estimated_completion_time = datetime.now() + timedelta(seconds=remaining_seconds)
        
        return BatchProcessResponse(
            task_id=task["task_id"],
            status=task["status"],
            processed_chunks=processed_chunks,
            inserted_chunks=inserted_chunks,
            duplicate_chunks=duplicate_chunks,
            failed_chunks=failed_chunks,
            processing_time_seconds=processing_time,
            processing_speed_chunks_per_sec=processing_speed,
            estimated_completion_time=estimated_completion_time,
            created_at=task["created_at"],
            started_at=task["started_at"],
            completed_at=task["finished_at"]
        )

    async def execute_batch_task(
        self,
        processed_after: Optional[datetime],
        processed_before: Optional[datetime],
        source: List[Source],
        show_progress: bool,
        background_tasks: BackgroundTasks
    ) -> BatchProcessResponse:
        """
        Создает задачу в БД и добавляет фоновую задачу для выполнения batch-обработки.
        """
        parameters = {
            "processed_after": processed_after.isoformat() if processed_after else None,
            "processed_before": processed_before.isoformat() if processed_before else None,
            "show_progress": show_progress
        }

        task_id = await task_storage.create_task(
            task_type=TaskType.EMBED,
            sources=[],
            parameters=parameters,
        )

        created_at = datetime.now()

        background_tasks.add_task(
            self._run_batch_processing,
            processed_after=processed_after,
            processed_before=processed_before,
            show_progress=show_progress,
            source=source,
            task_id=str(task_id)
        )

        response = BatchProcessResponse(
            task_id=task_id,
            status=TaskStatus.PENDING,
            processed_chunks=0,
            inserted_chunks=0,
            duplicate_chunks=0,
            failed_chunks=0,
            processing_time_seconds=0.0,
            processing_speed_chunks_per_sec=0.0,
            estimated_completion_time=None,
            created_at=created_at,
            started_at=None,
            completed_at=None
        )

        logger.info(f"Запущена batch-обработка: task_id={task_id}")
        
        return response
    
    async def _run_batch_processing(
        self,
        task_id: str,
        processed_after: Optional[datetime],
        processed_before: Optional[datetime],
        source: List[Source],
        show_progress: bool
    ):
        """Фоновая задача для выполнения batch-обработки."""
        try:
            await task_storage.update_task(
                task_id=uuid.UUID(task_id),
                status=TaskStatus.RUNNING,
                started_at=datetime.now()
            )
            
            logger.info(f"Начало обработки задачи {task_id}")
            
            stats = await self.process_all_chunks(
                processed_after=processed_after,
                processed_before=processed_before,
                source=source,
                show_progress=show_progress,
                task_id=task_id
            )
            
            await task_storage.update_task(
                task_id=uuid.UUID(task_id),
                status=TaskStatus.COMPLETED,
                finished_at=datetime.now(),
                result=stats
            )
            
            logger.info(f"Задача {task_id} завершена успешно")
            
        except Exception as e:
            logger.error(f"Ошибка при выполнении задачи {task_id}: {e}")
            try:
                await task_storage.update_task(
                    task_id=uuid.UUID(task_id),
                    status=TaskStatus.FAILED,
                    finished_at=datetime.now(),
                    error_message=str(e)
                )
            except Exception as update_error:
                logger.error(f"Не удалось обновить задачу {task_id} как FAILED: {update_error}")


    async def cleanup(self):
        """Очистка ресурсов."""
        if self.executor:
            self.executor.shutdown(wait=True)
            self.executor = None
            logger.info("ProcessPoolExecutor остановлен")

    def get_processing_speed(self) -> float:
        """
        Возвращает скорость обработки (чанков в секунду).
        
        Returns:
            Скорость обработки в чанках/сек
        """
        if not self.stats["processing_time"]:
            return 0.0
        
        return self.stats["processed_chunks"] / self.stats["processing_time"]
    
    async def delete_collection(self, collection_name: str):
        """
        Удаление коллекции Qdrant.
        
        Args:
            collection_name: Имя коллекции
        
        Returns:
            Флаг успешности удаления и инфоративное сообщение
        """
        return await qdrant_service.delete_collection(collection_name)

embedding_processor = EmbeddingProcessor()