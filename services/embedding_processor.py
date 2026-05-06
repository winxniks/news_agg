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

from config import settings
from services.database import database
from services.qdrant_service import qdrant_service
from services.task_manager import task_storage
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





#удалить
from services.embedding_service import embedding_service
#import psycopg2
#from qdrant_client import QdrantClient
#from fastembed import TextEmbedding
#from fastembed.rerank.cross_encoder import TextCrossEncoder






logger = logging.getLogger(__name__)


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
        
        # Executor для параллельной обработки
        self.executor: Optional[ProcessPoolExecutor] = None
        
        # Статистика обработки
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
        # Убеждаемся, что коллекция Qdrant существует
        await qdrant_service.ensure_collection()
        
        # Создаем executor для параллельной обработки
        self.executor = ProcessPoolExecutor(max_workers=self.max_workers)
        
        logger.info("EmbeddingProcessor инициализирован")

    async def get_chunks_batch(
        self,
        offset: int,
        limit: int,
        processed_after: Optional[datetime] = None,
        source: List[Source] = None
    ) -> List[Dict[str, Any]]:
        """
        Получает батч чанков из PostgreSQL.
        
        Args:
            offset: Смещение для пагинации
            limit: Количество чанков для получения
            processed_after: Фильтр по дате создания (только новые чанки)
        
        Returns:
            Список чанков с полями: chunk_id, doc_id, chunk_idx, chunk_text
        """
        query = """
            SELECT 
                c.chunk_id,
                c.doc_id,
                c.chunk_idx,
                c.chunk_text,
                c.valid_from_dttm,
                d.doc_src
            FROM news.chunks c
            LEFT JOIN news.documents d using(doc_id)
            WHERE 1=1
            """
        
        params = []
        param_counter = 1
        
        if processed_after:
            query += f" AND c.valid_from_dttm >= cast(${param_counter} AS timestamptz)"
            params.append(processed_after)
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
                        "valid_from_dttm": row["valid_from_dttm"],
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
            # Вставляем чанки в Qdrant (сервис сам проверяет дубликаты)
            inserted, duplicates = await qdrant_service.batch_insert_chunks(chunks)
            
            processed = len(chunks)
            return processed, inserted, duplicates
            
        except Exception as e:
            logger.error(f"Ошибка при обработке батча: {e}")
            return len(chunks), 0, 0

    async def process_all_chunks(
        self,
        processed_after: Optional[datetime] = None,
        source: List[Source] = None,
        show_progress: bool = True,
        task_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Обрабатывает все чанки из PostgreSQL.

        Args:
            processed_after: Фильтр по дате создания
            show_progress: Показывать ли прогресс-бар
            task_id: Идентификатор задачи в БД для отслеживания прогресса (опционально)

        Returns:
            Словарь со статистикой обработки
        """
        # Инициализация
        await self.initialize()
        
        # Получаем общее количество чанков
        total_chunks = await self.get_total_chunks_count_test() #get_total_chunks_count(processed_after, source)
        
        self.stats["total_chunks"] = total_chunks
        self.stats["start_time"] = datetime.now()
        
        logger.info(f"Начало обработки {total_chunks} чанков")
        
        # Обновляем статус задачи, если task_id передан
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
        
        # Создаем прогресс-бар
        pbar = tqdm(
            total=total_chunks,
            desc="Обработка чанков",
            unit="chunk",
            disable=not show_progress
        ) if show_progress else None
        
        # Обработка по батчам
        offset = 0
        batch_tasks = []
        
        while offset < total_chunks:
            # Получаем батч чанков
            '''chunks = await self.get_chunks_batch(
                offset=offset,
                limit=self.batch_size,
                processed_after=processed_after,
                source=source
            )'''

            chunks = await self.get_chunks_batch_test()
            
            if not chunks:
                break
            
            # Создаем задачу на обработку батча
            task = asyncio.create_task(self.process_chunks_batch(chunks))
            batch_tasks.append(task)
            
            # Обновляем offset
            offset += len(chunks)
            
            # Ограничиваем количество параллельных задач
            if len(batch_tasks) >= self.max_workers * 2:
                # Ждем завершения некоторых задач
                done, pending = await asyncio.wait(
                    batch_tasks, 
                    return_when=asyncio.FIRST_COMPLETED
                )
                
                # Обновляем статистику и прогресс
                for done_task in done:
                    try:
                        processed, inserted, duplicates = done_task.result()
                        self.stats["processed_chunks"] += processed
                        self.stats["inserted_chunks"] += inserted
                        self.stats["duplicate_chunks"] += duplicates
                        
                        # Обновляем прогресс задачи, если task_id передан
                        if task_id:
                            try:
                                # Обновляем результат с текущей статистикой
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
                    # Оцениваем количество неудачных чанков приблизительно
                    self.stats["failed_chunks"] += self.batch_size
                else:
                    processed, inserted, duplicates = result
                    self.stats["processed_chunks"] += processed
                    self.stats["inserted_chunks"] += inserted
                    self.stats["duplicate_chunks"] += duplicates
                    
                    if pbar:
                        pbar.update(processed)
        
        # Завершаем прогресс-бар
        if pbar:
            pbar.close()
        
        # Завершаем статистику
        self.stats["end_time"] = datetime.now()
        self.stats["processing_time"] = (
            self.stats["end_time"] - self.stats["start_time"]
        ).total_seconds()

        self.stats["end_time"] = self.stats["end_time"].isoformat()
        self.stats["start_time"] = self.stats["start_time"].isoformat()
        
        # Логируем результаты
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
        
        return self.stats.copy()

    async def get_total_chunks_count(
        self,
        processed_after: Optional[datetime] = None,
        source: List[Source] = None
    ) -> int:
        """
        Возвращает общее количество чанков в PostgreSQL.
        
        Args:
            processed_after: Фильтр по дате создания
        
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
            query += f" AND c.valid_from_dttm >= cast(${param_counter} AS timestamptz)"
            params.append(processed_after)
            param_counter += 1
        
        if source:
            query += f" AND d.doc_src = ${param_counter}"
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
        use_reranker: bool = True
    ) -> VectorSearchResponse:
        """
        Поиск дубликатов для входного текста.
        
        Выполняет двухэтапный поиск:
        1. Primary search по косинусному сходству в Qdrant
        2. Reranking с помощью reranker модели (если включено)
        
        Возвращает найденные чанки с оценками схожести.
        """
        start_time = datetime.now()
        
        # Выполняем поиск дубликатов
        results = await qdrant_service.search_duplicates_by_text(
            text=text,
            top_k=top_k,
            similarity_threshold=similarity_threshold,
            use_reranker=use_reranker
        )
        
        # Преобразуем результаты в формат ответа
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
                similarity_score=result["score"],
                reranker_score=result.get("reranker_score"),
                final_score=result.get("final_score"),
                chunk_text=payload.get("chunk_text", ""),
                is_duplicate=is_duplicate
            ))
        processing_time_ms = (datetime.now() - start_time).total_seconds() * 1000
        # Получаем размер эмбеддинга
        return VectorSearchResponse(
            query_text=text,
            results=duplicate_results,
            processing_time_ms=processing_time_ms,
            total_found=len(results),
            duplicates_found=duplicates_found
        )

    async def get_vector_stats(self) -> VectorStatsResponse:
        """
        Возвращает статистику по векторной БД.
        """
        collection_info = await qdrant_service.get_collection_info()
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
            warnings=collection_info.get("warnings", "None"),
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
        
        # Извлекаем статистику из result (если задача завершена)
        if result and isinstance(result, dict):
            processed_chunks = result.get("processed_chunks", 0)
            inserted_chunks = result.get("inserted_chunks", 0)
            duplicate_chunks = result.get("duplicate_chunks", 0)
            failed_chunks = result.get("failed_chunks", 0)
            processing_time = result.get("processing_time", 0.0)
        
        # Вычисляем скорость обработки
        processing_speed = 0.0
        if processing_time > 0:
            processing_speed = processed_chunks / processing_time
        
        # Оцениваем время завершения, если задача в процессе
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
        source: List[Source],
        show_progress: bool,
        background_tasks: BackgroundTasks
    ) -> BatchProcessResponse:
        """
        Создает задачу в БД и добавляет фоновую задачу для выполнения batch-обработки.
        """
        parameters = {
            "processed_after": processed_after.isoformat() if processed_after else None,
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
        source: List[Source],
        show_progress: bool
    ):
        """Фоновая задача для выполнения batch-обработки."""
        try:
            # Обновляем статус задачи на "в процессе"
            await task_storage.update_task(
                task_id=uuid.UUID(task_id),
                status=TaskStatus.RUNNING,
                started_at=datetime.now()
            )
            
            logger.info(f"Начало обработки задачи {task_id}")
            
            # Выполняем обработку с передачей task_id для отслеживания прогресса
            stats = await self.process_all_chunks(
                processed_after=processed_after,
                source=source,
                show_progress=show_progress,
                task_id=task_id
            )
            
            # Обновляем задачу как завершенную с результатами
            await task_storage.update_task(
                task_id=uuid.UUID(task_id),
                status=TaskStatus.COMPLETED,
                finished_at=datetime.now(),
                result=stats
            )
            
            logger.info(f"Задача {task_id} завершена успешно")
            
        except Exception as e:
            logger.error(f"Ошибка при выполнении задачи {task_id}: {e}")
            # Обновляем задачу как неудачную
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


    async def get_chunks_batch_test(
        self
    ) -> List[Dict[str, Any]]:
        """
        Получает батч чанков из PostgreSQL.
        
        Args:
            offset: Смещение для пагинации
            limit: Количество чанков для получения
            processed_after: Фильтр по дате создания (только новые чанки)
        
        Returns:
            Список чанков с полями: chunk_id, doc_id, chunk_idx, chunk_text
        """
        query = """
            SELECT 
                c.chunk_id,
                c.doc_id,
                c.chunk_idx,
                c.chunk_text,
                c.valid_from_dttm,
                d.doc_src
            FROM news.chunks_test c
            LEFT JOIN news.documents d using(doc_id)
            WHERE 1=1
            """
        try:
            async with database.get_connection() as conn:
                rows = await conn.fetch(query)
                
                chunks = []
                for row in rows:
                    chunks.append({
                        "chunk_id": row["chunk_id"],
                        "doc_id": row["doc_id"],
                        "chunk_idx": row["chunk_idx"],
                        "chunk_text": row["chunk_text"],
                        "valid_from_dttm": row["valid_from_dttm"],
                        "doc_src": row["doc_src"]
                    })
                
                logger.debug(f"Получено {len(chunks)} чанков")
                return chunks
            
        except Exception as e:
            logger.error(f"Ошибка при получении чанков: {e}")
            return []

    async def get_total_chunks_count_test(
        self
    ) -> int:
        """
        Возвращает общее количество чанков в PostgreSQL.
        
        Args:
            processed_after: Фильтр по дате создания
        
        Returns:
            Количество чанков
        """
        query = """
            SELECT COUNT(*) AS count 
            FROM news.chunks_test c
            LEFT JOIN news.documents d using(doc_id)
            WHERE 1=1
            """
        
        try:
            async with database.get_connection() as conn:
                result = await conn.fetchval(query)
                
                logger.info(f"Всего чанков в БД: {result}")
                return result
            
        except Exception as e:
            logger.error(f"Ошибка при подсчете чанков: {e}")
            return 0

# Глобальный экземпляр процессора для использования в приложении
embedding_processor = EmbeddingProcessor()