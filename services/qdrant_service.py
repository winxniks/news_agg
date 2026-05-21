"""
Сервис для работы с векторной базой данных Qdrant.
Обеспечивает создание коллекций, вставку векторов и поиск дубликатов.
"""
import asyncio
import logging
from typing import List, Optional, Dict, Any, Tuple
from uuid import uuid4
from datetime import datetime, timedelta

from qdrant_client import QdrantClient, models
from qdrant_client.http.exceptions import UnexpectedResponse
from qdrant_client.http.models import Distance, VectorParams, PointStruct, Filter, FieldCondition, MatchValue
import psutil
from services.database import database

from config import settings
from services.embedding_service import embedding_service
from models.schemas import (
    FilterDate
)

logger = logging.getLogger(__name__)


def log_system_resources_qdrant(prefix: str = ""):
    """Логирование использования памяти, CPU и диска для Qdrant сервиса."""
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


class QdrantService:
    """Сервис для взаимодействия с Qdrant."""

    def __init__(self):
        self.host = settings.QDRANT_HOST
        self.port = settings.QDRANT_PORT
        self.cluster_endpoint=settings.CLUSTER_ENDPOINT
        self.api_key=settings.API_KEY
        self.collection_name = settings.QDRANT_COLLECTION_NAME
        self.vector_dimension = settings.VECTOR_DIMENSION
        self.similarity_threshold = settings.SIMILARITY_THRESHOLD
        self.duplicate_threshold = settings.DUPLICATE_THRESHOLD
        self.top_k = settings.TOP_K

        if settings.DISTANCE == "cosine":
            self.distance = Distance.COSINE
        elif settings.DISTANCE == "euclidean":
            self.distance = Distance.EUCLID
        elif settings.DISTANCE == "dot":
            self.distance = Distance.DOT
        elif settings.DISTANCE == "manhattan":
            self.distance = Distance.MANHATTAN
        else:
            logger.info(f"Метрика расстояния {settings.DISTANCE} не поддерживается. Используется cosine.")
            self.distance = Distance.COSINE
        
        # Инициализация клиента (ленивая загрузка)
        self._client: Optional[QdrantClient] = None
        
        logger.info(
            f"QdrantService инициализирован: "
            f"collection={self.collection_name}, "
            f"dimension={self.vector_dimension}"
        )

    @property
    def client(
        self, 
        use_cloud: bool = True
        ) -> QdrantClient:
        """Ленивая загрузка клиента Qdrant."""
        if self._client is None:

            if use_cloud:
                logger.info(f"Подключение к Qdrant по адресу {self.cluster_endpoint}")
                # облачный qdrant
                self._client = QdrantClient(
                    url=self.cluster_endpoint, 
                    api_key=self.api_key,
                    timeout=30.0
                )

            else:
                logger.info(f"Подключение к Qdrant по адресу {self.host}:{self.port}")
                # локальный qdrant
                self._client = QdrantClient(
                    host=self.host,
                    port=self.port,
                    timeout=30.0
                )
            
            try:
                self._client.get_collections()
                logger.info("Успешное подключение к Qdrant")
            except Exception as e:
                logger.error(f"Ошибка подключения к Qdrant: {e}")
                raise
        
        return self._client

    async def ensure_collection(self) -> bool:
        """
        Создает коллекцию, если она не существует.
        
        Returns:
            True если коллекция создана или уже существует
        """
        try:
            collections = self.client.get_collections()
            collection_names = [col.name for col in collections.collections]
            
            if self.collection_name in collection_names:
                logger.info(f"Коллекция '{self.collection_name}' уже существует")
                return True
            
            self.client.create_collection(
                collection_name=self.collection_name,
                vectors_config={"text": models.VectorParams(
                    size=self.vector_dimension,  # CLIP embeddings are 512-dimensional
                    distance=self.distance,  # Use cosine similarity
                    #datatype=models.Datatype.FLOAT16,  # Use float16 for memory efficiency
                    on_disk=True  # Store vectors on disk to handle large dataset
                )},
                quantization_config=models.BinaryQuantization(
                    binary=models.BinaryQuantizationConfig(
                        always_ram=True,  # Keep quantized vectors in RAM for faster search
                    )
                ),
                optimizers_config=models.OptimizersConfigDiff(
                    default_segment_number=2, # Start number of segments
                    # Bigger size of segments are desired for faster search
                    # However it might be slower for indexing
                    #max_segment_size=20000,
                    indexing_threshold=1000,
                ),
                hnsw_config=models.HnswConfigDiff(
                    m=0,  # 0 - индекс не строится для первой большой загрузки, Balanced connections (16 - default), 6 - decrease M for lower memory usage
                    ef_construct=200,  # Good build quality (default)
                    full_scan_threshold=1000,  # Use brute force below this size (default)
                    #on_disk=False,  # Keep HNSW index in RAM for faster search
                    #max_indexing_threads=0,  # Number of threads for indexing (default) - все доступные ядра CPU минус одно
                ),
            )

            self.client.create_payload_index(
                collection_name=self.collection_name,
                field_name="public_dttm",
                field_schema=models.PayloadSchemaType.DATETIME,
            )

            self.client.create_payload_index(
                collection_name=self.collection_name,
                field_name="source",
                field_schema=models.PayloadSchemaType.KEYWORD,
            )
            
            logger.info(f"Коллекция '{self.collection_name}' создана")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при создании коллекции: {e}")
            return False

    async def insert_points(
        self,
        points: List[Dict[str, Any]]
    ) -> List[str]:
        """
        Вставляет точки (векторы) в коллекцию.
        
        Args:
            points: Список словарей с данными точек:
                - id: идентификатор точки (str)
                - vector: эмбеддинг (List[float])
                - payload: метаданные (dict)
        
        Returns:
            Список идентификаторов успешно вставленных точек
        """
        if not points:
            return []

        try:
            point_structs = []
            for point in points:
                point_struct = PointStruct(
                    id=point["id"],
                    vector={"text": point["vector"]},
                    payload=point.get("payload", {})
                )
                point_structs.append(point_struct)

            self.client.upload_points(
                collection_name=self.collection_name,
                points=point_structs,
                wait=True,
                parallel=4,
                max_retries=2 #3
            )

            logger.info(f"Вставлено {len(points)} точек")
            return [point["id"] for point in points]
            
        except Exception as e:
            logger.error(f"Ошибка при вставке точек: {e}")
            raise

    async def search_duplicates(
        self,
        vector: List[float],
        use_filter: FilterDate,
        filter_date: Optional[datetime],
        use_rescore: bool,
        top_k: Optional[int] = None,
        similarity_threshold: Optional[float] = None,
    ) -> List[Dict[str, Any]]:
        """
        Ищет дубликаты в коллекции.
        
        Args:
            vector: Вектор запроса
            top_k: Количество ближайших соседей для поиска
            similarity_threshold: Порог схожести (если None, используется настройка из конфига)
        
        Returns:
            Список найденных точек с метаданными и score
        """
        if similarity_threshold is None:
            similarity_threshold = self.similarity_threshold
        
        if top_k is None:
            top_k = self.top_k

        search_filter = None   
        if use_filter != FilterDate.NO:
            if use_filter == FilterDate.NOW:
                gte_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
                lte_date = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
            elif use_filter == FilterDate.DATE:
                gte_date = (datetime.fromisoformat(filter_date) - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
                lte_date = datetime.fromisoformat(filter_date).strftime("%Y-%m-%dT%H:%M:%SZ")

            search_filter = Filter(
                must=[
                    FieldCondition(
                        key="public_dttm",
                        range=models.DatetimeRange(
                            gte=gte_date,
                            lte=lte_date
                        )
                    )
                ]
            )

        try:
            search_result = self.client.query_points(
                collection_name=self.collection_name,
                query_filter=search_filter,
                query=vector,
                using="text",
                limit=top_k,
                score_threshold=similarity_threshold,
                with_payload=True, # возвращает метаданные в результате
                with_vectors=False, # не возвращает эмбеддинги в результате False
                search_params=models.SearchParams(
                    quantization=models.QuantizationSearchParams(
                        ignore=False, # игнорирование квантованных векторов
                        rescore=use_rescore, # повторная оценка на исходных векторах
                        oversampling=2.0, # сколько (x2 в данном случае) дополнительных векторов должно быть предварительно отобрано с помощью квантованного индекса, а затем переоценено с использованием исходных векторов
                    ),
                    #hnsw_ef=128 # количество ближайших соседей для поиска
                ),
            )
            
            results = []
            for hit in search_result.points:
                results.append({
                    "id": hit.id,
                    "score": hit.score,
                    "payload": hit.payload,
                    "vector": hit.vector if hasattr(hit, 'vector') else None,
                })
            
            logger.debug(f"{len(results)} дубликатов: {search_result}")
            logger.debug(f"Найдено {len(results)} из максимально возможного {top_k} дубликатов с порогом {similarity_threshold}")
            return results
            
        except Exception as e:
            logger.error(f"Ошибка при поиске дубликатов: {e}")
            return []

    async def search_duplicates_by_text(
        self,
        text: str,
        use_filter: FilterDate,
        filter_date: Optional[datetime],
        use_rescore: bool,
        use_reranker: bool,
        top_k: int = None,
        similarity_threshold: Optional[float] = None,
    ) -> List[Dict[str, Any]]:
        """
        Ищет дубликаты по тексту (двухэтапный поиск).
        
        Args:
            text: Текст для поиска
            top_k: Количество ближайших соседей для поиска
            similarity_threshold: Порог схожести
            use_reranker: Использовать ли reranker для переранжирования
            use_filter: Использовать фильтр по дате
            use_rescore: Использовать повторную оценку
        
        Returns:
            Список найденных точек с метаданными и score
        """
        embedding = await embedding_service.encode_single(text)
        vector = embedding.tolist()

        if top_k is None:
            top_k = self.top_k

        if similarity_threshold is None:
            similarity_threshold = self.similarity_threshold
        
        # Первичный поиск в Qdrant (увеличиваем top_k для reranker), результат отсортирован по score векторов
        primary_top_k = top_k * 3 if use_reranker else top_k
        primary_results = await self.search_duplicates(
            vector=vector,
            top_k=primary_top_k,
            similarity_threshold=similarity_threshold,
            use_filter=use_filter,
            filter_date=filter_date,
            use_rescore=use_rescore
        )

        logger.info(f"Поиск дубликатов bi-encoder моделью завершен: {len(primary_results[:top_k])} результатов")

        if not primary_results:
            return primary_results[:top_k]

        candidate_indices = []
        candidate_texts = []
        
        for idx, result in enumerate(primary_results):
            payload = result.get("payload", {})
            chunk_id = payload.get("chunk_id", 0)
            if chunk_id:
                candidate_indices.append(chunk_id)
        
        chunk_texts_map = await self.get_chunks_text(candidate_indices)

        for result in primary_results:
            payload = result.get("payload", {})
            chunk_id = payload.get("chunk_id", 0)
            if chunk_id in chunk_texts_map:
                result["payload"]["chunk_text"] = chunk_texts_map[chunk_id]
            else:
                result["payload"]["chunk_text"] = ""  # пустая строка, если текст не найден
            candidate_texts.append(result["payload"]["chunk_text"])

        logger.info(f"Извлечены тексты дубликатов")
        
        if not use_reranker or not chunk_texts_map:
            return primary_results[:top_k]
        
        rerank_scores = await embedding_service.rerank(
            query=text,
            candidates=candidate_texts,
            top_k=len(candidate_texts)
        )

        logger.info(f"Переранжирование с помощью reranker модели завершено")
        
        reranked_results = []
        for idx, score in rerank_scores:
            result = primary_results[idx].copy()
            result["reranker_score"] = score
            result["final_score"] = (result["score"] + score) / 2  # Среднее
            reranked_results.append(result)
        
        if similarity_threshold is not None:
            reranked_results = [
                r for r in reranked_results 
                if r["final_score"] >= similarity_threshold
            ]

        return reranked_results[:top_k]

    async def get_chunks_text(
        self,
        chunk_ids: List[int]
    ) -> Dict[int, str]:
        """
        Возвращает текст чанков-дубликатов из PostgreSQL.
        
        Args:
            chunk_ids: id чанков-дубликатов
        
        Returns:
            Словарь {chunk_id: chunk_text}
        """
        if not chunk_ids:
            logger.warning("Передан пустой список chunk_ids")
            return {}
        
        query = """
            SELECT chunk_id, chunk_text 
            FROM news.chunks
            WHERE chunk_id = ANY($1::int[])
            """
        
        try:
            async with database.get_connection() as conn:
                rows = await conn.fetch(query, chunk_ids)
                
                result = {row['chunk_id']: row['chunk_text'] for row in rows}
                
                logger.info(f"Получено {len(result)} чанков из {len(chunk_ids)} запрошенных")
                return result
            
        except Exception as e:
            logger.error(f"Ошибка при получении текстов чанков: {e}")
            return {}

    async def batch_insert_chunks(
        self,
        chunks: List[Dict[str, Any]]
    ) -> Tuple[int, int]:
        """
        Пакетная вставка чанков с вычислением эмбеддингов.
        
        Args:
            chunks: Список чанков с полями:
                - chunk_id: идентификатор чанка
                - doc_id: идентификатор документа
                - chunk_text: текст чанка
                - chunk_idx: индекс чанка в документе

        
        Returns:
            Кортеж (вставлено, пропущено_дубликатов)
        """
        if not chunks:
            return 0, 0

        logger.info(f"Начало пакетной вставки {len(chunks)} чанков")
        log_system_resources_qdrant("BATCH_INSERT_START")
        
        texts = [chunk["chunk_text"] for chunk in chunks]
        embeddings = await embedding_service.encode_batch(texts)
        log_system_resources_qdrant("AFTER_EMBEDDINGS")
        
        points = []
        inserted_count = 0
        duplicate_count = 0
        
        for i, (chunk, embedding) in enumerate(zip(chunks, embeddings)):
            # Проверяем дубликаты (точные) перед вставкой
            duplicate_results = await self.search_duplicates(
                vector=embedding.tolist(),
                top_k=1,
                similarity_threshold=self.duplicate_threshold
            )
            
            if duplicate_results:
                duplicate_count += 1
                logger.info(f"Чанк {chunk['chunk_id']} пропущен как дубликат")
                continue
            
            point_id = str(uuid4())
            point = {
                "id": point_id,
                "vector": embedding.tolist(),
                "payload": {
                    "chunk_id": chunk.get("chunk_id", 0),
                    "doc_id": chunk.get("doc_id", 0),
                    "chunk_idx": chunk.get("chunk_idx", 0),
                    "source": chunk.get("doc_src", ''),
                    "public_dttm": chunk.get("public_dttm", '')
                }
            }
            points.append(point)
            inserted_count += 1
        
        if points:
            log_system_resources_qdrant("BEFORE_INSERT_POINTS")
            await self.insert_points(points)
            log_system_resources_qdrant("AFTER_INSERT_POINTS")
            logger.info(f"Вставлено {inserted_count} уникальных чанков, пропущено {duplicate_count} дубликатов")
        
        log_system_resources_qdrant("BATCH_INSERT_FINISH")
        return inserted_count, duplicate_count
        
    async def get_collection_info(self) -> Dict[str, Any]:
        """
        Возвращает информацию о коллекции.
        
        Returns:
            Словарь с информацией о коллекции
        """
        try:
            collection_info = self.client.get_collection(
                collection_name=self.collection_name
            )
            
            count_result = self.client.count(
                collection_name=self.collection_name,
                exact=True
            )
            
            vectors_config = {}
            vectors = collection_info.config.params.vectors
            
            if isinstance(vectors, dict):
                for name, vector_params in vectors.items():
                    vectors_config[name] = {
                        "size": vector_params.size,
                        "distance": str(vector_params.distance),
                        "datatype": str(vector_params.datatype) if vector_params.datatype else None,
                        "on_disk": vector_params.on_disk,
                    }
            else:
                # На случай безымянного вектора
                vectors_config["default"] = {
                    "size": vectors.size,
                    "distance": str(vectors.distance),
                    "datatype": str(vectors.datatype) if vectors.datatype else None,
                    "on_disk": vectors.on_disk,
                }
            
            quantization_config = None
            if collection_info.config.quantization_config:
                quant = collection_info.config.quantization_config
                if hasattr(quant, 'binary'):
                    quantization_config = {
                        "type": "binary",
                        "always_ram": quant.binary.always_ram,
                        "encoding": str(quant.binary.encoding) if quant.binary.encoding else None,
                    }
                elif hasattr(quant, 'scalar'):
                    quantization_config = {
                        "type": "scalar",
                        "always_ram": quant.scalar.always_ram,
                        "type_scalar": str(quant.scalar.type) if quant.scalar.type else None,
                    }
            
            opt_config = collection_info.config.optimizer_config
            optimizers_config = {
                "deleted_threshold": opt_config.deleted_threshold,
                "vacuum_min_vector_number": opt_config.vacuum_min_vector_number,
                "default_segment_number": opt_config.default_segment_number,
                "max_segment_size": opt_config.max_segment_size,
                "memmap_threshold": opt_config.memmap_threshold,
                "indexing_threshold": opt_config.indexing_threshold,
                "flush_interval_sec": opt_config.flush_interval_sec,
                "max_optimization_threads": opt_config.max_optimization_threads,
            }
            
            hnsw = collection_info.config.hnsw_config
            hnsw_config = {
                "m": hnsw.m,
                "ef_construct": hnsw.ef_construct,
                "full_scan_threshold": hnsw.full_scan_threshold,
                "max_indexing_threads": hnsw.max_indexing_threads,
                "on_disk": hnsw.on_disk,
                "payload_m": hnsw.payload_m,
                "inline_storage": hnsw.inline_storage,
            }
            
            return {
                "name": self.collection_name,
                "status": str(collection_info.status),
                "optimizer_status": str(collection_info.optimizer_status),
                "vectors_count": count_result.count,
                "indexed_vectors_count": collection_info.indexed_vectors_count,
                "segments_count": collection_info.segments_count,
                "points_count": collection_info.points_count,
                "warnings": [str(w) for w in collection_info.warnings] if collection_info.warnings else [],
                "vectors_config": vectors_config,
                "optimizers_config": optimizers_config,
                "hnsw_config": hnsw_config,
                "quantization_config": quantization_config,
            }
            
        except Exception as e:
            logger.error(f"Ошибка при получении информации о коллекции: {e}")
            return {}
            
    async def health_check(
            self
        ) -> Dict[str, Any]:
        """
        Проверка здоровья векторных сервисов.
        
        Проверяет подключение к Qdrant и доступность моделей эмбеддингов.
        """
        health_status = {
            "qdrant": False,
            "embedding_model": False,
            "reranker_model": False,
        }
        try:
            await qdrant_service.ensure_collection()
            health_status["qdrant"] = True
        except Exception as e:
            logger.error(f"Ошибка подключения к Qdrant: {e}")
        try:
            embedding = await embedding_service.encode_single("test")
            health_status["embedding_model"] = len(embedding) > 0
        except Exception as e:
            logger.error(f"Ошибка доступа к embedding модели: {e}")
        try:
            scores = await embedding_service.rerank(
                query="test query",
                candidates=["test candidate"],
                top_k=1
            )
            health_status["reranker_model"] = len(scores) > 0
        except Exception as e:
            logger.error(f"Ошибка доступа к reranker модели: {e}")
        health_status["overall"] = all([
            health_status["qdrant"],
            health_status["embedding_model"],
            health_status["reranker_model"]
        ])
        return health_status

    async def delete_collection(self, collection_name: str) -> bool:
        """
        Удаляет коллекцию.
        
        Returns:
            True если коллекция удалена
        """
        try:
            collections = self.client.get_collections()
            collection_names = [col.name for col in collections.collections]
            
            if collection_name in collection_names:
                self.client.delete_collection(collection_name=collection_name)
                logger.info(f"Коллекция '{collection_name}' удалена")
                return True, f"Коллекция '{collection_name}' удалена"
            else:
                logger.info(f"Коллекция '{collection_name}' не существует")
                return False, f"Коллекция '{collection_name}' не существует"
        except Exception as e:
            logger.error(f"Ошибка при удалении коллекции: {e}")
            return False, f"Ошибка при удалении коллекции: {e}"

    async def close(self):
        """Закрывает соединение с Qdrant."""
        if self._client:
            self._client.close()
            self._client = None
            logger.info("Соединение с Qdrant закрыто")


qdrant_service = QdrantService()