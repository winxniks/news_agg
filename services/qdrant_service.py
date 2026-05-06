"""
Сервис для работы с векторной базой данных Qdrant.
Обеспечивает создание коллекций, вставку векторов и поиск дубликатов.
"""
import asyncio
import logging
from typing import List, Optional, Dict, Any, Tuple
from uuid import uuid4

from qdrant_client import QdrantClient, models
from qdrant_client.http.exceptions import UnexpectedResponse
from qdrant_client.http.models import Distance, VectorParams, PointStruct, Filter, FieldCondition, MatchValue

from config import settings
from services.embedding_service import embedding_service

logger = logging.getLogger(__name__)


class QdrantService:
    """Сервис для взаимодействия с Qdrant."""

    def __init__(self):
        self.host = settings.QDRANT_HOST
        self.port = settings.QDRANT_PORT
        #self.grpc_port = settings.QDRANT_GRPC_PORT
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
            f"host={self.host}:{self.port}, "
            f"collection={self.collection_name}, "
            f"dimension={self.vector_dimension}"
        )

    @property
    def client(self) -> QdrantClient:
        """Ленивая загрузка клиента Qdrant."""
        if self._client is None:
            logger.info(f"Подключение к Qdrant по адресу {self.host}:{self.port}")
            self._client = QdrantClient(
                host=self.host,
                port=self.port,
                #grpc_port=self.grpc_port,
                #prefer_grpc=True,
                timeout=30.0
            )
            
            # Проверяем соединение
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
            
            # Создаем новую коллекцию
            self.client.create_collection(
                collection_name=self.collection_name,
                vectors_config=models.VectorParams(
                    size=self.vector_dimension,  # CLIP embeddings are 512-dimensional
                    distance=self.distance,  # Use cosine similarity
                    #datatype=models.Datatype.FLOAT16,  # Use float16 for memory efficiency
                    #on_disk=True  # Store vectors on disk to handle large dataset
                ),
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
                    indexing_threshold=10000,
                ),
                hnsw_config=models.HnswConfigDiff(
                    m=0,  # 0 - индекс не строится для первой большой загрузки, Balanced connections (16 - default), 6 - decrease M for lower memory usage
                    ef_construct=200,  # Good build quality (default)
                    full_scan_threshold=10000,  # Use brute force below this size (default)
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
            # Преобразуем точки в формат PointStruct
            point_structs = []
            for point in points:
                point_struct = PointStruct(
                    id=point["id"],
                    vector=point["vector"],
                    payload=point.get("payload", {})
                )
                point_structs.append(point_struct)

            # Выполняем вставку
            self.client.upload_points(
                collection_name=self.collection_name,
                points=point_structs,
                #wait=True,
                parallel=4,
                max_retries=1 #3
            )
            
            logger.debug(f"Вставлено {len(points)} точек")
            return [point["id"] for point in points]
            
        except Exception as e:
            logger.error(f"Ошибка при вставке точек: {e}")
            raise

    async def search_duplicates(
        self,
        vector: List[float],
        top_k: int = None,
        similarity_threshold: Optional[float] = None
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

        '''search_filter = Filter(
            must=[
                FilterCondition(
                    #field="vector",
                    #value=vector,
                    #params=FilterParams(
                        #distance=self.distance
                    #)
                    key="public_dttm",
                    #match=models.MatchValue(value=published_at)
                )
            ]
        )'''

        try:
            search_result = self.client.query_points(
                collection_name=self.collection_name,
                #query_vector=vector,
                #query_filter=search_filter,
                query=vector,
                limit=top_k,
                score_threshold=similarity_threshold,
                with_payload=True,
                with_vectors=False
            )
            
            results = []
            for hit in search_result.points:
                results.append({
                    "id": hit.id,
                    "score": hit.score,
                    "payload": hit.payload,
                    "vector": hit.vector if hasattr(hit, 'vector') else None
                })
            
            logger.debug(f"Найдено {len(results)} дубликатов с порогом {similarity_threshold}")
            return results
            
        except Exception as e:
            logger.error(f"Ошибка при поиске дубликатов: {e}")
            return []

    async def search_duplicates_by_text(
        self,
        text: str,
        top_k: int = None,
        similarity_threshold: Optional[float] = None,
        use_reranker: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Ищет дубликаты по тексту (двухэтапный поиск).
        
        Args:
            text: Текст для поиска
            top_k: Количество ближайших соседей для поиска
            similarity_threshold: Порог схожести
            use_reranker: Использовать ли reranker для переранжирования
        
        Returns:
            Список найденных точек с метаданными и score
        """
        # 1. Вычисляем эмбеддинг текста
        embedding = await embedding_service.encode_single(text)
        vector = embedding.tolist()

        if top_k is None:
            top_k = self.top_k
        
        # 2. Первичный поиск в Qdrant (увеличиваем top_k для reranker)
        primary_top_k = top_k * 3 if use_reranker else top_k
        primary_results = await self.search_duplicates(
            vector=vector,
            top_k=primary_top_k,
            similarity_threshold=similarity_threshold
        )
        
        if not primary_results or not use_reranker:
            return primary_results[:top_k]
        
        # 3. Извлекаем тексты кандидатов для reranker
        candidate_texts = []
        candidate_indices = []
        
        for idx, result in enumerate(primary_results):
            payload = result.get("payload", {})
            chunk_text = payload.get("chunk_text", "")
            if chunk_text:
                candidate_texts.append(chunk_text)
                candidate_indices.append(idx)
        
        if not candidate_texts:
            return primary_results[:top_k]
        
        # 4. Переранжирование с помощью reranker модели
        rerank_scores = await embedding_service.rerank(
            query=text,
            candidates=candidate_texts,
            top_k=len(candidate_texts)
        )
        
        # 5. Собираем финальные результаты с учетом reranker scores
        reranked_results = []
        for idx, score in rerank_scores:
            original_idx = candidate_indices[idx]
            result = primary_results[original_idx].copy()
            # Обновляем score с учетом reranker
            result["reranker_score"] = score
            result["final_score"] = (result["score"] + score) / 2  # Среднее
            reranked_results.append(result)
        
        # 6. Фильтрация по порогу (если указан)
        if similarity_threshold is not None:
            reranked_results = [
                r for r in reranked_results 
                if r["final_score"] >= similarity_threshold
            ]
        
        return reranked_results[:top_k]

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
        
        # 1. Вычисляем эмбеддинги для всех чанков
        texts = [chunk["chunk_text"] for chunk in chunks]
        embeddings = await embedding_service.encode_batch(texts)
        
        # 2. Подготавливаем точки для вставки
        points = []
        inserted_count = 0
        duplicate_count = 0
        
        for i, (chunk, embedding) in enumerate(zip(chunks, embeddings)):
            # Проверяем дубликаты (точные) перед вставкой
            duplicate_results = await self.search_duplicates(
                vector=embedding.tolist(),
                top_k=1,
                similarity_threshold=self.duplicate_threshold #similarity_threshold
            )

            logger.info(f"Найдены дубликаты: {duplicate_results}")
            
            if duplicate_results:
                # Найден дубликат - пропускаем вставку
                duplicate_count += 1
                logger.debug(f"Чанк {chunk['chunk_id']} пропущен как дубликат")
                continue
            
            # Создаем точку для вставки
            point_id = str(uuid4())
            point = {
                "id": point_id,
                "vector": embedding.tolist(),
                "payload": {
                    "chunk_id": chunk.get("chunk_id", 0),
                    "doc_id": chunk.get("doc_id", 0),
                    "chunk_idx": chunk.get("chunk_idx", 0),
                    #"chunk_text": chunk["chunk_text"][:500],  # Ограничиваем длину для payload
                    "source": chunk.get("doc_src", '') # chunk["source"]
                }
            }
            points.append(point)
            inserted_count += 1
        
        # 3. Вставляем уникальные точки
        if points:
            await self.insert_points(points)
            logger.info(f"Вставлено {inserted_count} уникальных чанков, пропущено {duplicate_count} дубликатов")
        
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
            
            # Получаем количество точек
            count_result = self.client.count(
                collection_name=self.collection_name,
                exact=True
            )
            
            return {
               "name": self.collection_name,
                "status": collection_info.status,
                "vectors_count": count_result.count,
                "indexed_vectors_count": collection_info.indexed_vectors_count,
                "segments_count": collection_info.segments_count,
                "warnings": collection_info.warnings,
                "vectors_config": {
                    "size": collection_info.config.params.vectors[''].size,
                    "distance": str(collection_info.config.params.vectors[''].distance)
                },
                "optimizers_config": {
                    "default_segment_number": collection_info.config.optimizer_config.default_segment_number,
                    "indexing_threshold": collection_info.config.optimizer_config.indexing_threshold,
                    "memmap_threshold": collection_info.config.optimizer_config.memmap_threshold
                },
                "hnsw_config": {
                    "m": collection_info.config.hnsw_config.m,
                    "ef_construct": collection_info.config.hnsw_config.ef_construct,
                    "on_disk": collection_info.config.hnsw_config.on_disk,
                    "payload_m": collection_info.config.hnsw_config.payload_m,
                    "full_scan_threshold": collection_info.config.hnsw_config.full_scan_threshold,
                    "max_indexing_threads": collection_info.config.hnsw_config.max_indexing_threads,
                    "inline_storage": collection_info.config.hnsw_config.inline_storage
                },
                "quantization_config": collection_info.config.params.vectors[''].quantization_config,
                "on_disk": collection_info.config.params.vectors[''].on_disk,
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
            # Проверяем доступность primary модели
            embedding = await embedding_service.encode_single("test")
            health_status["embedding_model"] = len(embedding) > 0
        except Exception as e:
            logger.error(f"Ошибка доступа к embedding модели: {e}")
        try:
            # Проверяем доступность reranker модели через вызов rerank с тестовыми данными
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

    async def delete_collection(self) -> bool:
        """
        Удаляет коллекцию.
        
        Returns:
            True если коллекция удалена
        """
        try:
            self.client.delete_collection(collection_name=self.collection_name)
            logger.info(f"Коллекция '{self.collection_name}' удалена")
            return True
        except Exception as e:
            logger.error(f"Ошибка при удалении коллекции: {e}")
            return False

    async def close(self):
        """Закрывает соединение с Qdrant."""
        if self._client:
            self._client.close()
            self._client = None
            logger.info("Соединение с Qdrant закрыто")


# Глобальный экземпляр сервиса для использования в приложении
qdrant_service = QdrantService()