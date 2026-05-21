"""
Сервис для работы с эмбеддингами текстов.
Поддерживает primary модель для векторного поиска и reranker модель для переранжирования.
Использует FastEmbed для эффективного вычисления на CPU с поддержкой batch processing.
"""
import asyncio
import logging
from typing import List, Optional, Tuple, Union, Any
from concurrent.futures import ProcessPoolExecutor

import numpy as np
from fastembed import TextEmbedding
from fastembed.rerank.cross_encoder import TextCrossEncoder
from config import settings

logger = logging.getLogger(__name__)


class EmbeddingService:
    """Сервис для вычисления эмбеддингов текстов."""

    def __init__(self):
        self.primary_model_name = settings.EMBEDDING_MODEL_NAME
        self.reranker_model_name = settings.RERANKER_MODEL_NAME
        self.batch_size = settings.EMBEDDING_BATCH_SIZE
        self.max_workers = settings.EMBEDDING_MAX_WORKERS
        self.top_k = settings.TOP_K
        
        # Инициализация моделей (ленивая загрузка)
        self._primary_model: Optional[TextEmbedding] = None
        self._reranker_model: Optional[TextCrossEncoder] = None
        self._executor: Optional[ProcessPoolExecutor] = None
        
        logger.info(
            f"EmbeddingService инициализирован с моделями: "
            f"primary={self.primary_model_name}, "
            f"reranker={self.reranker_model_name}"
        )

    @property
    def primary_model(self) -> TextEmbedding:
        """Ленивая загрузка primary модели."""
        if self._primary_model is None:
            logger.info(f"Загрузка primary модели: {self.primary_model_name}")
            self._primary_model = TextEmbedding(
                model_name=self.primary_model_name,
                cache_dir="./models_cache"
            )
        return self._primary_model

    @property
    def reranker_model(self) -> TextCrossEncoder:
        """Ленивая загрузка reranker модели."""
        if self._reranker_model is None:
            logger.info(f"Загрузка reranker модели: {self.reranker_model_name}")
            self._reranker_model = TextCrossEncoder(
                model_name=self.reranker_model_name,
                cache_dir="./models_cache"
            )
        return self._reranker_model

    @property
    def executor(self) -> ProcessPoolExecutor:
        """Ленивое создание ProcessPoolExecutor."""
        if self._executor is None:
            self._executor = ProcessPoolExecutor(max_workers=self.max_workers)
        return self._executor

    async def encode_batch(
        self,
        texts: List[str],
        batch_size: Optional[int] = None
    ) -> List[np.ndarray]:
        """
        Вычисляет эмбеддинги для батча текстов.
        
        Args:
            texts: Список текстов для обработки
            batch_size: Размер батча (если None, используется настройка из конфига)
        
        Returns:
            Список numpy массивов с эмбеддингами
        """
        if not texts:
            return []

        batch_size = batch_size or self.batch_size
        model = self.primary_model
        
        logger.debug(f"Вычисление эмбеддингов для {len(texts)} текстов")
        
        # FastEmbed поддерживает batch inference напрямую
        # Для асинхронности используем run_in_executor в отдельном потоке (не процессе)
        # чтобы избежать проблем с сериализацией модели
        loop = asyncio.get_event_loop()
        
        def compute_embeddings_sync():
            # FastEmbed.embed возвращает генератор, преобразуем в список
            embeddings = list(model.embed(texts, batch_size=batch_size))
            return embeddings
        
        try:
            # Используем None для пула потоков по умолчанию, а не ProcessPoolExecutor
            embeddings = await loop.run_in_executor(
                None, compute_embeddings_sync
            )
            logger.debug(f"Вычислено {len(embeddings)} эмбеддингов")
            return embeddings
        except Exception as e:
            logger.error(f"Ошибка при вычислении эмбеддингов: {e}")
            raise

    async def encode_single(self, text: str) -> np.ndarray: #Any:
        """
        Вычисляет эмбеддинг для одного текста.
        
        Args:
            text: Текст для обработки
        
        Returns:
            Numpy массив с эмбеддингом
        """
        embeddings = await self.encode_batch([text])
        return embeddings[0] if embeddings else np.array([])


    async def rerank(
        self,
        query: str,
        candidates: List[str],
        top_k: int = None
    ) -> List[Tuple[int, float]]:
        """
        Переранжирование кандидатов с помощью reranker модели.
        
        Args:
            query: Запрос
            candidates: Список текстов-кандидатов
            top_k: Количество лучших результатов для возврата
        
        Returns:
            Список кортежей (индекс кандидата, score)
        """
        logger.info(f"Начало переранжирования {len(candidates)} кандидатов")

        if not candidates:
            return []

        loop = asyncio.get_event_loop()
        
        try:
            scores = await loop.run_in_executor(
                None, 
                lambda: list(self.reranker_model.rerank(query, candidates))
            )
        except Exception as e:
            logger.error(f"Ошибка при переранжировании: {e}")
            raise

        indexed_scores = list(enumerate(scores))

        if top_k is None:
            top_k = self.top_k
            
        return indexed_scores[:top_k]

    async def close(self):
        """Освобождение ресурсов."""
        if self._executor:
            self._executor.shutdown(wait=True)
            self._executor = None
            logger.info("ProcessPoolExecutor остановлен")

    def __del__(self):
        """Деструктор для очистки ресурсов."""
        if self._executor:
            self._executor.shutdown(wait=False)


embedding_service = EmbeddingService()