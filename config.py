import os
from typing import Optional
from pydantic_settings import BaseSettings
from models.schemas import Source
from parsers.lenta import LentaNewsParser
from parsers.rbc import RbcNewsParser
from parsers.ria import RiaNewsParser

class Settings(BaseSettings):
    """Настройки приложения из переменных окружения."""

    POSTGRES_USER: str = "user"
    POSTGRES_PASSWORD: str = "password"
    POSTGRES_HOST: str = "localhost"
    POSTGRES_PORT: str = "5432"
    POSTGRES_DB: str = "postgres"
    
    # База данных
    @property
    def DATABASE_URL(self) -> str:
        return f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        
    # Парсинг
    MAX_PARALLEL_PARSERS: int = 3
    PARSING_TIMEOUT_SECONDS: int = 600
    PARSING_TIMEOUT_SECONDS_BATCH: int = 1800
    RETRY_ATTEMPTS: int = 3
    PARSING_BATCH_SIZE: int = 500  # размер батча для сохранения новостей в БД

    PARSERS: dict = {
                        Source.LENTA: LentaNewsParser(),
                        Source.RBC: RbcNewsParser(),
                        Source.RIA: RiaNewsParser(),
                    }
    
    # Обработка
    CHUNK_SIZE: int = 500  # токенов на чанк
    CHUNK_OVERLAP: int = 50  # перекрытие между чанками в токенах
    ENCODING_NAME: str ="o200k_base"

    MAX_CONCURRENT_SAVES: int = 10
    
    # Логирование
    LOG_LEVEL: str = "INFO"
    
    # FastAPI
    API_V1_PREFIX: str = "/api/v1"
    PROJECT_NAME: str = "News Aggregator API"
    DEBUG: bool = False

    # Векторные операции и Qdrant
    QDRANT_HOST: str = "localhost"
    QDRANT_PORT: int = 6333
    QDRANT_GRPC_PORT: int = 6334
    QDRANT_COLLECTION_NAME: str = "news_test" #news
    VECTOR_DIMENSION: int = 1024  # 384 для paraphrase-multilingual-MiniLM-L12-v2 - можно для title - токенов 128 
    SIMILARITY_THRESHOLD: float = 0.7 #0.93  # порог для дубликатов (переписанные новости)
    DUPLICATE_THRESHOLD: float = 0.98  # порог для дубликатов (точные дубликаты)
    BATCH_SIZE: int = 100  # размер батча для обработки
    TOP_K: int = 10  # количество результатов для релевантности
    DISTANCE: str = "cosine"

    # Модели эмбеддингов
    EMBEDDING_MODEL_NAME: str = "jinaai/jina-embeddings-v3" #"sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
    RERANKER_MODEL_NAME: str = "jinaai/jina-reranker-v2-base-multilingual"
    EMBEDDING_CACHE_SIZE: int = 1000  # размер кэша эмбеддингов

    # Параллельная обработка
    EMBEDDING_MAX_WORKERS: int = 4  # количество воркеров для многопроцессной обработки
    EMBEDDING_BATCH_SIZE: int = 32  # размер батча для инференса модели
    
    class Config:
        env_file = ".env"
        case_sensitive = False
        extra = "ignore"


settings = Settings()