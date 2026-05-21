from pydantic import BaseModel, Field, validator
from typing import List, Optional, Dict, Any
from datetime import datetime
from enum import Enum
import uuid


class TaskStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class TaskType(str, Enum):
    LAST_HOURS = "last_hours"
    LAST_MINUTES = "last_minutes"
    INTERVAL = "interval"
    EMBED = "embed"


class Source(str, Enum):
    LENTA = "lenta"
    RBC = "rbc"
    RIA = "ria"

class FilterDate(str, Enum):
    NOW = "now"
    DATE = "date"
    NO = "no"


# Запросы парсинга
class LastHoursRequest(BaseModel):
    hours: int = Field(..., ge=1, le=24, description="Количество часов (1-24)")
    sources: List[Source] = Field(default=[Source.LENTA, Source.RBC, Source.RIA])
    parallel: bool = Field(default=True)


class LastMinutesRequest(BaseModel):
    minutes: int = Field(..., ge=1, le=60, description="Количество минут (1-60)")
    sources: List[Source] = Field(default=[Source.LENTA, Source.RBC, Source.RIA])
    parallel: bool = Field(default=True)


class IntervalRequest(BaseModel):
    start: datetime
    end: datetime
    sources: List[Source] = Field(default=[Source.LENTA, Source.RBC, Source.RIA])
    parallel: bool = Field(default=True)

    @validator("end")
    def end_after_start(cls, v, values):
        if "start" in values and v <= values["start"]:
            raise ValueError("end must be after start")
        return v


# Ответы задач
class TaskStats(BaseModel):
    source: Source
    total_articles: int = 0
    new_articles: int = 0
    failed_articles: int = 0
    duration_seconds: float = 0.0


class ParsingTaskResponse(BaseModel):
    task_id: uuid.UUID
    task_type: TaskType
    sources: List[Source]
    parameters: Dict[str, Any]
    status: TaskStatus
    created_at: datetime
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    result: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None


class TaskListResponse(BaseModel):
    tasks: List[ParsingTaskResponse]
    total: int
    limit: int
    offset: int


# Расписание
class ScheduleRequest(BaseModel):
    cron_expression: str = Field(..., pattern=r"^(\S+\s){4}\S+$", description="Cron выражение (5 полей)")
    sources: List[Source]
    parameters: Dict[str, Any]
    enabled: bool = True


class ScheduleResponse(BaseModel):
    schedule_id: uuid.UUID
    cron_expression: str
    sources: List[Source]
    parameters: Dict[str, Any]
    enabled: bool
    last_run: Optional[datetime] = None
    next_run: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime


# Экспорт
class ExportRequest(BaseModel):
    start: datetime
    end: datetime
    sources: List[Source]
    format: str = Field("csv", pattern="^(csv|json)$")
    fields: Optional[List[str]] = None # Поля для экспорта, если не указаны, экспортируются все поля
    compression: Optional[str] = Field(None, pattern="^(gzip|zip)$")


# Мониторинг
class HealthResponse(BaseModel):
    status: str
    database: bool
    parsers: Dict[str, bool]
    timestamp: datetime


class ParserInfo(BaseModel):
    name: str
    description: str
    enabled: bool
    last_success: Optional[datetime] = None
    success_rate: Optional[float] = None
    total_articles_parsed: Optional[int] = None


# Векторные операции
class VectorSearchRequest(BaseModel):
    """Запрос на поиск дубликатов по тексту."""
    text: str
    top_k: int = Field(5, ge=1, le=100, description="Количество ближайших соседей для поиска")
    similarity_threshold: Optional[float] = Field(None, ge=0.0, le=1.0, description="Порог схожести для фильтрации")
    use_filter: Optional[FilterDate] = Field(FilterDate.NO, description="Фильтр по дате (новости за последние 24 часа) для поиска дубликатов")
    filter_date: Optional[str] = Field(None, description="Дата для фильтрации (используется с FilterDate.DATE)", examples=["2026-01-01T23:59:59"])
    use_rescore: bool = Field(True, description="Использовать ли rescore на исходных векторах с oversampling для повышения релевантности")
    use_reranker: bool = Field(True, description="Использовать ли reranker для переранжирования")

class DuplicateResult(BaseModel):
    """Результат поиска дубликата."""
    chunk_id: Optional[int]
    doc_id: Optional[int]
    doc_src: Optional[Source] = None
    doc_public_dttm: Optional[str] = None
    similarity_score: Optional[float] = None
    reranker_score: Optional[float] = None
    final_score: Optional[float] = None
    is_duplicate: Optional[bool] = Field(..., description="Является ли найденный чанк дубликатом по порогу")
    chunk_text: Optional[str] = None


class VectorSearchResponse(BaseModel):
    """Ответ на поиск дубликатов."""
    query_text: str
    results: List[DuplicateResult]
    processing_time_ms: float
    total_found: int
    duplicates_found: int


class BatchProcessRequest(BaseModel):
    """Запрос на запуск batch-обработки чанков."""
    processed_after: Optional[datetime] = Field(None, description="Обрабатывать только чанки созданные после этой даты", examples=["2026-01-01T09:00:00"])
    processed_before: Optional[datetime] = Field(None, description="Обрабатывать только чанки созданные до этой даты", examples=["2026-01-01T09:00:00"])
    show_progress: bool = Field(True, description="Показывать ли прогресс-бар в логах")
    source: Optional[List[Source]] = Field(None, description="Обрабатывать только чанки из указанного источника")


class BatchProcessResponse(BaseModel):
    """Ответ на запуск batch-обработки."""
    task_id: uuid.UUID
    status: TaskStatus
    processed_chunks: int
    inserted_chunks: int
    duplicate_chunks: int
    failed_chunks: int
    processing_time_seconds: float
    processing_speed_chunks_per_sec: float
    estimated_completion_time: Optional[datetime] = None
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None


class VectorStatsResponse(BaseModel):
    """Статистика по векторной БД."""
    collection_name: str
    vectors_count: int
    vector_dimension: int
    similarity_threshold: float
    top_k: int
    embedding_model: str
    reranker_model: str
    distance_metric: str
    qdrant_status: str
    indexed_vectors_count: int
    indexed_vectors_size: Optional[int] = 0
    segments_count: int
    warnings: Optional[str] = ""
    vectors_config: Dict[str, Any]
    optimizers_config: Dict[str, Any]
    hnsw_config: Dict[str, Any]
    quantization_config: Optional[Dict[str, Any]] = {}
    on_disk: bool
