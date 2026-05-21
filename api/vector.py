"""
API эндпоинты для векторных операций.
Включает поиск дубликатов, batch-обработку чанков и получение статистики.
"""
import logging
import uuid
from typing import Dict, Any, Optional, List

from fastapi import APIRouter, HTTPException, BackgroundTasks

from config import settings
from models.schemas import (
    VectorSearchRequest,
    VectorSearchResponse,
    BatchProcessRequest,
    BatchProcessResponse,
    VectorStatsResponse,
    TaskStatus,
)
from services.embedding_processor import embedding_processor

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix=f"{settings.API_V1_PREFIX}/vector",
    tags=["Векторные операции"],
    responses={404: {"description": "Not found"}},
)


@router.post("/search_duplicates", response_model=VectorSearchResponse)
async def search_duplicates(
    request: VectorSearchRequest
) -> VectorSearchResponse:
    """
    Поиск дубликатов для входного текста.
    
    Выполняет двухэтапный поиск:
    1. Primary search по косинусному сходству в Qdrant
    2. Reranking с помощью reranker модели (если включено)
    
    Возвращает найденные чанки с оценками схожести.
    """
    try:
        response = await embedding_processor.search_duplicates(
            text=request.text,
            top_k=request.top_k,
            similarity_threshold=request.similarity_threshold,
            use_filter=request.use_filter,
            filter_date=request.filter_date,
            use_rescore=request.use_rescore,
            use_reranker=request.use_reranker,
        )
        logger.info(
            f"Поиск дубликатов выполнен: текст='{request.text[:50]}...', "
            f"найдено={response.total_found}, дубликатов={response.duplicates_found}, "
            f"время={response.processing_time_ms:.2f}мс"
        )
        return response
    except Exception as e:
        logger.error(f"Ошибка при поиске дубликатов: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка при поиске дубликатов: {str(e)}")


@router.post("/process_batch", response_model=BatchProcessResponse)
async def process_batch(
    request: BatchProcessRequest,
    background_tasks: BackgroundTasks
) -> BatchProcessResponse:
    """
    Запуск batch-обработки чанков.
    
    Задача выполняется в фоновом режиме. Возвращается task_id для отслеживания статуса.
    """
    # Создаем задачу и добавляем фоновую задачу через embedding_processor
    response = await embedding_processor.execute_batch_task(
        processed_after=request.processed_after,
        processed_before=request.processed_before,
        source=request.source,
        show_progress=request.show_progress,
        background_tasks=background_tasks
    )
    
    return response


@router.get("/tasks/{task_id}", response_model=BatchProcessResponse)
async def get_task_status(task_id: str) -> BatchProcessResponse:
    """
    Получение статуса batch-задачи.
    
    Возвращает текущий прогресс обработки и статистику.
    """
    try:
        task_uuid = uuid.UUID(task_id)
        response = await embedding_processor.get_batch_status(task_uuid)
        return response
    except ValueError as e:
        raise HTTPException(status_code=404, detail=f"Задача {task_id} не найдена")
    except Exception as e:
        logger.error(f"Ошибка при получении статуса задачи {task_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка при получении статуса задачи: {str(e)}")


@router.get("/tasks", response_model=List[BatchProcessResponse])
async def list_tasks(
    limit: int = 20,
    offset: int = 0,
    status: Optional[TaskStatus] = None
) -> List[BatchProcessResponse]:
    """
    Возвращает список векторных задач с пагинацией.
    
    Args:
        limit: Количество задач на странице (максимум 20)
        offset: Смещение
        status: Фильтр по статусу (опционально)
    """
    try:
        responses = await embedding_processor.list_vector_tasks(
            limit=limit,
            offset=offset,
            status=status
        )
        return responses
    except Exception as e:
        logger.error(f"Ошибка при получении списка задач: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка при получении списка задач: {str(e)}")


@router.get("/info", response_model=VectorStatsResponse)
async def get_info() -> VectorStatsResponse:
    """
    Получение статистики по векторной БД.
    
    Возвращает информацию о коллекции Qdrant, количестве векторов,
    используемых моделях и последней обработке.
    """
    try:
        response = await embedding_processor.get_vector_stats()
        return response
    except Exception as e:
        logger.error(f"Ошибка при получении статистики: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка при получении статистики: {str(e)}")


@router.get("/health")
async def health_check() -> Dict[str, Any]:
    """
    Проверка здоровья векторных сервисов.
    
    Проверяет подключение к Qdrant и доступность моделей эмбеддингов.
    """
    try:
        health_status = await embedding_processor.health_check()
        return health_status
    except Exception as e:
        logger.error(f"Ошибка при проверке здоровья: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка при проверке здоровья: {str(e)}")

@router.post("/delete_collection", response_model=str)
async def delete_collection(
    collection_name: str
) -> str:
    """
    Удаление коллекции.

    """
    response, message = await embedding_processor.delete_collection(collection_name)
    
    return message