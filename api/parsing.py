from typing import Optional
from uuid import UUID
from fastapi import APIRouter, BackgroundTasks, HTTPException, Query

from models.schemas import (
    LastHoursRequest,
    LastMinutesRequest,
    IntervalRequest,
    ParsingTaskResponse,
    TaskListResponse,
    TaskStatus,
)
from services.parsing_service import parsing_service

router = APIRouter(prefix="/parse", tags=["Парсинг"])


@router.post("/last_hours", status_code=202, response_model=ParsingTaskResponse)
async def parse_last_hours(
    request: LastHoursRequest,
    background_tasks: BackgroundTasks,
):
    """
    Запуск парсинга последних N часов.
    
    - **hours**: количество часов (1-12)
    - **sources**: список источников (lenta, rbc, ria)
    - **parallel**: параллельное выполнение (по умолчанию True)
    """
    task_id = await parsing_service.create_api_last_hours_task(request, background_tasks)
    task = await parsing_service.get_task(task_id)
    return task


@router.post("/last_minutes", status_code=202, response_model=ParsingTaskResponse)
async def parse_last_minutes(
    request: LastMinutesRequest,
    background_tasks: BackgroundTasks,
):
    """
    Запуск парсинга последних N минут.
    
    - **minutes**: количество минут (1-60)
    - **sources**: список источников
    - **parallel**: параллельное выполнение
    """
    task_id = await parsing_service.create_api_last_minutes_task(request, background_tasks)
    task = await parsing_service.get_task(task_id)
    return task


@router.post("/interval", status_code=202, response_model=ParsingTaskResponse)
async def parse_interval(
    request: IntervalRequest,
    background_tasks: BackgroundTasks,
):
    """
    Запуск парсинга в указанном интервале.
    
    - **start**: начало интервала (ISO 8601)
    - **end**: конец интервала (ISO 8601)
    - **sources**: список источников
    - **parallel**: параллельное выполнение
    """
    task_id = await parsing_service.create_api_interval_task(request, background_tasks)
    task = await parsing_service.get_task(task_id)
    return task


@router.get("/tasks/{task_id}", response_model=ParsingTaskResponse)
async def get_task_status(task_id: UUID):
    """
    Получение статуса задачи парсинга.
    """
    try:
        task = await parsing_service.get_task(task_id)
        return task
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/tasks", response_model=TaskListResponse)
async def list_tasks(
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    status: Optional[TaskStatus] = Query(None),
):
    """
    Список задач парсинга с пагинацией.
    
    - **limit**: количество задач на странице (максимум 100)
    - **offset**: смещение
    - **status**: фильтр по статусу (опционально)
    """
    result = await parsing_service.list_tasks(limit, offset, status)
    return result