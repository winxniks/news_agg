# обертка для запуска фоновой задачи и отслеживания парсинга (соединяет класс с задачами и парсингом)
import asyncio
import uuid
from datetime import datetime
from typing import Dict, Any, Optional, List

from fastapi import BackgroundTasks

from config import settings
from models.schemas import TaskStatus, TaskType, Source, LastHoursRequest, LastMinutesRequest, IntervalRequest
from services.task_manager import task_storage
from services.parser_service import parser_manager


class ParsingService:
    """Сервис для управления задачами парсинга."""

    @staticmethod
    async def create_task(
        task_type: TaskType,
        sources: List[Source],
        parameters: dict,
    ) -> uuid.UUID:
        """
        Создает задачу в хранилище без её запуска.
        Используется планировщиком и другими внутренними компонентами.
        """
        task_id = await task_storage.create_task(
            task_type=task_type,
            sources=sources,
            parameters=parameters,
        )
        return task_id
    
    @staticmethod
    async def execute_last_hours_task(task_id: uuid.UUID, request: LastHoursRequest):
        """Выполняет задачу парсинга последних N часов."""
        await task_storage.update_task(
            task_id, 
            status=TaskStatus.RUNNING, 
            started_at=datetime.now()
        )
        
        try:
            stats = await parser_manager.parse_last_hours(
                sources=request.sources,
                hours=request.hours,
                parallel=request.parallel,
            )
            
            result = {
                "stats": [stat.model_dump() for stat in stats],
                "total_new_articles": sum(stat.new_articles for stat in stats),
                "total_processed": sum(stat.total_articles for stat in stats),
                "errors": [],
            }
            
            await task_storage.update_task(
                task_id,
                status=TaskStatus.COMPLETED,
                finished_at=datetime.now(),
                result=result,
            )
        except Exception as e:
            await task_storage.update_task(
                task_id,
                status=TaskStatus.FAILED,
                finished_at=datetime.now(),
                error_message=str(e),
            )
    
    @staticmethod
    async def execute_last_minutes_task(task_id: uuid.UUID, request: LastMinutesRequest):
        """Выполняет задачу парсинга последних N минут."""
        await task_storage.update_task(
            task_id, 
            status=TaskStatus.RUNNING, 
            started_at=datetime.now()
        )
        
        try:
            stats = await parser_manager.parse_last_minutes(
                sources=request.sources,
                minutes=request.minutes,
                parallel=request.parallel,
            )
            
            result = {
                "stats": [stat.model_dump() for stat in stats],
                "total_new_articles": sum(stat.new_articles for stat in stats),
                "total_processed": sum(stat.total_articles for stat in stats),
                "errors": [],
            }
            
            await task_storage.update_task(
                task_id,
                status=TaskStatus.COMPLETED,
                finished_at=datetime.now(),
                result=result,
            )
        except Exception as e:
            await task_storage.update_task(
                task_id,
                status=TaskStatus.FAILED,
                finished_at=datetime.now(),
                error_message=str(e),
            )
    
    @staticmethod
    async def execute_interval_task(task_id: uuid.UUID, request: IntervalRequest):
        """Выполняет задачу парсинга интервала."""
        await task_storage.update_task(
            task_id, 
            status=TaskStatus.RUNNING, 
            started_at=datetime.now()
        )
        
        try:
            stats = await parser_manager.parse_interval(
                sources=request.sources,
                start_time=request.start,
                end_time=request.end,
                parallel=request.parallel,
            )
            
            result = {
                "stats": [stat.model_dump() for stat in stats],
                "total_new_articles": sum(stat.new_articles for stat in stats),
                "total_processed": sum(stat.total_articles for stat in stats),
                "errors": [],
            }
            
            await task_storage.update_task(
                task_id,
                status=TaskStatus.COMPLETED,
                finished_at=datetime.now(),
                result=result,
            )
        except Exception as e:
            await task_storage.update_task(
                task_id,
                status=TaskStatus.FAILED,
                finished_at=datetime.now(),
                error_message=str(e),
            )

    @staticmethod
    async def create_api_last_hours_task(
        request: LastHoursRequest,
        background_tasks: BackgroundTasks,
    ) -> uuid.UUID:
        """API метод: создает задачу парсинга последних N часов."""
        task_id = await ParsingService.create_task(
            task_type=TaskType.LAST_HOURS,
            sources=request.sources,
            parameters={
                "hours": request.hours, 
                "parallel": request.parallel
            },
        )
        
        background_tasks.add_task(
            ParsingService.execute_last_hours_task,
            task_id=task_id,
            request=request,
        )
        return task_id
    
    @staticmethod
    async def create_api_last_minutes_task(
        request: LastMinutesRequest,
        background_tasks: BackgroundTasks,
    ) -> uuid.UUID:
        """Создает задачу парсинга последних N минут."""
        task_id = await ParsingService.create_task(
            task_type=TaskType.LAST_MINUTES,
            sources=request.sources,
            parameters={
                "minutes": request.minutes, 
                "parallel": request.parallel
                },
        )
        
        background_tasks.add_task(
            ParsingService.execute_last_minutes_task,
            task_id=task_id,
            request=request,
        )
        return task_id
    
    @staticmethod
    async def create_api_interval_task(
        request: IntervalRequest,
        background_tasks: BackgroundTasks,
    ) -> uuid.UUID:
        """Создает задачу парсинга интервала."""
        task_id = await ParsingService.create_task(
            task_type=TaskType.INTERVAL,
            sources=request.sources,
            parameters={
                "start": request.start.isoformat(),
                "end": request.end.isoformat(),
                "parallel": request.parallel,
            },
        )
        
        background_tasks.add_task(
            ParsingService.execute_interval_task,
            task_id=task_id,
            request=request,
        )
        return task_id 
    
    @staticmethod
    async def get_task(task_id: uuid.UUID) -> Dict[str, Any]:
        """Возвращает информацию о задаче."""
        task = await task_storage.get_task(task_id)
        if not task:
            raise ValueError(f"Task {task_id} not found")
        return task
    
    @staticmethod
    async def list_tasks(limit: int = 20, offset: int = 0, status: Optional[TaskStatus] = None):
        """Возвращает список задач."""
        tasks = await task_storage.list_tasks(limit, offset, status)
        total = await task_storage.count_tasks(status)
        return {
            "tasks": tasks,
            "total": total,
            "limit": limit,
            "offset": offset,
        }


# Глобальный экземпляр сервиса
parsing_service = ParsingService()