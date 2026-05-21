import asyncio
import uuid
import logging
from typing import Dict, Any, Optional
from datetime import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.jobstores.memory import MemoryJobStore
from models.schemas import LastHoursRequest, LastMinutesRequest, IntervalRequest

from config import settings
from models.schemas import Source, ScheduleRequest
from services.parsing_service import parsing_service
from services.schedule_storage import schedule_storage

logger = logging.getLogger(__name__)


class Scheduler:
    """Планировщик задач парсинга на основе APScheduler."""

    def __init__(self):
        self.scheduler = AsyncIOScheduler(
            jobstores={'default': MemoryJobStore()},
            timezone='Europe/Moscow'
        )

    async def start(self):
        """Запускает планировщик и восстанавливает расписания из БД."""
        if not self.scheduler.running:
            self.scheduler.start()
            logger.info("Планировщик запущен")
            await self._restore_schedules()

    async def stop(self):
        """Останавливает планировщик."""
        if self.scheduler.running:
            self.scheduler.shutdown()
            logger.info("Планировщик остановлен")

    async def _restore_schedules(self):
        """Восстанавливает расписания из базы данных."""
        schedules = await schedule_storage.list_schedules(enabled=True)
        for schedule in schedules:
            schedule_id = schedule["schedule_id"]
            schedule_request = ScheduleRequest(
                cron_expression=schedule["cron_expression"],
                sources=schedule["sources"],
                parameters=schedule["parameters"],
                enabled=schedule["enabled"]
            )
            # Создаем задачу в планировщике без повторного сохранения в БД
            job = self.scheduler.add_job(
                func=self._run_scheduled_task,
                trigger=CronTrigger.from_crontab(schedule_request.cron_expression),
                args=[schedule_id, schedule],
                id=str(schedule_id),
                name=f"Парсинг {schedule_request.sources}",
                replace_existing=True,
            )
            if not schedule["enabled"]:
                job.pause()
            logger.debug(f"Восстановлено расписание {schedule_id}")

    async def add_schedule(self, schedule_request: ScheduleRequest) -> uuid.UUID:
        """Добавляет новое расписание."""
        schedule_id = await schedule_storage.create_schedule(schedule_request)

        schedule_dict = {
            "sources": schedule_request.sources,
            "parameters": schedule_request.parameters,
            "enabled": schedule_request.enabled
        }
            
        # Создаем задачу в планировщике
        job = self.scheduler.add_job(
            func=self._run_scheduled_task,
            trigger=CronTrigger.from_crontab(schedule_request.cron_expression),
            args=[schedule_id, schedule_dict],
            id=str(schedule_id),
            name=f"Парсинг {schedule_request.sources}",
            replace_existing=True,
        )
        
        # Обновляем next_run в БД
        await schedule_storage.update_schedule(
            schedule_id,
            next_run=job.next_run_time
        )
        
        # Если расписание отключено, приостанавливаем задачу
        if not schedule_request.enabled:
            job.pause()
        
        logger.info(f"Добавлено новое расписание {schedule_id}")
        return schedule_id

    async def remove_schedule(self, schedule_id: uuid.UUID) -> bool:
        """Удаляет расписание."""
        # Удаляем из планировщика
        job_id = str(schedule_id)
        if self.scheduler.get_job(job_id):
            self.scheduler.remove_job(job_id)
        
        # Удаляем из БД
        success = await schedule_storage.delete_schedule(schedule_id)
        if success:
            logger.info(f"Удалено расписание {schedule_id}")
        return success

    async def toggle_schedule(self, schedule_id: uuid.UUID, enabled: bool) -> bool:
        """Включает/выключает расписание."""
        job_id = str(schedule_id)
        job = self.scheduler.get_job(job_id)
        if not job:
            return False
        
        if enabled:
            job.resume()
        else:
            job.pause()
        
        # Обновляем в БД
        success = await schedule_storage.update_schedule(
            schedule_id,
            enabled=enabled
        )
        if success:
            logger.info(f"Расписание {schedule_id} {'включено' if enabled else 'выключено'}")
        return success

    async def run_schedule_now(self, schedule_id: uuid.UUID) -> bool:
        """Немедленный запуск расписания."""
        schedule = await schedule_storage.get_schedule(schedule_id)
        if not schedule:
            return False
        
        # Запускаем задачу в фоне
        asyncio.create_task(self._run_scheduled_task(schedule_id, schedule))
        return True

    async def _run_scheduled_task(self, schedule_id: uuid.UUID, schedule_data: Dict[str, Any]):
        """Выполняет задачу по расписанию через parsing_service."""
        logger.info(f"Запуск запланированной задачи {schedule_id}")
        
        # Обновляем время последнего запуска в БД
        await schedule_storage.update_schedule(
            schedule_id,
            last_run=datetime.now()
        )
        
        # Извлекаем параметры
        sources = schedule_data.get("sources", [])
        parameters = schedule_data.get("parameters", {})
        parallel = parameters.get("parallel", True)
        
        task_id = await parsing_service.create_task(
            task_type=self._get_task_type(parameters),
            sources=sources,
            parameters=parameters
        )
        
        try:
            # Запускаем задачу через parsing_service
            if "hours" in parameters:
                request = LastHoursRequest(
                    sources=sources,
                    hours=parameters["hours"],
                    parallel=parallel
                )
                await parsing_service.execute_last_hours_task(task_id, request)
                
            elif "minutes" in parameters:
                request = LastMinutesRequest(
                    sources=sources,
                    minutes=parameters["minutes"],
                    parallel=parallel
                )
                await parsing_service.execute_last_minutes_task(task_id, request)
                
            elif "start" in parameters and "end" in parameters:
                start = datetime.fromisoformat(parameters["start"])
                end = datetime.fromisoformat(parameters["end"])
                request = IntervalRequest(
                    sources=sources,
                    start=start,
                    end=end,
                    parallel=parallel
                )
                await parsing_service.execute_interval_task(task_id, request)
            else:
                logger.warning(f"Неизвестные параметры расписания {schedule_id}: {parameters}")
                return
            
            # Получаем результат задачи для логирования
            task = await parsing_service.get_task(task_id)
            logger.info(f"Статус задачи: {task.get('status').value}")
            if task.get("status") == "completed" and task.get("result"):
                result = task["result"]
                total_new = result.get("total_new_articles", 0)
                total_processed = result.get("total_processed", 0)
                logger.info(
                    f"Задача {schedule_id} (task_id: {task_id}) завершена. "
                    f"Обработано статей: {total_processed}, новых: {total_new}"
                )
                
        except Exception as e:
            logger.error(f"Ошибка выполнения запланированной задачи {schedule_id}: {e}", exc_info=True)

    def _get_task_type(self, parameters: dict):
        """Определяет тип задачи на основе параметров."""
        from models.schemas import TaskType
        if "hours" in parameters:
            return TaskType.LAST_HOURS
        elif "minutes" in parameters:
            return TaskType.LAST_MINUTES
        elif "start" in parameters and "end" in parameters:
            return TaskType.INTERVAL
        else:
            return TaskType.INTERVAL  # тип по умолчанию

    async def get_schedule(self, schedule_id: uuid.UUID) -> Optional[Dict[str, Any]]:
        """Возвращает информацию о расписании."""
        return await schedule_storage.get_schedule(schedule_id)

    async def list_schedules(self) -> list:
        """Возвращает список всех расписаний."""
        return await schedule_storage.list_schedules()

# Глобальный экземпляр планировщика
scheduler = Scheduler()