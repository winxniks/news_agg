from typing import List
from uuid import UUID
from fastapi import APIRouter, HTTPException

from models.schemas import ScheduleRequest, ScheduleResponse
from services.scheduler import scheduler

router = APIRouter(prefix="/schedule", tags=["Shedule"])


@router.post("", response_model=ScheduleResponse)
async def create_schedule(request: ScheduleRequest):
    """
    Создание нового расписания.
    
    - **cron_expression**: cron-выражение (5 полей)
    - **sources**: список источников
    - **parameters**: параметры парсинга (тип, часы и т.д.)
    - **enabled**: включено ли расписание
    """
    schedule_id = await scheduler.add_schedule(request)
    schedule = await scheduler.get_schedule(schedule_id)
    if not schedule:
        raise HTTPException(status_code=500, detail="Не удалось создать расписание")
    
    return ScheduleResponse(**schedule)


@router.get("", response_model=List[ScheduleResponse])
async def list_schedules():
    """
    Список всех расписаний.
    """
    schedules = await scheduler.list_schedules()
    return [ScheduleResponse(**s) for s in schedules]


@router.get("/{schedule_id}", response_model=ScheduleResponse)
async def get_schedule(schedule_id: UUID):
    """
    Получение информации о расписании.
    """
    schedule = await scheduler.get_schedule(schedule_id)
    if not schedule:
        raise HTTPException(status_code=404, detail="Расписание не найдено")
    return ScheduleResponse(**schedule)


@router.delete("/{schedule_id}")
async def delete_schedule(schedule_id: UUID):
    """
    Удаление расписания.
    """
    success = await scheduler.remove_schedule(schedule_id)
    if not success:
        raise HTTPException(status_code=404, detail="Расписание не найдено")
    return {"message": "Расписание удалено"}


@router.post("/{schedule_id}/toggle")
async def toggle_schedule(schedule_id: UUID, enabled: bool):
    """
    Включение/выключение расписания.
    
    - **enabled**: True для включения, False для выключения
    """
    success = await scheduler.toggle_schedule(schedule_id, enabled)
    if not success:
        raise HTTPException(status_code=404, detail="Расписание не найдено")
    return {"message": f"Расписание {'включено' if enabled else 'выключено'}"}


@router.post("/{schedule_id}/run_now")
async def run_schedule_now(schedule_id: UUID):
    """
    Немедленный запуск расписания.
    """
    success = await scheduler.run_schedule_now(schedule_id)
    if not success:
        raise HTTPException(status_code=404, detail="Расписание не найдено")
    return {"message": "Задача запущена"}