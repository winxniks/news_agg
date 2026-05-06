import uuid
import json
from typing import Dict, List, Optional
from datetime import datetime
from models.schemas import Source, ScheduleRequest
from services.database import database


class PostgreSQLScheduleStorage:
    """PostgreSQL хранилище расписаний парсинга."""

    def __init__(self):
        self.pool = None

    async def _ensure_pool(self):
        """Убедиться, что пул подключений инициализирован."""
        if not self.pool:
            await database.connect()
            self.pool = database.pool
        return self.pool

    async def create_schedule(self, schedule_request: ScheduleRequest) -> uuid.UUID:
        """Создает новое расписание и возвращает его ID."""
        pool = await self._ensure_pool()
        schedule_id = uuid.uuid4()
        created_at = datetime.now()

        # Преобразуем источники в список строк
        sources_list = [source.value for source in schedule_request.sources]
        # Преобразуем параметры в JSON
        parameters_json = json.dumps(schedule_request.parameters)

        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO parsing.parsing_schedules 
                (schedule_id, cron_expression, sources, parameters, enabled, 
                 created_at, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
            """, schedule_id, schedule_request.cron_expression, sources_list, 
               parameters_json, schedule_request.enabled, created_at, created_at)

        return schedule_id

    async def get_schedule(self, schedule_id: uuid.UUID) -> Optional[Dict]:
        """Возвращает расписание по ID."""
        pool = await self._ensure_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT 
                    schedule_id, cron_expression, sources, parameters, enabled,
                    last_run, next_run, created_at, updated_at
                FROM parsing.parsing_schedules 
                WHERE schedule_id = $1
            """, schedule_id)

        if not row:
            return None

        # Преобразуем строки обратно в enum
        sources = [Source(source) for source in row['sources']]
        parameters = json.loads(row['parameters']) if row['parameters'] else {}

        return {
            "schedule_id": row['schedule_id'],
            "cron_expression": row['cron_expression'],
            "sources": sources,
            "parameters": parameters,
            "enabled": row['enabled'],
            "last_run": row['last_run'],
            "next_run": row['next_run'],
            "created_at": row['created_at'],
            "updated_at": row['updated_at']
        }

    async def update_schedule(
        self,
        schedule_id: uuid.UUID,
        cron_expression: Optional[str] = None,
        sources: Optional[List[Source]] = None,
        parameters: Optional[dict] = None,
        enabled: Optional[bool] = None,
        last_run: Optional[datetime] = None,
        next_run: Optional[datetime] = None,
    ) -> bool:
        """Обновляет расписание."""
        pool = await self._ensure_pool()

        updates = []
        params = []
        param_index = 1

        if cron_expression is not None:
            updates.append(f"cron_expression = ${param_index}")
            params.append(cron_expression)
            param_index += 1
        if sources is not None:
            sources_list = [source.value for source in sources]
            updates.append(f"sources = ${param_index}")
            params.append(sources_list)
            param_index += 1
        if parameters is not None:
            updates.append(f"parameters = ${param_index}")
            params.append(json.dumps(parameters))
            param_index += 1
        if enabled is not None:
            updates.append(f"enabled = ${param_index}")
            params.append(enabled)
            param_index += 1
        if last_run is not None:
            updates.append(f"last_run = ${param_index}")
            params.append(last_run)
            param_index += 1
        if next_run is not None:
            updates.append(f"next_run = ${param_index}")
            params.append(next_run)
            param_index += 1

        if not updates:
            return False

        params.append(schedule_id)  # WHERE condition
        query = f"""
            UPDATE parsing.parsing_schedules 
            SET {', '.join(updates)}
            WHERE schedule_id = ${param_index}
        """

        async with pool.acquire() as conn:
            result = await conn.execute(query, *params)
            # Проверяем, была ли обновлена хотя бы одна строка
            return "UPDATE 1" in result

    async def delete_schedule(self, schedule_id: uuid.UUID) -> bool:
        """Удаляет расписание."""
        pool = await self._ensure_pool()
        async with pool.acquire() as conn:
            result = await conn.execute(
                "DELETE FROM parsing.parsing_schedules WHERE schedule_id = $1",
                schedule_id
            )
            return "DELETE 1" in result

    async def list_schedules(self, enabled: Optional[bool] = None) -> List[Dict]:
        """Возвращает список всех расписаний с опциональной фильтрацией по enabled."""
        pool = await self._ensure_pool()

        where_clause = ""
        params = []
        if enabled is not None:
            where_clause = "WHERE enabled = $1"
            params.append(enabled)

        query = f"""
            SELECT 
                schedule_id, cron_expression, sources, parameters, enabled,
                last_run, next_run, created_at, updated_at
            FROM parsing.parsing_schedules
            {where_clause}
            ORDER BY created_at DESC
        """

        async with pool.acquire() as conn:
            rows = await conn.fetch(query, *params)

        schedules = []
        for row in rows:
            sources = [Source(source) for source in row['sources']]
            parameters = json.loads(row['parameters']) if row['parameters'] else {}

            schedules.append({
                "schedule_id": row['schedule_id'],
                "cron_expression": row['cron_expression'],
                "sources": sources,
                "parameters": parameters,
                "enabled": row['enabled'],
                "last_run": row['last_run'],
                "next_run": row['next_run'],
                "created_at": row['created_at'],
                "updated_at": row['updated_at']
            })

        return schedules

    async def count_schedules(self, enabled: Optional[bool] = None) -> int:
        """Возвращает количество расписаний."""
        pool = await self._ensure_pool()

        if enabled is None:
            async with pool.acquire() as conn:
                count = await conn.fetchval("SELECT COUNT(*) FROM parsing.parsing_schedules")
                return count
        else:
            async with pool.acquire() as conn:
                count = await conn.fetchval(
                    "SELECT COUNT(*) FROM parsing.parsing_schedules WHERE enabled = $1",
                    enabled
                )
                return count


# Глобальный экземпляр хранилища (для совместимости)
postgres_schedule_storage = PostgreSQLScheduleStorage()