import uuid
import json
from typing import Dict, List, Optional
from datetime import datetime
from models.schemas import TaskStatus, TaskType, Source, ParsingTaskResponse
from services.database import database


class TaskStorage:
    """PostgreSQL хранилище задач парсинга."""

    def __init__(self):
        self.pool = None

    async def _ensure_pool(self):
        """Убедиться, что пул подключений инициализирован."""
        if not self.pool:
            await database.connect()
            self.pool = database.pool
        return self.pool

    async def create_task(
        self,
        task_type: TaskType,
        sources: List[Source],
        parameters: dict,
    ) -> uuid.UUID:
        """Создает новую задачу и возвращает её ID."""
        pool = await self._ensure_pool()
        task_id = uuid.uuid4()
        created_at = datetime.now()

        # Преобразуем источники в список строк
        sources_list = [source.value for source in sources]
        # Преобразуем параметры в JSON
        parameters_json = json.dumps(parameters)

        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO parsing.tasks 
                (task_id, task_type, sources, parameters, status, created_at)
                VALUES ($1, $2, $3, $4, $5, $6)
            """, task_id, task_type.value, sources_list, parameters_json, 
               TaskStatus.PENDING.value, created_at)

        return task_id

    async def get_task(self, task_id: uuid.UUID) -> Optional[Dict]:
        """Возвращает задачу по ID."""
        pool = await self._ensure_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT 
                    task_id, task_type, sources, parameters, status,
                    created_at, started_at, finished_at, result, error_message
                FROM parsing.tasks 
                WHERE task_id = $1
            """, task_id)

        if not row:
            return None

        # Преобразуем строки обратно в enum
        sources = [Source(source) for source in row['sources']]
        task_type = TaskType(row['task_type'])
        status = TaskStatus(row['status'])

        # Парсим JSON поля
        parameters = json.loads(row['parameters']) if row['parameters'] else {}
        result = json.loads(row['result']) if row['result'] else None

        return {
            "task_id": row['task_id'],
            "task_type": task_type,
            "sources": sources,
            "parameters": parameters,
            "status": status,
            "created_at": row['created_at'],
            "started_at": row['started_at'],
            "finished_at": row['finished_at'],
            "result": result,
            "error_message": row['error_message']
        }

    async def update_task(
        self,
        task_id: uuid.UUID,
        status: Optional[TaskStatus] = None,
        started_at: Optional[datetime] = None,
        finished_at: Optional[datetime] = None,
        result: Optional[dict] = None,
        error_message: Optional[str] = None,
    ) -> bool:
        """Обновляет задачу."""
        pool = await self._ensure_pool()

        updates = []
        params = []
        param_index = 1

        if status is not None:
            updates.append(f"status = ${param_index}")
            params.append(status.value)
            param_index += 1
        if started_at is not None:
            updates.append(f"started_at = ${param_index}")
            params.append(started_at)
            param_index += 1
        if finished_at is not None:
            updates.append(f"finished_at = ${param_index}")
            params.append(finished_at)
            param_index += 1
        if result is not None:
            updates.append(f"result = ${param_index}")
            params.append(json.dumps(result))
            param_index += 1
        if error_message is not None:
            updates.append(f"error_message = ${param_index}")
            params.append(error_message)
            param_index += 1

        if not updates:
            return False

        params.append(task_id)  # WHERE condition
        query = f"""
            UPDATE parsing.tasks 
            SET {', '.join(updates)}
            WHERE task_id = ${param_index}
        """

        async with pool.acquire() as conn:
            result = await conn.execute(query, *params)
            # Проверяем, была ли обновлена хотя бы одна строка
            return "UPDATE 1" in result

    async def list_tasks(
        self,
        limit: int = 20,
        offset: int = 0,
        status: Optional[TaskStatus] = None,
        task_type: Optional[TaskType] = None,
    ) -> List[Dict]:
        """Возвращает список задач с пагинацией и фильтрацией."""
        pool = await self._ensure_pool()

        conditions = []
        params = []
        param_idx = 1
        
        if status is not None:
            conditions.append(f"status = ${param_idx}")
            params.append(status.value)
            param_idx += 1
        if task_type is not None:
            conditions.append(f"task_type = ${param_idx}")
            params.append(task_type.value)
            param_idx += 1

        where_clause = ""
        if conditions:
            where_clause = "WHERE " + " AND ".join(conditions)

        # Базовый запрос с сортировкой
        query = f"""
            SELECT 
                task_id, task_type, sources, parameters, status,
                created_at, started_at, finished_at, result, error_message
            FROM parsing.tasks
            {where_clause}
            ORDER BY created_at DESC
            LIMIT ${param_idx} OFFSET ${param_idx + 1}
        """
        params.extend([limit, offset])

        async with pool.acquire() as conn:
            rows = await conn.fetch(query, *params)

        tasks = []
        for row in rows:
            sources = [Source(source) for source in row['sources']]
            task_type = TaskType(row['task_type'])
            status_enum = TaskStatus(row['status'])
            parameters = json.loads(row['parameters']) if row['parameters'] else {}
            result_data = json.loads(row['result']) if row['result'] else None

            tasks.append({
                "task_id": row['task_id'],
                "task_type": task_type,
                "sources": sources,
                "parameters": parameters,
                "status": status_enum,
                "created_at": row['created_at'],
                "started_at": row['started_at'],
                "finished_at": row['finished_at'],
                "result": result_data,
                "error_message": row['error_message']
            })

        return tasks

    async def count_tasks(self, status: Optional[TaskStatus] = None) -> int:
        """Возвращает количество задач."""
        pool = await self._ensure_pool()

        if status is None:
            async with pool.acquire() as conn:
                count = await conn.fetchval("SELECT COUNT(*) FROM parsing.tasks")
                return count
        else:
            async with pool.acquire() as conn:
                count = await conn.fetchval(
                    "SELECT COUNT(*) FROM parsing.tasks WHERE status = $1",
                    status.value
                )
                return count


# Глобальный экземпляр хранилища
task_storage = TaskStorage()