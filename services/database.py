import asyncpg
from typing import Optional
from contextlib import asynccontextmanager

from config import settings
from services.parsing_db_saver import init_db_saver, db_saver as global_db_saver


class Database:
    """Управление подключением к базе данных."""
    
    def __init__(self):
        self.pool: Optional[asyncpg.Pool] = None
    
    async def connect(self):
        """Создает пул подключений."""
        self.pool = await asyncpg.create_pool(settings.DATABASE_URL)
        # Инициализируем DBSaver
        await init_db_saver(settings.DATABASE_URL)
    
    async def close(self):
        """Закрывает пул подключений."""
        if self.pool:
            await self.pool.close()
        if global_db_saver:
            await global_db_saver.close()
    
    @asynccontextmanager
    async def get_connection(self):
        """Контекстный менеджер для получения соединения."""
        if not self.pool:
            await self.connect()
        async with self.pool.acquire() as conn:
            yield conn
    
    async def health_check(self) -> bool:
        """Проверяет доступность базы данных."""
        try:
            async with self.get_connection() as conn:
                await conn.execute("SELECT 1")
            return True
        except Exception:
            return False


# Глобальный экземпляр базы данных
database = Database()