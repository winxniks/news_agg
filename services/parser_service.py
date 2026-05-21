import asyncio
import time
import logging
from typing import List, Optional
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

from config import settings
from models.schemas import Source, TaskStats
from services.parsing_db_saver import get_db_saver
from utils.article_adapter import adapt_news_list


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

class ParserManager:
    """Управляет парсерами и выполняет задачи парсинга."""
    
    def __init__(self, max_parallel: Optional[int] = None):
        self.max_parallel = max_parallel or settings.MAX_PARALLEL_PARSERS
        self.parsers = settings.PARSERS
        self.executor = ThreadPoolExecutor(max_workers=self.max_parallel)
    
    async def parse_last_hours(
        self,
        sources: List[Source],
        hours: int,
        parallel: bool = True,
    ) -> List[TaskStats]:
        """Парсит последние N часов для указанных источников."""
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=hours)
        return await self._parse_interval(sources, start_time, end_time, parallel)
    
    async def parse_last_minutes(
        self,
        sources: List[Source],
        minutes: int,
        parallel: bool = True,
    ) -> List[TaskStats]:
        """Парсит последние N минут."""
        end_time = datetime.now()
        start_time = end_time - timedelta(minutes=minutes)
        return await self._parse_interval(sources, start_time, end_time, parallel)
    
    async def parse_interval(
        self,
        sources: List[Source],
        start_time: datetime,
        end_time: datetime,
        parallel: bool = True,
    ) -> List[TaskStats]:
        """Парсит указанный интервал."""
        return await self._parse_interval(sources, start_time, end_time, parallel)
    
    async def _parse_interval(
        self,
        sources: List[Source],
        start_time: datetime,
        end_time: datetime,
        parallel: bool,
    ) -> List[TaskStats]:
        """Общая логика парсинга интервала."""
        if parallel:
            tasks = []
            for source in sources:
                task = self._parse_source_interval(source, start_time, end_time)
                tasks.append(task)
            results = await asyncio.gather(*tasks, return_exceptions=True)  # предотвращает прерывание всех задач при ошибке в одной
        else:
            results = []
            for source in sources:
                result = await self._parse_source_interval(source, start_time, end_time)
                results.append(result)
        
        # Обработка результатов
        stats = []
        for source, result in zip(sources, results):
            if isinstance(result, Exception):
                logger.error(f"Ошибка парсинга источника {source}: {result}", exc_info=result)
                stats.append(TaskStats(
                    source=source,
                    total_articles=0,
                    new_articles=0,
                    failed_articles=0,
                    duration_seconds=0.0,
                ))
                continue
            stats.append(result)
        return stats
    
    async def _parse_source_interval(
        self,
        source: Source,
        start_time: datetime,
        end_time: datetime,
    ) -> TaskStats:
        """Парсит один источник в интервале и возвращает статистику."""
        parser = self.parsers[source]
        start = time.time()
        
        try:
            # Парсеры синхронные, запускаем в отдельном потоке
            loop = asyncio.get_event_loop()
            news_list = await loop.run_in_executor(
                self.executor,
                parser.get_news_in_interval,
                start_time,
                end_time,
            )
            
            total = len(news_list) if news_list else 0
            saved = 0
            failed = 0
            
            if total > 0:
                # Адаптируем новости и сохраняем в БД
                articles = adapt_news_list(news_list, source.value)
                db_saver = await get_db_saver()
                result = await db_saver.save_articles_batch(articles)
                saved = result.saved
                failed = result.failed
            
            duration = time.time() - start
            return TaskStats(
                source=source,
                total_articles=total,
                new_articles=saved,
                failed_articles=failed,
                duration_seconds=duration,
            )
        except Exception as e:
            duration = time.time() - start
            logger.error(f"Ошибка парсинга источника {source}: {e}", exc_info=True)
            return TaskStats(
                source=source,
                total_articles=0,
                new_articles=0,
                failed_articles=0,
                duration_seconds=duration,
            )
    
    def close(self):
        """Закрывает ресурсы."""
        self.executor.shutdown()
        for parser in self.parsers.values():
            parser.close()


# Глобальный экземпляр менеджера
parser_manager = ParserManager()