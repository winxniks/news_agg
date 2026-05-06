import hashlib
import asyncio
import logging
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, field
from contextlib import asynccontextmanager
import asyncpg
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from config import settings

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


@dataclass
class Article:
    """Модель статьи для сохранения в БД."""
    title: str
    url: str
    published_at: datetime
    source: str  # 'lenta', 'rbc', 'ria'
    text: str
    tags: List[str]
    tags_urls: List[str]
    eid: Optional[str] = None
    
    def __post_init__(self):
        """Валидация после инициализации."""
        #if not self.title or len(self.title) > 1000:
            #raise ValueError(f"Invalid title length: {len(self.title)}")
        if not self.url or len(self.url) > 500:
            raise ValueError(f"Invalid URL length: {len(self.url)}")
        if not self.text:
            raise ValueError("Empty text")
        if self.source not in ['lenta', 'rbc', 'ria']:
            raise ValueError(f"Unknown source: {self.source}")
        
        # Нормализация datetime
        if self.published_at.tzinfo is None:
            self.published_at = self.published_at.replace(tzinfo=timezone(timedelta(hours=3)))
        else:
            # Убедимся, что datetime в UTC+3
            if self.published_at.tzinfo != timezone(timedelta(hours=3)):
                logger.info(f"Converting datetime from {self.published_at.tzinfo} to UTC")
                self.published_at = self.published_at.astimezone(timezone(timedelta(hours=3)))


@dataclass
class SaveStats:
    """Статистика сохранения."""
    saved: int = 0
    failed: int = 0
    duplicates: int = 0
    errors: List[str] = field(default_factory=list)


@dataclass
class BatchStats:
    """Статистика по батчам."""
    total_batches: int = 0
    successful_batches: int = 0
    failed_batches: int = 0
    skipped_batches: int = 0
    total_articles: int = 0
    saved_articles: int = 0
    failed_articles: int = 0
    duplicate_articles: int = 0
    skipped_batches_info: List[Dict] = field(default_factory=list)
    batch_timings: List[float] = field(default_factory=list)  # время сохранения каждого батча
    
    def add_batch_result(self, save_stats: SaveStats, batch_size: int, duration: float):
        """Добавить результат сохранения батча."""
        self.total_batches += 1
        self.total_articles += batch_size
        self.saved_articles += save_stats.saved
        self.failed_articles += save_stats.failed
        self.duplicate_articles += save_stats.duplicates
        self.batch_timings.append(duration)
        
        if save_stats.failed == batch_size:  # весь батч не сохранен
            self.failed_batches += 1
        elif save_stats.failed > 0:  # частично сохранен
            self.successful_batches += 1
        else:  # полностью сохранен
            self.successful_batches += 1
    
    def add_skipped_batch(self, batch_info: Dict):
        """Добавить информацию о пропущенном батче."""
        self.skipped_batches += 1
        self.skipped_batches_info.append(batch_info)
    
    def to_dict(self) -> Dict:
        """Преобразовать в словарь для сериализации."""
        return {
            "total_batches": self.total_batches,
            "successful_batches": self.successful_batches,
            "failed_batches": self.failed_batches,
            "skipped_batches": self.skipped_batches,
            "total_articles": self.total_articles,
            "saved_articles": self.saved_articles,
            "failed_articles": self.failed_articles,
            "duplicate_articles": self.duplicate_articles,
            "avg_batch_time": sum(self.batch_timings) / len(self.batch_timings) if self.batch_timings else 0,
            "skipped_batches_info": self.skipped_batches_info,
        }


class DBSaver:
    """Сохранение статей в PostgreSQL с асинхронной обработкой."""
    
    # Константы
    CHUNK_SIZE = settings.CHUNK_SIZE
    CHUNK_OVERLAP = settings.CHUNK_OVERLAP
    ENCODING_NAME = settings.ENCODING_NAME

    MAX_CONCURRENT_SAVES = settings.MAX_CONCURRENT_SAVES
    
    def __init__(self, connection_string: str, max_pool_size: int = 20):
        self.connection_string = connection_string
        self.max_pool_size = max_pool_size
        self.pool: Optional[asyncpg.Pool] = None
        self._semaphore = asyncio.Semaphore(self.MAX_CONCURRENT_SAVES)
    
    async def init_connection(self, conn):
        """Инициализация подключения: устанавливаем часовой пояс UTC."""
        await conn.execute("SET timezone = 'UTC+3'")
        logger.debug("Database connection timezone set to UTC+3")
    
    async def connect(self):
        """Создает пул подключений."""
        if not self.pool:
            self.pool = await asyncpg.create_pool(
                self.connection_string,
                min_size=5,
                max_size=self.max_pool_size,
                command_timeout=60,
                max_queries=50000,
                max_inactive_connection_lifetime=300,
                init=self.init_connection
            )
            logger.info(f"Database pool created with max size {self.max_pool_size}, timezone set to UTC+3")
    
    async def close(self):
        """Закрывает пул подключений."""
        if self.pool:
            await self.pool.close()
            logger.info("Database pool closed")
    
    @asynccontextmanager
    async def transaction(self):
        """Контекстный менеджер для транзакций с автоматическим retry."""
        if not self.pool:
            await self.connect()
        
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                try:
                    yield conn
                except Exception as e:
                    logger.error(f"Transaction error: {e}", exc_info=True)
                    raise
    
    @staticmethod
    def _compute_text_hash(text: str) -> str:
        """Вычисляет хеш текста для дедупликации чанков."""
        return hashlib.sha256(text.encode()).hexdigest()
    
    async def _check_duplicate(self, conn, doc_url: str, doc_text_hash: str) -> Tuple[int, int, str]:
        """Проверяет наличие дубликата по хешу текста или url и возвращает тип дубликата:
            "full" - дубликат по url и тексту -> пропуск
            "url" - дубликат по url, но разные тексты -> условный дубликат - обновленная статья в рамках одного источника -> требуется обновление текста и чанков
            "hash" - дубликат по тексту, но разные url -> скопированная статья из другого источника -> требуется сравнить даты найденных статей и зависать таблицу с дублями более позднюю статью
             None - нет дубликатов -> требуется добавить новую статью
           Также возвращает версию документа, если документ уже был в базе, источник и дату публикации (чтобы понять на каком источнике текст статьи появился раньше, если тип hash).
        """
        row = await conn.fetchrow(
            """
            SELECT  doc_id,
                    ver,
                    CASE 
                      WHEN doc_url = $1 AND doc_text_hash = $2
                        THEN 'full'
                      WHEN doc_url = $1
                        THEN 'url'
                      ELSE 'hash'
                    END AS duplicate_type,
                    doc_src,
                    public_dttm
            FROM news.documents 
            WHERE doc_url = $1 OR doc_text_hash = $2
            LIMIT 1
            """,
            doc_url, 
            doc_text_hash
        )

        if row:
            doc_id_duplicate, duplicate_ver, duplicate_type, duplicate_src, duplicate_published_dttm = row
            logger.info(f"Duplicate found by {duplicate_type} for doc_url={doc_url}: doc_id={doc_id_duplicate}, ver={duplicate_ver}, src={duplicate_src}")
            
            return doc_id_duplicate, duplicate_ver, duplicate_type, duplicate_src, duplicate_published_dttm
    
        return None, None, None, None, None
            
    async def _save_chunks(self, conn, doc_id: int, text: str, duplicate_type: str) -> int:
        """Разбивает текст на чанки с помощью langchain и сохраняет их пакетно."""
        from langchain_text_splitters import RecursiveCharacterTextSplitter
        
        if not text:
            return 0
        
        # Создаем splitter с использованием токенов
        try:
            text_splitter = RecursiveCharacterTextSplitter.from_tiktoken_encoder(
                encoding_name=self.ENCODING_NAME,
                chunk_size=self.CHUNK_SIZE,
                chunk_overlap=self.CHUNK_OVERLAP,
                separators=["\n\n", "\n", ". ", "! ", "? ", " ", ""],
                keep_separator=False, 
                strip_whitespace=True
            )
            
            # Разбиваем текст на чанки
            chunk_texts = text_splitter.split_text(text)
        except Exception as e:
            logger.error(f"Error splitting text for doc_id={doc_id}: {e}")
            return 0
        
        if not chunk_texts:
            return 0
        
        # Подготавливаем данные для вставки
        chunks = []
        for idx, chunk_text in enumerate(chunk_texts, 1):
            chunk_hash = self._compute_text_hash(chunk_text)
            chunks.append((doc_id, idx, chunk_text, chunk_hash))
        
        # Удаляем старые чанки только если это обновление по URL
        if duplicate_type == "url":
            await conn.execute(
                """
                DELETE FROM news.chunks 
                WHERE doc_id = $1
                """,
                doc_id
            )
            logger.info(f"Deleted old versions chunks for doc_id={doc_id}")
        
        # Пакетная вставка чанков
        await conn.executemany(
            """
            INSERT INTO news.chunks 
            (doc_id, chunk_idx, chunk_text, chunk_hash)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (doc_id, chunk_idx) DO NOTHING
            """,
            chunks
        )
        
        logger.info(f"Saved {len(chunks)} chunks for doc_id={doc_id}")
        return len(chunks)
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=5),
        retry=retry_if_exception_type(asyncpg.exceptions.DeadlockDetectedError),
        reraise=True
    )
    async def _save_tags_batch(self, conn, doc_id: int, tags: List[str], tags_urls: List[str], source: str) -> int:
        """Пакетное сохранение тегов и их связей с защитой от deadlock."""
        if not tags:
            return 0
        
        # Сортируем теги для детерминированного порядка блокировок
        # Это предотвращает deadlock при параллельных транзакциях
        sorted_data = sorted(zip(tags, tags_urls), key=lambda x: (x[0], source))
        sorted_tags = [item[0] for item in sorted_data]
        sorted_tags_urls = [item[1] for item in sorted_data]
        
        # Используем advisory lock для группы тегов
        # Создаем хеш из тегов и источника для уникального ключа блокировки
        # Используем кортеж (отсортированные теги, источник) для детерминированного хеша
        lock_data = (tuple(sorted(tags)), source)
        lock_key = hash(lock_data) % (2**31 - 1)
        
        try:
            # Получаем advisory lock для этой группы тегов
            await conn.execute("SELECT pg_advisory_xact_lock($1)", lock_key)
            
            # Один запрос для всего: вставка/обновление тегов + создание связей
            result = await conn.fetchval(
                """
                -- Входные данные
                WITH input_data(tag_nm, tag_url) AS (
                    SELECT *
                    FROM unnest($1::text[], $2::text[])
                ),
                -- Добавляем source ко всем записям
                input_with_source AS (
                    SELECT tag_nm, tag_url, $3::text as tag_src
                    FROM input_data
                ),
                -- Вставка (полная)/обновление (url) тегов
                upserted_tags AS (
                    INSERT INTO news.tags (tag_nm, tag_url, tag_src)
                    SELECT tag_nm, tag_url, tag_src
                    FROM input_with_source
                        ON CONFLICT (tag_nm, tag_src) DO UPDATE
                        SET tag_url = COALESCE(EXCLUDED.tag_url, tags.tag_url)
                        WHERE tags.tag_url IS DISTINCT FROM EXCLUDED.tag_url  -- обновляем только если URL отличается
                    RETURNING tag_id, tag_nm, tag_src
                ),
                -- Получаем все ID (и новые, и существующие)
                all_tag_ids AS (
                    SELECT t.tag_id
                    FROM news.tags t
                    JOIN input_with_source i
                        ON t.tag_nm = i.tag_nm
                        AND t.tag_src = i.tag_src
                )
                -- Создаем связи (игнорируем конфликты)
                INSERT INTO news.docs_tags_lnk (doc_id, tag_id)
                SELECT $4, tag_id
                FROM all_tag_ids
                ON CONFLICT (tag_id, doc_id) DO NOTHING
                RETURNING 1
                """,
                sorted_tags,
                sorted_tags_urls,
                source,
                doc_id
            )
            
            saved_count = len(sorted_tags)
            logger.info(f"Saved {saved_count} tags for doc_id={doc_id}")
            return saved_count
        except asyncpg.exceptions.DeadlockDetectedError:
            # Логируем deadlock для мониторинга
            logger.warning(f"Deadlock detected for doc_id={doc_id}, tags={sorted_tags[:3]}...")
            raise  # Декоратор @retry обработает повторную попытку
        except Exception as e:
            logger.error(f"Error saving tags for doc_id={doc_id}: {e}", exc_info=True)
            return 0
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((asyncpg.exceptions.ConnectionDoesNotExistError,
                                       asyncpg.exceptions.TooManyConnectionsError)),
        reraise=True
    )
    async def save_article(self, article: Article) -> Tuple[bool, Optional[str]]:
        """
        Сохраняет статью в БД.
        
        Returns:
            Tuple[bool, Optional[str]]: (успех, сообщение об ошибке/дубликате)
        """
        async with self._semaphore:  # Ограничиваем параллелизм
            try:
                # Валидация статьи
                article.__post_init__()
                
                # Вычисляем хеши
                doc_text_hash = self._compute_text_hash(article.text)
                
                async with self.transaction() as conn:
                    # Проверка на дубликат
                    duplicate_doc_id, duplicate_ver, duplicate_type, duplicate_src, duplicate_published_dttm = await self._check_duplicate(conn, article.url, doc_text_hash)

                    doc_id = duplicate_doc_id
                    ver = 1 if not duplicate_ver else duplicate_ver + 1

                    if duplicate_type == "full":
                        logger.info(f"Skipping duplicate article: {article.url}, doc_id={duplicate_doc_id}")
                        return False, f"Full Duplicate (doc_id={duplicate_doc_id})"
                    elif duplicate_type == "url":
                        # обновляем текст существующего документа
                        await conn.execute(
                            """
                            UPDATE news.documents 
                            SET ver = $2,
                                title = $3,
                                doc_text = $4,
                                doc_text_hash = $5,
                                changed_dttm = NOW()
                            WHERE doc_id = $1
                            """,
                            doc_id,
                            ver,
                            article.title,
                            article.text,
                            doc_text_hash
                        )
                        logger.info(f"Updated article: doc_id={doc_id}, ver={ver}")
                    elif duplicate_type == "hash":
                        # скопированная статья из другого источника -> сравниваем даты найденных статей и, если надо, обновляем таблицу более метаданными более ранней статьи
                        logger.info(f"Duplicate article text from another source {duplicate_src}: {duplicate_doc_id}")
                        if duplicate_published_dttm and article.published_at < duplicate_published_dttm:
                            await conn.execute(
                                """
                                UPDATE news.documents
                                SET doc_src = $2,
                                    title = $3,
                                    public_dttm = $4,
                                    ver = $5,
                                    changed_dttm = NOW()
                                WHERE doc_id = $1
                                """,
                                doc_id,
                                article.source,
                                article.title,
                                article.published_at,
                                ver
                            )

                            tags_count = 0
                            tags_count = await self._save_tags_batch(conn, doc_id, article.tags, article.tags_urls, article.source)
                            
                            return True, None
                    else:
                        # вставляем новый документ
                        doc_id = await conn.fetchval(
                            """
                            INSERT INTO news.documents 
                            (title, public_dttm, eid, doc_url, doc_src, doc_text, doc_text_hash, ver)
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                            RETURNING doc_id
                            """,
                            article.title,
                            article.published_at,
                            article.eid,
                            article.url,
                            article.source,
                            article.text,
                            doc_text_hash,
                            ver
                        )
                        logger.info(f"Adding new article: {doc_id}")
                    
                    # Сохраняем чанки
                    chunks_count = 0
                    chunks_count = await self._save_chunks(conn, doc_id, article.text, duplicate_type)
                    
                    # Сохраняем теги (если дубликат статьи на разных источниках, то набор тегов обоих источников сохраняется)
                    tags_count = 0
                    tags_count = await self._save_tags_batch(conn, doc_id, article.tags, article.tags_urls, article.source)
                    
                    logger.info(
                        f"Saved article: doc_id={doc_id}, ver={ver}, "
                        f"chunks={chunks_count}, tags={tags_count}, url={article.url}"
                    )
                    
                    return True, None
                    
            except Exception as e:
                error_msg = f"Error saving article {article.url}: {e}"
                logger.error(error_msg, exc_info=True)
                return False, error_msg
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((asyncpg.exceptions.ConnectionDoesNotExistError,
                                       asyncpg.exceptions.TooManyConnectionsError)),
        reraise=True
    )
    async def save_batch_with_retry(self, articles: List[Article]) -> SaveStats:
        """
        Сохраняет батч статей с повторными попытками при ошибках соединения.
        
        Args:
            articles: Список статей для сохранения
            
        Returns:
            SaveStats: Статистика сохранения
        """
        return await self.save_articles_batch(articles)
    
    async def save_articles_batch(self, articles: List[Article]) -> SaveStats:
        """
        Сохраняет несколько статей параллельно.
        
        Args:
            articles: Список статей для сохранения
            
        Returns:
            SaveStats: Статистика сохранения
        """
        if not articles:
            return SaveStats()
        
        logger.info(f"Starting batch save of {len(articles)} articles")
        
        # Запускаем параллельное сохранение
        tasks = [self.save_article(article) for article in articles]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Анализируем результаты
        stats = SaveStats()
        
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                stats.failed += 1
                stats.errors.append(f"Article {i}: {str(result)}")
                logger.error(f"Batch save failed for article {i}: {result}")
            elif isinstance(result, tuple):
                success, message = result
                if success:
                    stats.saved += 1
                elif message and ("Full Duplicate" or "Duplicate article text") in message:
                    stats.duplicates += 1
                else:
                    stats.failed += 1
                    if message:
                        stats.errors.append(f"Article {articles[i].url}: {message}")
        
        logger.info(
            f"Batch save completed: saved={stats.saved}, "
            f"failed={stats.failed}, duplicates={stats.duplicates}"
        )
        
        return stats


# Глобальный экземпляр DBSaver
db_saver: Optional[DBSaver] = None


async def init_db_saver(connection_string: str, max_pool_size: int = 20) -> DBSaver:
    """Инициализирует глобальный DBSaver."""
    global db_saver
    if db_saver is None:
        db_saver = DBSaver(connection_string, max_pool_size)
        await db_saver.connect()
        logger.info("DBSaver initialized successfully")
    return db_saver


async def get_db_saver() -> DBSaver:
    """Возвращает глобальный экземпляр DBSaver."""
    if db_saver is None:
        raise RuntimeError("DBSaver not initialized. Call init_db_saver first.")
    return db_saver


async def close_db_saver():
    """Закрывает глобальный DBSaver."""
    global db_saver
    if db_saver:
        await db_saver.close()
        db_saver = None
        logger.info("DBSaver closed")