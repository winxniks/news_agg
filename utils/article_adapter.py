import logging
from typing import List, Dict, Any
from datetime import datetime
from services.parsing_db_saver import Article

logger = logging.getLogger(__name__)


def adapt_lenta_news(news_item: Dict[str, Any]) -> Article:
    """Адаптирует новость Lenta.ru в Article."""
    # Предполагаемая структура news_item (нужно уточнить по реальным данным)
    title = news_item.get("title", "")
    url = news_item.get("link", "")
    published_at = news_item.get("datetime")
    if isinstance(published_at, str):
        # Парсим строку в datetime (упрощенно)
        try:
            published_at = datetime.fromisoformat(published_at.replace('Z', '+00:00'))
        except:
            published_at = datetime.now()
    elif not isinstance(published_at, datetime):
        published_at = datetime.now()
    
    text = news_item.get("content", "")
    tags = news_item.get("tags", [])
    tags_urls = news_item.get("tag_url", [])
    eid = news_item.get("id")
    
    return Article(
        title=title,
        url=url,
        published_at=published_at,
        source="lenta",
        text=text,
        tags=tags,
        tags_urls=tags_urls,
        eid=eid,
    )


def adapt_rbc_news(news_item: Dict[str, Any]) -> Article:
    """Адаптирует новость RBC в Article."""
    title = news_item.get("title", "")
    url = news_item.get("link", "")
    published_at = news_item.get("datetime")
    if isinstance(published_at, str):
        try:
            published_at = datetime.fromisoformat(published_at.replace('Z', '+00:00'))
        except:
            published_at = datetime.now()
    elif not isinstance(published_at, datetime):
        published_at = datetime.now()
    
    text = news_item.get("content", "")
    tags = news_item.get("tags", [])
    tags_urls = news_item.get("tag_url", [])
    eid = news_item.get("id")
    
    return Article(
        title=title,
        url=url,
        published_at=published_at,
        source="rbc",
        text=text,
        tags=tags,
        tags_urls=tags_urls,
        eid=eid,
    )


def adapt_ria_news(news_item: Dict[str, Any]) -> Article:
    """Адаптирует новость RIA в Article."""
    title = news_item.get("title", "")
    url = news_item.get("link", "")
    published_at = news_item.get("datetime")
    if isinstance(published_at, str):
        try:
            published_at = datetime.fromisoformat(published_at.replace('Z', '+00:00'))
        except:
            published_at = datetime.now()
    elif not isinstance(published_at, datetime):
        published_at = datetime.now()
    
    text = news_item.get("content", "")
    tags = news_item.get("tags", [])
    tags_urls = news_item.get("tag_url", [])
    eid = news_item.get("id")
    
    return Article(
        title=title,
        url=url,
        published_at=published_at,
        source="ria",
        text=text,
        tags=tags,
        tags_urls=tags_urls,
        eid=eid,
    )


def adapt_news_list(news_list: List[Dict[str, Any]], source: str) -> List[Article]:
    """Адаптирует список новостей из указанного источника."""
    adapter = {
        "lenta": adapt_lenta_news,
        "rbc": adapt_rbc_news,
        "ria": adapt_ria_news,
    }.get(source)
    if not adapter:
        raise ValueError(f"Unknown source: {source}")
    
    articles = []
    for item in news_list:
        try:
            articles.append(adapter(item))
        except Exception as e:
            # Логирование ошибки преобразования
            logger.error(f"Ошибка адаптации новости: {e}")
            continue
    logger.info(f"Адаптировано {len(articles)} новости(ей) для источника {source}")
    return articles