import logging
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
import time
import re
from .base_parser import BaseParser

logger = logging.getLogger(__name__)

class RbcNewsParser(BaseParser):
    """Парсер новостей РБК в заданном временном диапазоне"""
    
    def __init__(self, base_url="https://www.rbc.ru"):
        super().__init__()
        self.base_url = base_url
        self.lenta_url = f"{base_url}/short_news"
        self.api_base = "https://www.rbc.ru/api/rbcnews/v1/rbcnews.rostov/newsfeed"
    
    def parse_news_list(self, html_content):
        """Парсит список новостей из HTML страницы"""
        soup = BeautifulSoup(html_content, "html.parser")
        script = soup.find("script", id="__NEXT_DATA__")
        
        if not script:
            logger.warning("RBC: Не найден __NEXT_DATA__")
            return [], None
        
        data = self._parse_next_data(script.string)
        return self._extract_news_from_data(data)
    
    def _parse_next_data(self, json_string):
        """Парсит JSON из __NEXT_DATA__"""
        import json
        return json.loads(json_string)
    
    def _extract_news_from_data(self, data):
        """Извлекает новости из данных __NEXT_DATA__"""
        page_props = data.get("props", {}).get("pageProps", {})
        initial_feed = page_props.get("initialFeed")
        newsfeed = page_props.get("newsfeed")
        
        items = []
        next_cursor = None
        
        if initial_feed:
            items = initial_feed.get("items", [])
            next_cursor = initial_feed.get("endCursor")
        elif newsfeed:
            items = newsfeed.get("items", [])
            next_cursor = newsfeed.get("endCursor")
        
        news_list = []
        for item in items:
            news_data = self._extract_news_item(item)
            if news_data:
                news_list.append(news_data)
        
        return news_list, next_cursor
    
    def _extract_news_item(self, item):
        """Извлекает данные из элемента новости"""
        title = item.get("header") or item.get("title") or "Без заголовка"
        link = item.get("url") or item.get("link", {}).get("url", "")
        
        dt = self._parse_publish_date(item.get("publishDate"))
        
        categories = item.get("categories") or []
        tags = item.get("tags") or []

        category_tags = [
            {"title": c.get("name"), "id": None, "url": None} 
            for c in categories 
            if c.get("name")
        ]
        all_tags = tags + category_tags
        
        return {
            "title": title,
            "link": link,
            "id": item.get("id"),
            "datetime": dt,
            #"categories": [c.get("name") for c in categories if c.get("name")],
            "tags": [t.get("title") for t in all_tags if t.get("title")],
            "tag_ids": [t.get("id") for t in all_tags if t.get("id")],
            "tag_url": [t.get("url") for t in all_tags if t.get("url")],
        }
    
    def _parse_publish_date(self, date_str):
        """Парсит дату публикации и конвертирует в UTC"""
        if not date_str:
            return None
        
        try:
            dt = datetime.strptime(date_str, "%a, %d %b %Y %H:%M:%S %z")
            # Конвертируем в UTC
            dt_utc = dt.astimezone(timezone.utc)
            return dt_utc
        except ValueError:
            return None
    
    def parse_api_response(self, api_data):
        """Парсит ответ API"""
        items = api_data.get("items", [])
        
        news_list = []
        for item in items:
            news_data = self._extract_api_item(item)
            if news_data:
                news_list.append(news_data)
        
        next_cursor = api_data.get("endCursor")
        return news_list, next_cursor
    
    def _extract_api_item(self, item):
        """Извлекает данные из API ответа"""
        dt = None
        if "publishDateT" in item:
            try:
                timestamp = int(item["publishDateT"])
                dt = datetime.fromtimestamp(timestamp)
            except (ValueError, TypeError):
                pass
        
        tags = item.get("tags") or []
        categories = item.get("categories") or []

        category_tags = [
            {"title": c.get("name"), "id": None, "url": None} 
            for c in categories 
            if c.get("name")
        ]
        all_tags = tags + category_tags
        
        return {
            "title": item.get("title", "Без заголовка"),
            "link": item.get("url", ""),
            "id": item.get("id"),
            "datetime": dt,
            #"categories": [c.get("name") for c in categories if c.get("name")],
            "tags": [t.get("title") for t in all_tags if t.get("title")],
            "tag_ids": [t.get("id") for t in all_tags if t.get("id")],
            "tag_url": [t.get("url") for t in all_tags if t.get("url")],
        }
    
    def parse_article_details(self, url):
        """Парсит текст статьи"""
        response = self.fetch_page(url, require_next_data=True)
        if not response:
            return ""
        
        soup = BeautifulSoup(response.text, "html.parser")
        return self._extract_article_text(soup, url)
    
    def _extract_article_text(self, soup, url):
        """Извлекает текст из HTML статьи с сохранением разделения на абзацы"""
        text_parts = []
        
        # Способ 1: Параграфы с классом "paragraph"
        paragraphs = soup.find_all("p", class_="paragraph")
        if paragraphs:
            # Сохраняем каждый абзац отдельно, разделяя переносами строк
            for p in paragraphs:
                text = p.get_text(separator=" ", strip=True)
                if text:
                    text_parts.append(text)
        
        # Способ 2: div с itemprop="articleBody"
        if not text_parts:
            article_div = soup.find("div", {"itemprop": "articleBody"})
            if article_div:
                for p in article_div.find_all("p"):
                    text = p.get_text(separator=" ", strip=True)
                    if text:
                        text_parts.append(text)
        
        # Объединяем абзацы через двойной перенос строки
        summary = "\n\n".join(text_parts)
        
        # Очищаем лишние пробелы, но сохраняем переносы строк
        summary = self._normalize_text(summary)  # заменяем специальные пробелы на обычные
        summary = re.sub(r"[ \t]+", " ", summary)  # заменяем множественные пробелы на один
        summary = re.sub(r"\n[ \t]+", "\n", summary)  # убираем пробелы после переносов
        
        if not summary:
            logger.warning(f"RBC: Предупреждение: не удалось извлечь текст для {url}")
        
        return summary
    
    def _normalize_text(self, text):
        """Нормализует текст: удаляет невидимые символы, заменяет спецпробелы"""
        # Замена специальных пробелов на обычные
        special_spaces = {
            '\xa0': ' ',   # Non-breaking space
            '\u2002': ' ', # En space
            '\u2003': ' ', # Em space  
            '\u2009': ' ', # Thin space
            '\u200b': '',  # Zero width space - удаляем
            '\u200c': '',  # Zero width non-joiner
            '\u200d': '',  # Zero width joiner
            '\uFEFF': '',  # Zero width no-break space (BOM)
        }
        
        for char, replacement in special_spaces.items():
            text = text.replace(char, replacement)
        
        # Очистка лишних пробелов
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text
    
    def get_news_in_interval(self, start_datetime, end_datetime):
        """Получает новости в заданном временном диапазоне"""
        news_to_process = []  # Новости, попадающие в целевой диапазон
        page_num = 1

        logger.info(f"RBC: Парсим новости за промежуток: {start_datetime.strftime('%Y-%m-%d %H:%M')} - {end_datetime.strftime('%Y-%m-%d %H:%M')}")
                
        # Получаем первую страницу
        response = self.fetch_page(self.lenta_url, require_next_data=True)
        if not response:
            return []
        
        news_on_page, next_cursor = self.parse_news_list(response.text)
        
        while True:
            logger.info(f"RBC: Парсим ленту, страница {page_num}")
            if page_num == 1:
                logger.info(f"RBC: URL: {self.lenta_url}")
            
            if page_num > 1 and next_cursor:
                news_on_page, next_cursor = self._fetch_api_page(next_cursor)
                if not news_on_page:
                    break
            
            cnt_news_on_page = len(news_on_page)
            
            # Проверяем новости на странице
            should_stop, news_in_range = self._check_news_by_range(news_on_page, start_datetime, end_datetime)

            logger.info(f"RBC: Найдено новостей на странице: {cnt_news_on_page}, из них в целевом диапазоне: {len(news_in_range)}")

            
            if len(news_in_range) > 0:
                full_news_in_range = self.enrich_with_content(news_in_range)
                filtered_news = [n for n in full_news_in_range if n.get("content")]
                news_to_process.extend(filtered_news)
            
            # Проверяем условия останова
            if should_stop or not next_cursor:
                if not next_cursor:
                    logger.info("RBC: Достигнут конец ленты")
                else:
                    logger.info(f"RBC: Достигнуты новости раньше start_datetime {start_datetime.strftime('%Y-%m-%d %H:%M')} — останавливаемся")
                break
            
            page_num += 1
            time.sleep(1)
        
        logger.info(f"RBC: Всего собрано новостей в диапазоне: {len(news_to_process)}")
        return news_to_process
    
    def _check_news_by_range(self, news_list, start_datetime, end_datetime, max_consecutive_out_of_range=2):
        """
        Проверяет новости на странице относительно временного диапазона.
        Возвращает (should_stop, news_in_range)
        - should_stop: True если нужно прекратить парсинг
        - news_in_range: список новостей, попадающих в диапазон [start_datetime, end_datetime]
        - max_consecutive_out_of_range: максимальное количество подряд идущих новостей
          вне диапазона, после которого парсинг останавливается
        """
        news_in_range = []
        should_stop = False
        consecutive_out_of_range = 0
        
        # Убедимся, что start_datetime и end_datetime имеют tzinfo (предполагаем UTC)
        if start_datetime.tzinfo is None:
            start_datetime = start_datetime.replace(tzinfo=timezone(timedelta(hours=3)))
        if end_datetime.tzinfo is None:
            end_datetime = end_datetime.replace(tzinfo=timezone(timedelta(hours=3)))
        
        for news in news_list:
            dt = news.get("datetime")
            if not isinstance(dt, datetime):
                continue
            
            # Убедимся, что dt имеет tzinfo
            if dt.tzinfo is None:
                # Если dt без tzinfo, предполагаем UTC
                dt = dt.replace(tzinfo=timezone(timedelta(hours=3)))
                
            if dt > end_datetime:
                # Новость новее верхней границы - сбрасываем счетчик выбросов, продолжаем парсить дальше
                consecutive_out_of_range = 0
                continue
            elif dt < start_datetime:
                # Новость старше нижней границы - увеличиваем счетчик выбросов
                consecutive_out_of_range += 1
                logger.info(f"RBC: Найдена новость вне диапазона ({dt.strftime('%Y-%m-%d %H:%M')}), счетчик выбросов: {consecutive_out_of_range}")
                
                if consecutive_out_of_range >= max_consecutive_out_of_range:
                    # Достигли лимита подряд идущих выбросов - останавливаемся
                    should_stop = True
                    logger.info(f"RBC: Достигнуто {consecutive_out_of_range} новостей подряд раньше {start_datetime.strftime('%Y-%m-%d %H:%M')} — останавливаемся")
                    return should_stop, news_in_range
                else:
                    # Единичный выброс - пропускаем эту новость, но не останавливаем парсинг
                    continue
            else:
                # Новость в целевом диапазоне - сохраняем и сбрасываем счетчик выбросов
                consecutive_out_of_range = 0
                news_in_range.append(news)
        
        return should_stop, news_in_range
    
    def _fetch_api_page(self, cursor):
        """Загружает страницу через API"""
        api_url = f"{self.api_base}?endCursor={cursor}&limit=20"
        logger.info(f"RBC: URL: {api_url}")
        
        response = self.fetch_page(api_url, require_next_data=True)
        if not response:
            return [], None
        
        return self.parse_api_response(response.json())
    
    def enrich_with_content(self, news_list, delay=0.5):
        """Добавляет текст статей к новостям"""
        logger.info(f"RBC: Парсим детали новостей...")
        for i, item in enumerate(news_list, 1):
            link = item.get("link")
            if not link:
                item["content"] = ""
                continue
            
            full_url = self._make_full_url(link)
            logger.info(f"RBC: [{i}/{len(news_list)}] {full_url}")
            item["content"] = self.parse_article_details(full_url)
            time.sleep(delay)
        
        return news_list
    
    def _make_full_url(self, link):
        """Формирует полный URL из относительного"""
        if link.startswith("/"):
            return self.base_url + link
        return link