import logging
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
import time
import re
from .base_parser import BaseParser

logger = logging.getLogger(__name__)

class RiaNewsParser(BaseParser):
    """Парсер новостей РИА Новости"""
    
    def __init__(self, base_url="https://ria.ru"):
        super().__init__()
        self.base_url = base_url
        self.lenta_url = f"{base_url}/lenta"
        self.api_url = f"{base_url}/services/lenta/more.html"
    
    def parse_news_list(self, html_content, is_first_page=True):
        """Парсит страницу ленты: получает заголовок, ссылку, id и дату"""
        soup = BeautifulSoup(html_content, "html.parser")
        news_items = soup.find_all("div", class_="list-item", attrs={"data-type": "article"})
        
        news_list = []
        for item in news_items:
            news_data = self._extract_news_from_item(item)
            if news_data:
                news_list.append(news_data)
        
        # Получаем данные для следующей страницы
        next_page_data = self._get_next_page_data(soup, news_list, is_first_page)
        return news_list, next_page_data
    
    def _extract_news_from_item(self, item):
        """Извлекает данные новости из элемента ленты"""
        title_elem = item.find("a", class_="list-item__title")
        if not title_elem:
            return None
        
        title = title_elem.text.strip()
        link = title_elem.get("href", "")
        
        news_id = self._extract_id_from_link(link)
        
        # Извлекаем дату из ленты
        datetime_obj = self._extract_datetime_from_list_item(item)
        
        return {
            'title': title,
            'link': link,
            'id': news_id,
            'datetime': datetime_obj
        }
    
    def _extract_datetime_from_list_item(self, item):
        """Извлекает и преобразует дату из элемента ленты"""
        date_elem = item.find("div", class_="list-item__info-item", attrs={"data-type": "date"})
        if not date_elem or not date_elem.text:
            return None
        
        date_str = date_elem.text.strip()
        return self._parse_datetime_string(date_str)
    
    def _parse_datetime_string(self, date_str):
        """Преобразует строку с датой в объект datetime"""
        now = datetime.now()
        
        # Формат: '13:11'
        if re.match(r'^\d{2}:\d{2}$', date_str):
            hour, minute = map(int, date_str.split(':'))
            return now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        
        # Формат: 'Вчера, 13:11'
        elif date_str.startswith('Вчера,'):
            time_match = re.search(r'(\d{2}:\d{2})$', date_str)
            if time_match:
                hour, minute = map(int, time_match.group(1).split(':'))
                yesterday = now - timedelta(days=1)
                return yesterday.replace(hour=hour, minute=minute, second=0, microsecond=0)
        
        # Формат: '10 апреля, 13:11' или '10 апреля 2025, 13:11'
        else:
            match = re.match(r'(\d{1,2})\s+([а-я]+)(?:[,\s]+(\d{4}))?[,\s]+(\d{2}:\d{2})', date_str)
            if match:
                day = int(match.group(1))
                month_ru = match.group(2).lower()
                year = int(match.group(3)) if match.group(3) else now.year
                time_str = match.group(4)
                
                month_map = {
                    'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4,
                    'мая': 5, 'июня': 6, 'июля': 7, 'августа': 8,
                    'сентября': 9, 'октября': 10, 'ноября': 11, 'декабря': 12
                }
                
                month = month_map.get(month_ru)
                if month:
                    hour, minute = map(int, time_str.split(':'))
                    try:
                        return datetime(year, month, day, hour, minute)
                    except ValueError:
                        pass
        
        return None
    
    def _extract_id_from_link(self, link):
        """Извлекает ID новости из ссылки"""
        match = re.search(r'(\d+)\.html$', link)
        return match.group(1) if match else None
    
    def _get_next_page_data(self, soup, news_list, is_first_page):
        """Получает данные для следующей страницы"""
        # Для первой страницы ищем кнопку
        if is_first_page:
            more_button = soup.find("div", class_="list-more")
            if more_button and more_button.has_attr('data-url'):
                return more_button['data-url']
        
        # Для последующих страниц или если кнопки нет, формируем запрос на основе последней новости
        if news_list:
            last_news = news_list[-1]
            last_id = last_news.get('id')
            last_datetime = last_news.get('datetime')
            
            if last_id and last_datetime:
                # Форматируем дату: YYYYMMDDTHHMMSS
                date_str = last_datetime.strftime("%Y%m%dT%H%M%S")
                return f"/services/lenta/more.html?id={last_id}&date={date_str}&articlemask=lenta_common"
        
        return None
    
    def parse_article_details(self, url):
        """Парсит текст статьи, теги и ссылки на ленты по тегам"""
        response = self.fetch_page(url)
        if not response:
            return "", [], []
        
        soup = BeautifulSoup(response.text, "html.parser")
        
        text = self._parse_article_text(soup)
        tags = self._parse_article_tags(soup)
        categories = self._parse_article_rubric(soup)
        
        return text, tags, categories
    
    def _parse_article_text(self, soup):
        """Парсит текст статьи"""
        article_blocks = soup.find_all("div", class_=["article__text", "article__quote-text"])
        
        text_parts = []
        for block in article_blocks:
            text = self._clean_block_text(block)
            if text:
                text_parts.append(text)
        
        full_text = '\n\n'.join(text_parts)
        
        # Удаляем префикс вида "ГОРОД, число месяц - РИА Новости. "
        # Паттерн учитывает:
        # - Город может содержать точки (например, "С.-ПЕТЕРБУРГ")
        # - Дефисы разных типов
        # - Пробелы после точки
        full_text = re.sub(r'^[А-Яа-я\s\.-]+?, \d+ [а-я]+ [—–-] РИА Новости\. ', '', full_text)
        
        # Удаляем простой паттерн "РИА Новости. " в начале текста
        full_text = re.sub(r'^РИА Новости\. ', '', full_text)
        
        return full_text
    
    def _parse_article_tags(self, soup):
        """Парсит теги статьи и ссылки на ленты по тегам"""
        tags_data = []
        
        tags_block = soup.find("div", class_="article__tags", attrs={"data-type": "tags"})
        
        if tags_block:
            tag_links = tags_block.find_all("a", class_="article__tags-item")
            
            for tag_link in tag_links:
                tag_name = tag_link.text.strip()
                tag_url = tag_link.get("href", "")
                
                if tag_url and tag_url.startswith("/"):
                    tag_url = self.base_url + tag_url
                
                if tag_name:
                    tags_data.append({
                        'name': tag_name,
                        'url': tag_url
                    })
        
        return tags_data
    
    def _parse_article_rubric(self, soup): # category
        """Парсит рубрику статьи"""        
        # Find all script tags
        scripts = soup.find_all("script")
        
        for script in scripts:
            if script.string and 'dataLayer.push' in script.string:
                # Look for page_rubric in the script content
                match = re.search(r"'page_rubric'\s*:\s*'([^']*)'", script.string)
                if match:
                    return match.group(1)
        
        return None
        
    def _clean_block_text(self, block):
        """Очищает текст блока от лишних пробелов и форматирования"""
        for link in block.find_all('a'):
            if link.previous_sibling and not str(link.previous_sibling).endswith(' '):
                link.insert_before(' ')
            next_element = link.next_sibling
            if next_element:
                next_text = str(next_element)
                if next_text and not re.match(r'^[.,!?;:)\]}]\s*', next_text):
                    link.insert_after(' ')
            else:
                link.insert_after(' ')
        
        text = block.get_text()
        text = re.sub(r'\s+', ' ', text).strip()
        text =  self._normalize_text(text)
        return text
    
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
        
        logger.info(f"RIA: Парсим новости за промежуток: {start_datetime.strftime('%Y-%m-%d %H:%M')} - {end_datetime.strftime('%Y-%m-%d %H:%M')}")
        
        # Начинаем с главной страницы ленты
        url = self.lenta_url
        is_first_page = True
        
        while True:
            logger.info(f"RIA: Парсим ленту, страница {page_num}, URL: {url}")
            
            response = self.fetch_page(url)
            if not response:
                logger.error("RIA: Ошибка: не удалось загрузить страницу")
                break
            
            # Парсим страницу
            news_on_page, next_url = self.parse_news_list(response.text, is_first_page)
            
            if not news_on_page:
                logger.warning("RIA: На странице нет новостей - завершаем")
                break
            
            # Проверяем новости на странице
            should_stop, news_in_range = self._check_news_by_range(news_on_page, start_datetime, end_datetime)

            logger.info(f"RIA: Найдено новостей на странице: {len(news_on_page)}, из них в целевом диапазоне: {len(news_in_range)}")

            full_news_in_range = self.enrich_with_content(news_in_range)

            news_to_process.extend(full_news_in_range)
            
            # Проверяем условия останова
            if should_stop:
                logger.info("RIA: Достигнуты новости раньше start_datetime — останавливаемся")
                break
            
            # Получаем URL для следующей страницы
            if not next_url:
                logger.info("RIA: Нет данных для следующей страницы - завершаем")
                break
            
            url = self._make_full_url(next_url)
            is_first_page = False
            page_num += 1
            
            # Небольшая задержка между запросами
            time.sleep(0.5)
        
        logger.info(f"RIA: Всего собрано новостей в диапазоне: {len(news_to_process)}")
        return news_to_process
    
    def _check_news_by_range(self, news_list, start_datetime, end_datetime):
        """
        Проверяет новости на странице относительно временного диапазона.
        Возвращает (should_stop, news_in_range)
        - should_stop: True если нужно прекратить парсинг
        - news_in_range: список новостей, попадающих в диапазон [start_datetime, end_datetime]
        """
        news_in_range = []
        should_stop = False
        
        # Убедимся, что start_datetime и end_datetime имеют tzinfo (предполагаем UTC)
        if start_datetime.tzinfo is None:
            start_datetime = start_datetime.replace(tzinfo=timezone(timedelta(hours=3)))
        if end_datetime.tzinfo is None:
            end_datetime = end_datetime.replace(tzinfo=timezone(timedelta(hours=3)))
        
        for news in news_list:
            dt = news.get('datetime')
            
            if not dt:
                logger.warning(f"RIA: Не удалось определить дату из ленты — пропускаем")
                continue
            
            # Убедимся, что dt имеет tzinfo
            if dt.tzinfo is None:
                # Если dt без tzinfo, предполагаем UTC
                dt = dt.replace(tzinfo=timezone(timedelta(hours=3)))
            
            if dt > end_datetime:
                # Новость новее верхней границы - продолжаем парсить дальше
                #print(f"  Новость новее {end_datetime.strftime('%Y-%m-%d %H:%M')}: {dt.strftime('%Y-%m-%d %H:%M:%S')}")
                continue
            elif dt < start_datetime:
                # Новость старше нижней границы - пора останавливаться
                #print(f"  Новость старше {start_datetime.strftime('%Y-%m-%d %H:%M')}: {dt.strftime('%Y-%m-%d %H:%M:%S')}")
                should_stop = True
                break
            else:
                # Новость в целевом диапазоне - сохраняем и продолжаем
                #print(f"  Новость в целевом диапазоне: {dt.strftime('%Y-%m-%d %H:%M:%S')} — {news['title'][:50]}...")
                news_in_range.append(news)
        
        return should_stop, news_in_range
    
    def enrich_with_content(self, news_list, delay=0.3):
        """Добавляет текст статей и теги к новостям"""
        logger.info("RIA: Парсим детали новостей...")
        
        for i, news in enumerate(news_list, 1):
            full_url = self._make_full_url(news['link'])
            logger.info(f"RIA: [{i}/{len(news_list)}] {news['title'][:50]}...")
            
            content, tags, categories = self.parse_article_details(full_url)
            
            news['content'] = content
            news['tags'] = [tg['name'].lower().strip() for tg in tags]
            news['tag_url'] = [tg['url'] for tg in tags]
            news['categories'] = categories
            
            time.sleep(delay)
        
        return news_list
    
    def _make_full_url(self, link):
        """Формирует полный URL из относительного"""
        if link.startswith("//"):
            return "https:" + link
        elif link.startswith("/"):
            return self.base_url + link
        return link