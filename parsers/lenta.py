import logging
from bs4 import BeautifulSoup
import random
import time
from datetime import datetime, timezone, timedelta
import re
from .base_parser import BaseParser

logger = logging.getLogger(__name__)

class LentaNewsParser(BaseParser):
    """Парсер новостей Lenta.ru с защитой от обнаружения"""
    
    def __init__(self, base_url="https://lenta.ru"):
        # Включаем расширенные возможности для Lenta
        super().__init__(
            enable_user_agent_rotation=True,
            user_agents=None,  # используем стандартный набор из BaseParser
            enable_random_delays=True,
            min_response_length=100,
            verbose_logging=True
        )
        self.base_url = base_url
        
        # Кэш для уже загруженных статей (избегаем повторных запросов)
        self.articles_cache = {}
        
        # Статистика запросов
        self.request_count = 0
        self.last_request_time = None
    
    def _rate_limit_check(self):
        """Проверяет частоту запросов и при необходимости делает паузу"""
        current_time = datetime.now()
        
        if self.last_request_time:
            elapsed = (current_time - self.last_request_time).total_seconds()
            # Лимит: 10 запросов в минуту
            if self.request_count >= 10 and elapsed < 60:
                # Задержка: от 5 до 10 секунд
                sleep_time = random.uniform(5, 10)
                logger.warning(f"Lenta: Достигнут лимит запросов (10/мин). Пауза {sleep_time:.1f} сек...")
                time.sleep(sleep_time)
                self.request_count = 0
        
        self.last_request_time = current_time
        self.request_count += 1
    
    def parse_news_list(self, html_content, day_date=None):
        """Парсит страницу ленты: получает заголовок, ссылку, id и время"""
        soup = BeautifulSoup(html_content, "html.parser")
        news_items = soup.find_all("li", class_="archive-page__item", attrs={"class": "_news"})
        
        news_list = []
        for item in news_items:
            news_data = self._extract_news_from_item(item, day_date)
            if news_data:
                news_list.append(news_data)
        
        next_page_data = self._get_next_page_data(soup)
        return news_list, next_page_data
    
    def _parse_lenta_article_datetime(self, date_str):
        """Парсит дату публикации статьи из ленты и конвертирует в UTC"""
        try:
            time_str, date_part = date_str.split(', ', 1)
            month_map = {
                'января': '01', 'февраля': '02', 'марта': '03', 'апреля': '04',
                'мая': '05', 'июня': '06', 'июля': '07', 'августа': '08',
                'сентября': '09', 'октября': '10', 'ноября': '11', 'декабря': '12'
            }
            
            day, month_name, year = date_part.split(' ')
            month = month_map.get(month_name.lower(), '01')
            
            normalized_date = f"{year}-{month}-{day} {time_str}"
            # Создаем наивный datetime (предполагаем MSK/UTC+3)
            naive_dt = datetime.strptime(normalized_date, "%Y-%m-%d %H:%M")
            # Добавляем часовой пояс MSK (UTC+3) и конвертируем в UTC
            msk_tz = timezone(timedelta(hours=3))
            dt_msk = naive_dt.replace(tzinfo=msk_tz)
            dt_utc = dt_msk.astimezone(timezone(timedelta(hours=3)))
            return dt_utc
            
        except (ValueError, IndexError) as e:
            logger.error(f"Lenta: Не удалось распарсить дату '{date_str}'")
            return None
            
    def _extract_news_from_item(self, item, day_date=None):
        """Извлекает данные новости из элемента ленты, включая время публикации"""
        title_elem = item.find("h3", class_="card-full-news__title")
        if not title_elem:
            return None
        
        title = title_elem.text.strip()
        link_elem = item.find("a", class_="card-full-news")
        link = self._make_full_url(link_elem.get("href", ""))
        
        # Извлечение времени из ленты
        news_datetime = None
        if day_date:
            # Ищем время в двух возможных форматах
            time_elem = item.find("time", class_="card-full-news__date")
            if not time_elem:
                time_elem = item.find("time", class_="card-full-other__date")
            
            if time_elem and time_elem.text:
                time_str = time_elem.text.strip()
                news_datetime = self._combine_date_time(day_date, time_str)
                if news_datetime == None:
                    news_datetime = self._parse_lenta_article_datetime(time_str)
        
        return {
            'title': title,
            'link': link,
            'datetime': news_datetime,
            'id': None
        }
    
    def _get_next_page_data(self, soup):
        """Получает данные для следующей страницы"""
        next_button = soup.find("div", class_="loadmore__button", string="Дальше")
        
        if next_button:
            next_link = next_button.find_parent("a")
            if next_link and next_link.has_attr('href'):
                return self._make_full_url(next_link['href'])
        
        return None
    
    
    def parse_article_details(self, url):
        """Парсит дату, текст статьи, теги и ссылки на ленты по тегам"""
        # Проверяем кэш
        if url in self.articles_cache:
            logger.info(f"Lenta: {url} — статья уже была загружена (из кэша)")
            return self.articles_cache[url]
        
        # Проверка частоты запросов
        self._rate_limit_check()
        
        response = self.fetch_page(url)
        if not response:
            return None, [], []
        
        soup = BeautifulSoup(response.text, "html.parser")
        
        text, tags = self._parse_article_text_and_tags(soup)
        tag_eng = self._extract_puid_tags(soup)
        
        result = (text, tags, tag_eng)
        
        # Сохраняем в кэш, кэшируем только успешно загруженные статьи
        if text:
            self.articles_cache[url] = result
        
        return result
    
    def _parse_article_text_and_tags(self, soup):
        """Парсит текст статьи и одновременно собирает теги из ссылок внутри текста"""
        article_blocks = soup.find_all("p", class_="topic-body__content-text")
        
        text_parts = []
        tags_data = []
        seen_tags = set()
        
        for block in article_blocks:
            for link in block.find_all("a", href=True):
                href = link.get('href', '')
                tag_name = link.get_text(strip=True)

                #normalized_tag = self._lemmatize_tag(tag_name)
                
                if tag_name and '/tags/' in href and tag_name not in seen_tags:
                    tag_url = self._make_full_url(href)
                    tags_data.append({'name': tag_name, 'url': tag_url})
                    seen_tags.add(tag_name)
            
            cleaned_text = self._clean_block_text(block)
            if cleaned_text:
                text_parts.append(cleaned_text)
        
        full_text = '\n\n'.join(text_parts)
        return full_text, tags_data
    
    def _extract_puid_tags(self, soup):
        """Извлекает и обрабатывает теги из window._lentaData.puids"""
        tag_eng = []
        
        # Ищем скрипт, содержащий window._lentaData.puids
        scripts = soup.find_all('script')
        
        for script in scripts:
            if not script.string:
                continue
                
            # Ищем объект puids
            match = re.search(r'window\._lentaData\s*=\s*window\._lentaData\s*\|\|\s*\{\};\s*window\._lentaData\.puids\s*=\s*(\{[^;]+\})', script.string, re.DOTALL)
            
            if match:
                try:
                    # Парсим JavaScript объект в Python dict
                    puids_str = match.group(1)
                    
                    # Извлекаем все пары ключ-значение
                    puid_pattern = r'["\']?(puid\d+)["\']?\s*:\s*([^,}]+)'
                    puids = {}
                    
                    for puid_match in re.finditer(puid_pattern, puids_str):
                        key = puid_match.group(1)
                        value = puid_match.group(2).strip()
                        
                        # Убираем кавычки, если они есть
                        if value.startswith('"') and value.endswith('"'):
                            value = value[1:-1]
                        elif value.startswith("'") and value.endswith("'"):
                            value = value[1:-1]
                        elif value == 'null':
                            value = None
                        
                        puids[key] = value
                    
                    # Обрабатываем puid6 для удаления из puid18
                    puid6_value = puids.get('puid6', '')
                    
                    for key, value in puids.items():
                        if value is None or not key.startswith('puid'):
                            continue
                        
                        # Условие 3: Из puid18 вырезаем текст puid6
                        if key == 'puid18' and puid6_value and puid6_value in value:
                            value = value.replace(puid6_value, '').strip(':')[1:]
                            if not value:  # Если после удаления ничего не осталось
                                continue
                        
                        # Условие 2: Разделяем по двоеточию
                        if ':' in value:
                            parts = value.split(':')
                            for part in parts:
                                part = part.strip()
                                if part and part not in tag_eng:
                                    tag_eng.append(part)
                        else:
                            if value and value not in tag_eng:
                                tag_eng.append(value.lower())
                    
                except Exception as e:
                    logger.error(f"Lenta: Ошибка при парсинге puids: {e}")
                
                break  # Нашли нужный скрипт, выходим
        
        return tag_eng
    
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
        return text
    
    def get_news_in_interval(self, start_datetime=None, end_datetime=None):
        """
        Получает новости за промежуток времени.
        
        Параметры:
        - hours: int - количество часов (обратная совместимость)
        - start_datetime: datetime - начало промежутка
        - end_datetime: datetime - конец промежутка
        
        Если указан hours, используется текущая логика (обратная совместимость).
        Если указаны start/end, парсит указанный промежуток.
        Если ничего не указано, парсит последний час (start=now-1hour, end=now).
        """
        from datetime import datetime, timedelta
        
        # Установка дефолтов
        if end_datetime is None:
            end_datetime = datetime.now()
        if start_datetime is None:
            start_datetime = end_datetime - timedelta(hours=1)
        
        # Проверка корректности промежутка
        if start_datetime > end_datetime:
            logger.error(f"Lenta: Ошибка: start_datetime ({start_datetime}) > end_datetime ({end_datetime})")
            start_datetime, end_datetime = end_datetime, start_datetime
            logger.info(f"Lenta: Автоматически поменял местами")
        
        logger.info(f"Lenta: Парсим новости за промежуток: {start_datetime.strftime('%Y-%m-%d %H:%M')} - {end_datetime.strftime('%Y-%m-%d %H:%M')}")
        
        all_news = []
        
        # Генерация URL дней
        day_urls = self._generate_day_urls(start_datetime, end_datetime)
        cnt_urls = len(day_urls)
        logger.info(f"Lenta: Дней для парсинга: {cnt_urls}")
        logger.debug(f"Lenta: {day_urls}")
        
        for num, day_url in enumerate(day_urls):
            # Извлекаем дату из URL
            day_date = self._extract_date_from_url(day_url)
            if not day_date:
                logger.warning(f"Lenta: Lenta: Не удалось извлечь дату из URL: {day_url}")
                continue

            if start_datetime.date() != end_datetime.date():
                is_last_day = (num == len(day_urls) - 1)
                day_news = self._parse_lenta_day(day_url, day_date, start_datetime, end_datetime, early_break=is_last_day)
            else:
                day_news = self._parse_lenta_one_day(day_url, day_date, start_datetime, end_datetime)
            
            # Парсим детали только для отфильтрованных новостей
            if day_news:
                detailed_news = self._parse_details_for_news(day_news)
                all_news.extend(detailed_news)
        
        logger.info(f"Lenta: Всего собрано новостей в диапазоне: {len(all_news)}")
        return all_news
    
    def _generate_day_urls(self, start_datetime, end_datetime):
        """Генерирует список URL дней от start_datetime до end_datetime"""
        from datetime import timedelta
        urls = []
        current_date = start_datetime.date()
        end_date = end_datetime.date()
        
        while current_date <= end_date:
            url = f"/{current_date.year}/{current_date.month:02d}/{current_date.day:02d}/"
            urls.append(self._make_full_url(url))
            current_date += timedelta(days=1)
        
        return urls
    
    def _extract_date_from_url(self, url):
        """Извлекает дату из URL формата https://lenta.ru/YYYY/MM/DD/"""
        # Ищем паттерн /YYYY/MM/DD/ в URL
        import re
        pattern = r'/(\d{4})/(\d{2})/(\d{2})/'
        match = re.search(pattern, url)
        if match:
            year, month, day = map(int, match.groups())
            from datetime import date
            return date(year, month, day)
        return None
    
    def _parse_lenta_day(self, day_url, day_date, start_datetime, end_datetime, early_break=True):
        """Парсит новости за конкретный день с опциональным ранним прерыванием"""
        filtered_news = []
        current_url = day_url
        page_num = 1
        
        logger.info(f"Lenta: Парсим день: {day_date.strftime('%Y-%m-%d')}")
        
        while current_url:
            logger.info(f"Lenta: Парсим ленту, URL страницы: {current_url}")
            response = self.fetch_page(current_url)
            if not response:
                logger.warning(f"Lenta: Не удалось загрузить страницу: {current_url}")
                break

             # Проверяем, что ответ не пустой
            if not response.text or len(response.text) < 100:
                logger.warning(f"Lenta: Пустой или слишком короткий ответ на странице {page_num}")
                break
            
            news_list, next_page = self.parse_news_list(response.text, day_date)
            
            if not news_list:
                logger.warning(f"Lenta: На странице {page_num} не найдено новостей (возможно, изменилась структура)")
                # Выводим первые 100 символов для отладки
                #print(f"  Первые 100 символов HTML: {response.text[:100]}")
                break
                        
            # Фильтруем по промежутку времени
            news_in_range = self._filter_time_interval(news_list, start_datetime, end_datetime)
            filtered_news.extend(news_in_range)
            
            logger.info(f"Lenta: Найдено новостей на странице: {len(news_list)}, из них в целевом диапазоне: {len(news_in_range)}")
            
            # Проверяем раннее прерывание
            if early_break:
                if len(news_in_range) < len(news_list):
                    logger.info(f"Lenta: Все новости на странице раньше start_datetime, прекращаем парсинг дня")
                    break
            
            current_url = next_page
            
            if current_url:
                time.sleep(random.uniform(1, 2))
            else:
                logger.info("Lenta: Достигнут конец ленты")
        
        logger.info(f"Lenta: Всего отфильтровано новостей за день: {len(filtered_news)}")
        return filtered_news
    
    def _parse_lenta_one_day(self, day_url, day_date, start_datetime, end_datetime):
        """Парсит новости за конкретный день с ранней фильтрацией по времени"""
        filtered_news = []
        current_url = day_url
        
        logger.info(f"Lenta: Парсим день: {day_date.strftime('%Y-%m-%d')}")
        
        while current_url:
            logger.info(f"Lenta: Парсим ленту, URL страницы: {current_url}")

            # Загружаем страницу
            response = self.fetch_page(current_url)
            if not response:
                logger.warning(f"Lenta: Не удалось загрузить страницу: {current_url}")
                break
            
            # Парсим новости с временем из ленты
            news_list, next_page = self.parse_news_list(response.text, day_date)
            
            if not news_list:
                logger.warning(f"Lenta: На странице не найдено новостей")
                break
            
            # Фильтруем по промежутку времени
            news_in_range, should_stop = self._filter_time_interval_one_day(news_list, start_datetime, end_datetime)
            filtered_news.extend(news_in_range)
            
            logger.info(f"Lenta: Найдено новостей на странице: {len(news_list)}, из них в целевом диапазоне: {len(news_in_range)}")

            if should_stop:
                logger.info(f"Lenta: Достигнут конец временного интервала, прекращаем парсинг дня")
                break

            current_url = next_page

            # Случайная задержка между страницами
            if current_url:
                time.sleep(random.uniform(1, 2))
        
        logger.info(f"Lenta: Всего отфильтровано новостей за день: {len(filtered_news)}")
        return filtered_news 
    
    
    def _filter_time_interval(self, news_list, start_datetime, end_datetime):
        filtered_news = []
        
        # Убедимся, что start_datetime и end_datetime имеют tzinfo (предполагаем UTC)
        if start_datetime.tzinfo is None:
            start_datetime = start_datetime.replace(tzinfo=timezone(timedelta(hours=3)))
        if end_datetime.tzinfo is None:
            end_datetime = end_datetime.replace(tzinfo=timezone(timedelta(hours=3)))
        
        for news in news_list:
            news_dt = news.get('datetime')
            
            if not news_dt:
                # Если время не удалось извлечь, пропускаем новость
                continue
            
            # Убедимся, что news_dt имеет tzinfo
            if news_dt.tzinfo is None:
                # Если news_dt без tzinfo, предполагаем UTC
                news_dt = news_dt.replace(tzinfo=timezone(timedelta(hours=3)))
            
            if start_datetime <= news_dt <= end_datetime:
                filtered_news.append(news)
            #elif news_dt < start_datetime:
                # Новости стали слишком старыми
                # Если список отсортирован от новых к старым, можно прервать
                #print(f"  Найдена новость раньше start_datetime: {news_dt}")
        return filtered_news
    
    def _filter_time_interval_one_day(self, news_list, start_datetime, end_datetime):
        """
        Фильтрует новости по временному интервалу.
        
        Возвращает:
        - список новостей в интервале [start_datetime, end_datetime]
        - флаг need_stop: нужно ли прекратить дальнейший парсинг
        """
        filtered_news = []
        should_stop = False
        
        # Убедимся, что start_datetime и end_datetime имеют tzinfo (предполагаем UTC)
        if start_datetime.tzinfo is None:
            start_datetime = start_datetime.replace(tzinfo=timezone(timedelta(hours=3)))
        if end_datetime.tzinfo is None:
            end_datetime = end_datetime.replace(tzinfo=timezone(timedelta(hours=3)))
        
        for news in news_list:
            news_time = news.get('datetime')
            
            # Если время новости не указано, пропускаем её
            if news_time is None:
                continue
            
            # Убедимся, что news_time имеет tzinfo
            if news_time.tzinfo is None:
                # Если news_time без tzinfo, предполагаем UTC
                news_time = news_time.replace(tzinfo=timezone(timedelta(hours=3)))
            
            # Если новость позже end_datetime - нужно остановиться
            if news_time > end_datetime:
                should_stop = True
                break
            
            # Если новость в интервале [start_datetime, end_datetime]
            if start_datetime <= news_time <= end_datetime:
                filtered_news.append(news)
        
        return filtered_news, should_stop

    def _parse_details_for_news(self, news_list):
        """Парсит детали (текст, теги) для списка новостей"""
        detailed_news = []
        
        logger.info(f"Lenta: Парсим детали новостей...")
        
        for i, news in enumerate(news_list, 1):
            link = news.get('link')
            if not link:
                continue
            
            logger.info(f"Lenta: [{i}/{len(news_list)}] Загружаем статью: {link}")
            
            # Случайная задержка между статьями
            self._random_delay(0.5, 1.5)
            
            try:
                content, tags, tag_eng = self.parse_article_details(link)
                
                # Обновляем новость деталями
                news['content'] = content
                news['tags'] = [tg['name'].lower().strip() for tg in tags] if tags else []
                news['tag_url'] = [tg['url'] for tg in tags] if tags else []
                news['tag_eng'] = tag_eng if tag_eng else []
                
                detailed_news.append(news)
            except Exception as e:
                logger.error(f"Lenta: Ошибка при парсинге статьи {link}: {e}")
                # Пропускаем эту новость, продолжаем парсинг остальных
        
        return detailed_news
    
    def _make_full_url(self, link):
        """Формирует полный URL из относительного"""
        if link.startswith("/"):
            return self.base_url + link
        return link
    
    def _combine_date_time(self, day_date, time_str):
        """Комбинирует дату дня и строку времени HH:MM в datetime"""
        try:
            # Парсим время "HH:MM"
            from datetime import datetime
            time_obj = datetime.strptime(time_str, "%H:%M").time()
            # Комбинируем с датой дня
            return datetime.combine(day_date, time_obj)
        except ValueError:
            return None
    
    def _random_delay(self, min_seconds=0.5, max_seconds=1.5):
        """Случайная задержка для имитации человеческого поведения"""
        delay = random.uniform(min_seconds, max_seconds)
        time.sleep(delay)