import requests
import time
import random
import logging
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


class BaseParser(ABC):
    """Абстрактный базовый класс для всех парсеров"""
    
    # Стандартный набор User-Agent для ротации
    DEFAULT_USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/112.0",
    ]
    
    def __init__(
        self,
        headers=None,
        enable_user_agent_rotation=False,
        user_agents=None,
        enable_random_delays=False,
        min_response_length=0,
        verbose_logging=False
    ):
        """
        Инициализация базового парсера.

        Параметры:
        - headers: dict - HTTP-заголовки (по умолчанию стандартный User-Agent)
        - enable_user_agent_rotation: bool - ротировать User-Agent перед каждым запросом
        - user_agents: list - список строк User-Agent для ротации (если None, используется DEFAULT_USER_AGENTS)
        - enable_random_delays: bool - добавлять случайные задержки между попытками
        - min_response_length: int - минимальная длина ответа в байтах (0 - отключить проверку)
        - verbose_logging: bool - подробное логирование в консоль
        """
        self.headers = headers or {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36"
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        
        # Флаги расширенных возможностей
        self.enable_user_agent_rotation = enable_user_agent_rotation
        self.enable_random_delays = enable_random_delays
        self.min_response_length = min_response_length
        self.verbose_logging = verbose_logging
        
        # Ротация User-Agent
        self.user_agents = user_agents or self.DEFAULT_USER_AGENTS
        self.current_ua_index = 0
        
        if self.enable_user_agent_rotation and self.verbose_logging:
            logger.debug(f"Ротация User-Agent включена, доступно {len(self.user_agents)} агентов")
    
    def _rotate_user_agent(self):
        """Ротирует User-Agent при каждом запросе"""
        if not self.user_agents:
            return
        self.current_ua_index = (self.current_ua_index + 1) % len(self.user_agents)
        new_ua = self.user_agents[self.current_ua_index]
        self.session.headers.update({"User-Agent": new_ua})
        if self.verbose_logging:
            logger.debug(f"  User-Agent изменён на: {new_ua[:50]}...")
    
    @abstractmethod
    def parse_news_list(self, html_content):
        """Парсит список новостей из HTML"""
        pass
    
    @abstractmethod
    def parse_article_details(self, url):
        """Парсит детали отдельной статьи"""
        pass
    
    def fetch_page(
        self,
        url,
        timeout=10,
        retries=3,
        retry_sleep=1.0,
        require_next_data=False,
        next_data_marker="__NEXT_DATA__",
    ):
        """
        Загружает страницу с ретраями.

        Параметры:
        - require_next_data: если True, то при успехе (HTTP 200) ретраим,
          пока не увидим marker в тексте (обычно для Next.js).
        - next_data_marker: что искать в HTML.
        """
        
        last_exc = None
        
        for attempt in range(1, retries + 1):
            try:
                # Ротация User-Agent перед запросом (если включена)
                if self.enable_user_agent_rotation:
                    self._rotate_user_agent()
                
                # Случайная задержка перед попыткой (кроме первой)
                if self.enable_random_delays and attempt > 1:
                    delay = retry_sleep * (attempt - 1) + random.uniform(0, 1)
                    if self.verbose_logging:
                        logger.debug(f"  Случайная задержка {delay:.2f} сек перед попыткой {attempt}")
                    time.sleep(delay)
                
                resp = self.session.get(
                    url,
                    timeout=timeout,
                    allow_redirects=True
                )
                resp.raise_for_status()
                
                # Проверка минимальной длины ответа
                if self.min_response_length > 0 and len(resp.text) < self.min_response_length:
                    if self.verbose_logging:
                        logger.warning(f"  Предупреждение: очень маленький ответ ({len(resp.text)} байт)")
                    if attempt < retries:
                        continue
                
                # если не требуем next-data — сразу возвращаем
                if not require_next_data:
                    return resp
                
                text = resp.text or ""
                has_marker = next_data_marker in text
                
                # если маркер не найден — решаем, ретраить или вернуть
                is_last_attempt = (attempt >= retries)
                
                if not has_marker and not is_last_attempt:
                    if self.verbose_logging:
                        logger.debug(f"  Маркер {next_data_marker} не найден, повторяем...")
                    time.sleep(retry_sleep)
                    continue
                
                return resp
                
            except requests.RequestException as e:
                last_exc = e
                if attempt < retries:
                    if self.verbose_logging:
                        logger.debug(f"  Ошибка (попытка {attempt}/{retries}): {e}. Повтор через {retry_sleep} сек")
                    time.sleep(retry_sleep)
                    continue
                logger.error(f"Ошибка загрузки страницы {url}: {e}")
        
        if last_exc:
            logger.error(f"Не удалось загрузить {url} после {retries} попыток: {last_exc}")
        return None
    
    def close(self):
        """Закрывает сессию"""
        self.session.close()