"""
Сервис для работы с LLM API.
Поддерживает анализ дубликатов новостей через промпты.
Использует httpx для асинхронных HTTP запросов.
Промпт-префикс загружается из внешнего файла для удобства редактирования.
"""
import logging
import os
from typing import List, Optional

import httpx

from config import settings
from models.schemas import DuplicateResult

logger = logging.getLogger(__name__)


class LLMService:
    """Сервис для анализа дубликатов новостей через LLM."""

    def __init__(self):
        self.api_key = settings.LLM_API_KEY
        self.endpoint = settings.LLM_FULL_URL
        self.timeout = settings.LLM_TIMEOUT
        self.max_tokens = settings.LLM_MAX_TOKENS
        self.temperature = settings.LLM_TEMPERATURE
        self._prompt_prefix: Optional[str] = None

    async def analyze_duplicates(
        self,
        query_text: str,
        filter_date: Optional[str],
        candidates: List[DuplicateResult],
        prompt_prefix: Optional[str] = None,
    ) -> str:
        """
        Анализирует кандидатов в дубликаты через LLM.

        Args:
            query_text: Исходный текст для поиска дубликатов
            filter_date: Дата публикации основного текста (опционально)
            candidates: Список кандидатов в дубликаты (максимум 10)
            prompt_prefix: Начало промпта (неизменяемая часть, если не указана — используется базовая)

        Returns:
            Текстовый анализ от LLM
        """
        if not candidates:
            logger.info("Нет кандидатов для LLM анализа")
            return "Нет кандидатов для анализа."

        # Формируем промпт
        prompt = self._build_prompt(query_text, filter_date, candidates, prompt_prefix)

        # Вызываем LLM API
        try:
            analysis = await self._call_llm_api(prompt)
            logger.info(f"LLM анализ завершен, длина ответа: {len(analysis)} символов")
            return analysis
        except Exception as e:
            logger.error(f"Ошибка при вызове LLM: {e}")
            return f"Ошибка анализа LLM: {str(e)}"

    def _load_prompt_prefix(self) -> str:
        """
        Загружает префикс промпта из файла, указанного в настройках.
        
        Если файл не найден или произошла ошибка — возвращает базовый промпт.
        """
        if self._prompt_prefix is not None:
            return self._prompt_prefix

        prompt_file = settings.LLM_PROMPT_FILE
        try:
            if os.path.exists(prompt_file):
                with open(prompt_file, "r", encoding="utf-8") as f:
                    content = f.read()
                    # Удаляем комментарии (строки, начинающиеся с #)
                    lines = [
                        line for line in content.split("\n")
                        if not line.strip().startswith("#")
                    ]
                    self._prompt_prefix = "\n".join(lines).strip()
                    logger.info(
                        f"Промпт-префикс загружен из файла: {prompt_file} "
                        f"({len(self._prompt_prefix)} символов)"
                    )
            else:
                logger.warning(
                    f"Файл промпта не найден: {prompt_file}. "
                    f"Используется базовый промпт."
                )
                self._prompt_prefix = ""
        except Exception as e:
            logger.error(
                f"Ошибка загрузки промпта из {prompt_file}: {e}. "
                f"Используется базовый промпт."
            )
            self._prompt_prefix = ""

        return self._prompt_prefix or ""

    def _build_prompt(
        self,
        query_text: str,
        filter_date: Optional[str],
        candidates: List[DuplicateResult],
        prompt_prefix: Optional[str],
    ) -> str:
        """
        Строит промпт для LLM на основе шаблона.

        Приоритет префикса промпта:
        1. prompt_prefix (переданный в analyze_duplicates)
        2. Файл из settings.LLM_PROMPT_FILE
        3. Базовый встроенный промпт

        Формат промпта:
        {prompt_prefix}

        Основной текст:
        {query_text}
        Дата публикации: {filter_date}

        Кандидаты в дубликаты:
        Текст 1:
        {candidate.chunk_text}
        Дата публикации: {candidate.doc_public_dttm}

        Текст 2:
        ...
        """
        # Форматируем кандидатов
        candidates_text = ""
        for i, candidate in enumerate(candidates, 1):
            chunk_text = (candidate.chunk_text or "").strip()
            pub_date = candidate.doc_public_dttm or "Не указана"
            candidates_text += f"""
Текст {i}:
{chunk_text}
Дата публикации: {pub_date}
"""

        # Определяем префикс промпта (приоритет: аргумент > файл > встроенный)
        if prompt_prefix:
            base = prompt_prefix
            logger.debug(f"Используется переданный prompt_prefix ({len(base)} символов)")
        else:
            file_prompt = self._load_prompt_prefix()
            if file_prompt:
                base = file_prompt
                logger.debug(f"Используется файловый промпт-префикс ({len(base)} символов)")
            else:
                base = """Проанализируй следующие тексты новостей и определи, какие из них являются дубликатами основного текста. Учитывай смысловое содержание, ключевые факты и даты публикации.
Проанализируй каждый кандидат и укажи, является ли он дубликатом основного текста, и почему."""
                logger.debug("Используется встроенный базовый промпт")

        dynamic_prompt = """
Основной текст:
{query_text}
Дата публикации: {filter_date}

Кандидаты в дубликаты:
{candidates_text}
"""
        prompt = base + dynamic_prompt
        
        # Подставляем переменные
        prompt = prompt.replace("{query_text}", query_text)
        prompt = prompt.replace("{filter_date}", filter_date or "Не указана")
        prompt = prompt.replace("{candidates_text}", candidates_text)

        # Логируем: есть ли {candidates_text} в итоговом промпте
        if "{candidates_text}" in prompt:
            logger.warning(
                "ВНИМАНИЕ: плейсхолдер {candidates_text} НЕ ЗАМЕНИЛСЯ в промпте!"
            )

        # Логируем: проверяем, что candidates_text присутствует в конце промпта
        if candidates_text and candidates_text.strip():
            if prompt.endswith(candidates_text.strip()[-200:]):
                logger.debug(
                    f"Кандидаты успешно добавлены в конец промпта "
                    f"(последние 200 символов совпадают)"
                )
            else:
                logger.warning(
                    f"Кандидаты НЕ найдены в конце промпта! "
                    f"prompt заканчивается на: ...{prompt[-300:]!r}"
                )

        # Ограничиваем длину промпта (примерно: 1 токен ≈ 4 символа)
        max_chars = self.max_tokens * 4
        if len(prompt) > max_chars:
            logger.warning(
                f"Промпт слишком длинный: {len(prompt)} символов "
                f"(максимум {max_chars}). Будет обрезан. "
                f"Потеряно {len(prompt) - max_chars} символов в конце."
            )
            prompt = prompt[:max_chars]

        logger.info(
            f"Сформирован промпт длиной {len(prompt)} символов, "
            f"кандидатов: {len(candidates)}, "
            f"текст кандидатов: {len(candidates_text)} символов"
        )
        return prompt

    async def _call_llm_api(self, prompt: str) -> str:
        """
        Вызывает LLM API и возвращает текстовый ответ.

        Использует формат запроса как в примере с ChadGPT:
        {
            "message": prompt,
            "api_key": CHAD_API_KEY
        }
        """
        payload = {
            "message": prompt,
            "api_key": self.api_key,
        }

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(
                url=self.endpoint,
                json=payload,
            )

            if response.status_code != 200:
                error_text = response.text
                logger.error(
                    f"LLM API вернул ошибку: статус={response.status_code}, "
                    f"тело={error_text[:500]}"
                )
                raise Exception(
                    f"LLM API error {response.status_code}: {error_text[:500]}"
                )

            resp_json = response.json()

            # Обработка формата ChadGPT
            if resp_json.get("is_success"):
                return resp_json["response"]
            elif resp_json.get("is_success") is False:
                error = resp_json.get("error_message", "Неизвестная ошибка")
                raise Exception(f"LLM API вернул ошибку: {error}")

            # Fallback для других форматов
            if "response" in resp_json:
                return resp_json["response"]
            elif "choices" in resp_json:
                return resp_json["choices"][0].get("text", "")
            elif "content" in resp_json:
                return resp_json["content"]
            else:
                logger.warning(
                    f"Неизвестный формат ответа LLM API: {str(resp_json)[:200]}"
                )
                return str(resp_json)


# Глобальный экземпляр сервиса
llm_service = LLMService()