import logging
import sys
from config import settings


def setup_logging():
    """Настройка логирования для всего приложения."""
    log_level = getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO)
    
    format_str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    
    logging.basicConfig(
        level=log_level,
        format=format_str,
        datefmt=datefmt,
        handlers=[
            logging.StreamHandler(sys.stdout),
            # При необходимости добавить FileHandler
        ]
    )
    
    # Установить уровень для сторонних библиотек, если нужно
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("apscheduler").setLevel(logging.WARNING)
    
    logging.info(f"Логирование настроено с уровнем {settings.LOG_LEVEL}")