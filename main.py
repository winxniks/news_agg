import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.parsing import router as parsing_router
from api.vector import router as vector_router
from api.schedule import router as schedule_router
from config import settings
from services.database import database
from services.scheduler import scheduler
from services.qdrant_service import qdrant_service
from utils.logger import setup_logging
#from api.dop_endpoints import schedule, export, monitoring

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управление жизненным циклом приложения."""
    # Настройка логирования
    setup_logging()
    
    # Запуск приложения
    try:
        logger.info("Запуск приложения...")
        await database.connect()
        await scheduler.start()
        
        # Инициализация векторной БД (создание коллекции если не существует)
        logger.info("Инициализация векторной БД...")
        await qdrant_service.ensure_collection()
        logger.info("Векторная БД инициализирована")
        
    except Exception as e:
        logger.error(f"Ошибка запуска: {e}")
        raise  # Или обработать иначе
    
    yield
    
    # Остановка приложения
    logger.info("Остановка приложения...")
    await scheduler.stop()
    await database.close()
    await qdrant_service.close()


# Создание приложения FastAPI
app = FastAPI(
    title=settings.PROJECT_NAME,
    version="1.0.0",
    description="API для парсинга новостей с сайтов Lenta.ru, RBC, RIA и векторного поиска дубликатов",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

# Настройка CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Подключение эндпоинтов
app.include_router(parsing_router, prefix=settings.API_V1_PREFIX)
app.include_router(vector_router, prefix=settings.API_V1_PREFIX)
#app.include_router(export.router, prefix=settings.API_V1_PREFIX)
app.include_router(schedule_router, prefix=settings.API_V1_PREFIX)
#app.include_router(monitoring.router, prefix=settings.API_V1_PREFIX)


@app.get("/")
async def root():
    """Корневой эндпоинт."""
    return {
        "message": "Добро пожаловать в News Aggregator API",
        "docs": "/docs",
        "version": "1.0.0",
        "features": [
            "Парсинг новостей с Lenta.ru, RBC, RIA",
            "Векторный поиск дубликатов",
            "Batch-обработка чанков",
            "REST API с документацией"
        ]
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.DEBUG,
        log_level=settings.LOG_LEVEL.lower(),
    )