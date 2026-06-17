# News Aggregator API

FastAPI приложение для парсинга новостей с сайтов Lenta.ru, RBC, RIA. Предоставляет API для управления задачами парсинга, расписанием, экспортом данных и мониторингом.

## Возможности

- **Парсинг новостей**:
  - Последние N часов (максимум 12)
  - Последние N минут (максимум 60)
  - Произвольный временной интервал
  - Поддержка источников: Lenta, RBC, RIA
  - Параллельное выполнение парсеров

- **Управление задачами**:
  - Асинхронное выполнение в фоне
  - Отслеживание статуса задач
  - История задач с пагинацией

- **Расписание**:
  - Создание периодических задач с cron-выражениями
  - Включение/выключение расписаний
  - Немедленный запуск
  - Отслеживание статуса расписаний

## Установка

### Требования

- Python 3.8+
- PostgreSQL 12+
- Установленные зависимости

### Шаги

1. Клонируйте репозиторий:
   ```bash
   git clone https://github.com/winxniks/news_agg.git
   cd news_agg
   ```

2. Поднимите контейнеры с базами данных (на данный момент используется облачное решение Qdrant):
   ```bash
   mkdir data\qdrant
   mkdir data\postgres

   docker-compose up -d
   ```

  Если используете локальный Qdrant, то он будет доступен по адресу: http://localhost:6333

2. Создайте виртуальное окружение:
   ```bash
   python -m venv venv
   source venv/bin/activate  # Linux/Mac
   venv\Scripts\activate     # Windows
   ```

3. Установите зависимости:
   ```bash
   pip install -r requirements.txt
   ```

4. Настройте базу данных:
   - Создайте базу данных `news_agg` в PostgreSQL
   - Выполните SQL скрипты из `sql_scripts/`:
     ```bash
     psql -U postgres -d news_agg -f sql_scripts/create_tables.sql
     psql -U postgres -d news_agg -f sql_scripts/create_parsing_tables.sql
     ```

5. Настройте переменные окружения:
   - Скопируйте `.env.example` в `.env`
   - Отредактируйте `.env`

6. Запустите приложение:
   ```bash
   uvicorn main:app --reload --host 0.0.0.0 --port 8000
   ```

7. Откройте документацию API:
   - Swagger UI: http://localhost:8000/docs
   - ReDoc: http://localhost:8000/redoc

## Использование API

### Базовый URL
```
http://localhost:8000/api/v1
```

### Примеры запросов

#### Запуск парсинга последних 3 часов
```bash
curl -X POST "http://localhost:8000/api/v1/parse/last_hours" \
  -H "Content-Type: application/json" \
  -d '{"hours": 3, "sources": ["lenta", "rbc"], "parallel": true}'
```

#### Получение статуса задачи
```bash
curl "http://localhost:8000/api/v1/parse/tasks/{task_id}"
```

#### Создание расписания (каждый час)
```bash
curl -X POST "http://localhost:8000/api/v1/schedule" \
  -H "Content-Type: application/json" \
  -d '{
    "cron_expression": "0 * * * *",
    "sources": ["lenta"],
    "parameters": {"type": "last_hours", "hours": 1},
    "enabled": true
  }'
```

#### Экспорт данных за последние 7 дней
```bash
curl -X POST "http://localhost:8000/api/v1/export" \
  -H "Content-Type: application/json" \
  -d '{
    "start": "2026-04-05T00:00:00Z",
    "end": "2026-04-12T00:00:00Z",
    "sources": ["lenta", "rbc"],
    "format": "csv"
  }'
```

#### Проверка здоровья системы
```bash
curl "http://localhost:8000/api/v1/health"
```

## Структура проекта

```
news_agg/
├── api/                    # FastAPI роутеры
│   ├── __init__.py
│   ├── parsing.py         # API для парсинга новостей
│   ├── schedule.py        # API для управления расписанием
│   └── vector.py          # API для операций с векторами
├── parsers/               # Парсеры новостных сайтов
│   ├── __init__.py
│   ├── base_parser.py     # Базовый класс парсера
│   ├── lenta.py           # Парсер Lenta.ru
│   ├── rbc.py             # Парсер RBC
│   └── ria.py             # Парсер RIA
├── services/              # Бизнес-логика и сервисы
│   ├── __init__.py
│   ├── database.py            # Подключение к PostgreSQL
│   ├── embedding_processor.py # Сервис управления задачами эмбеддингов
│   ├── embedding_service.py   # Сервис эмбеддингов
│   ├── llm_service.py         # Сервис анализа LLM
│   ├── parser_service.py      # Сервис парсинга
│   ├── parsing_service.py     # Сервис управления задачами парсинга
│   ├── parsing_db_saver.py    # Сохранение в БД PostgreSQL результатов парсинга
│   ├── schedule_storage.py    # Хранилище расписаний
│   ├── qdrant_service.py      # Сервис взаимодействия с векторной БД Qdrant
│   ├── scheduler.py           # Планировщик задач
│   └── task_manager.py        # Менеджер задач
├── models/                # Модели данных и схемы
│   ├── __init__.py
│   └── schemas.py         # Pydantic схемы
├── utils/                 # Вспомогательные утилиты
│   ├── __init__.py
│   ├── article_adapter.py # Адаптер статей
│   └── logger.py          # Настройка логирования
├── sql_scripts/           # SQL скрипты создания таблиц и индексов
│   ├── create_news_tables.sql
│   └── create_mange_tables.sql
├── .env.example           # Переменные окружения
├── config.py              # Конфигурация приложения
├── main.py                # Точка входа FastAPI
├── docker-compose.yaml    # Docker Compose конфигурация
├── requirements.txt       # Зависимости Python
└── README.md              # Документация
```

## Лицензия

MIT