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

- **Экспорт данных**:
  - Экспорт в CSV или JSON
  - Фильтрация по источникам и датам
  - Сжатие (gzip)

- **Мониторинг**:
  - Health check системы
  - Статус парсеров
  - Статистика

## Установка

### Требования

- Python 3.8+
- PostgreSQL 12+
- Установленные зависимости

### Шаги

1. Клонируйте репозиторий:
   ```bash
   git clone <repository-url>
   cd news_agg
   ```

2. Создайте виртуальное окружение:
   ```bash
   python -m venv venv
   source venv/bin/activate  # Linux/Mac
   venv\Scripts\activate     # Windows
   ```

3. Установите зависимости:
   ```bash
   pip install -r requirements-fastapi.txt
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
   - Отредактируйте `.env` (укажите DATABASE_URL и другие параметры)

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
  }' --output news_export.csv
```

#### Проверка здоровья системы
```bash
curl "http://localhost:8000/api/v1/health"
```

## Структура проекта

```
news_agg/
├── api/endpoints/          # Эндпоинты FastAPI
│   ├── parsing.py          # Парсинг
│   ├── schedule.py         # Расписание
│   ├── export.py           # Экспорт
│   └── monitoring.py       # Мониторинг
├── models/                 # Pydantic схемы
├── services/               # Бизнес-логика
│   ├── parser_service.py   # Управление парсерами
│   ├── task_manager.py     # Хранилище задач
│   ├── parsing_service.py  # Сервис парсинга
│   ├── scheduler.py        # Планировщик
│   ├── export_service.py   # Экспорт данных
│   └── database.py         # Подключение к БД
├── parsers/                # Существующие парсеры
├── savers/                 # Сохранение данных
├── utils/                  # Вспомогательные утилиты
├── sql_scripts/            # SQL скрипты
├── test/                   # Тесты
├── config.py               # Конфигурация
├── main.py                 # Точка входа FastAPI
├── requirements-fastapi.txt
└── README.md
```

## Разработка

### Запуск тестов
```bash
pytest test/
```

### Форматирование кода
```bash
black .
isort .
```

### Линтинг
```bash
flake8
```

## Лицензия

MIT