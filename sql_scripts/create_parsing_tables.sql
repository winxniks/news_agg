-- Таблица для задач парсинга
DROP TABLE IF EXISTS tasks CASCADE;
CREATE TABLE tasks (
    task_id UUID PRIMARY KEY,
    task_type VARCHAR(20) NOT NULL CHECK (task_type IN ('last_hours', 'interval', 'last_minutes', 'embed')),
    sources TEXT[] NOT NULL,
    parameters JSONB NOT NULL,
    status VARCHAR(20) NOT NULL CHECK (status IN ('pending', 'running', 'completed', 'failed')),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    result JSONB,
    error_message TEXT
);

-- Индексы для таблицы задач
CREATE INDEX idx_tasks_status ON tasks(status);
CREATE INDEX idx_tasks_created_at ON tasks(created_at DESC);
CREATE INDEX idx_tasks_task_type ON tasks(task_type);
CREATE INDEX idx_tasks_finished_at ON tasks(finished_at) WHERE finished_at IS NOT NULL;

-- Таблица для расписаний
DROP TABLE IF EXISTS schedules CASCADE;
CREATE TABLE schedules (
    schedule_id UUID PRIMARY KEY,
    cron_expression VARCHAR(100) NOT NULL,
    sources TEXT[] NOT NULL,
    parameters JSONB NOT NULL,
    enabled BOOLEAN DEFAULT TRUE,
    last_run TIMESTAMPTZ,
    next_run TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Индексы для таблицы расписаний
CREATE INDEX idx_schedules_enabled ON schedules(enabled) WHERE enabled = TRUE;
CREATE INDEX idx_schedules_next_run ON schedules(next_run) WHERE next_run IS NOT NULL;
CREATE INDEX idx_schedules_updated_at ON schedules(updated_at DESC);

-- Триггер для автоматического обновления updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_schedules_updated_at 
    BEFORE UPDATE ON schedules 
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();

-- Комментарии
COMMENT ON TABLE tasks IS 'Задачи парсинга новостей';
COMMENT ON TABLE schedules IS 'Расписания автоматического парсинга';