-- Миграция для добавления типа задачи 'embed' в таблицу задач
-- Поддерживает две возможные таблицы: parsing.parsing_tasks и public.tasks

-- 1. Для таблицы parsing.parsing_tasks (если существует)
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'parsing' AND table_name = 'parsing_tasks'
    ) THEN
        -- Удаляем старое ограничение
        ALTER TABLE parsing.parsing_tasks DROP CONSTRAINT IF EXISTS parsing_tasks_task_type_check;
        -- Добавляем новое ограничение с типом 'embed'
        ALTER TABLE parsing.parsing_tasks ADD CONSTRAINT parsing_tasks_task_type_check 
            CHECK (task_type IN ('last_hours', 'interval', 'last_minutes', 'embed'));
        
        RAISE NOTICE 'Ограничение обновлено для parsing.parsing_tasks';
    END IF;
END $$;

-- 2. Для таблицы public.tasks (если существует)
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'public' AND table_name = 'tasks'
    ) THEN
        -- Удаляем старое ограничение
        ALTER TABLE public.tasks DROP CONSTRAINT IF EXISTS tasks_task_type_check;
        -- Добавляем новое ограничение с типом 'embed'
        ALTER TABLE public.tasks ADD CONSTRAINT tasks_task_type_check 
            CHECK (task_type IN ('last_hours', 'interval', 'last_minutes', 'embed'));
        
        RAISE NOTICE 'Ограничение обновлено для public.tasks';
    END IF;
END $$;

-- 3. Для таблицы tasks без схемы (устаревший вариант)
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_name = 'tasks' AND table_schema NOT IN ('public', 'parsing')
    ) THEN
        -- Динамически определяем схему
        EXECUTE (
            SELECT format(
                'ALTER TABLE %I.tasks DROP CONSTRAINT IF EXISTS tasks_task_type_check',
                table_schema
            )
            FROM information_schema.tables 
            WHERE table_name = 'tasks' AND table_schema NOT IN ('public', 'parsing')
            LIMIT 1
        );
        EXECUTE (
            SELECT format(
                'ALTER TABLE %I.tasks ADD CONSTRAINT tasks_task_type_check 
                 CHECK (task_type IN (''last_hours'', ''interval'', ''last_minutes'', ''embed''))',
                table_schema
            )
            FROM information_schema.tables 
            WHERE table_name = 'tasks' AND table_schema NOT IN ('public', 'parsing')
            LIMIT 1
        );
        
        RAISE NOTICE 'Ограничение обновлено для нестандартной схемы tasks';
    END IF;
END $$;

-- Комментарий для разработчиков
COMMENT ON COLUMN parsing.parsing_tasks.task_type IS 'Тип задачи: last_hours, interval, last_minutes, embed';
COMMENT ON COLUMN public.tasks.task_type IS 'Тип задачи: last_hours, interval, last_minutes, embed';