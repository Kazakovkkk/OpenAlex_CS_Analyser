-- Локальный стенд: гранты из таблицы ТЗ 3.6.
-- Выполняется последним: пользователи (01), рабочая схема (02), нагрузочная
-- таблица (03), затем гранты.

-- etl_writer: обычная загрузка.
GRANT SELECT, INSERT ON openalex.works TO etl_writer;
GRANT SELECT, INSERT ON openalex.dim_topics TO etl_writer;
GRANT SELECT, INSERT ON openalex.etl_checkpoints TO etl_writer;
GRANT ALTER DELETE ON openalex.works TO etl_writer;

-- etl_writer: нагрузочный стенд пишет в отдельную таблицу.
GRANT SELECT, INSERT, TRUNCATE ON openalex.works_loadtest TO etl_writer;

-- pipeline_writer (пароль Go): состояние и подтверждённый перезапуск.
GRANT SELECT, INSERT ON openalex.pipeline_state TO pipeline_writer;
GRANT SELECT, INSERT, TRUNCATE ON openalex.etl_checkpoints TO pipeline_writer;
GRANT TRUNCATE ON openalex.works TO pipeline_writer;
GRANT TRUNCATE ON openalex.dim_topics TO pipeline_writer;
