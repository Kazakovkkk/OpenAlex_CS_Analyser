-- Нагрузочная таблица (не входит в рабочую схему MVP).
--
-- Нужна для замеров записи и слияний на выделенной площадке: нагрузочный тест
-- пишет сюда и не меняет состояние рабочего корпуса (pipeline_state, works).
-- Типы столбцов совместимы с openalex.works, движок MergeTree, сортировка по
-- work_id — как в рабочей таблице, чтобы число партов и слияния были
-- сопоставимы.


CREATE DATABASE IF NOT EXISTS openalex;
USE openalex;

CREATE TABLE IF NOT EXISTS works_loadtest
(
    work_id UInt64,
    source_updated_at DateTime64(6, 'UTC'),
    type LowCardinality(String),
    primary_field_id UInt16,
    is_retracted Bool,
    publication_year UInt16,
    publication_date UInt32,
    primary_topic_id UInt32,
    topic_ids Array(UInt32),
    topic_scores Array(Float32),
    primary_source_id UInt64,
    primary_source_type LowCardinality(String),
    fwci Float64,
    has_fwci Bool
)
ENGINE = MergeTree()
ORDER BY work_id;
