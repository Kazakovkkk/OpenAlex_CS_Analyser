-- Общая схема MVP для Go и Python. Выполнять при подготовке базы openalex.

-- Локальный стенд применяет файл автоматически (см. local/docker-compose.yml).
-- Проверка на ClickHouse 26.9: go test -tags=integration ./internal/sink
-- (раздел README «ClickHouse: sink, конфиг и локальный стенд»).

CREATE DATABASE IF NOT EXISTS openalex;
USE openalex;

CREATE TABLE IF NOT EXISTS works
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
    has_fwci Bool,
    CONSTRAINT valid_work_id CHECK work_id > 0,
    CONSTRAINT valid_publication_year CHECK publication_year > 0,
    CONSTRAINT matching_topic_arrays CHECK length(topic_ids) = length(topic_scores)
)
ENGINE = ReplacingMergeTree(source_updated_at)
ORDER BY work_id;

CREATE TABLE IF NOT EXISTS dim_topics
(
    topic_id UInt32,
    display_name String,
    subfield_id UInt16,
    subfield_name LowCardinality(String)
)
ENGINE = ReplacingMergeTree()
ORDER BY topic_id;

CREATE TABLE IF NOT EXISTS cache_period
(
    year UInt16,
    quarter UInt8,
    category_type Enum8('cs' = 0, 'topic' = 1, 'subfield' = 2),
    category_id UInt32,
    weight Float64,
    share_percent Float64,
    CONSTRAINT valid_period CHECK year > 0 AND quarter <= 4,
    CONSTRAINT valid_period_category CHECK (category_type = 'cs') = (category_id = 0)
)
ENGINE = MergeTree()
ORDER BY (year, quarter, category_type, category_id);

CREATE TABLE IF NOT EXISTS cache_task3_series
(
    category_type Enum8('topic' = 1, 'subfield' = 2),
    category_id UInt32,
    background_share Float64
)
ENGINE = MergeTree()
ORDER BY (category_type, category_id);

CREATE TABLE IF NOT EXISTS cache_task3_bursts
(
    category_type Enum8('topic' = 1, 'subfield' = 2),
    category_id UInt32,
    burst_id UInt32,
    start_year UInt16,
    start_quarter UInt8,
    end_year UInt16,
    end_quarter UInt8,
    peak_year UInt16,
    peak_quarter UInt8,
    peak_share Float64,
    peak_weight Float64,
    pre_burst_share Float64,
    venue_hhi Float64,
    venue_weight Float64,
    venue_coverage Float64,
    fwci_weighted_median Float64,
    fwci_known_weight Float64,
    fwci_total_weight Float64,
    CONSTRAINT valid_burst_quarters CHECK start_quarter BETWEEN 1 AND 4
        AND end_quarter BETWEEN 1 AND 4 AND peak_quarter BETWEEN 1 AND 4,
    CONSTRAINT peak_inside_burst CHECK (start_year, start_quarter) <= (peak_year, peak_quarter)
        AND (peak_year, peak_quarter) <= (end_year, end_quarter)
)
ENGINE = MergeTree()
ORDER BY (category_type, category_id, burst_id);

CREATE TABLE IF NOT EXISTS cache_report_topics
(
    topic_id UInt32,
    display_name String,
    subfield_id UInt16,
    subfield_name LowCardinality(String)
)
ENGINE = MergeTree()
ORDER BY topic_id;

CREATE TABLE IF NOT EXISTS pipeline_state
(
    id UInt8,
    state_revision UInt64,
    snapshot_date Nullable(Date),
    data_updated_at Nullable(DateTime64(6, 'UTC')),
    data_version UInt64,
    load_state Enum8('idle' = 0, 'loading' = 1),
    loading_snapshot_date Nullable(Date),
    loading_manifest_id String,
    instance_id String,
    job_id String,
    analysis_data_version UInt64,
    analysis_snapshot_date Nullable(Date),
    analysis_completed_at Nullable(DateTime64(6, 'UTC')),
    load_phase Enum8('none' = 0, 'resetting' = 1, 'full' = 2, 'incremental' = 3) DEFAULT 'none',
    CONSTRAINT singleton_state CHECK id = 1
)
ENGINE = MergeTree()
ORDER BY (id, state_revision);

-- Инициализатор запускается администратором до старта Go и Python.
INSERT INTO pipeline_state
(
    id, state_revision, snapshot_date, data_updated_at, data_version, load_state,
    loading_snapshot_date, loading_manifest_id, instance_id, job_id,
    analysis_data_version, analysis_snapshot_date, analysis_completed_at
)
SELECT 1, 0, NULL, NULL, 0, 'idle', NULL, '', '', '', 0, NULL, NULL
WHERE NOT EXISTS (SELECT 1 FROM pipeline_state WHERE id = 1);

-- Актуальная логическая строка:
-- SELECT * FROM pipeline_state WHERE id = 1 ORDER BY state_revision DESC LIMIT 1
-- Публикация — синхронный INSERT полной строки с ревизией +1 под pipeline_writer.

CREATE TABLE IF NOT EXISTS etl_checkpoints
(
    entity LowCardinality(String),
    object_key String,
    completed_at DateTime64(6, 'UTC')
)
ENGINE = ReplacingMergeTree(completed_at)
ORDER BY (entity, object_key);
