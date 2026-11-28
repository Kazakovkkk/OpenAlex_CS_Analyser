# Проверка готовности VPS (read-only, 28.11.2026)

Стенд: `168.113.253.42:9440`, нативный TCP + TLS с проверкой сертификата, база `openalex`, ClickHouse `26.9.40.9`.
Метод: временная программа `tmp/vpsprobe` (в git не входит), только `SELECT`/`SHOW GRANTS`; пароли брались из окружения и в этот файл не попадают.

## Состояние `pipeline_state`

Колонки фактической таблицы:

```
id, state_revision, snapshot_date, data_updated_at, data_version, load_state,
loading_snapshot_date, loading_manifest_id, instance_id, job_id,
analysis_data_version, analysis_snapshot_date, analysis_completed_at, load_phase
```

Текущая строка: `state_revision=0`, `load_state=idle`, `data_version=0`, `snapshot_date=NULL`,
`loading_manifest_id=""`, `load_phase=none`, `analysis_data_version=0`.

Вывод: колонка `load_phase` на VPS уже есть и соответствует контракту ТЗ (шаг 5); база пуста и готова к первой полной загрузке.

## Корпус и чекпоинты (`etl_writer`)

| Проверка | Значение |
|---|---|
| `SELECT count() FROM works` | 0 |
| `SELECT count(work_id) FROM works` | 0 |
| `SELECT count() FROM etl_checkpoints` | 0 |

## Гранты

`pipeline_writer`:

```
GRANT SELECT, INSERT, TRUNCATE ON openalex.cache_period
GRANT SELECT, INSERT, TRUNCATE ON openalex.cache_report_topics
GRANT SELECT, INSERT, TRUNCATE ON openalex.cache_task3_bursts
GRANT SELECT, INSERT, TRUNCATE ON openalex.cache_task3_series
GRANT SELECT, TRUNCATE ON openalex.dim_topics
GRANT TRUNCATE ON openalex.etl_checkpoints
GRANT SELECT, INSERT ON openalex.pipeline_state
GRANT SELECT, TRUNCATE ON openalex.works
```

`etl_writer`:

```
GRANT INSERT ON openalex.dim_topics
GRANT SELECT, INSERT ON openalex.etl_checkpoints
GRANT INSERT, ALTER DELETE, SELECT(work_id) ON openalex.works
```

Вывод: роли разведены по ТЗ (1.3): `pipeline_writer` не может вставлять в `works`/`dim_topics`, `etl_writer` не может писать состояние; чтение `work_id` для индекса известных ID разрешено.

## Лимит одновременных сеансов

Проверка: удерживается первое нативное соединение (`db.Conn` + `SELECT 1`), затем открывается второе.

```
first session: ok
second conn: code: 700, message: User ... has overflown session count 1
```

Вывод: `max_sessions_for_user=1` на VPS действует, второй сеанс отклоняется. Это же поведение должен воспроизводить локальный стенд 26.9 (шаг 1.3).

## Следствие для приёмки (шаг 7.4)

- VPS можно принимать «с нуля»: `idle`, `data_version=0`, `works=0`, `load_phase=none`.
- После полной загрузки ожидаем: заполненные `works` и `dim_topics`, `pipeline_state` с датой снимка, `data_version=1`, `idle`, `load_phase=none`.
- Пароли, использованные для проверки, были переданы в переписке — после сдачи их нужно перевыпустить.
