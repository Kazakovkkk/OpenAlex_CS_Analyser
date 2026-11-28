# Проверка VPS: таблицы и права

> ⚠️ В файле есть пароль admin-пользователя. `docs/` сейчас в `.gitignore`, поэтому в репозиторий он не попадёт — но не пересылай файл дальше и не коммить. После сдачи пароль стоит перевыпустить.
>
> Контейнер: `23ba268ec622`. Пользователь `bigdata_admin` настроен `HOST LOCAL`, поэтому команды выполняются через `docker exec` (клиент внутри контейнера, источник `127.0.0.1`).

## 1. Где на самом деле лежат таблицы

```bash
docker exec 23ba268ec622 clickhouse-client --user bigdata_admin --password '<ADMIN_PASSWORD>' \
  -q "SELECT database, name FROM system.tables WHERE name IN ('pipeline_state','works_loadtest','cache_period','cache_task3_series','cache_task3_bursts','cache_report_topics') ORDER BY database, name"
```

Если вывод пустой — схема не применялась или легла в другой инстанс. Тогда шаг 4.

## 2. Что видно в базе `openalex`

```bash
docker exec 23ba268ec622 clickhouse-client --user bigdata_admin --password '<ADMIN_PASSWORD>' \
  -q "SHOW TABLES FROM openalex"
```

Снаружи под `etl_writer` сейчас видно только три таблицы:

```
dim_topics        ReplacingMergeTree   rows=0
etl_checkpoints   ReplacingMergeTree   rows=0
works             ReplacingMergeTree   rows=0
```

Должно быть **восемь** (это вся общая схема): `works`, `dim_topics`, `cache_period`, `cache_task3_series`, `cache_task3_bursts`, `cache_report_topics`, `pipeline_state`, `etl_checkpoints`. Девятая, `works_loadtest`, лежит отдельно в `schema_loadtest.sql` и нужна только для нагрузочных тестов.

Важно: `system.tables` фильтруется по правам пользователя — под `etl_writer` видно только таблицы, на которые у него есть гранты, поэтому такой проверкой отсутствие таблиц не доказать.

## 3. Права пользователей

```bash
docker exec 23ba268ec622 clickhouse-client --user bigdata_admin --password '<ADMIN_PASSWORD>' \
  -q "SHOW GRANTS FOR pipeline_writer"

docker exec 23ba268ec622 clickhouse-client --user bigdata_admin --password '<ADMIN_PASSWORD>' \
  -q "SHOW GRANTS FOR etl_writer"
```

Что должно быть:

| Пользователь | Права | Зачем |
|---|---|---|
| `etl_writer` | `INSERT` на `works`, `dim_topics` | запись данных и справочника |
| `etl_writer` | `SELECT, INSERT` на `etl_checkpoints` | чекпоинты в ClickHouse |
| `etl_writer` | `ALTER DELETE` на `works` | удаление работ из `deleted_ids` |
| `etl_writer` | `SELECT(work_id)` на `works` | индекс известных ID |
| `pipeline_writer` | `SELECT, INSERT` на `pipeline_state` | чтение состояния и публикация версии |
| `pipeline_writer` | `TRUNCATE` на `etl_checkpoints` | очистка леджера при старте новой загрузки |
| `pipeline_writer` | `TRUNCATE` на `works`, `dim_topics` | очистка при подтверждённом перезапуске со сменой снимка |

Если у `pipeline_writer` нет `TRUNCATE`/`INSERT` — первая же загрузка упадёт на `BeginLoad` или `Publish`.

Необязательно, для мониторинга нагрузки из Go:

```sql
GRANT SELECT ON system.parts TO etl_writer;
GRANT SELECT ON system.mutations TO etl_writer;
```

## 4. Применить схему (если таблиц нет)

```bash
# из каталога, где лежит schema.sql (корень go-etl)
docker exec -i 23ba268ec622 clickhouse-client --user bigdata_admin --password '<ADMIN_PASSWORD>' \
  --multiquery < schema.sql
```

Если файл не рядом:

```bash
docker cp schema.sql 23ba268ec622:/tmp/schema.sql
docker exec 23ba268ec622 clickhouse-client --user bigdata_admin --password '<ADMIN_PASSWORD>' \
  --multiquery < /tmp/schema.sql
```

Файл идемпотентный: `CREATE TABLE IF NOT EXISTS` + `INSERT ... WHERE NOT EXISTS`, повторный запуск безопасен.

## 5. Что прислать обратно

1. вывод шага 1 (или 2) — куда легла схема;
2. вывод `SHOW GRANTS FOR pipeline_writer`;
3. вывод `SHOW GRANTS FOR etl_writer`, если он отличается от таблицы выше.

Этого достаточно, чтобы снять вопрос по VPS и перейти к финальной загрузке.

