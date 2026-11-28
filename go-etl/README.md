# go-etl — загрузка OpenAlex в ClickHouse

Go-часть проекта: читает публичный Parquet-снимок OpenAlex (Works + Topics),
нормализует записи, пишет их в ClickHouse и публикует версию набора в
`pipeline_state`.


## Сборка

```bash
CGO_ENABLED=0 go build -o bin/etl ./cmd/etl
```

## Конфигурация

Рабочий конфиг обязателен. Без `--config` процессы не запускаются; диагностический
`NoopSink` включается только явным `--dry-run` или в тестах.

```toml
[clickhouse]
host = "clickhouse"
port = 9000
database = "openalex"
secure = false
pipeline_writer_user = "pipeline_writer"

[go_etl]
base_url = "http://go-etl:8081"
etl_writer_user = "etl_writer"
etl_writer_password_env = "CLICKHOUSE_ETL_WRITER_PASSWORD"
go_pipeline_writer_password_env = "CLICKHOUSE_GO_WRITER_PASSWORD"
# test_target = true   # разрешает ограниченным прогонам писать в эту базу
```

Паролей в файле нет: только имена переменных. Значения приходят из окружения и
никогда не попадают в ответы и журналы. Для VPS — нативный TCP с TLS и проверкой
сертификата (`secure = true`).

## Команды

```bash
# HTTP-сервис для Python/TUI
etl serve --config config/app.toml --addr :8081

# Штатная загрузка/обновление выпуска Works + Topics
etl ingest --config config/app.toml --source s3

# Только справочник (рабочее обновление с повышением версии)
etl sync-dims --config config/app.toml --force-reload

# Диагностика: без ClickHouse, свой журнал прогресса
etl ingest --config config/app.toml --source s3 --max-files 2 --dry-run

# Нагрузочный тест в отдельную таблицу works_loadtest
etl loadtest --config config/app.toml --duration 30 --target-rps 20000
```

Ограниченные прогоны (`--max-files`, `--max-records`, `--from-date`, отдельный
шард) не трогают `pipeline_state` и `etl_checkpoints`; при ClickHouse-приёмнике
нужен `test_target = true`, иначе запуск отклоняется.

## Поведение загрузки

- **Release ID** — SHA-256 от типизированного списка манифестов Works+Topics
  (сущности по имени, копия файлов по URL). Один и тот же ID используется при
  старте, возобновлении, проверке перед публикацией и подтверждении перезапуска.
- **Первая загрузка / подтверждённый перезапуск** — все файлы Works и Topics.
- **Инкремент** — разделы Works с `updated_date` позже опубликованной даты;
  разово при переходе со снимка `2026-10-14` включается и этот день. Topics
  читаются полностью для нового выпуска, `deleted_ids.csv.gz` обрабатывается
  всегда, даже если новых Works-файлов нет.
- **Один писатель** — воркеры читают и валидируют файлы и передают батчи через
  ограниченный канал; единственная горутина-писатель делает INSERT и ставит
  чекпоинты только после подтверждения ClickHouse. Дедупликация — по
  `work_id + source_updated_at + содержимое строки`; конфликт содержимого даёт
  `corpus_incomplete`. Аналитика читает `works` через `FINAL`.
- **Stop** — прекращает выдачу файлов, дописывает уже прочитанные порции и
  хвост батча под отдельным контекстом с пределом 60 с. Незавершённый файл при
  следующем запуске читается с начала.

## Состояние и публикация

`pipeline_state` хранит полную строку на каждую ревизию. Колонка
`load_phase`: `none | resetting | full | incremental`.

- новая загрузка: `loading/full|incremental` под управляющей сессией, затем
  запись и `idle/none` с `data_version + 1`;
- возобновление: та же фаза, дата и Release ID, продолжение по чекпоинтам;
- подтверждённый перезапуск: `loading/resetting` → очистка `works`, `dim_topics`,
  `etl_checkpoints` → `loading/full` → полная загрузка;
- каждый новый проверенный снимок повышает `data_version` ровно на 1, даже без
  вставок и удалений; уже опубликованный снимок версию не меняет.

Управляющий пользователь `pipeline_writer` держит **один физический слот**:
переход целиком идёт на закреплённом `database/sql`-соединении и закрывается
сразу после. Простаивающий `serve` слот не занимает. Go и Python используют один
логин с разными паролями; на VPS и локальном стенде действует
`max_sessions_for_user=1`.

## HTTP API

| Метод | Путь | Ответ |
|---|---|---|
| GET | `/health` | `{"status":"ok","instance_id":"…"}` |
| POST | `/api/v1/ingest/start` | `202` с `job_id`, `instance_id`, `state`; повтор — тот же job |
| GET | `/api/v1/jobs/{job_id}` | статус или `404` |
| POST | `/api/v1/jobs/{job_id}/stop` | `{"stopped":true|false}` |
| POST | `/api/v1/ingest/restart` | `202`; неизвестный/просроченный токен — `409 restart_offer_expired` |
| POST | `/api/v1/dimensions/sync` | `202`; общий write-slot с ingest |
| POST | `/api/v1/loadtest/start` | `202`, пишет в `works_loadtest` |
| POST | `/api/v1/openalex/request-key` | статус запроса; ключ остаётся внутри Go |

Конкурирующая операция записи получает `409 session_busy`. Ошибки содержат
`code`, `message` и совместимое `error`. `snapshot_changed` передаёт
`previous_snapshot_date`, `available_snapshot_date`, `target_database` и
`restart_token` (TTL 30 минут, только в памяти процесса).

REST-источник (`source: api`) доступен только в ручном CLI-режиме на выделенной
цели; HTTP его отклоняет, чтобы не менять рабочий корпус.

## Локальный стенд и тесты

```bash
docker compose -f local/docker-compose.yml up -d --wait
export CLICKHOUSE_ETL_WRITER_PASSWORD=dev-etl-writer-password
export CLICKHOUSE_GO_WRITER_PASSWORD=dev-go-writer-password

go test ./...
go test -tags=integration -p 1 ./internal/sink   # живой ClickHouse
go test -tags=integration -run TestLocalIngestPublishesWorksAndTopics ./internal/app
```

Локальный стенд повторяет VPS: схема с `load_phase`, два пароля
`pipeline_writer` на один логин, профиль `pipeline_limited` с
`max_sessions_for_user=1`. Второй одновременный нативный сеанс отклоняется
(`code 700`). Пересоздать чисто: `docker compose … down -v`.

## Docker

```bash
docker build -t go-etl:2026.11.28 .
docker image save -o dist/go-etl-2026.11.28.tar go-etl:2026.11.28
```

Образ: `linux/amd64`, non-root, порт `8081`, команда
`serve --config /app/config/app.toml`. Compose монтирует конфиг в
`/app/config/app.toml:ro` и задаёт `CLICKHOUSE_ETL_WRITER_PASSWORD` и
`CLICKHOUSE_GO_WRITER_PASSWORD`.

## Коды ошибок

`manifest_unavailable`, `disk_full`, `clickhouse_unavailable`, `session_busy`,
`auth_failed`, `access_denied`, `corpus_incomplete`, `snapshot_changed`,
`restart_offer_expired`, `plan_required`, `api_key_required`, `rate_limited`.

## Структура

| Пакет | Ответственность |
|---|---|
| `internal/adapters/s3` | манифесты, Range/Full GET, Parquet-чтение, `deleted_ids` |
| `internal/adapters/api` | OpenAlex REST, курсор, лимиты, запрос ключа |
| `internal/normalizer` | контракт валидации и нормализация Works/Topics |
| `internal/ingest` | воркеры, канал порций, единственный писатель, чекпоинты |
| `internal/sink` | `BatchSink`, `NoopSink`, `ClickHouseSink`, батчи |
| `internal/clickhouse` | соединения, TLS, классификация ошибок |
| `internal/pipeline` | управляющий слот, `pipeline_state`, очистки, токены |
| `internal/jobs` | задачи, write-slot, состояния, статусы |
| `internal/httpapi` | HTTP-контракт |
| `internal/app` | release/plan, операции, сборка |
