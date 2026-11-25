## Сборка

```bash
CGO_ENABLED=0 go build -o bin/etl ./cmd/etl
```

## Команды

```bash
# HTTP API для python-tui (порт :8081 по контракту ТЗ)
etl serve --addr :8081 --sample tmp/works_sample_12k.jsonl.gz --checkpoint-dir tmp/checkpoints

# Заливка снапшота из S3 (шарды + чекпоинт; повторный запуск продолжит с места обрыва)
etl ingest --source s3 --workers 4 --batch-size 10000 \
           --shard-index 0 --shard-total 4 --checkpoint-dir tmp/checkpoints

# Демо на нескольких файлах
etl ingest --source s3 --max-files 5 --workers 2

# Инкрементальная догрузка через REST API OpenAlex
etl ingest --source api --api-key "$OPENALEX_API_KEY" --rps-limit 20 \
           --from-date 2026-10-15 --date-filter-mode updated --max-records 100000

# Справочники
etl sync-dims --force-reload

# Нагрузочный тест (без сети): кольцо из реальных записей
etl loadtest --target-rps 20000 --duration 30 --workers 4 --batch-size 10000

# CPU-профиль (Метрика №1)
etl loadtest --duration 6 --cpuprofile tmp/prof/lt.prof
go tool pprof -top bin/etl tmp/prof/lt.prof
```

Порог `Full GET ↔ Range` зависит от канала, поэтому вынесен в флаг:
`etl ingest --full-get-threshold-mb 8` (по умолчанию — значение библиотеки, 16 MiB).

## API-ключ OpenAlex

### Куда положить ключ

Приоритет (первое найденное побеждает): **флаг `--api-key` → переменная окружения
`OPENALEX_API_KEY` → файл `.env` → значение из конфига `serve`**. Реальное окружение
всегда сильнее `.env`, поэтому `export`/systemd не перебиваются файлом.

`.env` (локально).** В репозитории `.env.example`;


# сквозная проверка пагинации и расхода квоты (2 страницы):
go test -v -count=1 -tags=inspect ./internal/adapters/api -run TestDiagRealAPI
```

Если `serve` поднят с `OPENALEX_API_KEY` в окружении, то запрос
`POST /api/v1/ingest/start` **без** поля `api_key` возьмёт ключ из окружения; если TUI
передаёт `"api_key"` в теле — приоритет у него.


### Что проверено на живом API

* Без ключа работает только первая страница (`cursor=*`). Вторая страница отвечает
  `401 {"error":"API key required for cursor paging"}` — клиент ловит это и **до запроса**
  возвращает `ErrAPIKeyRequired` (лишний запрос не тратится).
* **Бесплатный тир не умеет инкрементальную синхронизацию по датам**: `from_updated_date`,
  `from_created_date`, `sort=created_date:desc` и `sort=updated_date:desc` отвечают
  `403 {"error":"Plan upgrade required"}` (это «daily sync», планы Member+/Partner).
  Доступны `from_publication_date` и `publication_year`.
  Поэтому в запрос добавлен **аддитивный** режим `date_filter_mode=publication`
  (значения `updated`/`created` остались и дают понятную ошибку `ErrPlanRequired`):

  ```powershell
  .\bin\etl.exe ingest --source api --date-filter-mode publication \
      --from-date 2026-10-01 --max-records 500 --rps-limit 10
  # → pages=5 records=500 cost_usd=0.0005 bytes=6684597
  ```

* `select` (projection pushdown для API) включён по умолчанию: запрашиваются только
  поля, которые читает нормализатор (`api.DefaultWorksSelect`). Поле `authors_count`
  в `select` **недопустимо** — нормализатор считает его как `len(authorships)`.
* Расход квоты: API сам отдаёт `meta.cost_usd`; мы суммируем в `Stats.CostUSD` и пишем в
  лог. Замер: **$0.10 на 100 000 записей**, ~13.4 KB трафика на запись.
* Пустой/битый email → `400 {"code":"email_required"}`.
* Эндпоинт ограничен **по сети**: `429 {"code":"rate_limit_network","message":"This network
  has asked for keys too often today..."}` — поэтому клиент делает ровно одну попытку и не ретраит.
* Без ключа квота общая на IP и составляет **$0.10/день** (из `meta.notice`), с ключом —
  собственные **$1/день** (≈1 млн записей), то есть API упирается в квоту, а не в сеть.
* Фильтр `primary_topic.field.id:17` на живом API даёт **12 074 740** работ (Computer Science).


## Семантика полей `/api/v1/ingest/start`

| Поле | `source=s3` | `source=api` |
|---|---|---|
| `workers` | число файлов параллельно | не используется (один поток страниц) |
| `batch_size` | строк в батче | то же |
| `shard_index` / `shard_total` | выборка файлов `i % total == index` | — |
| `max_files` | ограничение числа файлов (после фильтра даты) | — |
| `max_records` | **лимит на джобу**: останавливает подачу файлов и чтение внутри файла (проверка после каждой добавленной строки) | лимит на джобу |
| `from_date` | фильтр по `updated_date=` партиции: `>= from_date` (проверено: 587 файлов из 1898 для `2026-10-01`) | `from_*_date` фильтр |
| `date_filter_mode` | игнорируется (снапшот уже разложен по датам) | `updated`/`created` → 403 на free-tier; `publication` → `from_publication_date` |
| `rps_limit` | — | токен-бакет |
| `api_key` | — | ключ (иначе `OPENALEX_API_KEY` / `.env`) |

Чекпоинт называется по **выборке**, а не по id джобы:
`tmp/checkpoints/works-<source>-shard<i>-of<n>-max<k>.jsonl`. Повторный запуск с теми же
параметрами продолжит с места обрыва; если всё уже сделано, джоба **честно падает** с
текстом «all N selected file(s) were already processed…», а не рапортует «completed»
с нулями (этот баг поймал живой прогон через HTTP).

## HTTP API (`:8081`)

| Метод | Путь | Ответ |
|---|---|---|
| GET | `/health` | `{"status":"ok"}` |
| POST | `/api/v1/dimensions/sync` | `202 {"job_id":"dim-1","state":"running"}` |
| POST | `/api/v1/ingest/start` | `202 {"job_id":"ing-1","state":"running"}` |
| POST | `/api/v1/loadtest/start` | `202 {"job_id":"lt-1","state":"running"}` |
| GET | `/api/v1/jobs/{job_id}` | `200` снапшот джобы (см. ТЗ) |
| POST | `/api/v1/jobs/{job_id}/stop` | `200 {"stopped":true}` |
| POST | `/api/v1/openalex/request-key` | `200 {"requested":true,"message":"..."}` |

Джобы живут в менеджере (`internal/jobs`): не более 4 одновременно, состояния
`running/completed/stopped/failed`, отмена через `context`, gracefully — с
дописыванием текущего батча. Контекст джобы **не** привязан к HTTP-запросу.

## Пакеты

| Пакет | Ответственность |
|---|---|
| `internal/adapters/s3` | манифесты, `ReaderAt` поверх Range-запросов, префетч по футеру, чтение works/topics/institutions |
| `internal/adapters/api` | OpenAlex REST: курсор, token bucket, ретраи 429/5xx, `POST /api-keys` |
| `internal/normalizer` | projection-модель, фильтр IT, $O(N)$ абстракт, дедуп (roaring64) |
| `internal/sink` | `BatchSink`, `NoopSink`, SoA-батчи и пулы |
| `internal/metrics` | атомарные счётчики, p50/p95/p99 латентности батчей |
| `internal/ingest` | воркер-пул по файлам, шарды, чекпоинт, graceful shutdown |
| `internal/jobs` | менеджер джоб и снапшоты для TUI |
| `internal/loadtest` | кольцевой генератор на реальных записях |
| `internal/httpapi` | 7 эндпоинтов контракта |
| `internal/app` | сборка адаптеров и операций (Backend для HTTP/CLI) |

## Тесты и бенчмарки

```bash
go test ./...                     # юнит-тесты
go test -race ./...               
go test -run '^$' -bench . -benchmem ./internal/normalizer

# измерения на реальном S3
go test -v -count=1 -tags=inspect ./internal/adapters/s3 -run TestDiagRowGroupLayout
go test -v -count=1 -tags=inspect ./internal/adapters/s3 -run TestBenchPartitionPairedFormats
```

Результаты всех замеров — в [docs/stage1-benchmarks.md](docs/stage1-benchmarks.md).
