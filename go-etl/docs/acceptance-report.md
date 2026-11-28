# Отчёт приёмки (черновик, 28.11.2026)

Полный контракт — `docs/go-etl-quality-review.md`. Отчёт заполняется по мере
прохождения шага 7.

## Среда

| Компонент | Значение |
|---|---|
| Go | 1.27.1, `CGO_ENABLED=0` |
| Локальный ClickHouse | 26.9.41.4 (Docker, `local/docker-compose.yml`) |
| VPS ClickHouse | 26.9.40.9, `168.113.253.42:9440`, TLS |
| Образ | `go-etl:2026.11.28`, linux/amd64, 56.5 МБ, архив `dist/go-etl-2026.11.28.tar` |

## Локальный стенд

Пересоздан с нуля. Схема содержит `load_phase`; у `pipeline_writer` два пароля
(Go/Python) на один логин и профиль `pipeline_limited` с
`max_sessions_for_user=1`.

- Второй одновременный нативный сеанс отклоняется: `code 700 overflown
  session count 1` (проверено и для Go-, и для Python-пароля).
- Единый e2e-тест `TestLocalIngestPublishesWorksAndTopics` (тег `integration`)
  поднимает фиктивный S3-выпуск (Works + Topics Parquet, два манифеста),
  загружает его через боевой `app` и проверяет:
  `works=2`, `dim_topics=1`, `etl_checkpoints=2`, `pipeline_state` revision 2,
  `idle/none`, `data_version=1`, `snapshot_date=2026-11-07`.

## Проверки кода

- `go build ./...` — ок.
- `go vet ./...` — ок.
- `go test ./...` — ок.
- Юнит-тесты писателя: объединение файлов в один батч, дедупликация
  `work_id+версия+содержимое`, `corpus_incomplete` при конфликте, дописывание
  хвоста при Stop.
- Тесты Release/Plan: воспроизводимость SHA-256, независимость от порядка
  файлов, full/incremental, правило `2026-10-14`, «нет новых Works — не конец».
- `TestPrefetchMemoryStaysWithinBudget` исправлен (окна снимаются под мьютексом,
  ожидается `ready` каждого окна); `go test -race` с mingw gcc:
  `-run TestPrefetchMemoryStaysWithinBudget -count=10` — ок,
  `go test -race ./...` — ок (все пакеты).

## Docker

Registry Docker Hub недоступен из-за прокси, поэтому финальный образ собран из
локального базового `go-etl:mvp` и Linux-бинарника (эквивалент runtime-слоя).
Мультистейдж `Dockerfile` остаётся для сборки в окружении с доступом к registry.

- `docker run … go-etl:2026.11.28` → `/health` отвечает
  `{"status":"ok","instance_id":"…"}` на локальном ClickHouse.
- `docker image save` → `dist/go-etl-2026.11.28.tar` (57.4 МБ).

## Нагрузочный стенд

`etl loadtest --config config/app.local.toml --duration 10 --target-rps 5000
--workers 4 --batch-size 5000` на локальном ClickHouse:

- записано 70 000 строк в `works_loadtest` (14 батчей), таблица подтверждена
  запросом `count() = 70000`;
- фактическая скорость 7 329 строк/с, p50 = 71.8 мс, p95 = 99.0 мс,
  p99 = 99.0 мс.

## VPS

Исходное состояние до загрузки: `pipeline_state` revision 0, `idle`,
`load_phase=none`, `works=0`, `etl_checkpoints=0`; лимит 1 действует.

Полный прогон `etl ingest --config config/app.vps.toml --source s3`:
снимок `2026-10-14`, Release ID `7cc0c25e…`, 1898 Works-файлов и 6 Topics-файлов.

Статус: **завершено** 28.11.2026. Итог прогона:

| Показатель | Значение |
|---|---|
| Release ID | `7cc0c25ec43994ee5afa9992a2896526b916577184021d787d301e7571159b68` |
| Снимок | `2026-10-14` |
| Works в базе | `12 589 941` (уникальные `work_id`, дублей нет) |
| Topics в базе | `4 516` (`dim_topics`, по счётчику публикации) |
| Чекпоинты | `1 904` = 1898 Works + 6 Topics |
| `pipeline_state` | revision 4, `idle`, `load_phase=none`, `data_version=1` |
| Сканировано / записано в этом прогоне | 443 815 580 / 11 673 152 (11 668 636 Works + 4 516 Topics) |
| Отбраковано | 39 487 (`bad_year` 37 153, `bad_is_retracted` 2 334) |
| Батчей | 1 168 |
| Прочитано | 14 268 025 573 байт, 94 670 HTTP-запросов |
| Время загрузки | 3 332.6 с (≈55.5 мин), с учётом 125 файлов, пропущенных по чекпоинтам после перезапуска |
| Память процесса | ≈325–420 МБ RSS |

Слот `pipeline_writer` освобождён после завершения; состояние доступно для
пересчёта Python со своим паролем.

Отдельно зафиксировано: `SHOW GRANTS FOR pipeline_writer` показывает
`SELECT, TRUNCATE ON openalex.dim_topics`, но фактический
`SELECT count(topic_id) FROM openalex.dim_topics` под этим пользователем
возвращает `code 497` (нет SELECT). Число тем 4 516 взято из счётчика загрузки
(`topics_written`) и подтверждено публикацией словаря. Перед отчётом Python
нужно проверить/перевыдать этот грант на VPS.

## Ограничения

- Docker Hub недоступен из-за прокси, поэтому стандартный мультистейдж
  `Dockerfile` не собирался в этом окружении; финальный образ собран из
  локального `go-etl:mvp` и Linux-бинарника. Для сборки у Python-разработчика
  мультистейдж остаётся штатным путём.
