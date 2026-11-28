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
  ожидается `ready` каждого окна); прогон `-count=10` проходит.

## Docker

Registry Docker Hub недоступен из-за прокси, поэтому финальный образ собран из
локального базового `go-etl:mvp` и Linux-бинарника (эквивалент runtime-слоя).
Мультистейдж `Dockerfile` остаётся для сборки в окружении с доступом к registry.

- `docker run … go-etl:2026.11.28` → `/health` отвечает
  `{"status":"ok","instance_id":"…"}` на локальном ClickHouse.
- `docker image save` → `dist/go-etl-2026.11.28.tar` (57.4 МБ).

## VPS

Исходное состояние до загрузки: `pipeline_state` revision 0, `idle`,
`load_phase=none`, `works=0`, `etl_checkpoints=0`; лимит 1 действует.

Полный прогон `etl ingest --config config/app.vps.toml --source s3`:
снимок `2026-10-14`, Release ID `7cc0c25e…`, 1898 Works-файлов и 6 Topics-файлов.

Статус: **выполняется**. Прогон с 4 воркерами идёт с 14:25, прогресс сохраняется
в `etl_checkpoints`; на последнем срезе 1 331 305 работ и 191 чекпоинт. Попытка
ускорить прогон 16 воркерами привела к остановке без сетевой активности, поэтому
она была прервана и загрузка продолжена с 4 воркерами; ранее подтверждённые
чекпоинты при возобновлении пропускаются. Итоговые counters, `pipeline_state`
и передача Python будут добавлены после `completed`.

## Ограничения

- `go test -race ./...` не запускался: в окружении нет gcc/cgo, а Docker Hub
  недоступен для образа с toolchain. Тест prefetch подготовлен под race и
  прогоняется на CI/Linux.
