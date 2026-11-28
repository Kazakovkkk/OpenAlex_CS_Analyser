# План: слот `pipeline_writer` на закреплённом соединении

Документ для продолжения работы в новом чате. Всё, что нужно, чтобы начать с нуля и не потерять контекст.

## Задача

По ТЗ управляющий слот `pipeline_writer` **держится физическим соединением**: взял на время перехода (`BeginLoad`, `Publish`), освободил после. Лимит `max_sessions_for_user=1` — это и есть защита от одновременной публикации Go и Python. Занято → пауза и повтор, код ошибки `session_busy`.

Сейчас это не работает: клиент ходит через пул `clickhouse.Open` (driver-стиль), пул открывает новое TCP-соединение между операторами, и **второй оператор внутри перехода получает `code 700 overflown session count 1`**.

## Проверено на живом VPS

| Что проверяли | Результат |
|---|---|
| Пул (`clickhouse.Open`, `MaxOpenConns=1`) — 2 запроса подряд | 700 на втором |
| То же с мьютексом в `Store` | 700 |
| То же с `Query` + `rows.Close()` вместо `QueryRow` | 700 |
| **`clickhouse.OpenDB` + `db.Conn(ctx)` — 4 запроса подряд** | **все прошли, 0 ошибок** |

Вывод: для управляющего пользователя нужен `database/sql` и закреплённое `*sql.Conn`, пул непригоден.

Стенд: `168.113.253.42:9440`, TLS с проверкой сертификата, база `openalex`, ClickHouse 26.9.40.9. Состояние: `revision=0`, `idle`, `data_version=0`, `works` и `etl_checkpoints` пусты.

## Правки по файлам

### 1. `internal/clickhouse/client.go`

Добавить рядом с `Open`:

```go
// OpenSQL returns a database/sql handle: the management user needs a pinned
// physical connection, not a pool.
func OpenSQL(ctx context.Context, role Role, o Options) (*sql.DB, error) {
    o = o.withDefaults(role)
    opts := buildOptions(o)          // вынести общую сборку из Open
    db := ch.OpenDB(opts)
    db.SetMaxOpenConns(1)
    db.SetMaxIdleConns(1)
    if err := db.PingContext(ctx); err != nil { ... }
    return db, nil
}
```

`buildOptions(o)` — вынести из `Open` всё построение `*ch.Options` (Addr, Auth, TLS с `ServerName` из хоста, таймауты, сжатие), чтобы `Open` и `OpenSQL` не расходились.

### 2. `internal/pipeline/conn.go` (новый)

```go
type Conn interface {
    ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
    QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
    QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}
```

`*sql.DB` и `*sql.Conn` оба удовлетворяют этому интерфейсу.

### 3. `internal/pipeline/slot.go` (новый)

```go
type Slot struct{ db *sql.DB }

// Run выполняет переход целиком на одном закреплённом соединении.
// - занятость слота (session_busy) → пауза 1 с, до 3 попыток;
// - ошибка соединения посреди перехода → переход начинается заново (fn вызывается повторно);
// - таймаут перехода 15 с.
func (s *Slot) Run(ctx context.Context, fn func(Conn) error) error
```

Правила:
1. `conn, err := db.Conn(ctx)` — взяли слот.
2. `fn(conn)` — весь переход.
3. при `clickhouse.KindSessionBusy` → `time.Sleep(1s)`, повторить (макс. 3).
4. при ошибке, похожей на обрыв соединения → `fn` вызывается заново (он обязан сам перечитать состояние; это и есть «прозрачное переподключение посреди перехода требует начать проверку заново»).
5. `conn.Close()` — слот освобождён.

### 4. `internal/pipeline/state.go`

- `Store` держит `db *sql.DB` вместо `Conn`.
- `Read`, `BeginLoad`, `Publish` — каждый оборачивается в `slot.Run`, внутри — работа с переданным `Conn` (`ExecContext`, `QueryContext`/`QueryRowContext`).
- Логику `read`/`beginLoad`/`publish` вынести в методы, принимающие `Conn`, чтобы их можно было вызывать внутри `fn`.
- Убрать `sync.Mutex` из `Store`: сериализацию даёт слот.

### 5. `internal/pipeline/checkpoint.go`, `deletions.go`

- Перевести на те же методы (`ExecContext`/`QueryContext`).
- `CheckpointStore` и `DeleteWorks` работают под `etl_writer`, лимита там нет — можно брать `*sql.DB` напрямую, но сигнатуры принимают `Conn`, чтобы тесты подходили.
- `ClearCheckpoints`/`ResetCorpus` — через слот (это управляющая сессия).

### 6. `cmd/etl/chsink.go`

- Для `pipeline_writer`: `clickhouse.OpenSQL(...)` → `*sql.DB`, передать в `pipeline.NewStore`.
- Для `etl_writer`: оставить `clickhouse.Open` (пул 8) для sink и чекпоинтов; `DeleteWorks` можно оставить на пуле.
- Закрытие обоих в `closeFn`.

### 7. Интеграционные тесты

`internal/pipeline/*_integration_test.go`, `internal/knownids/build_integration_test.go` — открывать `*sql.DB` через `OpenSQL` вместо driver-коннекта. Админ для `TRUNCATE` в тестах — через `OpenSQL(..., RolePipelineWriter)` с пользователем `default` (локальный стенд).

### 8. Юнит-тесты для слота (без ClickHouse)

Фейковый `Conn`, который:
- возвращает ошибку с текстом `overflown session count` первые N раз → проверяем, что `Run` повторил ровно N раз;
- рвёт соединение посреди перехода → проверяем, что `fn` вызван заново и состояние перечитано;
- превышает таймаут → `Run` вернул ошибку, а не завис.

## Порядок работ

1. `buildOptions` + `OpenSQL` (сборка зелёная, старый путь не тронут).
2. `conn.go` + `slot.go` + юнит-тесты слота (без ClickHouse).
3. `state.go` на слот; локальные тесты; интеграционный тест протокола на `*sql.DB`.
4. `checkpoint.go`, `deletions.go`, `chsink.go`.
5. Проверка на VPS: два перехода подряд и полный цикл `BeginLoad → Publish` — без 700 при лимите 1.
6. Только после этого — архитектура «воркеры → канал → один писатель» (фаза 2, отдельный заход).

## Проверка на VPS

```bash
# 1. Четыре чтения состояния подряд (должны пройти все)
go run ./tmp/vpssql

# 2. Полный переход: BeginLoad и Publish на одном процессе
#    ожидаем: revision 0 → 1 (loading) → 2 (idle), ни одной 700
go run ./tmp/vpsstore
```

`tmp/vpssql` и `tmp/vpsstore` — временные программы, лежат в `tmp/` (в git не попадают). Пароль `pipeline_writer` брать из окружения: `PIPE_PW`.

## Критерии готовности

- [ ] при лимите 1 два перехода подряд проходят без 700;
- [ ] переход целиком идёт на одном физическом соединении (проверяется логом/счётчиком);
- [ ] занятость слота даёт `session_busy` и повтор, а не падение;
- [ ] обрыв посреди перехода перезапускает переход, а не продолжает с середины;
- [ ] локальные тесты и `go vet` зелёные;
- [ ] поведение при занятом слоте Python: Go ждёт, затем уступает/повторяет.

## Контекст окружения (важно для нового чата)

- Go: `CGO_ENABLED=0`, `go 1.27.1`; сборка требует переменных:
  `GOPATH=D:\OpenAlex_CS_Analyser\go-etl\.gopath`, `GOCACHE=...\.gocache`, `GOTMPDIR=...\.gotmp`, `GOFLAGS=-mod=mod`.
- Песочница DSH в этой сессии сломана (`--temp is not an existing directory`), поэтому команды выполнялись с `sandbox_permissions: danger-full-access`.
- Пароли не хранить в файлах: `CLICKHOUSE_ETL_WRITER_PASSWORD`, `CLICKHOUSE_GO_WRITER_PASSWORD` из окружения.
- Интеграционные тесты ClickHouse запускать последовательно: `go test -tags=integration -p 1 ./internal/pipeline ./internal/sink ./internal/knownids` — они чистят одну таблицу `works`.

## Что осталось за пределами этой задачи (не забыть)

1. **Справочник внутри `ingest`** — одна загрузка = одна версия; отложено до проверки на VPS.
2. **Приёмка §7** на VPS.
3. **`benchstat` и пик RSS** для §6.17.
4. **`.gitignore`**: из дерева убрать отладочные `*_out.txt` и `bench_*.txt` либо вернуть точечные правила.
5. **Пароли сменить** — они были в переписке.
