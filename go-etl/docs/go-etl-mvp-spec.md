# ТЗ для Go-разработчика. Загрузка OpenAlex для MVP

## 1. Цель и работа команды

Проект исследует научные публикации Computer Science (CS) из OpenAlex. Задача 1 выявляет некорректные записи при загрузке. Задача 2 показывает годовую динамику публикаций, тем и направлений. Задача 3 выявляет квартальные всплески и показывает их в HTML. Задачи 4–6 расширят проект поиском зарождающихся тем, проверкой по последующим годам и географией распространения.

Ты дорабатываешь существующую директорию `go-etl`. Твоя ответственность — источники OpenAlex, нормализация, ClickHouse sink, метаданные загрузки, чекпоинты, HTTP, CLI и готовый контейнер Go. Python-разработчик пишет аналитику, TUI, отчёт, Compose, скрипты запуска и собирает дистрибутив из готовых компонентов. Ответственный за инфраструктуру поднимает только удалённый ClickHouse, применяет общую схему, настраивает доступы и передаёт параметры. Заполнение удалённой базы выполняет Go-разработчик, подготовку удалённых результатов — Python-разработчик.

Каждый работает на своём компьютере и коммитит свою часть в общий Git-репозиторий. До готовности VPS подними Docker ClickHouse 26.9 локально и проверь запись реальных данных OpenAlex на ограниченном наборе. Подготовь воспроизводимую команду проверки. После готовности VPS используй тот же код и DDL с другими параметрами подключения. Координируй интеграцию через схему и контракт ниже.

## 2. Входные точки и конфигурация

Поддержи `etl serve --config PATH --addr :8081` для кнопки TUI «Скачать/обновить данные» и `etl ingest --config PATH --source s3` для прямого ручного запуска. Оба входа используют один pipeline. Для проверки CLI поднимай ClickHouse и запускай `ingest` при выключенном HTTP-процессе, который мог бы писать в ту же базу. В штатном MVP на одну базу приходится один управляющий экземпляр Go. Повторный запрос через один HTTP-сервер должен вернуть активную задачу. Второй автономный CLI-процесс с записью в ту же базу требует остановленного первого экземпляра.

Рабочий файл дистрибутива называется `config/app.toml`. Это пример для запуска через Compose.

```toml
[compose]
start_clickhouse = true
start_go_etl = true

[actions]
download_data = true
refresh_analysis = true
offer_shutdown_on_exit = true

[clickhouse]
host = "clickhouse"
port = 9000
database = "openalex"
secure = false
reader_user = "report_reader"
reader_password_env = "CLICKHOUSE_REPORT_READER_PASSWORD"
pipeline_writer_user = "pipeline_writer"
python_pipeline_writer_password_env = "CLICKHOUSE_PYTHON_WRITER_PASSWORD"

[go_etl]
base_url = "http://go-etl:8081"
etl_writer_user = "etl_writer"
etl_writer_password_env = "CLICKHOUSE_ETL_WRITER_PASSWORD"
go_pipeline_writer_password_env = "CLICKHOUSE_GO_WRITER_PASSWORD"
```

Для Go нужны `clickhouse.host`, `port`, `database`, `secure`, `pipeline_writer_user` и данные `[go_etl]` для двух паролей. `base_url` задаёт, куда обращается TUI. Адрес, на котором слушает Go, задаёт `--addr`. При запуске с хоста подставь доступный с хоста адрес и проброшенный порт ClickHouse. При работе с VPS укажи его адрес и нативный TCP с TLS и проверкой сертификата. Go и Python должны подключаться к одной базе на одном сервере. При любом запуске значения `*_password_env` — имена переменных, а реальные пароли приходят из окружения. Для OpenAlex API используй `OPENALEX_API_KEY` или существующий явный параметр. Пароли и ключи не выводи в ответы и журналы.

Сохрани существующие флаги источника, воркеров, размера батча, формата даты, лимитов и диагностики. Для ручного теста `--max-files`, `--max-records` и отдельный шард запускай только на выделенной тестовой базе или в диагностическом режиме. Такой прогон не публикует усечённый корпус как полный снимок и не создаёт чекпоинты, которые пропустят данные при последующей полной загрузке. В дистрибутиве HTTP использует один шард `shard_total=1`, `shard_index=0`. Для будущего масштабирования сохраняй отбор файлов в стабильном списке одного манифеста по `file_index % shard_total == shard_index`. Независимый шард сам не публикует общую версию.

## 3. Источники, отбор и задача 1

- **Первая загрузка.** Прочитай один публичный S3-снимок OpenAlex в Parquet по `manifest.json`. Загрузи справочник тем и все подходящие работы CS за все доступные годы. Пользователь выбирает годы в отчёте. Дату снимка возьми из манифеста, сохрани идентификатор манифеста и воспроизводимый порядок файлов. Проверь, что релиз не сменился в ходе чтения, прежде чем опубликовать результат.
- **Обновление.** Сравни манифест с последним опубликованным `snapshot_date`. Обработай разделы `updated_date` после даты предыдущего релиза, а при первом переходе со снимка 14.10.2026 включи разделы за день релиза. `updated_date` раздела определяет изменения, год публикации определяет аналитический период. Учитывай актуальный `deleted_ids.csv.gz` и дождись подтверждения удаления до публикации. При отсутствии нового релиза или изменений оставь версию набора и кэш. Поддержи догрузку через API по дате публикации, если этот сценарий запускают явно. Догрузка по дате публикации не обнаруживает исправления старых работ. Для синхронизации через `from_created_date` и `from_updated_date` нужен доступ OpenAlex с поддержкой этих фильтров. [Устройство снимков и удалений](https://help.openalex.org/access/sync/) · [Фильтры API](https://help.openalex.org/api/filtering/)
- **Корпус.** CS определяется пригодным `primary_topic.field.id=17`. Допустимые типы — `article`, `book-chapter`, `conference-paper`, `data-paper`, `software-paper`, `preprint`. При первичной загрузке записывай подходящие работы. При обновлении для записей с валидным типом вне списка или известным переходом из CS в другую область проверяй их `work_id` в ClickHouse одним запросом на пачку. Для известных ID сохраняй актуальную версию с исходным типом и областью, чтобы аналитика исключила её после выбора версии. По той же причине сохраняй актуальный признак отзыва уже существующей работы. Учитывай исключение служебных записей и согласованный ранний отсев `is_xpac`.
- **Поля.** Запиши исходный `updated_date` в `source_updated_at`, исходный тип, числовые ID и обе даты публикации в общих типах. Тема и оценка сохраняются в соответствующих позициях массивов, в исходном порядке. Сохраняй все пригодные тематические ID, включая темы за пределами CS. При невалидной оценке используй представление, позволяющее применить согласованный выбор первой пригодной темы при отсутствии пригодной положительной суммы. Невалидная или отсутствующая точная дата хранится как `publication_date=0` при пригодном годе. `primary_source_id=0` и пустой тип площадки означают отсутствие. Известный FWCI 0 имеет `has_fwci=true`, отсутствующий или непригодный — `false`.
- **Отбраковка.** Увеличь `rejected_records` один раз для неразбираемой отдельной записи, непригодного или отсутствующего `work_id`, непригодного года, расхождения года с пригодной полной датой, отсутствующего пригодного `primary_topic.field.id`, отсутствия тем в обоих источниках, непригодного `updated_date`, пустого или непригодного `type` либо отсутствующего или небулева `is_retracted`. Год должен быть целым от 1 до года снимка, для API до текущего года UTC. Если область известна и в `topics` есть пригодный ID, `primary_topic.id` может отсутствовать. При пустом `topics` пригодная основная тема даёт запасную привязку. Площадка, FWCI, точная дата, DOI, аннотация и оценки тем могут отсутствовать. Валидная запись вне корпуса, отзыв, дубликат и ошибка чтения файла или вставки — отдельные случаи, а не счётчик некорректных записей. [Поля OpenAlex Works](https://help.openalex.org/data/works/attributes/)
- **Справочник.** Преобразуй Topics в `dim_topics` с уникальным `topic_id`, названием, ID и названием направления. Публикуй публикации и справочник как один срез. При изменении или повторной поставке справочника запрос обязан видеть актуальные значения. Для `force_reload` повторная загрузка должна происходить даже при неизменившемся релизе, обычный запуск может пропустить уже загруженный справочник. Существующую поддержку Institutions сохрани для этапа 1 и дальнейших задач.

## 4. ClickHouse sink, версии и чекпоинты

DDL восьми таблиц приведён в приложении. Базу и пользователей готовит администратор, на локальном стенде подготовь их сам. Используй нативные соединения с пакетной вставкой, явным списком столбцов, тайм-аутами и подтверждением каждой записи батча. `written_records` растёт после подтверждения ClickHouse. `NoopSink` оставь для диагностики. Для удаления всех локальных версий ID из журнала OpenAlex используй отдельный подтверждённый шаг Go. Для аналитики актуальная строка `works` выбирается через `FINAL` по `work_id` и `source_updated_at` до применения аналитических фильтров.

| Учётные данные | Операции Go |
|---|---|
| `etl_writer` | Вставка в `works` и `dim_topics`, чтение `work_id` для проверки известных работ, обработка удалений в `works`, чтение и вставка `etl_checkpoints` |
| `pipeline_writer` с паролем Go | Чтение и вставка `pipeline_state`, очистка `etl_checkpoints`, а при подтверждённом перезапуске очистка `works` и `dim_topics` |
| `pipeline_writer` с паролем Python | Доступ к анализу и кэшам. Go этот пароль не получает |

Для `pipeline_writer` действует `max_sessions_for_user=1` на общий логин, независимо от пароля. Занятому слоту соответствует ограничение по сессиям. Отличай его от неверного пароля, нехватки прав и недоступности. Подключайся с ограниченным ожиданием отдельных запросов и паузой между попытками. Слот держится физическим соединением. Прозрачное переподключение посреди перехода требует начать проверку заново.

1. Читай состояние запросом `SELECT * FROM pipeline_state WHERE id=1 ORDER BY state_revision DESC LIMIT 1`. Начальная строка `idle` с обеими версиями 0. Публикация — один синхронный `INSERT` полной строки с увеличенной ревизией под управляющей сессией. Сохраняй поля анализа без изменений.
2. Для новой загрузки под управляющей сессией повторно проверь, что другой загрузки нет, очисти `etl_checkpoints` и дождись подтверждения. Одним переходом запиши `loading`, `loading_snapshot_date`, `loading_manifest_id`, `instance_id` и `job_id`. До подтверждения `loading` запись исходных данных не начинается. После подтверждения освободи управляющий слот и пиши через `etl_writer`. Python при `loading` ждёт, а Go при занятом Python слоте ждёт до начала новой загрузки.
3. В `etl_checkpoints` одна логическая отметка на `(entity, object_key)`. Ставь её только после подтверждения всех батчей файла. Незавершённый файл при повторе перечитывается с начала. Повтор отправки после потери подтверждения допустим, актуальный `work_id` считается один раз. Дедупликация в памяти не должна блокировать повтор неподтверждённого батча. После прежней опубликованной версии чекпоинты хранятся до начала следующей новой загрузки. Если при восстановлении все файлы уже отмечены, доведи оставшиеся шаги и публикацию без искусственного `failed`.
4. После записи файлов, справочника и удалений проверь согласованный манифест и повторно займи управляющий слот. Опубликуй `idle`, новую `snapshot_date`, `data_version+1` и `data_updated_at`, если набор изменился. При успешной проверке без изменений оставь метаданные опубликованного набора прежними. Освободи слот. Существующий кэш Go сохраняет. При потере подтверждения публикации сначала перечитай состояние.
5. При Stop прекрати набор новых файлов, дай уже начатым операциям записи завершиться в пределах контролируемого тайм-аута, сохраняй только подтверждённые файлы. Итоговый `stopped` означает, что активной записи больше нет. При отказе ClickHouse или исчерпании места выставь `failed`. После перехода в `loading` оставь прежнюю опубликованную версию и состояние `loading`. Дальнейший старт возобновляет тот же снимок. При достоверной ошибке места отдай `error_code=disk_full`. UI предложит освободить место и продолжить или запустить `remote`.
6. Если манифест незавершённой загрузки больше недоступен, обычный запуск сообщает о смене снимка. Очистка разрешена после отдельного подтверждения. Под управляющей сессией повторно проверь базу, манифест и отсутствие предыдущей активной записи. Оставляя `loading`, очисти `works`, `dim_topics` и `etl_checkpoints`. Закрепи новый манифест только после подтверждения всей очистки, затем начни полную загрузку. Если очистка прервалась, не помечай её как завершённую. Версия публикуется только после полной загрузки. Кэш анализа и созданный HTML сохраняются.

Для ручной записи в рабочую базу применяй тот же протокол, включая отдельную синхронизацию справочника. Если она меняет опубликованный справочник, увеличивай версию данных. Нагрузочный тест пишет в отдельную тестовую таблицу и не меняет состояние рабочего корпуса.

## 5. HTTP и процесс Go

| Метод и путь | Ответ и поведение |
|---|---|
| `GET /health` | `200 {"status":"ok","instance_id":"…"}`. Создавай новый `instance_id` на каждый запуск процесса |
| `POST /api/v1/ingest/start` | Асинхронный запуск. Штатный JSON `{"source":"s3","shard_total":1,"shard_index":0}`. Ответ `202` содержит `job_id`, `instance_id`, `state`. Повтор активной загрузки возвращает её ID |
| `GET /api/v1/jobs/{job_id}` | `200` со статусом или `404` для неизвестной задачи. Состояния `running`, `completed`, `failed`, `stopped`. Задачи живут в процессе, пара `instance_id` и `job_id` идентифицирует попытку |
| `POST /api/v1/jobs/{job_id}/stop` | `200 {"stopped":true}` означает принятие запроса. Итог проверяется опросом статуса. Возврат `stopped:false` возможен, если задача уже завершена |
| `POST /api/v1/ingest/restart` | JSON `{"restart_token":"…"}`. Ответ `202` с `job_id`, `instance_id`, `state`. Повтор того же подтверждения возвращает уже созданную задачу |

В статусе сохраняй `job_id`, `kind`, `state`, `scanned_records`, `matched_records`, `written_records`, `dedup_dropped`, `bytes_read`, `http_requests`, `elapsed_sec`, `current_rps`, `current_item`, `avg_batch_ms`, `p50_batch_ms`, `p95_batch_ms`, `p99_batch_ms`, `error`, добавь `rejected_records` и машинный `error_code` для различимых ошибок. `dedup_dropped` относится к повторам в пределах задачи. `error` содержит безопасное для пользователя описание. Сохрани существующие маршруты справочников, нагрузки и запроса API-ключа. Go корректно обрабатывает сигнал завершения контейнера, прекращая принимать новую работу и дожидаясь завершения активных записей.

Признак смены снимка приходит через статус обычной задачи как `state=failed` и `error_code=snapshot_changed`, с полями `previous_snapshot_date`, `available_snapshot_date` (YYYY-MM-DD), `target_database` (объект `host`, `port`, `database`) и `restart_token`. Токен привязан к двум манифестам, базе и ревизии. При утрате токена после рестарта Go или изменении условий верни `409` и `error_code=restart_offer_expired`. Если условия сменились после принятия запроса, сообщи тот же код через статус новой задачи. Повтор подтверждённого токена в одном процессе не запускает повторную очистку. При отмене TUI не отправляет запрос на перезапуск.

### Запрос ключа REST API OpenAlex

Сохрани `POST /api/v1/openalex/request-key` с JSON `{"email":"<адрес пользователя>"}`. Вызывай его только по явному запросу владельца адреса. Go отправляет email в `POST https://api.openalex.org/api-keys`. OpenAlex возвращает ключ в JSON со статусом `pending_activation` и отправляет на почту ссылку. Пользователь активирует ключ по ссылке. До этого Go сообщает о необходимости активации при `403 key_not_activated`. Учитывай `409 activation_pending`, `409 agent_key_exists` и лимит `429`. Запрос ключа не следует автоматически повторять. Альтернативный путь — пользователь передаёт свой ключ из настроек OpenAlex через `OPENALEX_API_KEY`. [Получение и активация ключа OpenAlex](https://help.openalex.org/api/authentication/#getting-a-key-for-your-user)

Успешно полученный ключ должен быть доступен следующей API-задаче того же локального `go-etl`. Храни и передавай его потоко-безопасно. В ответе Go оставляй признак принятия и сообщение про активацию, а значение ключа сохраняй у Go. Проверь обработку статусов на HTTP-заглушке. Загрузка штатного публичного снимка S3 не зависит от получения ключа REST API.

## 6. Что исправить в текущем коде

### Исправления, влияющие на загрузку и интеграцию

1. `internal/ingest/ingest.go` и тесты. Сделай `max_records` общим жёстким лимитом для воркеров. Добавь проверку нескольких воркеров.
2. `internal/ingest/ingest.go`, `internal/adapters/s3/parquet_reader.go`, `internal/app/app.go`. Усечённый файл сегодня может получить чекпоинт «готов». Различай тестовое ограничение и завершённый файл. Замени локальные чекпоинты нормального пути ClickHouse-таблицей.
3. `internal/app/app.go`, `internal/adapters/s3/parquet_reader.go`, `internal/loadtest/loadtest.go`, `internal/sink/sink.go`. Исправь завершение записи при отмене. Финальный ограниченный flush не использует уже отменённый контекст, ошибки записи учитываются, незавершённый файл остаётся без отметки.
4. `cmd/etl/main.go`, `internal/adapters/s3/manifest.go`. Добавь форму `--shard=k/N` поверх существующего отбора и сохрани раздельные флаги и JSON-поля.
5. `internal/app/app.go`, `internal/ingest/ingest.go`. Любой окончательно провалившийся обязательный файл даёт ошибку всей загрузки. Верни понятный итог и исключи публикацию неполного набора.
6. `internal/normalizer/models.go`, `internal/adapters/s3/parquet_reader.go`, `internal/normalizer/normalizer.go`. Сверь проекцию с реальным Parquet. Если вложенные структуры и списки подтверждаются, сохрани верное декодирование. Выполняй раннюю проверку `is_xpac` и области по лёгкой проекции до тяжёлых данных там, где это позволяет формат. Зафиксируй, что было проверено на реальном файле.
7. `internal/adapters/api/client.go`, `works.go`, `internal/normalizer/normalizer.go`. Перенеси декодирование горячего пути, включая страницу `results`, на согласованный `sonic`.
8. `internal/httpapi/requests.go`, `internal/app/app.go`, `internal/adapters/api/client.go`. Соблюдай проектный предел 30 HTTP-запросов/с на общий ключ для параллельных задач, считай повторы и соблюдай полный `Retry-After`.
9. `internal/app/app.go`, `internal/httpapi/requests.go`, README. Объясняй отказ тарифа на фильтрах синхронизации. Режим `publication` остаётся отдельной догрузкой по дате публикации. Штатный публичный MVP обновляется снапшотами S3.
10. `internal/app/app.go`, `cmd/etl/main.go`. Сделай `force_reload` работающим флагом для CLI и HTTP.
11. `internal/app/app.go`, `internal/adapters/api/apikey.go`. Устрани гонку записи ключа и обеспечь использование ключа последующим API-ingest по контракту запроса и активации. Сейчас Go меняет ключ одного клиента, а следующая задача может создать другой.

### Нагрузочный стенд и отчёт о производительности

12. `internal/loadtest/loadtest.go`, README. Опиши получение локальной выборки около 5 000 реальных записей. Чётко сообщай об отсутствии файла и неподходящей выборке.
13. Там же. Проверяй тайм-аут и отмену даже при потоке отфильтрованных записей. Проверка должна завершаться, когда батч не заполняется.
14. `internal/adapters/api/client.go`, `internal/adapters/s3/reader_at.go`, `internal/app/app.go`. Показывай фактические HTTP-попытки и байты в процессе, Scan и Ingest в записях/с и МБ/с, объём нормализованных батчей. Используй атомарные счётчики и измерения на уровне батча или файла.
15. `internal/adapters/s3/manifest.go`, `internal/app/app.go`. Получай объём снимка суммой `content_length` файлов.
16. `internal/adapters/s3/bench_s3_test.go`, `bench_partition_test.go`, `parquet_reader.go`. Для JSONL.gz, Parquet Full GET и Parquet Buffered Range сравнивай одинаковые реальные публикации и проверяй ID и содержимое нормализованных батчей. Для базовой линии без буфера отключи буфер, кэш и prefetch. Замерь диапазон размеров и буферы 512 КБ, 2 МБ и 4 МБ, время, байты, запросы, скорость и пиковую память.
17. `internal/normalizer/benchmark_test.go`, README. На 10 000+ реальных записях сравни JSON-парсеры, ранний и полный разбор области, два алгоритма сборки абстракта, AoS и SoA с пулом. Набор набирай из реальных записей. Выведи `ns/op`, `MB/s`, `B/op`, `allocs/op` и GC.

Для материалов курсовой сохрани команды и условия замеров. Сними сквозной Scan/Ingest и pprof, воронку объёмов от S3 до `system.parts`, сравнение S3 и API с RPS, паузами, ретраями, квотой и расчётным временем, а также нагрузку через отдельную `works_loadtest`. Для кривых варьируй воркеры 1, 2, 4, 8, 16 и батчи 100, 1 000, 10 000, 50 000, 100 000, измеряй RPS, CPU, память и p50/p95/p99. Сравни LZ4 и ZSTD по скорости, CPU и трафику. Используй выделенную нагрузочную площадку и фиксируй ограничения источника. Сборка дистрибутива и функциональная приёмка могут идти до завершения длинных замеров.

## 7. Сдача и приёмка

Коммить изменения Go, Dockerfile образа, инструкцию и результаты проверок. Передай Python-разработчику имя и тег образа, команду запуска `serve`, порт и требования к окружению. Контейнер работает как фоновый HTTP-сервис при запуске из Compose. Дай отдельную команду ручного ingest. Для локального теста начни с реального ограниченного набора OpenAlex и проверь запись и чтение порядка 100 работ и справочника. Команда с ограничениями должна иметь выделенную тестовую базу.

Проверь полную публикацию на ограниченном тестовом снимке, отсутствие изменений, обновление и удаление известных ID, переход типа и области, `Stop`, перезапуск процесса, потерю подтверждения батча, нехватку места, встречу с новым снимком при возобновлении, подтверждение и отмену перезапуска. Только полная обработка выбранного **тестового снимка** может публиковаться в его тестовой базе. Сверь, что активная запись и пересчёт Python взаимно исключаются и два пароля `pipeline_writer` делят один слот. Выполни `go test -race ./...` и сборку. Приведи команду повторного локального запуска и проверь с Python реальный HTTP-путь. После готовности VPS повтори интеграцию с удалёнными параметрами и подготовь опубликованный набор для расчёта Python.

## Приложение. Общий schema.sql

Сохрани этот SQL как `schema.sql` при работе вне общей копии репозитория. Схема подходит локальному тесту и удалённому ClickHouse. Учётные записи и пароли задаются при развёртывании.

```sql
-- Общая схема MVP для Go и Python. Выполнять при подготовке базы openalex.
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

```
