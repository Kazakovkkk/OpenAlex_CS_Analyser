# Передача данных Python-разработчику

Дата: 28.11.2026. База: `openalex` на VPS `168.113.253.42:9440` (TLS).

## Сообщение

> Данные OpenAlex загружены на VPS, можно запускать пересчёт кэша и отчёт.
>
> - `snapshot_date` = **2026-10-14**
> - `data_version` = **1**
> - Release ID = `7cc0c25ec43994ee5afa9992a2896526b916577184021d787d301e7571159b68`
> - Подтверждённые работы: **12 589 941** (уникальные `work_id`, без дублей)
> - Темы в `dim_topics`: **4 516**
> - `pipeline_state`: revision 4, `load_state=idle`, `load_phase=none`
> - Чекпоинтов: **1 904** (1898 Works + 6 Topics), все обязательные файлы подтверждены
>
> Сессия `pipeline_writer` освобождена: загрузка завершена, соединение
> закрыто. Можно подключаться своим паролем и пересчитывать кэш/отчёт.

## Как проверить

```sql
SELECT state_revision, snapshot_date, data_version, load_state, load_phase
FROM openalex.pipeline_state
ORDER BY state_revision DESC LIMIT 1;

SELECT count() FROM openalex.works;
SELECT uniqExact(work_id) FROM openalex.works;
SELECT count() FROM openalex.dim_topics;
SELECT count() FROM openalex.etl_checkpoints FINAL;
```

Ожидаемые значения: revision 4, `2026-10-14`, версия 1, `idle`, `none`;
`12 589 941`; `12 589 941`; `4 516`; `1 904`.

Внимание: на момент передачи `SELECT` на `dim_topics` под `pipeline_writer`
возвращает `code 497`, хотя `SHOW GRANTS` показывает грант
`SELECT, TRUNCATE ON openalex.dim_topics`. Число тем 4 516 взято из счётчика
загрузки; перед расчётом отчёта грант нужно проверить/перевыдать
администратору VPS.

## Важно

Пароли `pipeline_writer` (Go и Python) были переданы в переписке — после сдачи
проекта их нужно перевыпустить. Go-часть пароль Python не получает: в конфиге
Go читает только `CLICKHOUSE_GO_WRITER_PASSWORD`.
