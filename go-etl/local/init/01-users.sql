-- Локальный стенд: пользователи из рабочего ТЗ (3.6).
--
-- Пароли здесь — ТОЛЬКО для локальной базы в Docker; в дистрибутиве и на VPS
-- пользователей и гранты создаёт администратор, а Go получает пароли из
-- окружения (см. config/app.example.toml).
--
-- Пользователи создаются через SQL, а не в users.xml: гранты на пользователя из
-- users_xml ClickHouse отклоняет (ACCESS_STORAGE_READONLY), проверено на 26.9.
--
-- max_sessions_for_user=1 здесь НЕ выставляется: на 26.9 настройка не
-- ограничивает второй нативный TCP-сеанс (проверено: два одновременных
-- clickhouse-client проходят). Ограничение остаётся задачей инфраструктуры на
-- VPS; протокол публикации всё равно проверяет state_revision перед переходом.

CREATE USER IF NOT EXISTS etl_writer IDENTIFIED BY 'dev-etl-writer-password';
CREATE USER IF NOT EXISTS pipeline_writer IDENTIFIED BY 'dev-go-writer-password';
