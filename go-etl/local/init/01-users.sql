-- Локальный стенд: пользователи из рабочего ТЗ (3.6).
--
-- Пароли здесь — ТОЛЬКО для локальной базы в Docker; в дистрибутиве и на VPS
-- пользователей и гранты создаёт администратор, а Go получает пароли из
-- окружения (см. config/app.example.toml).
--
-- Пользователи создаются через SQL, а не в users.xml: гранты на пользователя из
-- users_xml ClickHouse отклоняет (ACCESS_STORAGE_READONLY), проверено на 26.9.
--
-- У pipeline_writer два пароля (Go и Python) на один логин: ClickHouse 26.9
-- поддерживает несколько методов sha256_password у одного пользователя. Профиль
-- pipeline_limited (users.d/users.xml) задаёт max_sessions_for_user=1, поэтому
-- оба пароля делят один физический слот.

CREATE USER IF NOT EXISTS etl_writer IDENTIFIED BY 'dev-etl-writer-password';

CREATE USER IF NOT EXISTS pipeline_writer IDENTIFIED BY 'dev-go-writer-password';
ALTER USER pipeline_writer ADD IDENTIFIED WITH sha256_password BY 'dev-python-writer-password';
ALTER USER pipeline_writer SETTINGS PROFILE 'pipeline_limited';
