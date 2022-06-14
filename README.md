# Бот Фейловер Бара

### Запуск

Для запуска достаточно установить env переменные
`TELEGRAM_TOKEN`, `YDB_DSN` и любую переменную для авторизации в ydb,
например `YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS`.

TELEGRAM_TOKEN можно получить у https://t.me/BotFather, создав бота для тестов.

YDB_DSN — конкатенация `endpoint` и `database`. Подробнее:
https://ydb.tech/ru/docs/concepts/connect

Креды из окружения берутся так:
https://github.com/ydb-platform/ydb-go-sdk-auth-environ

### Тесты

Тесты проводятся с живым окружением, т.ч. нужны env переменные.

### Разработка

Таски на разработку ведутся в этом же проекте, см. issue