# AiR_Whatsbot

![air_whatsbot](logo.png)

[🇬🇧 English version](README.md)

![Версия Go](https://img.shields.io/badge/Go-1.26.0-00ADD8?logo=go)
![Лицензия](https://img.shields.io/badge/license-MIT-blue)
[![Telegram](https://img.shields.io/badge/Telegram-Join%20Chat-blue?logo=telegram)](https://t.me/marusia_dev)

`air_whatsbot` — сервис WhatsApp-ботов платформы AiR. Он управляет пользовательскими WhatsApp-сессиями, предоставляет HTTP/WebSocket API и отдельный gRPC API для голосовых звонков.

## Возможности

- подключение и управление WhatsApp-ботами без Graph API;
- запуск, остановка и перезапуск пользовательского бота;
- получение имени бота и проверка доступности сервиса;
- WebSocket-подключение для авторизации и обмена сообщениями;
- потоковая передача контактов через WebSocket;
- исходящие голосовые звонки через WhatsApp;
- server-streaming события звонка: транскрипция, ответ AI, ошибки и завершение;
- хранение состояния и конфигурации в MariaDB/MySQL;
- восстановление состояния взаимодействия через Redis;
- Prometheus-метрики.

## Архитектура

```text
air_whatsbot
├── HTTP :8080
│   ├── /whats/available
│   ├── /whats/getname
│   ├── /whats/enable
│   ├── /whats/disable
│   ├── /whats/restart
│   ├── /whats/ws
│   ├── /whats/contacts/ws
│   └── /metrics
└── gRPC :9090
    └── calls.v1.Calls
        ├── StartOutgoingCall
        ├── SubscribeCallEvents
        └── HangupCall
```

Сервис получает конфигурацию WhatsApp-ботов через gRPC от `air_orchestrator`. Для работы также используются MariaDB/MySQL и, опционально, Redis.

## HTTP API

Полное описание маршрутов находится в [OpenAPI-спецификации](doc/openapi.yaml).

Для всех маршрутов, работающих с пользовательским ботом, требуется query-параметр `uid`:

```text
GET /whats/getname?uid=42
```

Основные маршруты:

| Метод | Путь | Назначение |
|---|---|---|
| GET | `/whats/available` | Проверка доступности |
| GET | `/whats/getname?uid=...` | Имя бота |
| GET | `/whats/enable?uid=...` | Запуск бота |
| GET | `/whats/disable?uid=...` | Остановка бота |
| GET | `/whats/restart?uid=...` | Перезапуск бота |
| GET | `/whats/ws?uid=...` | WebSocket авторизации и сообщений |
| GET | `/whats/contacts/ws?uid=...` | WebSocket поток контактов |
| GET | `/metrics` | Prometheus-метрики |

WebSocket-маршруты требуют заголовок `Upgrade: websocket`. При отсутствии `uid` сервер возвращает `400` с JSON-ошибкой.

## gRPC API звонков

gRPC-сервер слушает `:9090` и реализует сервис `calls.v1.Calls`. В Docker-сети адрес сервиса обычно `whatsbot_app:9090`, локально — `127.0.0.1:9090`.

Контракт: [calls.proto](internal/delivery/rpc/calls.proto).

Типовой сценарий:

```text
StartOutgoingCall
        ↓ call_id
SubscribeCallEvents
        ↓ события в реальном времени
HangupCall (при необходимости)
        ↓
CALL_ENDED
```

`StartOutgoingCall` принимает `user_id`, `provider` и `target`, запускает звонок и возвращает `call_id`. `SubscribeCallEvents` поддерживает `after_sequence` для восстановления потока после переподключения. Аудио через gRPC не передаётся: оно обрабатывается внутри WhatsApp-сервиса.

## Требования

- Go 1.25 или новее;
- MariaDB/MySQL;
- Redis — необязательно, но рекомендуется для восстановления состояния;
- доступ к `air_orchestrator` по gRPC;
- сервисный ключ в файле `.service_key`.

## Конфигурация

Основные переменные окружения:

| Переменная | Назначение |
|---|---|
| `DB_HOST` | Адрес MariaDB/MySQL |
| `DB_NAME` | Имя базы данных |
| `DB_USER` | Пользователь базы данных |
| `DB_PASSWORD` | Пароль базы данных |
| `REDIS_ADDR` | Адрес Redis, может быть пустым |
| `REDIS_PASSWORD` | Пароль Redis |
| `REDIS_DB` | Номер базы Redis |
| `GRPC_CONFIG_HOST` | gRPC-адрес `air_orchestrator` |
| `SERVICE_KEY_FILE` | Путь к сервисному ключу |
| `REAL_URL` | Публичный домен сервиса |
| `LOG_LEVEL` | Уровень логирования |
| `GLOB_USER_MODEL_TTL` | TTL пользовательской модели в минутах |

Значения для разработки и production приведены в [dev.yml](dev.yml) и [prod.yml](prod.yml). Секреты не следует добавлять в репозиторий.

## Запуск

Локальный запуск приложения:

```bash
go run ./cmd
```

Запуск в Docker Compose:

```bash
docker compose -f dev.yml up -d --build
```

Для production используется `prod.yml`:

```bash
docker compose -f prod.yml up -d --build
```

Перед запуском должны существовать внешние сети, указанные в compose-файлах:

```bash
docker network create air_shared
docker network create monitoring_shared
```

## Разработка

Проверка форматирования и тестов:

```bash
gofmt -w ./cmd ./internal
go test ./...
```

## Связанные проекты

- [air_orchestrator](https://github.com/ikermy/air_orchestrator) — конфигурация и координация сервисов AiR;
- [air-common](https://github.com/ikermy/air-common) — общие модели, realtime-провайдеры и инфраструктурные компоненты;
- [air-logger](https://github.com/ikermy/air-logger) — логирование;
- [air_front](https://github.com/ikermy/air_front) — пользовательский интерфейс платформы.

## Лицензия

Проект распространяется по лицензии [MIT](LICENSE). Она разрешает свободно использовать, копировать, изменять и распространять программное обеспечение при сохранении текста лицензии и уведомления об авторских правах.

Полный текст лицензии доступен в файле [`LICENSE`](LICENSE).

## Контакты

[![Telegram](https://img.shields.io/badge/Telegram-Contact-blue?logo=telegram)](https://t.me/ikermy)

