# План реализации: транзакционный outbox/inbox через Kafka

> **Статус: реализовано и проверено сквозным сценарием** (`docker compose up` → миграции → curl → outbox → Kafka → inbox → баланс в service B). Детали и найденные по пути нюансы — в конце файла.

## Резюме дизайна

Два сервиса на NestJS в одном monorepo, каждый со своей Postgres. Service A принимает
транзакцию, атомарно обновляет баланс и пишет outbox-запись. Фоновый поллер отправляет
outbox в Kafka. Service B читает Kafka, атомарно пишет inbox и обновляет свой баланс.
Всё поднимается через docker-compose.

### Принятые решения

| Аспект | Решение |
|---|---|
| Терминология | `idempotency_id` (не `independency_id`) |
| Структура репо | monorepo, `apps/service-a`, `apps/service-b`, один корневой `package.json`, Nest CLI monorepo mode |
| Пакетный менеджер | npm + Node 22 LTS |
| Docker-образы | отдельный `Dockerfile.service-a` / `Dockerfile.service-b` |
| ORM | Drizzle ORM (pg driver, `db.transaction()`, row locking) |
| Деньги | Postgres `numeric`, в TS — строки + `decimal.js` |
| Конкурентность баланса | Optimistic lock (колонка `version`), retry всей транзакции до 5 раз при CAS-конфликте |
| Повтор idempotency_id | Вернуть существующую транзакцию (200), не создавать дубль |
| Статус транзакции | Без поля `status` — запись в `transactions` = всегда `completed` |
| Автосоздание user | И в A, и в B: если `user_id` не найден — upsert с `balance=0`, затем применить `amount` |
| API service A | REST `POST /transactions`, JSON body `{idempotency_id, user_id, amount}` |
| Outbox publisher | Поллинг в том же процессе (`@nestjs/schedule`), интервал 1с, батч 100 |
| Outbox delivery | At-least-once: сперва `send` в Kafka (ack), затем `UPDATE sent_at` |
| Kafka client | Чистый `kafkajs` (producer в A, consumer в B) |
| Kafka топик | `balance-updates`, ключ сообщения = `user_id`, 3 партиции |
| DLQ | `balance-updates.dlq`, после 3 неудачных попыток обработки |
| Inbox/offset (service B) | DB-транзакция: insert в `inbox` (unique по `idempotency_id`, конфликт → skip) + update баланса → затем ручной commit Kafka offset |
| Kafka в compose | `confluentinc/cp-kafka`, KRaft-режим (без Zookeeper), брокер на host-порту 9092 + `kafka-ui` |
| Миграции | `drizzle-kit`, запускаются вручную: `docker compose exec <service> npm run db:migrate` |
| Порты | service-a: 3000, service-b: 3001, pg-a: 5433→5432, pg-b: 5434→5432, kafka: 9092, kafka-ui: 8080 |
| Тесты | Не требуются |

---

## Шаги реализации

### 1. Каркас monorepo
- [x] Инициализировать корневой `package.json`, `tsconfig.json`, `nest-cli.json` с двумя projects (`service-a`, `service-b`) в monorepo-режиме.
- [x] Установить зависимости: `@nestjs/*`, `@nestjs/schedule`, `@nestjs/config`, `drizzle-orm`, `drizzle-kit`, `pg`, `kafkajs`, `decimal.js`, `class-validator`, `class-transformer`.
- [x] Сгенерировать структуру `apps/service-a/src`, `apps/service-b/src`.

### 2. Схема БД service A (Drizzle)
- [x] `apps/service-a/src/db/schema.ts`:
  - `users` (`id` (user_id, PK), `balance numeric`, `version integer`)
  - `transactions` (`id` PK, `idempotency_id` unique, `user_id`, `amount numeric`, `created_at`)
  - `outbox` (`id` PK, `payload jsonb`, `created_at`, `sent_at nullable`)
- [x] Настроить `drizzle.config.ts` для service A, добавить npm-скрипт `db:migrate` (drizzle-kit generate + migrate).

### 3. Схема БД service B (Drizzle)
- [x] `apps/service-b/src/db/schema.ts`:
  - `users` (`id` (user_id, PK), `balance numeric`, `version integer`)
  - `inbox` (`id` PK, `idempotency_id` unique, `user_id`, `amount numeric`, `processed_at`)
- [x] Настроить `drizzle.config.ts` для service B, npm-скрипт `db:migrate`.

### 4. Service A — бизнес-логика транзакции
- [x] `TransactionsController`: `POST /transactions`, DTO с `class-validator` (`idempotency_id: string`, `user_id: string`, `amount: string/number`).
- [x] `TransactionsService.createTransaction()`:
  1. Открыть Drizzle-транзакцию.
  2. Проверить существование `transactions` по `idempotency_id` — если есть, вернуть её (не создавать дубль).
  3. Upsert `users` (создать с `balance=0`, `version=0`, если не существует).
  4. Прочитать `balance`+`version`, посчитать новый баланс через `decimal.js`.
  5. `UPDATE users SET balance=?, version=version+1 WHERE id=? AND version=?` — если 0 строк изменено → CAS-конфликт → retry с шага 4 (до 5 раз).
  6. Insert в `transactions`.
  7. Insert в `outbox` (payload: `idempotency_id`, `user_id`, `amount`, `transaction_id`).
  8. Commit.
- [x] Обработка исчерпания retry (500/409 клиенту).

### 5. Service A — outbox publisher
- [x] `OutboxPublisherService` с `@Cron`/`@Interval(1000)`:
  1. Выбрать до 100 записей `outbox` где `sent_at IS NULL`, упорядочить по `id`.
  2. Для каждой — `producer.send({ topic: 'balance-updates', messages: [{ key: user_id, value: JSON.stringify(payload) }] })`, дождаться ack.
  3. После успешной отправки — `UPDATE outbox SET sent_at = now() WHERE id = ?`.
  4. Ошибки отправки — залогировать, запись останется неотправленной и будет подхвачена следующим тиком.
- [x] Инициализация kafkajs producer при старте модуля (`OnModuleInit`), graceful shutdown (`OnModuleDestroy`).

### 6. Service B — Kafka consumer
- [x] `KafkaConsumerService` (`OnModuleInit`): подписка на `balance-updates`, `eachMessage` handler, ручной commit offset (`autoCommit: false`).
- [x] Обработка сообщения:
  1. Распарсить JSON, провалидировать поля.
  2. Открыть Drizzle-транзакцию:
     - Insert в `inbox` (`idempotency_id` unique) — при конфликте (дубль) откатить транзакцию, считать сообщение обработанным.
     - Upsert `users` (создать с `balance=0`, если не существует).
     - Обновить баланс с optimistic lock + retry (как в service A), до 5 раз.
  3. Commit DB-транзакции.
  4. Вручную закоммитить Kafka offset для этого сообщения.
- [x] Счётчик попыток обработки на сообщение (in-memory по offset или через заголовок retry-count в самом сообщении при re-produce): после 3 неудачных попыток — отправить сообщение в `balance-updates.dlq` через отдельный producer, закоммитить offset, продолжить обработку.

### 7. Общие утилиты
- [x] Обёртка над Drizzle для retry-with-CAS (общая логика между A и B, либо продублировать — решить при код-ревью).
- [x] Kafka config (`brokers`, `clientId`) через `@nestjs/config` + `.env`.
- [x] Логирование через встроенный `Logger` NestJS.

### 8. Docker
- [x] `Dockerfile.service-a` — multi-stage build (deps → build `nest build service-a` → runtime), копирует только нужный `dist/apps/service-a`.
- [x] `Dockerfile.service-b` — аналогично для service-b.
- [x] `.dockerignore` (node_modules, dist, .git).

### 9. docker-compose.yml
- [x] `pg-a`: `postgres:16`, volume, `POSTGRES_DB/USER/PASSWORD`, host-порт 5433.
- [x] `pg-b`: `postgres:16`, volume, host-порт 5434.
- [x] `kafka`: `confluentinc/cp-kafka` (последняя версия с KRaft), env для `KAFKA_PROCESS_ROLES`, `KAFKA_NODE_ID`, `KAFKA_CONTROLLER_QUORUM_VOTERS`, `KAFKA_LISTENERS`/`KAFKA_ADVERTISED_LISTENERS` (внутренний + внешний на 9092), volume для логов.
- [x] `kafka-ui`: `provectuslabs/kafka-ui`, порт 8080, подключение к `kafka`.
- [x] `service-a`: build `Dockerfile.service-a`, `depends_on: [pg-a, kafka]` (healthcheck), env (`DATABASE_URL`, `KAFKA_BROKERS`), порт 3000.
- [x] `service-b`: build `Dockerfile.service-b`, `depends_on: [pg-b, kafka]`, env, порт 3001.
- [x] Healthchecks для `pg-a`/`pg-b`/`kafka`, чтобы `depends_on: condition: service_healthy` работал корректно (миграции по-прежнему запускаются вручную).
- [x] Общая docker-сеть.

### 10. Проверка сквозного сценария
- [x] `docker compose up -d`.
- [x] `docker compose exec service-a npm run db:migrate`.
- [x] `docker compose exec service-b npm run db:migrate`.
- [x] `curl -X POST localhost:3000/transactions -d '{"idempotency_id":"...","user_id":"u1","amount":"10.50"}'`.
- [x] Проверить через `kafka-ui` (localhost:8080), что сообщение попало в `balance-updates`.
- [x] Проверить в `pg-b` (порт 5434), что баланс `u1` обновился и появилась запись в `inbox`.
- [x] Повторить тот же запрос с тем же `idempotency_id` — убедиться, что вернулась исходная транзакция и баланс не изменился повторно.

---

## Заметки по реализации

Структура: корневой `package.json`/`tsconfig.json`/`nest-cli.json` (Nest monorepo, projects `service-a`/`service-b`), схемы и миграции Drizzle сгенерированы (`apps/*/drizzle`), общая утилита `applyBalanceDelta` (retry-with-CAS) продублирована в `apps/*/src/common/balance.util.ts`, `Dockerfile.service-a`/`Dockerfile.service-b` (multi-stage), `docker-compose.yml`, `.env.example`.

Проверено сквозным прогоном: `POST /transactions` → 201 при создании, 200 с той же записью при повторе `idempotency_id`, баланс не задваивается; outbox публикуется в Kafka за ~1с; service B поднимает `inbox`-запись и баланс синхронно с service A; авто-создание пользователя с `balance=0` работает в обеих базах; `class-validator` отклоняет невалидный payload (400).

### Найденные и исправленные проблемы

- **Гонка авто-создания топика**: `KAFKA_AUTO_CREATE_TOPICS_ENABLE=true` создавал `balance-updates` с 1 партицией до того, как наш код успевал явно создать топик с 3 партициями (race между consumer'ом service B и producer'ом service A на старте). Исправлено: `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false` в `docker-compose.yml`, `allowAutoTopicCreation: false` у обоих kafkajs-продюсеров, и explicit `admin.createTopics({ numPartitions: 3 })` (идемпотентно, с игнорированием "already exists") в `KafkaProducerService.onModuleInit` и `KafkaConsumerService.onModuleInit` — оба топика (`balance-updates`, `balance-updates.dlq`) гарантированно создаются с нужным числом партиций до `connect()`/`subscribe()`.
- **Consumer падал при старте**: до фикса выше, subscribe на ещё не существующий топик кидал `KafkaJSProtocolError: UNKNOWN_TOPIC_OR_PARTITION`, который не перехватывался и ронял весь процесс service B. Устранено тем же fix'ом (топик уже существует к моменту `subscribe`).
- **`drizzle-kit` не находился в рантайм-контейнере**: `ENV NODE_ENV=production` был выставлен в Dockerfile *до* `npm ci`, из-за чего npm автоматически пропускал `devDependencies` (в т.ч. `drizzle-kit`), даже без `--omit=dev`. `docker compose exec <service> npm run db:migrate` падал с `drizzle-kit: not found`. Исправлено: `ENV NODE_ENV=production` перенесён на строку после `npm ci` в обоих Dockerfile'ах.
- Первый rebalance/join консьюмера в group `service-b-balance-updates` занимает ~20-25с на holoй ноде Kafka (дефолтные session/rebalance timeouts `cp-kafka` при одном брокере) — это ожидаемое поведение конкретно этого docker-образа, не баг приложения; сообщения, отправленные до этого момента, всё равно подхватываются благодаря `fromBeginning: true`.
