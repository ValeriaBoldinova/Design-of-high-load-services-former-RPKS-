# BigDataFlink
Анализ больших данных - лабораторная работа №3 - Streaming processing с помощью Flink

## Запуск

### Предварительный сброс (если Docker запускался ранее)

```bash
docker compose down -v
```

### Запуск лабораторной

```bash
./run_all.sh
```

Скрипт выполняет следующие шаги:
1. Скачивает JAR-коннектор Flink → Kafka (однократно, сохраняется в `jars/`)
2. Собирает Docker-образы для Flink-джобы и Kafka-продюсера
3. Поднимает **PostgreSQL** и **Kafka** (KRaft, без Zookeeper), ждёт их готовности
4. Создаёт Kafka-топик `pet_store_sales`
5. Запускает **Flink-джобу** — читает топик и пишет данные в схему «звезда» в PostgreSQL
6. Запускает **Kafka-продюсер** — читает 10 CSV-файлов и отправляет каждую строку как JSON-сообщение в топик
7. Ждёт завершения обработки и выводит количество строк по таблицам

> Flink-джоба продолжает работать после завершения скрипта (streaming-режим).  
> Для остановки всех контейнеров: `docker compose down`

---

## Проверка результатов

### Способ 1 — скрипт verify.sh

```bash
./verify.sh
```

Выводит в консоль:
- количество строк в каждой таблице схемы «звезда»
- продажи по месяцам
- топ-3 покупателей по сумме
- топ-5 магазинов по выручке
- средний чек по категориям товаров
- продажи по типу питомца
- проверку FK-целостности (все значения должны быть 0)

### Способ 2 — подключение к PostgreSQL вручную (DBeaver или psql)

| Параметр | Значение |
|----------|----------|
| Host     | `localhost` |
| Port     | `5436` |
| Database | `lab3` |
| User     | `flink` |
| Password | `flink` |

Подключение через psql:
```bash
docker exec -it lab3_postgres psql -U flink -d lab3
```

Или через DBeaver с параметрами выше.

**Основные запросы для проверки:**

```sql
-- Количество строк в каждой таблице
SELECT table_name, count FROM (
    SELECT 'dim_customer' AS table_name, COUNT(*) AS count FROM dim_customer
    UNION ALL SELECT 'dim_seller',  COUNT(*) FROM dim_seller
    UNION ALL SELECT 'dim_product', COUNT(*) FROM dim_product
    UNION ALL SELECT 'dim_store',   COUNT(*) FROM dim_store
    UNION ALL SELECT 'dim_supplier',COUNT(*) FROM dim_supplier
    UNION ALL SELECT 'dim_date',    COUNT(*) FROM dim_date
    UNION ALL SELECT 'fact_sales',  COUNT(*) FROM fact_sales
) t ORDER BY table_name;

-- Продажи по месяцам
SELECT d.month_name, COUNT(*) AS sales, ROUND(SUM(f.total_price)::numeric, 2) AS revenue
FROM fact_sales f
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY d.month, d.month_name ORDER BY d.month;

-- Топ-3 покупателей по сумме
SELECT c.first_name || ' ' || c.last_name AS customer,
       COUNT(*) AS orders, ROUND(SUM(f.total_price)::numeric, 2) AS total_spent
FROM fact_sales f
JOIN dim_customer c ON f.customer_id = c.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name
ORDER BY total_spent DESC LIMIT 3;

-- Проверка FK-целостности (все должны быть 0)
SELECT
    (SELECT COUNT(*) FROM fact_sales WHERE store_id    IS NULL) AS null_stores,
    (SELECT COUNT(*) FROM fact_sales WHERE supplier_id IS NULL) AS null_suppliers,
    (SELECT COUNT(*) FROM fact_sales WHERE date_id     IS NULL) AS null_dates;
```

---

Одним из самых популярных фреймворков для работы со streaming processing является Apache Flink. Apache Flink - мощный фреймворк, который предлагает широкий набор функциональности для простого написания streaming processing.

Что необходимо сделать? 

Необходимо реализовать потоковую обработку данных с помощью Flink, который читает топик Kafka, трансформирует данные в режиме streaming в модель звезда и пишет результат в PostgreSQL. Данные в Kafka-топиках хранятся в формате json. Данные в топик kafka нужно отправлять самостоятельно, эмулируя источник данных.

Какие данные отправляются в Kafka?
 - Каждое сообщение в Kafka-топике - это строчка из csv файлов, преобразованная в формат json.

Какие данные отправляются в PostgreSQL?
 - Трансформированные данные в модель данных звезда.

![Лабораторная работа №3](https://github.com/user-attachments/assets/d3c1544d-3fe6-4c15-b673-9aa5d27dbd76)


Алгоритм:

1. Клонируете к себе этот репозиторий.
2. Устанавливаете инструмент для работы с запросами SQL (рекомендую DBeaver).
3. Устанавливаете базу данных PostgreSQL (рекомендую установку через docker).
4. Устанавливаете Apache Flink (рекомендую установку через Docker).
5. Устанавливаете Apache Kafka (рекомендую установку через Docker).
6. Скачиваете файлы с исходными данными mock_data( * ).csv, где ( * ) номера файлов. Всего 10 файлов, каждый по 1000 строк.
7. Реализуете приложение, которое каждую строчку из исходных csv-файлов преобразует в json и отправляет в виде сообщения в Kafka-топик.
8. Реализуете приложение на Flink, которое читает Kafka-топик, преобразует данные в модель звезда и сохраняет в PostgreSQL в режиме streaming.
9. Проверяете конечные данные в PostgreSQL.
10. Отправляете работу на проверку лаборантам.

Что должно быть результатом работы?

1. Репозиторий, в котором есть исходные данные mock_data().csv, где () номера файлов. Всего 10 файлов, каждый по 1000 строк.
2. Файл docker-compose.yml с установкой PostgreSQL, Flink, Kafka и запуском приложения, которое из файлов mock_data(*).csv создает сообщения json в Kafka.
3. Инструкция, как запускать Flink-джобу и приложение для отправки данных в Kafka для проверки лабораторной работы.
4. Код Apache Flink для трансформации данных в режиме streaming.
