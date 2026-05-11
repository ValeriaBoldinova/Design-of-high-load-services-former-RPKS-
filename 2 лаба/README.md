# BigDataSpark

Анализ больших данных - лабораторная работа №2 - ETL реализованный с помощью Spark

## Запуск

### Предварительный сброс (если Docker запускался ранее)

Если контейнеры уже запускались, перед новым запуском необходимо удалить их вместе с volumes:

```bash
docker compose down -v
```

### Запуск лабораторной

```bash
docker compose up -d
```
Поднимает три контейнера: **PostgreSQL** (с автоимпортом CSV-данных), **ClickHouse** и **Spark**. Ждёт их готовности через healthcheck.

```bash
./run_all.sh
```
Последовательно запускает три Spark-джоба:
1. `1_etl_star_schema.py` — читает исходные данные из `mock_data` в PostgreSQL, строит схему «звезда» (`dim_customer`, `dim_product`, `dim_date`, `fact_sales`).
2. `2_etl_clickhouse.py` — читает схему «звезда» из PostgreSQL и формирует 6 аналитических отчётов в ClickHouse.
3. `verify_results.py` — проверяет результаты: выводит содержимое таблиц PostgreSQL и всех 6 отчётов ClickHouse.

---

## Проверка результатов

### Способ 1 — автоматически (встроен в run_all.sh)

`verify_results.py` запускается автоматически в конце `run_all.sh` и выводит в консоль:
- таблицы схемы «звезда» в PostgreSQL с количеством строк
- все 6 отчётных таблиц ClickHouse с количеством строк и первыми 10 записями

Для ручного повторного запуска:
```bash
docker exec lab2_spark /opt/spark/bin/spark-submit \
  --master local[*] \
  --jars /opt/spark-jars/postgresql-42.7.3.jar,/opt/spark-jars/clickhouse-jdbc-0.6.3-all.jar \
  /opt/spark-apps/verify_results.py
```

### Способ 2 — подключение к базам данных вручную

#### PostgreSQL

| Параметр | Значение |
|----------|----------|
| Host     | `localhost` |
| Port     | `5435` |
| Database | `lab2` |
| User     | `spark` |
| Password | `spark` |

Подключение через psql:
```bash
docker exec -it lab2_postgres psql -U spark -d lab2
```

Или через DBeaver / любой SQL-клиент с параметрами выше.

**Основные запросы для проверки (каждую группу запросов выполнять отдельно):**

```sql
-- Список таблиц схемы звезда
SELECT table_name FROM information_schema.tables
WHERE table_schema = 'public'
ORDER BY table_name;

-- Количество строк в каждой таблице
SELECT 'mock_data'    AS tbl, COUNT(*) FROM mock_data    UNION ALL
SELECT 'dim_customer' AS tbl, COUNT(*) FROM dim_customer UNION ALL
SELECT 'dim_product'  AS tbl, COUNT(*) FROM dim_product  UNION ALL
SELECT 'dim_date'     AS tbl, COUNT(*) FROM dim_date     UNION ALL
SELECT 'fact_sales'   AS tbl, COUNT(*) FROM fact_sales
ORDER BY tbl;

-- Топ-10 самых продаваемых продуктов (проверка факт-таблицы)
SELECT p.name, SUM(f.quantity) AS total_qty
FROM fact_sales f
JOIN dim_product p USING (product_id)
GROUP BY p.name
ORDER BY total_qty DESC
LIMIT 10;
```

---

#### ClickHouse

| Параметр | Значение |
|----------|----------|
| Host     | `localhost` |
| HTTP Port | `8123` |
| Native Port | `9000` |
| Database | `default` |
| User     | `default` |
| Password | *(пустой)* |

Подключение через clickhouse-client:
```bash
docker exec -it lab2_clickhouse clickhouse-client
```

Или через DBeaver с драйвером ClickHouse на порту `8123`.

**Основные запросы для проверки (каждую группу запросов выполнять отдельно (витрины тоже по одной выполнять)):**

```sql
-- Список всех отчётных таблиц
SHOW TABLES;

-- Количество строк в каждом отчёте
SELECT 'report1_product_sales'   AS report, count() FROM report1_product_sales   UNION ALL
SELECT 'report2_customer_sales'  AS report, count() FROM report2_customer_sales  UNION ALL
SELECT 'report3_time_sales'      AS report, count() FROM report3_time_sales      UNION ALL
SELECT 'report4_store_sales'     AS report, count() FROM report4_store_sales     UNION ALL
SELECT 'report5_supplier_sales'  AS report, count() FROM report5_supplier_sales  UNION ALL
SELECT 'report6_product_quality' AS report, count() FROM report6_product_quality;

-- Витрина 1: топ продукты по выручке
SELECT * FROM report1_product_sales ORDER BY total_revenue DESC LIMIT 10;

-- Витрина 2: топ клиенты по сумме покупок
SELECT * FROM report2_customer_sales ORDER BY total_amount DESC LIMIT 10;

-- Витрина 3: тренды продаж по месяцам
SELECT * FROM report3_time_sales ORDER BY year, month;

-- Витрина 4: топ магазины по выручке
SELECT * FROM report4_store_sales ORDER BY total_revenue DESC LIMIT 5;

-- Витрина 5: топ поставщики по выручке
SELECT * FROM report5_supplier_sales ORDER BY total_revenue DESC LIMIT 5;

-- Витрина 6: качество продукции
SELECT * FROM report6_product_quality ORDER BY rating DESC LIMIT 10;
```

---

Одним из самых популярных фреймворков для работы с Big Data является Apache Spark. Apache Spark - мощный фреймворк, который предлагает широкий набор функциональности для простого написания ETL-пайплайнов.

Что необходимо сделать? 

Необходимо реализовать ETL-пайплайн с помощью Spark, который трансформирует данные из источника (файлы mock_data.csv с номерами) в модель данных звезда в PostgreSQL, а затем на основе модели данных звезда создать ряд отчетов по данным в одной из NoSQL базах данных обязательно и в нескольких других опционально (будет бонусом). Каждый отчет представляет собой отдельную таблицу в NoSQL БД.

Какие отчеты надо создать?
1. Витрина продаж по продуктам
Цель: Анализ выручки, количества продаж и популярности продуктов.
 - Топ-10 самых продаваемых продуктов.
 - Общая выручка по категориям продуктов.
 - Средний рейтинг и количество отзывов для каждого продукта.
2. Витрина продаж по клиентам
Цель: Анализ покупательского поведения и сегментация клиентов.
 - Топ-10 клиентов с наибольшей общей суммой покупок.
 - Распределение клиентов по странам.
 - Средний чек для каждого клиента.
3. Витрина продаж по времени
Цель: Анализ сезонности и трендов продаж.
 - Месячные и годовые тренды продаж.
 - Сравнение выручки за разные периоды.
 - Средний размер заказа по месяцам.
4. Витрина продаж по магазинам
Цель: Анализ эффективности магазинов.
 - Топ-5 магазинов с наибольшей выручкой.
 - Распределение продаж по городам и странам.
 - Средний чек для каждого магазина.
5. Витрина продаж по поставщикам
Цель: Анализ эффективности поставщиков.
 - Топ-5 поставщиков с наибольшей выручкой.
 - Средняя цена товаров от каждого поставщика.
 - Распределение продаж по странам поставщиков.
6. Витрина качества продукции
Цель: Анализ отзывов и рейтингов товаров.
 - Продукты с наивысшим и наименьшим рейтингом.
 - Корреляция между рейтингом и объемом продаж.
 - Продукты с наибольшим количеством отзывов.

В каких NoSQL БД должны быть эти отчеты:
1. **Clickhouse** **(обязательно)**
2. Cassandra (опционально, если будет реализация, то это бонус)
3. Neo4J (опционально, если будет реализация, то это бонус)
4. MongoDB (опционально, если будет реализация, то это бонус)
5. Valkey (опционально, если будет реализация, то это бонус)

![Лабораторная работа №2](https://github.com/user-attachments/assets/2b854382-4c36-4542-a7fb-04fe82a6f6fa)


Алгоритм:

1. Клонируете к себе этот репозиторий.
2. Устанавливаете себе инструмент для работы с запросами SQL (рекомендую DBeaver).
3. Устанавливаете базу данных PostgreSQL (рекомендую установку через docker).
4. Устанавливаете Apache Spark (рекомендую установку через Docker. Для удобства написания кода на Python можно запустить вместе со JupyterNotebook. Для Java - подключить volume и собрать образ Docker, который будет запускать команду spark-submit с java jar-файлом при старте контейнера, сам jar файл собирается отдельно и кладется в подключенный volume)
5. Скачиваете файлы с исходными данными mock_data( * ).csv, где ( * ) номера файлов. Всего 10 файлов, каждый по 1000 строк.
6. Импортируете данные в БД PostgreSQL (например, через механизм импорта csv в DBeaver). Всего в таблице mock_data должно находиться 10000 строк из 10 файлов.
7. Анализируете исходные данные с помощью запросов.
8. Выявляете сущности фактов и измерений.
9. Реализуете приложение на Spark, которое по аналогии с первой лабораторной работой перекладывает исходные данные из PostgreSQL в модель снежинку/звезда в PostgreSQL. (Убедитесь в коннективности Spark и PostgreSQL, настройте сеть между Spark и PostgreSQL, если используете Docker).
10. Устанавливаете ClickHouse (рекомендую установку через Docker. Убедитесь в коннективности Spark и Clickhouse, настройте сеть между Spark и ClickHouse). **(обязательно)**
11. Реализуете приложение на Spark, которое создаёт все 6 перечисленных выше отчетов в виде 6 отдельных таблиц в ClickHouse. **(обязательно)**
12. Устанавливаете Cassandra (рекомендую установку через Docker. Убедитесь в коннективности Spark и Cassandra, настройте сеть между Spark и Cassandra). (опционально)
13. Реализуете приложение на Spark, которое создаёт все 6 перечисленных выше отчетов в виде 6 отдельных таблиц в Cassandra. (опционально)
14. Устанавливаете Neo4j (рекомендую установку через Docker. Убедитесь в коннективности Spark и Neo4j, настройте сеть между Spark и Neo4j). (опционально)
15. Реализуете приложение на Spark, которое создаёт все 6 перечисленных выше отчетов в виде отдельных сущностей в Neo4j. (опционально)
16. Устанавливаете MongoDB (рекомендую установку через Docker. Убедитесь в коннективности Spark и MongoDB, настройте сеть между Spark и MongoDB). (опционально)
17. Реализуете приложение на Spark, которое создаёт все 6 перечисленных выше отчетов в виде 6 отдельных коллекций в MongoDB. (опционально)
18. Устанавливаете Valkey (рекомендую установку через Docker. Убедитесь в коннективности Spark и Valkey, настройте сеть между Spark и Valkey). (опционально)
19. Реализуете приложение на Spark, которое создаёт все 6 перечисленных выше отчетов в виде отдельных записей в Valkey. (опционально)
20. Проверяете отчеты в каждой базе данных средствами языка самой БД (ClickHouse - SQL (DBeaver), Cassandra - CQL (DBeaver), Neo4J - Cipher (DBeaver), MongoDB - MQL (Compass), Valkey - redis-cli).
21. Отправляете работу на проверку лаборантам.

Что должно быть результатом работы?

1. Репозиторий, в котором есть исходные данные mock_data().csv, где () номера файлов. Всего 10 файлов, каждый по 1000 строк.
2. Файл docker-compose.yml с установкой PostgreSQL, Spark, ClickHouse **(обязательно)**, Cassandra (опционально), Neo4j (опционально), MongoDB (опционально), Valkey (опционально) и заполненными данными в PostgreSQL из файлов mock_data(*).csv.
3. Инструкция, как запускать Spark-джобы для проверки лабораторной работы.
4. Код Apache Spark трансформации данных из исходной модели в снежинку/звезду в PostgreSQL.
5. Код Apache Spark трансформации данных из снежинки/звезды в отчеты в ClickHouse.
6. Код Apache Spark трансформации данных из снежинки/звезды в отчеты в Cassandra.
7. Код Apache Spark трансформации данных из снежинки/звезды в отчеты в Neo4j.
8. Код Apache Spark трансформации данных из снежинки/звезды в отчеты в MongoDB.
9. Код Apache Spark трансформации данных из снежинки/звезды в отчеты в Valkey.
