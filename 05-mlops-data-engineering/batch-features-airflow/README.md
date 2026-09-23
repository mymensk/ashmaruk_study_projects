# Проект спринта 19 — Пайплайн батч-признаков в Airflow (NorthCart)

DAG в Apache Airflow, который рассчитывает воспроизводимые **батч-признаки**
для пары `customer_id` + `run_date`: читает данные из PostgreSQL,
считает признаки по окнам 7 и 30 дней (только по данным строго до `run_date`,
без утечки из будущего) и сохраняет результат в S3.

Одна и та же логика используется и для **исторических** срезов (обучение),
и для **инференсного** среза — меняется только дата `run_date`.

## Что делает DAG

1. Берёт `run_date` из Airflow Variable `batch_features_run_date`.
2. Читает таблицы `customers`, `sessions`, `events`, `orders` из PostgreSQL.
3. Вызывает единую логику расчёта из модуля `dags/calculate_batch_features.py`.
4. Сохраняет таблицу признаков в S3 по отдельному пути `run_date=<дата>/batch_features.csv`.
5. Валидирует результат (файл существует в S3 и не пустой).

## Структура репозитория

```
dags/
  calculate_batch_features.py   # логика: чтение, предобработка, расчёт, сохранение
  batch_features.py             # DAG: только оркестрация, импортирует функции модуля
notebooks/
  features_eda.ipynb            # EDA и отладка расчёта (вспомогательно, не для прода)
requirements.txt
README.md
```

Вся бизнес-логика — в `calculate_batch_features.py`. DAG и ноутбук её не дублируют.

## Рассчитываемые признаки (на пару customer_id + run_date)

| Признак | Окна | Источник |
|---|---|---|
| `page_view_{7,30}d` — число просмотров страниц | 7, 30 | events |
| `add_to_cart_{7,30}d` — число добавлений в корзину | 7, 30 | events |
| `cr_view_cart_{7,30}d` — конверсия `add_to_cart / page_view` | 7, 30 | events |
| `cr_cart_purchase_{7,30}d` — конверсия `purchase / add_to_cart` | 7, 30 | events |
| `unique_products_{7,30}d` — уникальные `product_id` | 7, 30 | events (где задан) |
| `session_count_{7,30}d` — число сессий | 7, 30 | sessions |
| `avg_session_sec_30d` — средняя длина сессии (сек) | 30 | sessions + events |
| `days_since_last_purchase` — дней с последней покупки (−1 если покупок нет) | — | orders |
| `order_count_30d` — число заказов | 30 | orders |
| `sum_total_usd_30d` — суммарная выручка | 30 | orders |
| `avg_total_usd_30d` — средний чек | 30 | orders |

**Правила обработки краевых случаев:** деление на ноль → `0.0`; счётчики при
отсутствии данных → `0`; вещественные/денежные → `0.0`; давность покупки → `−1`;
сессия без событий → длина `0`; нечисловой `total_usd` → `0.0`; строки с
невалидными датами в расчёт не попадают; события дедуплицируются по `event_id`.

**Защита от утечки:** в расчёт идут только `timestamp < run_date`,
`start_time < run_date`, `order_time < run_date`.

## Запуск

### 1. Зависимости

```bash
pip install -r requirements.txt
```

Окружение: Airflow 2.7+ с провайдерами `postgres` и `amazon`.
Модуль `dags/calculate_batch_features.py` лежит рядом с DAG в папке `dags/`,
поэтому импорт `from calculate_batch_features import ...` работает из коробки.

### 2. Airflow Variables

| Variable | Значение | Назначение |
|---|---|---|
| `batch_features_run_date` | напр. `2025-09-01` | дата среза `run_date` |

### 3. Airflow Connections

**`ecommerce_db`** — тип *Postgres*. Хост, порт, БД, логин и пароль берутся из
сниппета `===Активация БД===`. Секреты хранятся только в Connection, не в коде.

**`s3_default`** — тип *Amazon Web Services*. Ключи задаются в полях
**AWS Access Key ID** и **AWS Secret Access Key**, а в поле **Extra** — только
бакет и endpoint:

```json
{
  "bucket": "<ваш S3-бакет>",
  "endpoint_url": "https://storage.yandexcloud.net"
}
```

### 4. Прогон DAG

DAG ручной (`schedule=None`). Запускается из Airflow UI:

1. Установить Variable `batch_features_run_date = 2025-09-01` → запустить DAG
   (исторический срез).
2. Поменять Variable на `2025-10-01` → запустить DAG снова (поздний срез).

Результаты лежат раздельно: `run_date=2025-09-01/...` и `run_date=2025-10-01/...`.

### 5. Проверка результата

Задача `validate_saved_result` в DAG проверяет, что объект в S3 существует и
не пустой. Дополнительно можно глазами сверить, что в S3 появились два префикса
`run_date=...`, набор колонок в обоих одинаковый, а различаются только значения.

## Доступ к данным для ревью

Параметры выдаёт сниппет `===Активация БД===`

- **База данных (dbname):** `<your-db>`
- **user:** `<your-db>`

Пароль и ключи S3 в репозитории не хранятся — они задаются только в
Airflow Connections.
