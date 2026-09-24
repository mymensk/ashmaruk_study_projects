# Прогнозирование продаж для сети дарксторов «Прилавок»

Проект модуля 5 (Яндекс Практикум, Data Scientist Plus): пакетное внедрение ML-модели
прогнозирования недельных продаж.

## Описание проекта

**Бизнес-задача.** Сеть дарксторов «Прилавок» (45 дарксторов, ~3000 отделов) планирует закупки
вручную: аналитики тратят 15–20 часов в неделю, ошибки планирования приводят к дефицитам
(потери 3–5% выручки в пиковые периоды) и списаниям скоропортящихся товаров (2–3% выручки).

**Цель.** Автоматизировать прогнозирование недельных продаж на уровне «даркстор — отдел — неделя»
и внедрить модель пакетно: еженедельное переобучение и батч-прогноз на неделю вперёд,
результаты — в витрину Postgres для системы планирования закупок.

**Подход.** Градиентный бустинг (CatBoostRegressor) на стабильных (по PSI) признаках;
обучение — в Jupyter Notebook с сохранением модели в S3; инференс — DAG в Airflow
с единым модулем предобработки.

## Структура репозитория

```
.
├── model_training.ipynb      # Исследовательская часть: EDA, признаки, PSI, обучение CatBoost
├── dags/
│   ├── batch_inference_dag.py  # Airflow DAG батч-инференса
│   └── preprocessing.py        # Общий модуль предобработки (обучение = инференс)
├── requirements.txt
└── README.md
```

## Описание данных

Данные хранятся в PostgreSQL:

| Таблица | Строк | Описание |
|---|---|---|
| `sales` | 421 570 | История продаж: `store`, `dept`, `date`, `weekly_sales` (таргет), `is_holiday`. Период 2023-02-05 — 2025-10-26 |
| `stores` | 45 | Справочник дарксторов: `type` (A/B/C), `size` |
| `features` | 421 570 | Внешние факторы: `temperature`, `fuel_price`, `factor1–5` (промоакции), `cpi`, `unemployment` |
| `plan` | 115 064 | Сетка для прогноза (2025-11-02 — 2026-07-26), без таргета |

Особенности данных:

- 1 285 отрицательных значений продаж (возвраты, 0.3%) — при предобработке заменяются на 0;
- пропуски в `factor2–factor5` (64–74%): акции проводятся не каждую неделю — заполняются средним;
- 11.5% комбинаций «даркстор—отдел—неделя» отсутствуют (отделы открывались/закрывались) —
  не восстанавливаются, лаги строятся merge-ом по точной дате и корректно обходят разрывы;
- дубликатов нет.

## Описание признаков

Признаки создаются функциями модуля `dags/preprocessing.py` (те же функции — в ноутбуке):

| Группа | Признаки | Как созданы |
|---|---|---|
| Временные | `year`, `month`, `quarter`, `week_of_year` | из `date` (`day_of_week` константен — удалён) |
| Агрегированные | `store_avg_sales`, `store_median_sales`, `dept_avg_sales`, `dept_median_sales` | средние/медианные продажи по даркстору и отделу, **только по данным до cutoff-даты** (защита от утечки) |
| Лаговые | `sales_lag_1`, `sales_lag_2`, `sales_lag_4` | merge по `date − N недель` |
| Скользящие | `sales_rolling_mean_4/8/12` | скользящее среднее с `shift(1)` — только прошлые недели |
| Категориальные | `store`, `dept`, `is_holiday`, `type` | передаются в CatBoost нативно |
| Прочие | `size` + внешние факторы | внешние факторы исключены по PSI (см. ниже) |

**Защита от утечек данных:** разделение train/valid строго по времени; агрегаты считаются только
по обучающему периоду; лаги и скользящие используют только прошлое; при инференсе cutoff — первая
дата плана.

## Логика обучения

- **Модель:** `CatBoostRegressor` (loss RMSE, `iterations=800`, `learning_rate=0.1`, `depth=8`,
  `early_stopping_rounds=50`, `random_seed=42`). Подбор гиперпараметров по условию не требуется;
  ранняя остановка по валидации защищает от переобучения (остановилась на 32-й итерации).
- **Разбиение:** train — 2023-02 … 2025-08 (375 744 строк), valid — 2025-09 … 2025-10 (23 094 строк).
- **PSI-анализ:** 10 квантильных бинов по train; исключены 13 признаков с PSI ≥ 0.2:
  календарные (`month`, `week_of_year`, `year`, `quarter` — структурный дрейф короткого
  валидационного окна), макропоказатели (`fuel_price`, `cpi`, `unemployment`), факторы промоакций
  (`factor1–5`), `temperature`. Оставлены 15 стабильных признаков (PSI < 0.1).
- **Метрики:**

| Выборка | MAE | RMSE | R² |
|---|---|---|---|
| train | 2 157 | 5 618 | 0.940 |
| valid | **1 732** | **3 349** | **0.977** |
| бейзлайн (lag-1) | 1 591 | 3 657 | 0.972 |

  Модель превосходит наивный бейзлайн по RMSE; переобучения нет (метрики valid не хуже train).
- **Важность признаков:** доминируют лаги и скользящие средние (`sales_lag_1` ≈ 40%),
  далее `is_holiday` и агрегаты по отделу.
- **Артефакт:** модель сериализуется в pickle и загружается в S3
  (`catboost_sales_model.pkl`, бакет из Variable `s3_bucket_name`).

## Архитектура пайплайна

DAG `sales_prediction_batch_inference` (расписание: `0 20 * * 0` — каждое воскресенье в 20:00):

```
load_data ──> preprocess_features ──┐
                                    ├──> run_inference ──> save_predictions
load_model (S3) ────────────────────┘
```

| Задача | Что делает |
|---|---|
| `load_data` | Собирает в Postgres временную таблицу `inference_data_temp`: история (`sales`) + план (`plan`, таргет NULL) + признаки (`stores`, `features`); логирует число строк и границы дат; в XCom — только первая дата плана |
| `preprocess_features` | Читает `inference_data_temp`, применяет `preprocess_data` (тот же модуль, что при обучении), оставляет строки плана, пропуски лагов заполняет медианой имеющихся значений (ноль означал бы «магазин закрыт»), удаляет NaN; компактный датафрейм — в XCom; временную таблицу удаляет |
| `load_model` | Загружает `catboost_sales_model.pkl` из S3 в память (`io.BytesIO`), сохраняет во временный файл; путь — в XCom |
| `run_inference` | Применяет модель, отрицательные предсказания заменяет на 0; результат — в XCom |
| `save_predictions` | Пишет прогнозы в витрину `predictions` (upsert по `store, dept, date`); логирует количество строк и первые 5 строк |

**Источники данных:** PostgreSQL (таблицы `sales`, `stores`, `features`, `plan`).
**Хранение артефактов:** Yandex Object Storage (S3), эндпоинт `https://storage.yandexcloud.net`.
**Витрина результатов:** таблица `predictions` в той же БД.

**Секреты** в коде не хранятся:

- подключение к Postgres — Airflow Connection `postgres_sales_db`;
- доступ к S3 — Airflow Variables `s3_access_key`, `s3_secret_key`, `s3_bucket_name`, `s3_model_key`.

## Инструкция по запуску

### 1. Окружение для обучения

```bash
pip install -r requirements.txt
```

Задайте переменные окружения (значения — из сниппета активации БД на платформе):

```bash
export DB_NAME=... DB_HOST=... DB_PORT=6432 DB_USER=... DB_PASSWORD=...
export S3_ACCESS_KEY=... S3_SECRET_KEY=... S3_BUCKET=...
```

### 2. Обучение модели

Запустите `model_training.ipynb` (Jupyter). Ноутбук загрузит данные из Postgres, построит
признаки, выполнит PSI-анализ, обучит CatBoost и загрузит `catboost_sales_model.pkl` в S3.

### 3. Настройка Airflow

1. Запустите Airflow сниппетом `===Запуск Airflow===`, указав SSH-адрес этого репозитория.
2. Admin → Connections: создайте `postgres_sales_db` (тип Postgres; host, database, login,
   password, port — из параметров подключения).
3. Admin → Variables: `s3_access_key`, `s3_secret_key`, `s3_bucket_name`,
   `s3_model_key` = `catboost_sales_model.pkl`.

### 4. Запуск инференса

В интерфейсе Airflow найдите DAG `sales_prediction_batch_inference` и нажмите **Trigger DAG**.
После успешного выполнения прогнозы появятся в таблице `predictions` (115 064 строки на период
2025-11-02 — 2026-07-26).

Проверка результата:

```sql
SELECT COUNT(*) FROM predictions;
SELECT * FROM predictions ORDER BY store, dept, date LIMIT 5;
```

## In English

**Dark store weekly sales forecast with batch inference in Airflow**

CatBoostRegressor on PSI-stable features (lags, rolling means, aggregates), model stored in S3, weekly batch inference DAG in Airflow sharing one preprocessing module with training.

**Key result:** R² = 0.977, model in S3, inference DAG

_Notebooks and code comments are in Russian._
