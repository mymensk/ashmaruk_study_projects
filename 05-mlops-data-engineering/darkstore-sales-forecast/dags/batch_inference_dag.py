"""
DAG для batch-инференса продаж "Прилавок".

Пайплайн: загрузка данных из Postgres -> предобработка признаков ->
загрузка модели из S3 -> батч-инференс -> запись предсказаний в витрину.

Секреты не хранятся в коде: подключение к Postgres — через Airflow Connection
(postgres_sales_db), ключи S3 — через Airflow Variables.
"""

from datetime import datetime, timedelta

import logging

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator

# Тяжёлые импорты (pandas, boto3, pickle, preprocessing) выполняются
# внутри функций тасков: Airflow Scheduler пересканирует dags/ каждые
# 30–60 секунд, и импорты на уровне модуля выполнялись бы при каждом
# цикле парсинга, а не только при реальном запуске задачи на воркере.

# ========== Конфигурация ==========

S3_BUCKET = Variable.get("s3_bucket_name", default_var="your-bucket-name")
S3_ACCESS_KEY = Variable.get("s3_access_key", default_var=None)
S3_SECRET_KEY = Variable.get("s3_secret_key", default_var=None)
S3_MODEL_KEY = Variable.get("s3_model_key", default_var="catboost_sales_model.pkl")
S3_ENDPOINT_URL = "https://storage.yandexcloud.net"

POSTGRES_CONN_ID = "postgres_sales_db"  # ID подключения в Airflow Connections

CAT_FEATURES = ['store', 'dept', 'is_holiday', 'type']

# Колонки с лагами и скользящими средними — их NaN заполняются медианой
LAG_ROLLING_COLS = [
    'sales_lag_1', 'sales_lag_2', 'sales_lag_4',
    'sales_rolling_mean_4', 'sales_rolling_mean_8', 'sales_rolling_mean_12',
]

# Итоговый набор признаков модели (стабильные по PSI, см. model_training.ipynb).
# Должен совпадать с признаками, на которых обучена модель в S3.
FINAL_FEATURES = [
    'store', 'dept', 'is_holiday', 'type', 'size',
    'store_avg_sales', 'store_median_sales',
    'dept_avg_sales', 'dept_median_sales',
    'sales_lag_1', 'sales_lag_2', 'sales_lag_4',
    'sales_rolling_mean_4', 'sales_rolling_mean_8', 'sales_rolling_mean_12',
]

logger = logging.getLogger(__name__)


# ========== Задачи DAG ==========

def load_data_from_postgres(**context):
    """
    Загрузка данных для инференса из Postgres.

    Создаёт таблицу inference_data_temp: исторические продажи (sales)
    и плановые строки (plan, weekly_sales = NULL), обогащённые признаками
    из stores и features. Большие данные через XCom не передаются —
    в XCom уходит только первая дата плана.
    """
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS inference_data_temp;")
    conn.commit()

    # Исторические данные + план в единой структуре с признаками
    create_query = """
    CREATE TABLE inference_data_temp AS
    SELECT
        s.store,
        s.dept,
        s.date,
        s.weekly_sales,
        s.is_holiday,
        st.type,
        st.size,
        f.temperature,
        f.fuel_price,
        f.factor1, f.factor2, f.factor3, f.factor4, f.factor5,
        f.cpi,
        f.unemployment
    FROM sales AS s
    LEFT JOIN stores AS st ON st.store = s.store
    LEFT JOIN features AS f
        ON f.store = s.store AND f.dept = s.dept AND f.date = s.date

    UNION ALL

    SELECT
        p.store,
        p.dept,
        p.date,
        NULL AS weekly_sales,
        p.is_holiday,
        st.type,
        st.size,
        f.temperature,
        f.fuel_price,
        f.factor1, f.factor2, f.factor3, f.factor4, f.factor5,
        f.cpi,
        f.unemployment
    FROM plan AS p
    LEFT JOIN stores AS st ON st.store = p.store
    LEFT JOIN features AS f
        ON f.store = p.store AND f.dept = p.dept AND f.date = p.date;
    """
    cursor.execute(create_query)
    conn.commit()

    cursor.execute("""
        SELECT COUNT(*), MIN(date), MAX(date) FROM inference_data_temp;
    """)
    n_rows, min_date, max_date = cursor.fetchone()
    logging.info("inference_data_temp: строк = %s", n_rows)
    logging.info("inference_data_temp: минимальная дата = %s", min_date)
    logging.info("inference_data_temp: максимальная дата = %s", max_date)

    cursor.execute("SELECT MIN(date) FROM plan;")
    first_plan_date = cursor.fetchone()[0]
    logging.info("Первая дата плана: %s", first_plan_date)

    cursor.close()
    conn.close()

    context['ti'].xcom_push(key='first_plan_date', value=str(first_plan_date))


def preprocess_features(**context):
    """
    Предобработка признаков для инференса.

    Читает inference_data_temp, применяет preprocess_data (логика идентична
    обучению), оставляет только плановые строки и передаёт компактный
    датафрейм в XCom. Временная таблица удаляется.
    """
    import pandas as pd

    from preprocessing import preprocess_data

    ti = context['ti']
    first_plan_date = ti.xcom_pull(task_ids='load_data', key='first_plan_date')
    logging.info("Первая дата плана из XCom: %s", first_plan_date)

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    df = pg_hook.get_pandas_df("SELECT * FROM inference_data_temp;")
    logging.info("Загружено строк из inference_data_temp: %d", len(df))

    # Предобработка идентична обучению; агрегаты считаются только по истории
    df = preprocess_data(df, cutoff_date=first_plan_date)

    # Оставляем только строки плана
    df = df[df['date'] >= pd.Timestamp(first_plan_date)].copy()

    # Целевой колонки в плане нет (всегда NULL) — удаляем
    df = df.drop(columns=['weekly_sales'])

    # Пропуски в лагах и скользящих средних заполняем медианой имеющихся
    # значений: для плановых недель 2+ реальных продаж ещё нет, и ноль
    # означал бы для модели «магазин закрыт» — паттерн, которого она
    # не видела при обучении. Медиана заметно ближе к реальности.
    for col in LAG_ROLLING_COLS:
        if col in df.columns:
            median_val = df[col].median()
            df[col] = df[col].fillna(median_val if not pd.isna(median_val) else 0)

    # Оставляем только ключи и признаки модели — не тащим лишние колонки в XCom
    df = df[['date'] + FINAL_FEATURES]

    # Удаляем строки с оставшимися NaN
    n_before = len(df)
    df = df.dropna()
    logging.info("Удалено строк с NaN: %d, осталось: %d", n_before - len(df), len(df))

    df['date'] = df['date'].dt.strftime('%Y-%m-%d')
    ti.xcom_push(key='inference_df', value=df.to_json(orient='records'))

    # Удаляем временную таблицу
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS inference_data_temp;")
    conn.commit()
    cursor.close()
    conn.close()
    logging.info("Временная таблица inference_data_temp удалена")


def load_model_from_s3(**context):
    """
    Загрузка обученной модели CatBoost из S3 (Yandex Cloud) через pickle.

    Модель сохраняется в файл с фиксированным путём, путь передаётся через XCom.
    """
    import io
    import pickle

    import boto3

    s3_client = boto3.client(
        's3',
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
    )

    buffer = io.BytesIO()
    s3_client.download_fileobj(S3_BUCKET, S3_MODEL_KEY, buffer)
    buffer.seek(0)
    model = pickle.load(buffer)
    logging.info("Модель загружена из s3://%s/%s", S3_BUCKET, S3_MODEL_KEY)

    # Фиксированный путь вместо NamedTemporaryFile: случайные имена tmp*
    # могут удаляться процессами очистки /tmp между тасками
    # в Kubernetes-окружении, что приводит к FileNotFoundError.
    local_model_path = '/tmp/catboost_model.pkl'
    with open(local_model_path, 'wb') as f:
        pickle.dump(model, f)
    logging.info("Модель сохранена в файл: %s", local_model_path)

    context['ti'].xcom_push(key='model_path', value=local_model_path)


def run_batch_inference(**context):
    """
    Batch-инференс: применение модели к подготовленным данным.

    Аномальные (отрицательные) предсказания заменяются на 0.
    """
    import io
    import pickle

    import pandas as pd

    ti = context['ti']
    inference_json = ti.xcom_pull(task_ids='preprocess_features', key='inference_df')
    model_path = ti.xcom_pull(task_ids='load_model', key='model_path')

    df = pd.read_json(io.StringIO(inference_json), orient='records')
    logging.info("Данные для инференса: %d строк", len(df))

    with open(model_path, 'rb') as f:
        model = pickle.load(f)

    feature_names = model.feature_names_
    X = df[feature_names].copy()
    for col in CAT_FEATURES:
        if col in X.columns:
            X[col] = X[col].astype(str)

    df['predicted_weekly_sales'] = model.predict(X)

    # Обработка аномальных предсказаний: отрицательные -> 0
    n_neg = int((df['predicted_weekly_sales'] < 0).sum())
    df.loc[df['predicted_weekly_sales'] < 0, 'predicted_weekly_sales'] = 0.0
    logging.info("Аномальных (отрицательных) предсказаний заменено на 0: %d", n_neg)

    result = df[['store', 'dept', 'date', 'predicted_weekly_sales']]
    ti.xcom_push(key='predictions_df', value=result.to_json(orient='records'))


def save_predictions_to_postgres(**context):
    """
    Запись результатов предсказаний в таблицу predictions в Postgres.
    """
    import io

    import pandas as pd

    predictions_json = context['ti'].xcom_pull(
        task_ids='run_inference', key='predictions_df'
    )

    predictions_df = pd.read_json(io.StringIO(predictions_json), orient='records')

    # Преобразуем date в правильный формат для PostgreSQL
    predictions_df['date'] = pd.to_datetime(predictions_df['date']).dt.date

    # Подключение к БД
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    drop_table_query = """
    DROP TABLE IF EXISTS predictions;
    """
    cursor.execute(drop_table_query)
    conn.commit()

    create_table_query = """
    CREATE TABLE IF NOT EXISTS predictions (
        store INT,
        dept INT,
        date DATE,
        predicted_weekly_sales FLOAT,
        prediction_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (store, dept, date)
    );
    """
    cursor.execute(create_table_query)
    conn.commit()

    from psycopg2.extras import execute_values

    values = predictions_df[
        ['store', 'dept', 'date', 'predicted_weekly_sales']
    ].to_records(index=False).tolist()

    insert_query = """
    INSERT INTO predictions (store, dept, date, predicted_weekly_sales)
    VALUES %s
    ON CONFLICT (store, dept, date)
    DO UPDATE SET
        predicted_weekly_sales = EXCLUDED.predicted_weekly_sales,
        prediction_timestamp = CURRENT_TIMESTAMP;
    """

    execute_values(cursor, insert_query, values, page_size=10_000)
    conn.commit()

    # Количество строк итоговой таблицы и первые 5 строк
    cursor.execute("SELECT COUNT(*) FROM predictions;")
    n_rows = cursor.fetchone()[0]
    logging.info("Всего строк в таблице predictions: %s", n_rows)

    cursor.execute("""
        SELECT store, dept, date, predicted_weekly_sales, prediction_timestamp
        FROM predictions
        ORDER BY store, dept, date
        LIMIT 5;
    """)
    for row in cursor.fetchall():
        logging.info("predictions: %s", row)

    cursor.close()
    conn.close()


# ========== Определение DAG ==========

default_args = {
    'owner': 'Aleks',
    'depends_on_past': False,
    'email': ['your-email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'sales_prediction_batch_inference',
    default_args=default_args,
    description='Batch-инференс прогнозирования продаж для Прилавка',
    schedule_interval='0 20 * * 0',  # Каждое воскресенье в 20:00
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['sales', 'ml', 'batch-inference', 'production'],
)

task_load_data = PythonOperator(
    task_id='load_data',
    python_callable=load_data_from_postgres,
    provide_context=True,
    dag=dag,
)

task_preprocess = PythonOperator(
    task_id='preprocess_features',
    python_callable=preprocess_features,
    provide_context=True,
    dag=dag,
)

task_load_model = PythonOperator(
    task_id='load_model',
    python_callable=load_model_from_s3,
    provide_context=True,
    dag=dag,
)

task_inference = PythonOperator(
    task_id='run_inference',
    python_callable=run_batch_inference,
    provide_context=True,
    dag=dag,
)

task_save_predictions = PythonOperator(
    task_id='save_predictions',
    python_callable=save_predictions_to_postgres,
    provide_context=True,
    dag=dag,
)

task_load_data >> task_preprocess
task_load_model >> task_inference
task_preprocess >> task_inference >> task_save_predictions
