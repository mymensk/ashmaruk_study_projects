"""DAG расчёта батч-признаков NorthCart.

DAG только оркестрирует процесс, вся бизнес-логика лежит в модуле
``calculate_batch_features``. Параметр среза run_date берётся из
Airflow Variable ``batch_features_run_date``. Результат для каждой даты
сохраняется в S3 по отдельному пути и не перезаписывает прошлые прогоны.

Одна и та же логика используется и для исторических срезов, и для
инференса — меняется только run_date.
"""

from datetime import datetime
from pathlib import Path
from typing import Any

import boto3
from airflow import DAG
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from botocore.exceptions import ClientError

# Единая логика расчёта признаков импортируется из модуля.
from calculate_batch_features import build_batch_features, save_features

# Имя DAG, которое будет отображаться в Airflow UI.
DAG_ID = "batch_features"
# Connection ID для чтения необработанных таблиц из PostgreSQL.
POSTGRES_CONN_ID = "ecommerce_db"
# Connection ID для загрузки результата в S3.
S3_CONN_ID = "s3_default"
# Схема в PostgreSQL, где лежат исходные таблицы.
DEFAULT_SOURCE_SCHEMA = "public"
# Локальная папка для временного сохранения файла перед загрузкой в S3.
DEFAULT_OUTPUT_DIR = "/tmp/batch_features"


def get_postgres_conn():
    """Возвращает psycopg2-соединение к PostgreSQL из Airflow Connection.

    Используем сырое DBAPI-соединение (а не SQLAlchemy), потому что
    pandas.read_sql работает с ним напрямую через cursor() и это устойчиво
    к версиям pandas/SQLAlchemy в окружении Airflow.
    """
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    return hook.get_conn()


def get_s3_client_and_bucket() -> tuple[Any, str]:
    """Создаёт S3-клиент и возвращает bucket из Airflow Connection `extra`."""
    connection = BaseHook.get_connection(S3_CONN_ID)
    extras = connection.extra_dejson

    bucket = extras.get("bucket")
    if not bucket:
        raise ValueError(
            f"Missing required `bucket` in extras for connection `{S3_CONN_ID}`"
        )
    # Ключи берём из стандартных полей AWS-коннекшена (login/password),
    # либо из extra — поддерживаем оба варианта заполнения формы.
    client = boto3.client(
        "s3",
        aws_access_key_id=extras.get("aws_access_key_id") or connection.login,
        aws_secret_access_key=extras.get("aws_secret_access_key") or connection.password,
        endpoint_url=extras.get("endpoint_url"),
    )
    return client, bucket


with DAG(
    dag_id=DAG_ID,
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["batch-features"],
) as dag:

    @task(task_id="build_and_upload_features")
    def build_and_upload_features() -> dict[str, str | int]:
        """Считывает run_date, считает batch-признаки и грузит результат в S3."""
        run_date = Variable.get("batch_features_run_date")

        # 1. Считаем признаки на данных строго до run_date (логика — в модуле).
        conn = get_postgres_conn()
        try:
            features = build_batch_features(
                conn, run_date, schema=DEFAULT_SOURCE_SCHEMA
            )
        finally:
            conn.close()

        # 2. Сохраняем во временный файл под конкретный run_date.
        local_path = Path(DEFAULT_OUTPUT_DIR) / f"run_date={run_date}" / "batch_features.csv"
        save_features(features, local_path)

        # 3. Грузим в S3 по отдельному пути для каждой даты среза.
        s3_client, s3_bucket = get_s3_client_and_bucket()
        s3_key = f"run_date={run_date}/batch_features.csv"
        s3_client.upload_file(str(local_path), s3_bucket, s3_key)

        return {
            "run_date": run_date,
            "rows": len(features),
            "s3_bucket": s3_bucket,
            "s3_key": s3_key,
        }

    @task(task_id="validate_saved_result")
    def validate_saved_result(result_info: dict[str, str | int]) -> None:
        """Проверяет, что файл с batch-признаками существует в S3 и не пустой."""
        if int(result_info["rows"]) <= 0:
            raise ValueError("Saved feature file is empty")

        s3_client, _ = get_s3_client_and_bucket()
        s3_bucket = str(result_info["s3_bucket"])
        s3_key = str(result_info["s3_key"])

        try:
            metadata = s3_client.head_object(Bucket=s3_bucket, Key=s3_key)
        except ClientError as error:
            error_code = error.response.get("Error", {}).get("Code")
            if error_code in {"404", "NoSuchKey", "NotFound"}:
                raise FileNotFoundError(
                    f"S3 object was not found: s3://{s3_bucket}/{s3_key}"
                ) from error
            raise

        if int(metadata.get("ContentLength", 0)) <= 0:
            raise ValueError(f"S3 object is empty: s3://{s3_bucket}/{s3_key}")

    validate_saved_result(build_and_upload_features())
