"""
Проект «Книжные рекомендации» — ETL-пайплайн на PySpark.

Запуск через spark-submit:
    spark-submit --master yarn --deploy-mode cluster book_recs_pipeline.py \
        --start_date 2024-09-01 --end_date 2024-12-11 \
        --geo_id "Москва, Россия" \
        --columns usage_platform_ru genre \
        --audition_path s3a://s3-ds-source/audition.parquet \
        --content_path s3a://s3-ds-source/content.parquet \
        --output_path s3a://<bucket>/results/S24_project_data

Ключи S3 берутся из переменных окружения S3_ACCESS_KEY и S3_SECRET_KEY.
"""

import argparse
import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

S3_ENDPOINT = "https://storage.yandexcloud.net"


# ============================================================================
# Задача 1. Создание Spark-сессии
# ============================================================================
def create_spark_session(access_key, secret_key, buckets):
    builder = (
        SparkSession.builder
        .master("yarn")
        .appName("book_recs_pipeline")
        .config("spark.executor.memory", "2g")
        .config("spark.executor.cores", "2")
        .config("spark.executor.instances", "2")
        .config("spark.driver.cores", "4")
        .config("spark.driver.memory", "8g")
        .config("spark.sql.shuffle.partitions", "32")
        .config("spark.network.timeout", "300s")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
        .config("spark.hadoop.fs.s3a.connection.maximum", "100")
        .config("spark.hadoop.fs.s3a.attempts.maximum", "10")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
    )
    # ключи задаются для каждого бакета отдельно (per-bucket настройки s3a)
    for bucket in buckets:
        builder = (
            builder
            .config(f"spark.hadoop.fs.s3a.access.key.{bucket}", access_key)
            .config(f"spark.hadoop.fs.s3a.secret.key.{bucket}", secret_key)
            .config(f"spark.hadoop.fs.s3a.endpoint.{bucket}", S3_ENDPOINT)
        )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ============================================================================
# Задача 2. Первичное знакомство с данными
# ============================================================================
def read_data(spark, audition_path, content_path):
    audition = spark.read.parquet(audition_path)
    content = spark.read.parquet(content_path)
    return audition, content


def show_info(df, name):
    print(f"\n=== {name} ===")
    print("Количество строк:", df.count())
    print("Количество колонок:", len(df.columns))
    df.printSchema()
    df.show(5, truncate=False)


# ============================================================================
# Задача 3. Предобработка и анализ данных
# ============================================================================
def preprocess(audition, content, start_date, end_date, geo_id):
    # 3.1 строковая дата -> timestamp / date
    audition = (
        audition
        .withColumn("event_ts", F.to_timestamp("msk_business_dt_str"))
        .withColumn("event_date", F.to_date("event_ts"))
    )

    # 3.2 пропуски
    print("\n=== Пропуски в audition ===")
    audition.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in audition.columns]).show()
    print("=== Пропуски в content ===")
    content.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in content.columns]).show()

    # 3.3 аномалии
    print("=== Аномалии ===")
    audition.select(F.min("hours"), F.max("hours"), F.avg("hours")).show()
    print("Сессий с hours < 0:", audition.filter(F.col("hours") < 0).count())
    print("Сессий с hours > 24:", audition.filter(F.col("hours") > 24).count())
    print("Строк с нераспознанной датой:", audition.filter(F.col("event_ts").isNull()).count())
    print("Дублей audition_id:", audition.count() - audition.dropDuplicates(["audition_id"]).count())
    print("Дублей main_content_id в content:",
          content.count() - content.dropDuplicates(["main_content_id"]).count())
    print("Контент с длительностью <= 0:",
          content.filter(F.col("main_content_duration_hours") <= 0).count())

    # 3.4 устранение аномалий и заполнение пропусков
    audition = (
        audition
        .dropDuplicates(["audition_id"])
        .filter(F.col("puid").isNotNull() & F.col("main_content_id").isNotNull())
        .filter(F.col("event_ts").isNotNull())
        .filter((F.col("hours") >= 0) & (F.col("hours") <= 24))
        .fillna({"usage_platform_ru": "unknown", "app_version": "unknown",
                 "adult_content_flg": False, "kids_content_flg": False,
                 "hours": 0.0, "hours_sessions_long": 0.0})
    )
    median_duration = content.filter(F.col("main_content_duration_hours") > 0) \
        .approxQuantile("main_content_duration_hours", [0.5], 0.01)[0]
    content = (
        content
        .dropDuplicates(["main_content_id"])
        .fillna({"main_content_type": "unknown", "main_content_name": "unknown",
                 "published_topic_title_list": ""})
        .withColumn(
            "main_content_duration_hours",
            F.when(F.col("main_content_duration_hours").isNull()
                   | (F.col("main_content_duration_hours") <= 0), F.lit(median_duration))
            .otherwise(F.col("main_content_duration_hours")),
        )
    )
    print("Медиана длительности контента для заполнения:", median_duration)

    # 3.5a фильтр по периоду (обе даты включительно) и геолокации — до join,
    # чтобы соединять с content только нужные строки (~126 тыс. вместо ~1 млн)
    audition.select(F.min("event_date").alias("min_date"), F.max("event_date").alias("max_date")).show()
    audition = audition.filter(
        (F.col("event_date") >= F.lit(start_date)) & (F.col("event_date") <= F.lit(end_date))
    )
    if geo_id:
        audition = audition.filter(F.col("usage_geo_id") == geo_id)
    print(f"Строк audition после фильтра [{start_date}; {end_date}], geo_id={geo_id}:", audition.count())

    # 3.5 объединение таблиц и проверка на дубли
    rows_before = audition.count()
    df = audition.join(content, on="main_content_id", how="left")
    rows_after = df.count()
    print(f"Строк до join: {rows_before}, после join: {rows_after}, "
          f"дубли: {'нет' if rows_before == rows_after else 'ЕСТЬ'}")
    df = df.fillna({"main_content_type": "unknown", "published_topic_title_list": ""}) \
           .withColumn("main_content_duration_hours",
                       F.coalesce("main_content_duration_hours", F.lit(median_duration)))


    # 3.7 строка жанров -> список (split)
    genres = F.split(F.regexp_replace("published_topic_title_list", r"[\[\]'\"]", ""), r"\s*,\s*")
    genres = F.filter(F.transform(genres, lambda x: F.trim(x)), lambda x: x != "")
    df = df.withColumn("genres", genres)
    df = df.withColumn("day_of_week", F.date_format("event_date", "EEEE"))
    return df.cache()


def analyze(df, columns):
    # распределение пользователей по колонкам из аргумента запуска
    for col in columns:
        print(f"\n=== Распределение пользователей по {col} ===")
        if col == "genre":
            df.select("puid", F.explode("genres").alias("genre")) \
              .groupBy("genre").agg(F.countDistinct("puid").alias("users")) \
              .orderBy(F.desc("users")).show(15, truncate=False)
        else:
            df.groupBy(col).agg(F.countDistinct("puid").alias("users"), F.count("*").alias("sessions")) \
              .orderBy(F.desc("users")).show(15, truncate=False)

    print("\n=== Популярность типов контента (по уникальным пользователям) ===")
    df.groupBy("main_content_type") \
      .agg(F.count("*").alias("sessions"), F.countDistinct("puid").alias("users"),
           F.round(F.sum("hours"), 2).alias("hours")) \
      .orderBy(F.desc("users"), F.desc("sessions")).show(truncate=False)

    print("=== Популярность жанров (по уникальным пользователям) ===")
    df.select("puid", "hours", F.explode("genres").alias("genre")).groupBy("genre") \
      .agg(F.count("*").alias("sessions"), F.countDistinct("puid").alias("users"),
           F.round(F.sum("hours"), 2).alias("hours")) \
      .orderBy(F.desc("users"), F.desc("sessions")).show(15, truncate=False)

    print("=== Длительность сессий по дню недели ===")
    df.groupBy(F.dayofweek("event_date").alias("dow"), "day_of_week") \
      .agg(F.count("*").alias("sessions"), F.round(F.avg("hours"), 4).alias("avg_hours"),
           F.round(F.sum("hours"), 2).alias("total_hours")) \
      .orderBy("dow").show()

    print("=== Длительность сессий по геолокации ===")
    df.groupBy("usage_geo_id") \
      .agg(F.count("*").alias("sessions"), F.countDistinct("puid").alias("users"), F.round(F.avg("hours"), 4).alias("avg_hours"),
           F.round(F.sum("hours"), 2).alias("total_hours")) \
      .orderBy(F.desc("users"), F.desc("sessions")).show(15, truncate=False)


# ============================================================================
# Задача 4. Построение витрин данных
# ============================================================================
def build_user_content_mart(df):
    return (
        df.groupBy("puid", "main_content_id")
        .agg(
            F.sum("hours").alias("total_hours"),
            F.count("audition_id").alias("sessions_count"),
            F.first("main_content_duration_hours").alias("duration"),
            F.max("event_date").alias("last_listening_date"),
            F.min("event_date").alias("first_listening_date"),
            F.avg("hours").alias("avg_session_hours"),
        )
        .withColumn("finished_percent", F.col("total_hours") / F.col("duration"))
        .withColumn("is_completed", (F.col("finished_percent") >= 1).cast("int"))
        .withColumn("days_spent", F.datediff("last_listening_date", "first_listening_date") + 1)
        .select("puid", "main_content_id", "total_hours", "sessions_count", "finished_percent",
                "is_completed", "last_listening_date", "days_spent", "avg_session_hours")
    )


def build_user_genre_mart(df, user_content_mart):
    exploded = (
        df.select("puid", "main_content_id", "audition_id", "hours",
                  "adult_content_flg", "kids_content_flg", F.explode("genres").alias("genre"))
        .join(user_content_mart.select("puid", "main_content_id", "is_completed"),
              on=["puid", "main_content_id"], how="left")
    )
    window = Window.partitionBy("puid").orderBy(F.desc("total_hours"))
    return (
        exploded.groupBy("puid", "genre")
        .agg(
            F.sum("hours").alias("total_hours"),
            F.count("audition_id").alias("sessions_count"),
            F.countDistinct(F.when(F.col("is_completed") == 1, F.col("main_content_id"))).alias("completed_count"),
            F.countDistinct(F.when(F.col("adult_content_flg"), F.col("main_content_id"))).alias("adult_count"),
            F.countDistinct(F.when(F.col("kids_content_flg"), F.col("main_content_id"))).alias("kids_count"),
        )
        .withColumn("genre_rank", F.dense_rank().over(window))
        .select("puid", "genre", "total_hours", "sessions_count", "completed_count",
                "genre_rank", "adult_count", "kids_count")
    )


def favorite_value(df, col, alias):
    """Значение col с максимальным суммарным временем у пользователя."""
    window = Window.partitionBy("puid").orderBy(F.desc("hours_sum"), F.desc("sessions"), col)
    return (
        df.groupBy("puid", col)
        .agg(F.sum("hours").alias("hours_sum"), F.count("*").alias("sessions"))
        .withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
        .select("puid", F.col(col).alias(alias))
    )


def build_user_mart(df, user_content_mart, user_genre_mart):
    base = df.groupBy("puid").agg(
        F.sum("hours").alias("total_hours"),
        F.countDistinct("event_date").alias("active_days"),
        F.countDistinct("main_content_id").alias("unique_content_count"),
        F.max("event_date").alias("last_date"),
        F.countDistinct(F.when(F.col("adult_content_flg"), F.col("main_content_id"))).alias("adult_content_count"),
        F.countDistinct(F.when(F.col("kids_content_flg"), F.col("main_content_id"))).alias("kids_content_count"),
    )
    genres_count = user_genre_mart.groupBy("puid").agg(F.countDistinct("genre").alias("unique_genres_count"))
    completion = user_content_mart.groupBy("puid").agg(F.avg("finished_percent").alias("completion_rate"))
    # genre_rank = dense_rank: при равных total_hours несколько жанров имеют ранг 1.
    # Для user_mart нужен ровно один любимый жанр -> детерминированный tie-break
    # (больше сессий, затем алфавит), чтобы join не размножал строки.
    fav_window = Window.partitionBy("puid").orderBy(
        "genre_rank", F.desc("sessions_count"), "genre"
    )
    favorite_genre = (
        user_genre_mart.filter(F.col("genre_rank") == 1)
        .withColumn("rn", F.row_number().over(fav_window))
        .filter(F.col("rn") == 1)
        .select("puid", F.col("genre").alias("favorite_genre"))
    )
    favorite_type = favorite_value(df, "main_content_type", "favorite_content_type")
    favorite_platform = favorite_value(df, "usage_platform_ru", "favourite_platform")
    return (
        base.join(genres_count, "puid", "left")
        .join(completion, "puid", "left")
        .join(favorite_genre, "puid", "left")
        .join(favorite_type, "puid", "left")
        .join(favorite_platform, "puid", "left")
        .fillna({"unique_genres_count": 0})
        .select("puid", "total_hours", "active_days", "unique_content_count", "unique_genres_count",
                "completion_rate", "favorite_genre", "favorite_content_type", "favourite_platform",
                "last_date", "adult_content_count", "kids_content_count")
    )


def build_content_mart(df):
    total_users = df.select("puid").distinct().count()
    per_user = (
        df.groupBy("main_content_id", "puid")
        .agg(F.sum("hours").alias("user_total_hours"),
             F.first("main_content_duration_hours").alias("duration"))
        .withColumn("user_finished_percent", F.col("user_total_hours") / F.col("duration") * 100)
    )
    platforms = df.groupBy("main_content_id").agg(F.countDistinct("usage_platform_ru").alias("platforms_count"))
    return (
        per_user.groupBy("main_content_id")
        .agg(F.countDistinct("puid").alias("unique_users"),
             F.sum("user_total_hours").alias("total_hours"),
             F.avg("user_finished_percent").alias("avg_finished_percent"))
        .join(platforms, "main_content_id", "left")
        .withColumn("popularity", F.col("unique_users") / F.lit(total_users))
        .select("main_content_id", "unique_users", "total_hours", "avg_finished_percent",
                "platforms_count", "popularity")
    )


def build_daily_user_mart(df):
    return (
        df.groupBy("puid", F.col("event_date").alias("session_date"))
        .agg(F.first("day_of_week").alias("day_of_week"),
             F.countDistinct("main_content_id").alias("content_count"),
             F.sum("hours").alias("hours_spent"),
             F.count("audition_id").alias("sessions_count"))
        .select("puid", "session_date", "day_of_week", "content_count", "hours_spent", "sessions_count")
    )


# ============================================================================
# Задача 5. Объединение всех витрин в одной функции
# ============================================================================
def build_all_marts(df):
    user_content_mart = build_user_content_mart(df)
    user_genre_mart = build_user_genre_mart(df, user_content_mart)
    return {
        "user_content_mart": user_content_mart,
        "user_genre_mart": user_genre_mart,
        "user_mart": build_user_mart(df, user_content_mart, user_genre_mart),
        "content_mart": build_content_mart(df),
        "daily_user_mart": build_daily_user_mart(df),
    }


# ============================================================================
# Задача 6. Сохранение витрин в S3
# ============================================================================
def save_marts(marts, output_path):
    for name, mart in marts.items():
        path = f"{output_path.rstrip('/')}/{name}.parquet"
        mart.write.mode("overwrite").parquet(path)
        print(f"Сохранено: {path} (строк: {mart.count()})")


# ============================================================================
# Задача 7. Оформление скрипта: аргументы запуска
# ============================================================================
def parse_args():
    parser = argparse.ArgumentParser(description="ETL-пайплайн витрин Яндекс Книг")
    parser.add_argument("--start_date", required=True, help="начало периода, YYYY-MM-DD")
    parser.add_argument("--end_date", required=True, help="конец периода, YYYY-MM-DD")
    parser.add_argument("--geo_id", default=None, help="геолокация usage_geo_id")
    parser.add_argument("--columns", nargs="+", default=["usage_platform_ru"],
                        help="колонки для распределения, например usage_platform_ru genre")
    parser.add_argument("--audition_path", required=True, help="путь к audition.parquet")
    parser.add_argument("--content_path", required=True, help="путь к content.parquet")
    parser.add_argument("--output_path", required=True, help="путь для сохранения витрин")
    return parser.parse_args()


def main():
    args = parse_args()
    access_key = os.environ["S3_ACCESS_KEY"]
    secret_key = os.environ["S3_SECRET_KEY"]
    buckets = {p.split("/")[2] for p in (args.audition_path, args.content_path, args.output_path)
               if p.startswith("s3a://")}

    # Задача 1
    spark = create_spark_session(access_key, secret_key, buckets)

    # Задача 2
    audition, content = read_data(spark, args.audition_path, args.content_path)
    show_info(audition, "audition")
    show_info(content, "content")

    # Задача 3
    df = preprocess(audition, content, args.start_date, args.end_date, args.geo_id)
    analyze(df, args.columns)

    # Задачи 4–5
    marts = build_all_marts(df)
    for name, mart in marts.items():
        print(f"\n=== {name} ===")
        mart.printSchema()
        mart.show(5, truncate=False)
    print("\n=== План выполнения user_mart ===")
    marts["user_mart"].explain(mode="extended")

    # Задача 6
    save_marts(marts, args.output_path)

    spark.stop()


if __name__ == "__main__":
    main()
