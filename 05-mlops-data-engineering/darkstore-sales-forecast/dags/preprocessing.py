"""
Модуль предобработки данных для проекта «Прилавок».

Содержит функции подготовки признаков, единые для обучения (Jupyter Notebook)
и батч-инференса (Airflow DAG). Логика идентична в обоих контурах —
это исключает расхождение признаков train/inference (training-serving skew).
"""

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# Колонки внешних факторов, в которых допустимы пропуски
FACTOR_COLS = ['factor1', 'factor2', 'factor3', 'factor4', 'factor5']
NUMERIC_FILL_COLS = ['temperature', 'fuel_price', 'cpi', 'unemployment'] + FACTOR_COLS

LAG_WEEKS = [1, 2, 4]
ROLLING_WINDOWS = [4, 8, 12]


def normalize_columns(df):
    """Приводит названия колонок к нижнему регистру, дату — к datetime."""
    df = df.copy()
    df.columns = [c.lower() for c in df.columns]
    df['date'] = pd.to_datetime(df['date'])
    return df


def handle_anomalous_sales(df):
    """
    Обрабатывает аномальные продажи: отрицательные значения (возвраты)
    заменяются на 0 — модель прогнозирует спрос, отрицательный спрос не имеет
    смысла для планирования закупок.
    """
    df = df.copy()
    if 'weekly_sales' in df.columns:
        neg_mask = df['weekly_sales'] < 0
        n_neg = int(neg_mask.sum())
        if n_neg:
            logger.info('Заменено отрицательных продаж на 0: %d', n_neg)
        df.loc[neg_mask, 'weekly_sales'] = 0.0
    return df


def fill_missing_with_mean(df):
    """Заполняет пропуски в числовых внешних факторах средним по колонке."""
    df = df.copy()
    for col in NUMERIC_FILL_COLS:
        if col in df.columns:
            df[col] = df[col].fillna(df[col].mean())
    return df


def create_temporal_features(df):
    """Создаёт временные признаки из колонки date."""
    df = df.copy()
    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month
    df['quarter'] = df['date'].dt.quarter
    df['week_of_year'] = df['date'].dt.isocalendar().week.astype(int)
    df['day_of_week'] = df['date'].dt.dayofweek
    return df


def create_avg_sales_feature(df, cutoff_date=None):
    """
    Создаёт агрегированные признаки: средние и медианные продажи
    по дарксторам и по отделам.

    Агрегаты считаются ТОЛЬКО по историческим строкам (weekly_sales не NaN)
    и только до cutoff_date — это исключает утечку данных из
    валидационного / инференсного периода.
    """
    df = df.copy()
    hist = df[df['weekly_sales'].notna()]
    if cutoff_date is not None:
        hist = hist[hist['date'] < pd.Timestamp(cutoff_date)]

    store_agg = (hist.groupby('store')['weekly_sales']
                 .agg(store_avg_sales='mean', store_median_sales='median')
                 .reset_index())
    dept_agg = (hist.groupby('dept')['weekly_sales']
                .agg(dept_avg_sales='mean', dept_median_sales='median')
                .reset_index())

    df = df.merge(store_agg, on='store', how='left')
    df = df.merge(dept_agg, on='dept', how='left')
    return df


def create_lag_features(df):
    """
    Создаёт лаговые признаки: продажи 1, 2 и 4 недели назад.

    Реализовано merge-ом по точной дате (date - N недель), а не сдвигом строк:
    это корректно обрабатывает пропущенные недели в истории.
    """
    df = df.copy()
    base = df[['store', 'dept', 'date', 'weekly_sales']]
    for lag in LAG_WEEKS:
        shifted = base.copy()
        shifted['date'] = shifted['date'] + pd.Timedelta(weeks=lag)
        shifted = shifted.rename(columns={'weekly_sales': f'sales_lag_{lag}'})
        df = df.merge(shifted, on=['store', 'dept', 'date'], how='left')
    return df


def create_rolling_features(df):
    """
    Создаёт скользящие средние продаж за 4, 8 и 12 недель.

    Используется shift(1): в окно попадают только прошлые недели,
    текущее значение таргета не участвует — утечки нет.
    """
    df = df.copy()
    df = df.sort_values(['store', 'dept', 'date']).reset_index(drop=True)
    grouped = df.groupby(['store', 'dept'])['weekly_sales']
    for window in ROLLING_WINDOWS:
        df[f'sales_rolling_mean_{window}'] = grouped.transform(
            lambda s: s.shift(1).rolling(window, min_periods=1).mean()
        )
    return df


def preprocess_data(df, cutoff_date=None):
    """
    Полная предобработка данных (идентично логике обучения).

    Шаги:
    1. Нормализация колонок и типов.
    2. Удаление дубликатов по ключу (store, dept, date).
    3. Обработка аномальных продаж (отрицательные -> 0).
    4. Заполнение пропусков внешних факторов средним.
    5. Генерация признаков: временные, агрегированные, лаговые, скользящие.

    Параметры
    ---------
    df : pd.DataFrame
        Объединённый датасет (sales/plan + stores + features).
    cutoff_date : str | pd.Timestamp | None
        Дата, до которой считаются агрегаты (начало валидации при обучении,
        первая дата плана при инференсе). Защита от утечки данных.
    """
    df = normalize_columns(df)
    df = df.drop_duplicates(subset=['store', 'dept', 'date'], keep='first')
    df = handle_anomalous_sales(df)
    df = fill_missing_with_mean(df)
    df = create_temporal_features(df)
    df = create_avg_sales_feature(df, cutoff_date=cutoff_date)
    df = create_lag_features(df)
    df = create_rolling_features(df)
    logger.info('Предобработка завершена: %d строк, %d колонок', *df.shape)
    return df
