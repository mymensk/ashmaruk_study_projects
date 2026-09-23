"""Единая логика расчёта батч-признаков для пары (customer_id, run_date).

Этот модуль — единственный источник правды по расчёту признаков.
Его используют и Jupyter-тетрадь (для отладки), и DAG в Airflow.
Никакой бизнес-логики в DAG и в ноутбуке дублироваться не должно.

Главное правило: для даты среза run_date в расчёт попадают ТОЛЬКО данные,
доступные строго ДО run_date (timestamp/start_time/order_time < run_date).
Это исключает утечку данных из будущего и делает расчёт воспроизводимым
для любого run_date — как для исторических срезов, так и для инференса.
"""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import Iterable

import pandas as pd

# Стандартные окна агрегации (в днях):
#   7  — оперативное окно;
#   30 — стабилизированное окно.
WINDOWS: tuple[int, ...] = (7, 30)

# Итоговый порядок колонок в таблице признаков.
FEATURE_COLUMNS: list[str] = [
    "customer_id",
    "run_date",
    "page_view_7d",
    "page_view_30d",
    "add_to_cart_7d",
    "add_to_cart_30d",
    "cr_view_cart_7d",
    "cr_view_cart_30d",
    "cr_cart_purchase_7d",
    "cr_cart_purchase_30d",
    "unique_products_7d",
    "unique_products_30d",
    "session_count_7d",
    "session_count_30d",
    "avg_session_sec_30d",
    "days_since_last_purchase",
    "order_count_30d",
    "sum_total_usd_30d",
    "avg_total_usd_30d",
]


# ---------------------------------------------------------------------------
# 1. Чтение исходных данных
# ---------------------------------------------------------------------------
def load_source_tables(conn, schema: str = "public") -> dict[str, pd.DataFrame]:
    """Читает исходные таблицы из PostgreSQL в DataFrame.

    Parameters
    ----------
    conn : SQLAlchemy Engine/Connection или DBAPI-соединение
        Любой объект, который понимает ``pandas.read_sql``.
    schema : str
        Схема, в которой лежат исходные таблицы.

    Returns
    -------
    dict[str, pandas.DataFrame]
        Словарь с ключами customers, sessions, events, orders.
    """
    queries = {
        "customers": f"SELECT customer_id, signup_date FROM {schema}.customers",
        "sessions": f"SELECT session_id, customer_id, start_time FROM {schema}.sessions",
        "events": (
            f"SELECT event_id, session_id, timestamp, event_type, product_id "
            f"FROM {schema}.events"
        ),
        "orders": (
            f"SELECT order_id, customer_id, order_time, subtotal_usd, total_usd "
            f"FROM {schema}.orders"
        ),
    }
    return {name: pd.read_sql(sql, conn) for name, sql in queries.items()}


# ---------------------------------------------------------------------------
# 2. Предобработка
# ---------------------------------------------------------------------------
def preprocess_tables(tables: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """Приводит типы, чистит данные и связывает события с пользователем.

    Правила:
      * временные поля приводятся к единому datetime; невалидные строки
        (NaT) выбрасываются из расчёта;
      * дубликаты событий удаляются по event_id;
      * total_usd приводится к числу, непреобразуемые значения -> 0.0;
      * к таблице events добавляется customer_id через связь с sessions
        (в events его нет).
    """
    customers = tables["customers"].copy()
    sessions = tables["sessions"].copy()
    events = tables["events"].copy()
    orders = tables["orders"].copy()

    # Единый тип времени.
    customers["signup_date"] = pd.to_datetime(customers["signup_date"], errors="coerce")
    sessions["start_time"] = pd.to_datetime(sessions["start_time"], errors="coerce")
    events["timestamp"] = pd.to_datetime(events["timestamp"], errors="coerce")
    orders["order_time"] = pd.to_datetime(orders["order_time"], errors="coerce")

    # Денежное поле -> число; то, что не приводится, становится 0.0.
    orders["total_usd"] = pd.to_numeric(orders["total_usd"], errors="coerce").fillna(0.0)

    # Дубликаты событий по event_id.
    events = events.drop_duplicates(subset="event_id")

    # Строки с невалидными датами не должны попадать в расчёт.
    sessions = sessions[sessions["start_time"].notna()]
    events = events[events["timestamp"].notna()]
    orders = orders[orders["order_time"].notna()]

    # events не содержит customer_id — получаем его через sessions.
    events = events.merge(
        sessions[["session_id", "customer_id"]], on="session_id", how="inner"
    )

    return {
        "customers": customers,
        "sessions": sessions,
        "events": events,
        "orders": orders,
    }


# ---------------------------------------------------------------------------
# Вспомогательное: фильтрация по окну [run_date - days, run_date)
# ---------------------------------------------------------------------------
def _window(df: pd.DataFrame, col: str, run_date: pd.Timestamp, days: int) -> pd.DataFrame:
    """Возвращает строки, попадающие в окно [run_date - days, run_date)."""
    low = run_date - timedelta(days=days)
    return df[(df[col] >= low) & (df[col] < run_date)]


# ---------------------------------------------------------------------------
# 3. Признаки по событиям (счётчики и уникальные товары)
# ---------------------------------------------------------------------------
def add_event_window_features(
    features: pd.DataFrame,
    events: pd.DataFrame,
    run_date: pd.Timestamp,
    windows: Iterable[int] = WINDOWS,
) -> pd.DataFrame:
    """Считает по окнам количество событий по типам и число уникальных товаров.

    Внутренне сохраняет колонку purchase_{d}d — она нужна функции
    add_conversion_features и удаляется после расчёта конверсий.
    """
    ids = features["customer_id"]
    for d in windows:
        win = _window(events, "timestamp", run_date, d)

        page_view = win[win["event_type"] == "page_view"].groupby("customer_id").size()
        add_to_cart = win[win["event_type"] == "add_to_cart"].groupby("customer_id").size()
        purchase = win[win["event_type"] == "purchase"].groupby("customer_id").size()
        # Уникальные товары — только по строкам с заданным product_id.
        uniq_products = (
            win[win["product_id"].notna()].groupby("customer_id")["product_id"].nunique()
        )

        features[f"page_view_{d}d"] = ids.map(page_view).fillna(0).astype(int)
        features[f"add_to_cart_{d}d"] = ids.map(add_to_cart).fillna(0).astype(int)
        features[f"purchase_{d}d"] = ids.map(purchase).fillna(0).astype(int)
        features[f"unique_products_{d}d"] = ids.map(uniq_products).fillna(0).astype(int)
    return features


# ---------------------------------------------------------------------------
# 4. Признаки воронки (конверсии)
# ---------------------------------------------------------------------------
def add_conversion_features(
    features: pd.DataFrame, windows: Iterable[int] = WINDOWS
) -> pd.DataFrame:
    """Считает конверсии воронки. При делении на ноль возвращает 0.0.

    Требует, чтобы add_event_window_features уже отработала
    (нужны колонки page_view/add_to_cart/purchase).
    """
    for d in windows:
        pv = features[f"page_view_{d}d"]
        ac = features[f"add_to_cart_{d}d"]
        pu = features[f"purchase_{d}d"]
        features[f"cr_view_cart_{d}d"] = (ac / pv.where(pv > 0)).fillna(0.0)
        features[f"cr_cart_purchase_{d}d"] = (pu / ac.where(ac > 0)).fillna(0.0)

    # Вспомогательные purchase-колонки больше не нужны в итоговой таблице.
    features = features.drop(columns=[f"purchase_{d}d" for d in windows])
    return features


# ---------------------------------------------------------------------------
# 5. Признаки по сессиям
# ---------------------------------------------------------------------------
def add_session_features(
    features: pd.DataFrame,
    sessions: pd.DataFrame,
    events: pd.DataFrame,
    run_date: pd.Timestamp,
    windows: Iterable[int] = WINDOWS,
) -> pd.DataFrame:
    """Количество сессий по окнам и средняя длина сессии (сек) за 30 дней.

    Длина сессии = разница между последним и первым событием сессии.
    Если у сессии нет событий — длина равна 0.
    """
    ids = features["customer_id"]

    # Длина каждой сессии из её событий.
    span = events.groupby("session_id")["timestamp"].agg(["min", "max"])
    sess = sessions.merge(span, on="session_id", how="left")
    sess["length_sec"] = (sess["max"] - sess["min"]).dt.total_seconds().fillna(0.0)

    for d in windows:
        win = _window(sess, "start_time", run_date, d)
        cnt = win.groupby("customer_id")["session_id"].count()
        features[f"session_count_{d}d"] = ids.map(cnt).fillna(0).astype(int)

    win30 = _window(sess, "start_time", run_date, 30)
    avg_len = win30.groupby("customer_id")["length_sec"].mean()
    features["avg_session_sec_30d"] = ids.map(avg_len).fillna(0.0)
    return features


# ---------------------------------------------------------------------------
# 6. Признаки по заказам (денежные)
# ---------------------------------------------------------------------------
def add_order_features(
    features: pd.DataFrame, orders: pd.DataFrame, run_date: pd.Timestamp
) -> pd.DataFrame:
    """Денежные признаки и давность последней покупки.

    days_since_last_purchase = -1, если у пользователя ещё не было заказов.
    Денежные агрегаты при отсутствии данных = 0.0, счётчики = 0.
    """
    ids = features["customer_id"]

    win30 = _window(orders, "order_time", run_date, 30)
    features["order_count_30d"] = (
        ids.map(win30.groupby("customer_id")["order_id"].count()).fillna(0).astype(int)
    )
    features["sum_total_usd_30d"] = (
        ids.map(win30.groupby("customer_id")["total_usd"].sum()).fillna(0.0)
    )
    features["avg_total_usd_30d"] = (
        ids.map(win30.groupby("customer_id")["total_usd"].mean()).fillna(0.0)
    )

    # Давность последней покупки — по всем заказам строго до run_date.
    before = orders[orders["order_time"] < run_date]
    last_order = before.groupby("customer_id")["order_time"].max()
    days_since = (run_date - ids.map(last_order)).dt.days
    features["days_since_last_purchase"] = days_since.fillna(-1).astype(int)
    return features


# ---------------------------------------------------------------------------
# 7. Сборка итоговой таблицы признаков
# ---------------------------------------------------------------------------
def build_batch_features(conn, run_date, schema: str = "public") -> pd.DataFrame:
    """Главная функция: собирает итоговую таблицу батч-признаков.

    Одна и та же логика для исторических срезов и для инференса —
    меняется только run_date.

    Parameters
    ----------
    conn : SQLAlchemy Engine/Connection или DBAPI-соединение.
    run_date : str | datetime | pandas.Timestamp
        Дата среза T. В расчёт идут только данные строго до неё.
    schema : str
        Схема с исходными таблицами.

    Returns
    -------
    pandas.DataFrame
        Одна строка на пару (customer_id, run_date) со всеми признаками.
    """
    run_ts = pd.Timestamp(run_date).normalize()

    tables = preprocess_tables(load_source_tables(conn, schema=schema))

    # База — все пользователи, чтобы набор customer_id не менялся между срезами.
    features = tables["customers"][["customer_id"]].drop_duplicates().reset_index(drop=True)
    features["run_date"] = run_ts.date()

    features = add_event_window_features(features, tables["events"], run_ts)
    features = add_conversion_features(features)
    features = add_session_features(features, tables["sessions"], tables["events"], run_ts)
    features = add_order_features(features, tables["orders"], run_ts)

    features = features[FEATURE_COLUMNS]
    _validate(features)
    return features


def _validate(features: pd.DataFrame) -> None:
    """Базовые проверки итоговой таблицы перед сохранением."""
    if features.duplicated(subset=["customer_id", "run_date"]).any():
        raise ValueError("Найдены дубликаты по паре (customer_id, run_date)")
    missing = set(FEATURE_COLUMNS) - set(features.columns)
    if missing:
        raise ValueError(f"В таблице отсутствуют колонки: {sorted(missing)}")
    if features.empty:
        raise ValueError("Итоговая таблица признаков пустая")


# ---------------------------------------------------------------------------
# 8. Сохранение результата
# ---------------------------------------------------------------------------
def save_features(features: pd.DataFrame, path: str | Path) -> Path:
    """Сохраняет таблицу признаков в CSV (каталог создаётся при необходимости)."""
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    features.to_csv(out, index=False)
    return out
