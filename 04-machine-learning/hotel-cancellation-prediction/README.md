# Прогноз отмен бронирования в сети отелей

**Задача.** Предсказать отмену брони, чтобы сократить потери и удержать загрузку отелей. Модель оптимизируется по бизнес-метрике Incremental Revenue (прирост выручки относительно сценария без модели).

## Что сделано

- загрузка из PostgreSQL (SQLAlchemy) с фолбэком на CSV, очистка дубликатов и аномалий;
- привязка истории отзывов через `merge_asof` без утечки из будущего, TF-IDF по текстам отзывов внутри Pipeline;
- хронологическое разбиение 60/20/20, `TimeSeriesSplit`;
- LightGBM и CatBoost + Optuna с оптимизацией IR, изотоническая калибровка, подбор порога;
- интерпретация через SHAP (структурные признаки и слова из отзывов).

## Результат

- целевые бизнес-показатели на тесте выполнены: доля отмен ≤ 10%, прирост IR ≥ 50%;
- сильнее всего на отмену влияют срок до заезда, стоимость брони, канал продаж и история отмен.

**Стек:** Python, pandas, SQLAlchemy, LightGBM, CatBoost, Optuna, SHAP, scikit-learn

**Решение:** [`hotel_cancellation_prediction.ipynb`](hotel_cancellation_prediction.ipynb)

> Данные не публикуются: это учебные датасеты Яндекс Практикума. Ноутбук сохранён с выводами ячеек, поэтому результаты видны без запуска.

---

## In English

**Hotel booking cancellation prediction**

Leak-free review history via merge_asof, TF-IDF on review texts inside the Pipeline, chronological split with TimeSeriesSplit, LightGBM and CatBoost tuned with Optuna on an Incremental Revenue business metric, isotonic calibration, threshold search and SHAP.

**Key result:** Cancellations ≤ 10%, Incremental Revenue ≥ +50%

_Notebooks and code comments are in Russian._
