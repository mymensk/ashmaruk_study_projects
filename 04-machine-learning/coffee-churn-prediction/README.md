# Прогноз оттока клиентов сервиса доставки кофе

**Задача.** Сервис теряет ~10% клиентской базы в месяц. Нужна модель, которая заранее находит клиентов, склонных к уходу, чтобы маркетинг вовремя запускал удерживающие механики.

## Что сделано

- EDA, проверка дисбаланса классов, корреляционный анализ (phik);
- пайплайн предобработки (`Pipeline` + `ColumnTransformer`);
- логистическая регрессия, эксперименты с гиперпараметрами и отбором признаков;
- валидация, тест и сохранение модели для продакшена (joblib).

## Результат

- сильнее всего с оттоком связаны сбои в работе приложения и платная подписка;
- рекомендации: стабилизировать приложение и пересмотреть подписочную сетку.

**Стек:** Python, pandas, scikit-learn, phik, joblib

**Решение:** [`coffee_churn_prediction.ipynb`](coffee_churn_prediction.ipynb)

> Данные не публикуются: это учебные датасеты Яндекс Практикума. Ноутбук сохранён с выводами ячеек, поэтому результаты видны без запуска.

---

## In English

**Customer churn prediction for a coffee delivery service**

Binary classification to flag customers likely to churn next month: EDA, class imbalance check, Pipeline + ColumnTransformer preprocessing, logistic regression tuning, feature selection, model export.

**Key result:** Main churn drivers: app failures and paid subscription

_Notebooks and code comments are in Russian._
