# Выявление мошеннических транзакций с трекингом экспериментов в MLflow

**Задача.** Построить модель для выявления мошеннических транзакций и полностью зафиксировать процесс в MLflow.

## Что сделано

- EDA и проверка гипотез;
- случайный поиск гиперпараметров `XGBClassifier`: один набор = один run;
- логирование параметров, метрик, графиков, датасета и модели; регистрация лучшей модели в Model Registry;
- автоматическая проверка эксперимента валидатором, фиксация окружения. Ключи доступа — только из `.env`.

**Стек:** Python, XGBoost, MLflow (Tracking, Model Registry), S3, SHAP

**Решение:** [`fraud_detection_mlflow.ipynb`](fraud_detection_mlflow.ipynb)

> Данные не публикуются: это учебные датасеты Яндекс Практикума. Ноутбук сохранён с выводами ячеек, поэтому результаты видны без запуска.

---

## In English

**Fraud detection with MLflow experiment tracking**

XGBoost with random hyperparameter search (one set = one run), logging params, metrics, plots, dataset and model to MLflow, best model registered in the Model Registry; credentials read from .env only.

**Key result:** Experiments and Model Registry in MLflow

_Notebooks and code comments are in Russian._
