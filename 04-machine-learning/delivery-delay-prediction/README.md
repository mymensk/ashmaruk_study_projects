# Прогноз риска задержки доставки заказа

**Задача.** Дать логистам вероятность задержки уже в момент оформления заказа, чтобы заранее менять приоритет или режим доставки. Целевой порог — ROC-AUC > 0,75.

## Что сделано

- проверка гипотезы: задержки объясняются профилем клиента (география и поведение);
- кластеризация KMeans: географические и RFM-кластеры клиентов как признаки;
- логистическая регрессия и CatBoost, подбор числа кластеров как гиперпараметра;
- честная валидация через `GroupShuffleSplit` по клиентам.

## Результат

- CatBoost с кластерами: ROC-AUC = 0,768 на тесте, целевой порог достигнут;
- поведенческий кластер вошёл в топ-3 признаков, а география оказалась неинформативной; главные факторы — режим доставки и время оформления заказа.

**Стек:** Python, pandas, scikit-learn (KMeans), CatBoost, phik

**Решение:** [`delivery_delay_prediction.ipynb`](delivery_delay_prediction.ipynb)

> Данные не публикуются: это учебные датасеты Яндекс Практикума. Ноутбук сохранён с выводами ячеек, поэтому результаты видны без запуска.

---

## In English

**Delivery delay risk prediction**

Delay probability at order time for dispatchers: geographic and RFM customer clusters (KMeans) as features, logistic regression vs CatBoost, number of clusters tuned as a hyperparameter, GroupShuffleSplit by customer.

**Key result:** ROC-AUC = 0.768 (target > 0.75)

_Notebooks and code comments are in Russian._
