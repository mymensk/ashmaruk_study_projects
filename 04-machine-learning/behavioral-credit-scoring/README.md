# Поведенческий скоринг: просрочка 90+ дней на горизонте 12 месяцев

**Задача.** Построить модель, которая оценивает вероятность просрочки 90+ дней у действующего клиента банка в ближайшие 12 месяцев.

## Что сделано

- сборка данных из нескольких источников: транзакции, анкетные и кредитные данные, просрочки, макроэкономика;
- формирование таргета с учётом временной структуры, feature engineering;
- сравнение логистической регрессии и Random Forest (с балансировкой классов и без);
- оптимизация Random Forest в Optuna по доле пропущенных дефолтов, калибровка вероятностей, подбор порога под бизнес-ограничения, проверка стабильности на новых данных.

## Результат

- без балансировки модели плохо находят дефолты; у Random Forest высокий ROC-AUC, но до оптимизации — слабые бизнес-метрики.

**Стек:** Python, pandas, scikit-learn, Optuna, mlxtend

**Решение:** [`behavioral_credit_scoring.ipynb`](behavioral_credit_scoring.ipynb)

> Данные не публикуются: это учебные датасеты Яндекс Практикума. Ноутбук сохранён с выводами ячеек, поэтому результаты видны без запуска.

---

## In English

**Behavioral credit scoring: 90+ days past due within 12 months**

Target built with respect to time structure from transactions, credit, delinquency and macro data; logistic regression vs Random Forest (with/without class balancing), Optuna tuning on missed-defaults rate, probability calibration, business-driven threshold selection and stability check.

**Key result:** Calibrated Random Forest with a business-driven threshold

_Notebooks and code comments are in Russian._
