# Предсказание вероятности клика по рекламному баннеру (CTR)

**Задача.** Построить модель бинарной классификации для AdTech-платформы, чьи предсказанные вероятности клика максимально близки к фактической частоте.

## Что сделано

- EDA и разбиение train/valid/test;
- пайплайны предобработки числовых и категориальных признаков, отбор признаков;
- DummyClassifier → LogisticRegression → SVM, ручная кросс-валидация по PR-AUC, подбор гиперпараметров;
- калибровка вероятностей, анализ калибровочной кривой, сохранение артефактов.

## Результат

- модель превзошла бейзлайн; калибровка улучшила согласие вероятностей в области малых значений;
- главные признаки — приложение и сайт показа, а также их категории.

**Стек:** Python, pandas, scikit-learn (SVM, LogisticRegression, калибровка), mlxtend, phik

**Решение:** [`adtech_ctr_prediction.ipynb`](adtech_ctr_prediction.ipynb)

> Данные не публикуются: это учебные датасеты Яндекс Практикума. Ноутбук сохранён с выводами ячеек, поэтому результаты видны без запуска.

---

## In English

**Ad click-through rate (CTR) prediction**

Binary classifier whose probabilities match real click rates: preprocessing pipelines, feature selection, Dummy → LogisticRegression → SVM, manual cross-validation on PR-AUC, hyperparameter tuning and probability calibration.

**Key result:** Calibrated CTR model, top-5 features identified

_Notebooks and code comments are in Russian._
