# Определение возрастной группы пользователя по цифровому следу

**Задача.** По поведению анонимного пользователя в интернете определить его возрастную группу, чтобы точнее таргетировать рекламу и не показывать взрослую рекламу несовершеннолетним.

## Что сделано

- объединение пяти источников: посещения сайтов, реклама, глубина переходов, устройства, облачные сервисы (вынесено в модуль `merge_utils.py`);
- агрегированные поведенческие признаки: активность по категориям сайтов и времени суток, глубина сессий;
- многоклассовая классификация: multinomial LogReg, One-vs-Rest, One-vs-One, GridSearchCV по F1-macro;
- анализ важности признаков, сохранение пайплайна.

## Результат

- агрегированные поведенческие признаки заметно повышают качество; сильнее всего влияют категории сайтов и время активности.

**Стек:** Python, pandas, scikit-learn, phik, joblib

**Решение:** [`user_age_prediction.ipynb`](user_age_prediction.ipynb)

> Данные не публикуются: это учебные датасеты Яндекс Практикума. Ноутбук сохранён с выводами ячеек, поэтому результаты видны без запуска.

---

## In English

**Predicting user age group from digital footprint**

Five data sources merged (site visits, ads, click depth, devices, cloud usage), behavioral aggregate features, multiclass classification (multinomial LogReg, OvR, OvO) tuned with GridSearchCV on macro F1.

**Key result:** Multiclass model on behavioral features

_Notebooks and code comments are in Russian._
