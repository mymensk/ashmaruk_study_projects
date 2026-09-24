# Предобработка данных о продажах видеоигр

**Задача.** Подготовить данные о продажах и рейтингах игр 2000–2013 годов для аналитической статьи.

## Что сделано

- приведение названий и типов столбцов, обработка строковых заглушек;
- анализ и обработка пропусков, поиск явных и неявных дубликатов;
- срез актуального периода и категоризация игр по оценкам пользователей и критиков;
- топ-7 платформ по числу игр.

## Результат

- в 2000–2013 годах рынок платформ делили Sony и Nintendo, Microsoft (X360) заметно отставала;
- пользователи ставят оценки эмоциональнее критиков: у них больше и высоких, и низких оценок.

**Стек:** Python, pandas

**Решение:** [`games_market_preprocessing.ipynb`](games_market_preprocessing.ipynb)

> Данные не публикуются: это учебные датасеты Яндекс Практикума. Ноутбук сохранён с выводами ячеек, поэтому результаты видны без запуска.

---

## In English

**Video game sales data preprocessing**

Cleaning and preparing 2000–2013 video game sales and ratings data for an analytical article: column normalization, type fixes, missing values, explicit and implicit duplicates, user/critic score categorization and top platforms.

**Key result:** Sony and Nintendo led the 2000–2013 market

_Notebooks and code comments are in Russian._
