# Исследование возвратности пользователей Яндекс Афиши

**Задача.** Понять, какие пользователи возвращаются на платформу и совершают повторные заказы, чтобы точнее настраивать таргетинг и маркетинговые бюджеты.

## Что сделано

- выгрузка данных из PostgreSQL через SQLAlchemy (креды — из переменных окружения);
- приведение выручки к рублям по историческому курсу, фильтрация выбросов по 99-му перцентилю;
- построение профиля пользователя (21,7 тыс. пользователей, 290 тыс. заказов);
- анализ возвратности по типу мероприятия, региону, устройству, чеку; корреляционный анализ (phik).

## Результат

- 61,7% пользователей делают 2+ заказа, 28,9% — 5+; средний интервал между заказами — 15,9 дня;
- чаще всего возвращаются покупатели билетов на выставки, концерты и в театры (60%+ возвратов).

**Стек:** Python, pandas, SQLAlchemy, PostgreSQL, matplotlib, seaborn, phik

**Решение:** [`yandex_afisha_eda.ipynb`](yandex_afisha_eda.ipynb)

> Данные не публикуются: это учебные датасеты Яндекс Практикума. Ноутбук сохранён с выводами ячеек, поэтому результаты видны без запуска.

---

## In English

**User retention analysis for Yandex Afisha (ticketing service)**

Data pulled from PostgreSQL via SQLAlchemy, revenue converted to RUB by historical rates, outliers filtered, user profiles built (21.7K users, 290K orders) and repeat purchases analysed by event type, region, device and ticket price.

**Key result:** 61.7% of users return; key retention drivers found

_Notebooks and code comments are in Russian._
