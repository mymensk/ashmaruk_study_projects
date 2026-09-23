# Проект «Книжные рекомендации» — анализ данных и витрин

**Скрипт:** `book_recs_pipeline.py` (PySpark 3.3.2, запуск через `spark-submit`, YARN).
**Параметры финального запуска:** `--start_date 2024-09-01 --end_date 2024-12-11 --geo_id "Москва, Россия" --columns usage_platform_ru genre`.
**Результат:** 5 витрин в `s3a://<your-bucket>/results/S24_project_data/` (Parquet).
Полный вывод запуска — `pipeline_stdout.txt`, план `user_mart` — `explain_user_mart.txt`.

Команда запуска:

```bash
export S3_ACCESS_KEY=... S3_SECRET_KEY=...
spark-submit --master yarn --deploy-mode cluster \
  --conf spark.yarn.appMasterEnv.S3_ACCESS_KEY=$S3_ACCESS_KEY \
  --conf spark.yarn.appMasterEnv.S3_SECRET_KEY=$S3_SECRET_KEY \
  book_recs_pipeline.py \
  --start_date 2024-09-01 --end_date 2024-12-11 --geo_id "Москва, Россия" \
  --columns usage_platform_ru genre \
  --audition_path s3a://s3-ds-source/audition.parquet \
  --content_path s3a://s3-ds-source/content.parquet \
  --output_path s3a://<your-bucket>/results/S24_project_data
```

---

## Задача 1. Spark-сессия

Сессия создана в `create_spark_session()` с параметрами из задания (`executor.memory=2g`, `executor.cores=2`, `executor.instances=2`, `driver.cores=4`, `driver.memory=8g`, `sql.shuffle.partitions=32`, `network.timeout=300s`) и per-bucket настройками S3A (`access.key`, `secret.key`, `endpoint` для каждого бакета). Ключи передаются через переменные окружения — в код не зашиты.

Особенность среды: под JupyterLab ограничен 1 ГБ RAM, поэтому драйвер на 8 ГБ в client-режиме убивает под. Запуск выполнен в `--deploy-mode cluster` (драйвер на YARN).

## Задача 2. Первичное знакомство с данными

| Таблица | Строк | Колонок |
|---|---|---|
| `audition` | 1 002 896 | 12 (11 из описания + служебная `__index_level_0__`) |
| `content` | 31 668 | 6 |

Наблюдения:
- `msk_business_dt_str` — строка вида `2024-11-26`; `hours`, `hours_sessions_long`, `main_content_duration_hours` — `double`; флаги — `boolean`.
- **`usage_geo_id` — строка** («Москва, Россия», «Алматы, Казахстан»), а не числовой идентификатор; аргумент `--geo_id` принимает строку.
- `published_topic_title_list` — строка вида `'Бизнес', 'Саморазвитие', 'Личные финансы'`, требует разбора в список.
- `main_content_type` принимает значения `Audiobook`, `Book`, `Comicbook`.

## Задача 3. Предобработка и анализ

### Пропуски
- `audition`: пропуски только в `app_version` — **702 511 из 1 002 896 (70%)**; заполнены значением `unknown`.
- `content`: 6 пропусков в `main_content_duration_hours` и 1 нулевая длительность; заполнены медианой **7.467 ч**.

### Аномалии
- `hours`: min 2.8e-7, max **321.3 ч**, среднее 0.42 ч. Отрицательных значений нет; **56 сессий > 24 ч** удалены.
- Дублей `audition_id` и `main_content_id` нет; строк с нераспознанной датой нет.
- После очистки в `audition` 1 002 840 строк.

### Объединение
- Фильтр по периоду (обе границы включительно) и `geo_id = "Москва, Россия"` применяется **к `audition` до join** — из 1 002 840 очищенных сессий остаётся **126 266**, и только они соединяются с `content`. Так join и последующий `explode` жанров обрабатывают ~8× меньше строк.
- `audition LEFT JOIN content` по `main_content_id`: 126 266 строк до и после — **дублей нет** (в `content` перед join удалены дубли по `main_content_id`).
- 784 сессии (0,6%) не имеют описания в `content` — тип заполнен `unknown`, жанры — пустой список.
- Диапазон дат в исходных данных: **2024-09-01 — 2024-12-11**.
- Замечание: физический план `user_mart` при переносе фильтра не изменился — Catalyst и раньше проталкивал предикаты под join (predicate pushdown). Явный фильтр до join закрепляет это поведение в коде и не зависит от оптимизатора.
### Распределения (Москва)
- **Платформы:** Станция — 8 347 пользователей / 87 492 сессий (69%), Букмейт Android — 3 146, Букмейт iOS — 2 343, Музыка Android — 1 947, Музыка iOS — 1 839. Остальные платформы (Web, Кинопоиск, ПП) — единицы процентов.
- **Типы контента (основной критерий — уникальные пользователи):** Audiobook — **14 193** пользователя (112 234 сессии, 41 983 ч), Book — **3 794** (12 327 сессий, 9 764 ч), Comicbook — **387** (921 сессия). Ранжирование по `countDistinct("puid")`, а не по числу сессий: сессии завышают популярность контента, к которому пользователи возвращаются многократно. Порядок лидеров при этом совпадает — аудиоформат доминирует и по охвату, и по часам.
- **Жанры (основной критерий — уникальные пользователи):** Аудио (14 167), Детская проза и поэзия (8 412), Художественная литература (7 539), Синхронизировано (5 860), Сказки и фольклор (4 685). По часам художественная литература (31 630 ч) уступает только «Аудио»; детский контент даёт много коротких сессий (77 тыс. сессий, 13,8 тыс. ч).
- **День недели:** число сессий выше в будни Пн–Ср (~19 тыс.) и ниже в Пт–Сб (~16 тыс.); средняя длительность сессии стабильна — 0,40–0,43 ч, максимум в Чт–Пт, минимум в выходные.
- **Геолокация:** по полной выборке (без фильтра) Москва — 126 266 сессий, Санкт-Петербург — 60 755, Московская область — 37 717; средняя длительность сессии практически не зависит от гео (0,40–0,42 ч).

## Задача 4. Витрины (финальный запуск, Москва)

| Витрина | Строк | Ключ | Замечания по расчёту |
|---|---|---|---|
| `user_content_mart` | 89 081 | `puid, main_content_id` | `finished_percent = total_hours / duration`; `is_completed = finished_percent >= 1`; `days_spent = last − first + 1` |
| `user_genre_mart` | 117 534 | `puid, genre` | `explode(genres)`; `genre_rank` — `dense_rank` по `total_hours` внутри `puid` (жанры с равным временем получают одинаковый ранг); `completed_count` берётся из `user_content_mart` |
| `user_mart` | 16 443 | `puid` | `favorite_*` — значение с максимальными часами (окно по `puid`); `completion_rate` — среднее `finished_percent` |
| `content_mart` | 14 525 | `main_content_id` | `avg_finished_percent` через `user_total_hours / duration * 100`, `popularity = unique_users / всего пользователей` |
| `daily_user_mart` | 95 258 | `puid, session_date` | `content_count` — число уникальных произведений за день |

Проверка на выборочных строках: `finished_percent` в диапазоне 0–1 для незавершённых, `genre_rank` начинается с 1, `favorite_genre`/`favorite_content_type`/`favourite_platform` согласованы с распределениями (Аудио / Audiobook / Станция).

## Задача 5. План выполнения `user_mart`

`build_all_marts()` строит все пять витрин; план получен через `marts["user_mart"].explain(mode="extended")`.

Статистика физического плана:

| Оператор | Кол-во |
|---|---|
| `Exchange hashpartitioning` (shuffle) | 32 |
| `BroadcastExchange` | 9 |
| `SortMergeJoin` | 6 |
| `BroadcastHashJoin` | 9 |
| `HashAggregate` / `SortAggregate` | 22 / 32 |
| `Window` | 3 |
| `Generate explode` | 2 |
| `Expand` | 1 |
| `InMemoryTableScan` (кэш `df`) | 8 |
| AQE | включён |

**Что вызывает перемешивание (shuffle):**
1. Все `groupBy("puid")` и `groupBy("puid", <col>)` — `Exchange hashpartitioning(puid, 32)` в каждой ветке (базовая агрегация, `unique_genres_count`, `completion_rate`, `favorite_*`).
2. `countDistinct` по трём разным выражениям в одной агрегации (`main_content_id`, `event_date`, условные `adult`/`kids`) → оператор `Expand`, размножающий строки, и **двухуровневая агрегация с двумя shuffle**.
3. Ветка жанров: `explode(genres)` → join с `user_content_mart` по `(puid, main_content_id)` → `Exchange hashpartitioning(puid, main_content_id)`, затем ещё один shuffle на `(puid, genre)`.
4. Оконные функции (`row_number` по `puid` для `genre_rank`, `favorite_content_type`, `favourite_platform`) — `Exchange` + `Sort` перед `Window`.
5. Объединение веток `user_mart` — четыре `SortMergeJoin` по `puid` требуют сортировки обеих сторон.

**Типы соединений:**
- `SortMergeJoin LeftOuter` — соединения крупных промежуточных наборов по `puid` (базовая агрегация ↔ `unique_genres_count`, `completion_rate`, `favorite_type`, `favorite_platform`) и `explode → user_content_mart` по `(puid, main_content_id)`.
- `BroadcastHashJoin` — `audition ↔ content` по `main_content_id` (справочник 31 тыс. строк рассылается на исполнители) и одна из веток `user_mart` (`favorite_genre`), которую оптимизатор оценил как малую.

**Самые дорогие операции:**
1. `Expand` + двойная агрегация с `countDistinct` — увеличивает объём данных в несколько раз перед shuffle.
2. Цепочка `explode(genres)` → `SortMergeJoin` по составному ключу → агрегация по `(puid, genre)`: объём после explode в ~2–3 раза больше исходного.
3. Три `Window` с сортировкой внутри партиций `puid`.
4. Четыре последовательных `SortMergeJoin` по `puid` с сортировкой каждой ветки.

**Предложения по оптимизации (без реализации):**
1. **Фильтрация до join и explode.** Фильтр по периоду и `geo_id` применяется к объединённому `df`; его можно применить к `audition` до join с `content` — join и explode обработают в ~8 раз меньше строк (126 тыс. вместо 1 млн).
2. **Убрать `countDistinct` по нескольким выражениям в одной агрегации** (`adult_content_count`, `kids_content_count`, `unique_content_count`, `active_days`): сначала агрегировать по `(puid, main_content_id)` (это уже есть в `user_content_mart`), затем считать `count`/`sum` по `puid` — исчезает `Expand`.
3. **Считать `completion_rate`, `unique_content_count`, `adult/kids_content_count` из `user_content_mart`** одной агрегацией по `puid` вместо отдельных веток — два join и два shuffle меньше.
4. **Заменить окна на агрегации** для `favorite_content_type`/`favourite_platform`: `groupBy(puid, col)` → `max_by(col, hours_sum)` через `struct`/`max` — убирает `Sort` + `Window`.
5. **Явный `broadcast()`** для маленьких веток (`favorite_*`, `genres_count` — 16 тыс. строк) вместо `SortMergeJoin`; либо предварительно `repartition("puid")` и кэшировать `user_content_mart`, чтобы все ветки использовали одно партиционирование без повторных shuffle.
6. Порядок группировки: сначала группировать по `puid` на уже отфильтрованном и закэшированном наборе, потом соединять узкие результаты, а не наоборот.

## Задача 6. Сохранение

Витрины сохранены в `results/S24_project_data/` собственного бакета в формате Parquet (`mode=overwrite`): `user_content_mart.parquet`, `user_genre_mart.parquet`, `user_mart.parquet`, `content_mart.parquet`, `daily_user_mart.parquet`.

## Задача 7. Оформление

Скрипт принимает аргументы `--start_date`, `--end_date`, `--geo_id`, `--columns`, `--audition_path`, `--content_path`, `--output_path`; ключи У3 — из окружения. Код разбит на функции по задачам 1–7.

---

## Доработки по результатам ревью

| Замечание | Что сделано |
|---|---|
| Популярность типов контента/жанров сортировалась по числу сессий | Основной критерий — `countDistinct("puid")`; сортировка `orderBy(desc("users"), desc("sessions"))`. В гео-таблицу также добавлено число уникальных пользователей |
| Фильтрация по периоду и гео после join | Фильтр перенесён на `audition` до join с `content` (блок 3.5a): join/explode обрабатывают 126 266 строк вместо ~1 млн |
| `genre_rank` через `row_number` | Заменён на `dense_rank()` по `total_hours` — равное время даёт равный ранг. Побочный эффект: у части пользователей несколько жанров с рангом 1 (например, «Аудио» и жанр той же книги), и прямой join по `genre_rank == 1` размножал `user_mart` до 40 798 строк. Для `favorite_genre` добавлен детерминированный tie-break (`row_number` по числу сессий и алфавиту среди жанров с рангом 1) — `user_mart` снова 16 443 строки, по одной на пользователя. `row_number` также оставлен в `favorite_value()` |

Финальный запуск после доработок: `application_1769503135769_2400`, статус SUCCEEDED; витрины перезаписаны в `s3a://<your-bucket>/results/S24_project_data/` (`user_content_mart` — 89 081 строк, `user_genre_mart` — 117 534 строк, `user_mart` — 16 443 строк, `content_mart` — 14 525 строк, `daily_user_mart` — 95 258 строк).
