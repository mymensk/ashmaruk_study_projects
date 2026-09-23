/* Проект «Секреты Тёмнолесья»
 * Цель проекта: изучить влияние характеристик игроков и их игровых персонажей 
 * на покупку внутриигровой валюты «райские лепестки», а также оценить 
 * активность игроков при совершении внутриигровых покупок
 * 
 * Автор: Александр Шмарук
 * Дата: 13.09.2025
*/

-- Часть 1. Исследовательский анализ данных
-- Задача 1. Исследование доли платящих игроков

-- 1.1. Доля платящих пользователей по всем данным:
SELECT COUNT(*) AS total_users, -- общее число пользователей
       (SELECT COUNT(*) AS pay_users_total
               FROM fantasy.users u
               WHERE payer = 1) AS pay_users_total, --- платящее число пользователей по признаку payer 
       ROUND(AVG(payer),3) AS pay_users_share --- доля платящих пользователей
FROM fantasy.users

-- 1.2. Доля платящих пользователей в разрезе расы персонажа:
SELECT r.race AS race_name, -- название расы
       SUM(payer) AS race_pay_total_users, -- число платящих пользователей у данной расы
       COUNT(u.id) AS race_total_users, -- общее число пользователей у данной расы
       AVG(payer) AS race_payer_share -- доля платящих пользователей у данной расы
FROM fantasy.users AS u
LEFT JOIN fantasy.race AS r USING(race_id) --соединяем для получения имени расы
GROUP BY r.race

-- Задача 2. Исследование внутриигровых покупок
-- 2.1. Статистические показатели по полю amount:
SELECT COUNT(*) AS total_events,-- общее число покупок
       SUM(amount) AS all_events_sum, -- общая сумма покупок
       MIN(amount) AS min_event, -- минимальная сумма покупки
       MAX(amount) AS max_event, -- максимальная сумма покупки
       AVG(amount) AS avg_event, -- средняя сумма покупки (среднее арифметическое)
       PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY amount) AS mediana_event, -- медиана покупок (50% персентиль)
       STDDEV(amount) AS stant_dev_event -- стандартное отклонение 
FROM fantasy.events  

-- 2.2: Аномальные нулевые покупки:
WITH zero_amount AS(
                   SELECT COUNT(*) AS total_amount, --  общее число покупок
                  (SELECT COUNT(*)
                          FROM fantasy.events e 
                          WHERE amount = 0) AS zero_amount_count -- число нулевых покупок в подзапросе
                   FROM fantasy.events)    -- сбор статистики в CTE
SELECT zero_amount_count, -- общще число нулевых покупок
       zero_amount_count::float/total_amount AS zero_amount_share -- доля нулевых покупок
FROM zero_amount;

-- 2.3: Популярные эпические предметы:
WITH sold_stat_items AS( -- расчет статистики вынесен в CTE
       SELECT i.game_items AS game_item, 
              COUNT(*) AS total_sold_item, -- всего продано артефактов
              (SELECT COUNT(*) 
                      FROM fantasy.events 
                      WHERE amount !=0) AS total_sold, -- общее число ненулевых покупок для расчета доли артефакта
               COUNT(DISTINCT fe.id) AS total_item_x_users, -- общее число уникальных покупателей конкретного артефакта для расчета доли покупателей
               (SELECT COUNT(DISTINCT id) FROM fantasy.events WHERE amount IS NOT NULL) AS total_users -- общее число уникальных пользователей, сделавших ненулевые покупки
FROM fantasy.events AS fe
LEFT JOIN fantasy.items AS i USING (item_code) -- соединение таблиц для получения названий артефактов
WHERE amount !=0 -- фильтр для выборки ненулевых покупок
GROUP BY i.game_items) -- группировка по названию артефакта
SELECT game_item, -- название артефакта
       total_sold_item, -- абсолютное число продаж артефакта
       total_sold_item/total_sold::float AS item_sold_share, -- доля артефакта в продажах
       total_item_x_users/total_users::float AS user_x_item_share  -- доля купивших артефакт (уникальных)
FROM sold_stat_items -- выборка из подготовительного CTE
ORDER BY user_x_item_share DESC; --  сортировка по популярности среди игроков

-- Часть 2. Решение ad hoc-задачи
-- Задача: Зависимость активности игроков от расы персонажа:
WITH race_stat AS ( -- CTE для подсчета общего числа пользователей по расам
                   SELECT race_id,
                          COUNT(id) AS race_users
                   FROM fantasy.users
                   GROUP BY race_id
),
race_buyers_stat AS ( -- CTE для подсчета общего числа пользователей, делающих внутриигровые покупки  по расам
                     SELECT race_id,
                            COUNT(DISTINCT e.id) AS buyers
                     FROM fantasy.events AS e
                     LEFT JOIN fantasy.users AS u USING (id)
                     GROUP BY race_id
),
race_pay_buyers_stat AS ( -- CTE для подсчета общего числа платящих пользователей по расам
                         SELECT race_id,
                                COUNT(DISTINCT e.id) AS pay_buyers
                         FROM fantasy.events AS e
                         LEFT JOIN fantasy.users AS u USING (id)
                         WHERE payer = 1 AND amount !=0
                         GROUP BY race_id
),
race_amount_stat AS ( -- CTE для подсчета статистики по покупкам по расам
                     SELECT race_id, 
                            COUNT(transaction_id) AS number_of_events_by_race,
                            AVG(amount) AS avg_race_amount,
                            SUM(amount) AS sum_amount_by_race
                     FROM fantasy.events AS e
                     LEFT JOIN fantasy.users AS u USING (id)
                     WHERE amount !=0
                     GROUP BY race_id
)
SELECT race, -- название расы
       race_users, -- общее количество зарегистрированных игроков по расам
       buyers, -- количество игроков, которые совершают внутриигровые покупки,
       buyers/race_users::float AS buyers_share, -- доля игроков, которые совершают внутриигровые покупки,
       pay_buyers/buyers::float AS pay_buyers_share, -- доля платящих игроков, среди тех, кто совершает внутриигровые покупки
       number_of_events_by_race/buyers::float AS avg_num_of_purchases, -- среднее число покупок на одного игрока, который совершает внутриигровые покупки
       avg_race_amount, -- средняя стоимость одной покупки на одного игрока, совершившего внутриигровые покупки 
       sum_amount_by_race/buyers::float AS sum_amount_per_user -- средняя суммарная стоимость покупок на одного игрока, совершающего внутриигровые покупки
FROM race_stat AS rs -- соединяем все CTE + таблицу с названиями рас 
JOIN race_buyers_stat AS rbs USING (race_id)
JOIN race_pay_buyers_stat AS rpbs USING (race_id)
JOIN fantasy.race AS r USING (race_id)
JOIN race_amount_stat AS ras USING (race_id)