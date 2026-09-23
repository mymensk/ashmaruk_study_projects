/* Проект «Разработка витрины и решение ad-hoc задач»
 * Цель проекта: подготовка витрины данных маркетплейса «ВсёТут»
 * и решение четырех ad hoc задач на её основе
 * 
 * Автор: Александр Шмарук
 * Дата: 20.09.2025
*/



/* Часть 1. Разработка витрины данных */

/* Подзапрос для определения 3 регионов с наибольшим числом заказов - соединяем таблицу заказов и таблицу пользователей для получения списка регионов */
/* Подзапрос для определения 3 регионов с наибольшим числом заказов - соединяем таблицу заказов и таблицу пользователей для получения списка регионов */
WITH orders_num AS ( 
                   SELECT region, 
                          COUNT(*) AS order_num
                   FROM ds_ecom.orders
                   LEFT JOIN ds_ecom.users USING (buyer_id)
                   GROUP BY region
                   ORDER BY order_num DESC
                   LIMIT 3
                   ),
 /* Готовим фильтр на основе статусов заказов и полученного ранее списка из трех регионов */
     prefilter AS (
                   SELECT user_id, 
                          region,
                          COUNT(order_id) AS total_orders  
                   FROM ds_ecom.users AS u
                   JOIN ds_ecom.orders AS o USING (buyer_id)
                   WHERE o.order_status IN ('Доставлено', 'Отменено') 
                   AND u.region IN (SELECT region
                                    FROM orders_num)
                   GROUP BY  user_id, region
                   ),
/* Подзапрос для расчета статистики по времени первого и последнего заказа */
    order_time_stat AS ( 
                        SELECT user_id,
                               MIN(order_purchase_ts) AS first_order_ts,
                               MAX(order_purchase_ts) AS last_order_ts,
                               (MAX(order_purchase_ts::date) - MIN(order_purchase_ts::date)) AS lifetime
                        FROM ds_ecom.orders
                        JOIN ds_ecom.users USING (buyer_id)
                        GROUP BY user_id
                        ),
  /* В данный подзапрос выносим статистику по оценкам пользователя */
     order_stat AS ( /* Оценки рейтинга приводим к единой шкале */
                    WITH norma AS (
                                   SELECT user_id,
                                          CASE 
                                          	WHEN review_score > 5 THEN review_score/10
                                          	ELSE review_score
                                          END AS review_score_n
                                    FROM ds_ecom.orders
                                    JOIN ds_ecom.users USING (buyer_id)
                                    LEFT JOIN ds_ecom.order_reviews USING (order_id) /*учитываем, что заказ может быть без отзыва - поэтому LEFT JOIN */
                                   )
                    SELECT user_id, 
                           ROUND(AVG(review_score_n), 2) AS avg_order_rating,
                           COUNT(review_score_n) AS num_orders_with_rating
                    FROM norma
                    GROUP BY user_id
                    ORDER BY avg_order_rating DESC
                    ),
 /* В данный подзапрос выносим статистику по отмененным заказам */                   
     order_cancel_stat AS (
                          SELECT user_id, 
                                 COUNT(*) AS num_canceled_orders
                          FROM ds_ecom.orders
                          JOIN DS_ecom.users USING (buyer_id)
                          WHERE order_status = 'Отменено'
                          GROUP BY user_id
                          ),
/* В данный подзапрос выносим статистику по стоимости заказов */      
      order_payment_stat AS ( 
                             SELECT user_id,
                                    (SUM(price)+SUM(delivery_cost)) AS total_order_costs,
                                    ROUND(AVG(price + delivery_cost), 2) AS avg_order_cost
                             FROM ds_ecom.orders
                             JOIN ds_ecom.users USING (buyer_id)
                             JOIN ds_ecom.order_payments USING (order_id)
                             JOIN ds_ecom.order_items USING (order_id)
                             WHERE order_status = 'Доставлено'
                             GROUP BY user_id
                             ),
/* В данный подзапрос выносим статистику по числу заказов с рассрочками */  
      order_installments_stat AS (
                                  SELECT user_id,
                                         COUNT(order_id) AS num_installment_orders
                                  FROM ds_ecom.orders
                                  JOIN ds_ecom.users USING (buyer_id)
                                  JOIN ds_ecom.order_payments USING (order_id)
                                  WHERE payment_installments >1
                                  GROUP BY user_id
                                  ),
/* В данный подзапрос выносим статистику по оплатам заказов промокодами */  
      order_promo_stat AS (
                           SELECT user_id,
                                  COUNT(order_id) AS num_orders_with_promo
                           FROM ds_ecom.orders
                           JOIN ds_ecom.users USING (buyer_id)
                           JOIN ds_ecom.order_payments USING (order_id)
                           WHERE payment_type = 'промокод'
                           GROUP BY user_id
                           ),
 /* В данный подзапрос выносим статистику по оплатам заказов денежным переводом */                        
      money_transfer_stat AS (SELECT user_id,
                                     COUNT(order_id) AS money_transfer_num
                              FROM ds_ecom.orders
                              JOIN ds_ecom.users USING (buyer_id)
                              JOIN ds_ecom.order_payments USING (order_id)
                              WHERE payment_type = 'денежный перевод'
                              GROUP BY user_id
                              )
 /* В основном запросе соединим все СТЕ по user_id и выведем нужные поля */                             
SELECT user_id,
       region,
       first_order_ts,
       last_order_ts,
       lifetime,
       total_orders,
       COALESCE (avg_order_rating, 0) AS avg_order_rating,
       num_orders_with_rating,
       COALESCE (num_canceled_orders, 0) AS num_canceled_orders,
       COALESCE (ROUND(num_canceled_orders/total_orders::numeric, 2), 0) AS canceled_orders_ratio,
       COALESCE (total_order_costs, 0) AS total_order_costs, /* если у пользователя все заказы отменены, то запишем 0 вместо NULL */
       COALESCE (avg_order_cost, 0) AS avg_order_cost, /* если у пользователя все заказы отменены, то запишем 0 вместо NULL */
       COALESCE (num_installment_orders, 0) AS num_installment_orders, 
       COALESCE (num_orders_with_promo, 0) AS num_orders_with_promo,
       CASE
       	   WHEN money_transfer_num > 0 THEN 1
       	   ELSE 0
       END AS used_money_transfer,
       CASE 
       	   WHEN num_installment_orders > 0 THEN 1
       	   ELSE 0
       END AS used_installments,
       CASE 
       	   WHEN num_canceled_orders > 0 THEN 1
       	   ELSE 0
       END AS used_cancel   
FROM prefilter
LEFT JOIN order_time_stat USING (user_id)
LEFT JOIN order_stat USING (user_id)
LEFT JOIN order_cancel_stat USING (user_id)
LEFT JOIN order_payment_stat USING (user_id)
LEFT JOIN order_installments_stat USING (user_id)
LEFT JOIN order_promo_stat USING (user_id)
LEFT JOIN money_transfer_stat USING (user_id)
ORDER BY total_orders DESC;




/* Часть 2. Решение ad hoc задач */

/* Задача 1. Сегментация пользователей */

WITH sermentation AS (
                      SELECT user_id,
                             total_orders,
                             avg_order_cost,
                             CASE 
                             	WHEN total_orders = 1 THEN '1 заказ'
                            	WHEN total_orders >= 2 AND total_orders <= 5 THEN '2—5 заказов'
       	                        WHEN total_orders >= 6 AND total_orders <= 10 THEN '6–10 заказов'
                             	ELSE '11 и более заказов'
                             END AS segment    
                      FROM ds_ecom.product_user_features
                      )
SELECT segment,
       COUNT(user_id) AS user_number,
       ROUND(AVG(total_orders), 2) AS avg_order_number,
       ROUND(AVG(avg_order_cost), 2) AS avg_orders_costs
FROM sermentation
GROUP BY segment
ORDER BY user_number DESC;

/* Выводы:
 * Большая часть пользователей делает всего 1 заказ - порядка 96%.
 * В сегменте 2-5 заказов среднее количество заказов 2.09, что говорит о том, что в основном, пользователи, сделавшие более 1 заказа, делают всего 2.
 * Количество пользователей, сделавших более 5 заказов на уровне статистической погрешности - всего 6 штук из выборки более 60 000 пользователей
 * Средняя стоимость заказа уменьшается с числом заказов
 * Команде маркетологов стоит подумать о повышении лояльности пользователей, увеличении числа повторных заказов
*/


/* Задача 2. Ранжирование пользователей */

SELECT user_id,
       region,
       total_orders, 
       avg_order_cost 
FROM ds_ecom.product_user_features
WHERE total_orders >= 3
ORDER BY avg_order_cost DESC
LIMIT 15;

/* Выводы:
 * Большая часть пользователей с высокой средней стоимостью заказа сделала 3 заказа.
 * Средняя стоимость заказа для 15 пользователей различается почти в 3 раза, что говорит небольшом количестве крупных покупок на выборке из более чем 60 000 пользователей
*/


/* Задача 3. Статистика по регионам. */

WITH regions_stat AS (SELECT region, /* Собираем нужную статистику в CTE */
                             SUM(num_installment_orders) AS installment_orders,
                             SUM(num_orders_with_promo) AS promo_orders,
                             SUM(used_cancel) AS users_with_cancel,
                             COUNT (user_id) AS users_count,
                             SUM(total_orders) AS orders_count,
                             ROUND(AVG(avg_order_cost), 2) AS avg_order_region_cost
                      FROM ds_ecom.product_user_features
                      GROUP BY region 
                      )
SELECT region, /* В основном запросе проводим расчет нужных полей */
       users_count,
       orders_count,
       avg_order_region_cost,
       ROUND(installment_orders/orders_count::numeric, 3) AS installment_ratio,
       ROUND(promo_orders/orders_count::numeric, 3) AS promo_ratio,
       ROUND(users_with_cancel/users_count::numeric, 3) AS cancel_ratio
FROM regions_stat
ORDER BY users_count DESC;

/* Выводы:
 * Пользователи в Москве совершили 60% всех заказов.
 * Средняя стоимость заказа в Москве ниже, чем в иных регионах
 * Доля рассрочек в Москве ниже, чем в регионах, что говорит о более высокой покупательской способности.
 * Однако, в целом рассрочка является очень популярным инструментом (порядка 50% заказов).
 * Доля оплат промокодами примерно одинаковая во всех регионах - около 4%.
 * Количество отмен заказов на низком уровне, менее 1%
*/


/* Задача 4. Активность пользователей по первому месяцу заказа в 2023 году */

SELECT DATE_TRUNC('MONTH', first_order_ts) AS month_of_first_order,
       COUNT(user_id) AS num_of_clients,
       SUM(total_orders) AS number_of_orders,
       ROUND(AVG(avg_order_cost), 2) AS avg_orders_cost,
       ROUND(AVG(avg_order_rating),2) AS avg_orders_rating,
       ROUND(SUM(used_money_transfer)/COUNT(user_id)::numeric, 2) AS money_transfer_ratio,
       DATE_TRUNC('MINUTE', AVG(lifetime)) AS avg_lifetime
FROM ds_ecom.product_user_features
WHERE EXTRACT(YEAR FROM first_order_ts) = 2023
GROUP BY month_of_first_order;

/* Выводы:
 * Число заказов и клиентов росло в течение 2023 года.
 * Самые активные месяцы по числу клиентов, сделавших первый заказ были ноябрь и декабрь 2023 года.
 * Оценка заказов снизилась с ростом числа клиентов к концу 2023 года
 * Число пользователей, делающих оплату денежным переводом равномерно в течение года, как и средняя стоимость заказа.
 * Срок активности пользователей снижался в течение 2023 года.
 * Можно сделать вывод, что число клиентов росло, но при этом не росла доля повторных заказов, снижался рейтинг
 * - качество выполнения заказов требует проведения анализа для повышения лояльности клиентов
*/
