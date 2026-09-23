# Портфолио Data Science

Проекты, выполненные во время обучения на курсе «Data Scientist Plus» в [Яндекс Практикуме](https://practicum.yandex.ru/) (2025–2026): от SQL и исследовательского анализа до классического машинного обучения и вывода моделей в продакшен.

**Автор:** Александр Шмарук

**Стек:** Python · pandas · NumPy · SQL (PostgreSQL) · scikit-learn · CatBoost · LightGBM · XGBoost · Optuna · SHAP · statsmodels · matplotlib · seaborn · PySpark · Apache Airflow · MLflow · S3

## SQL

| Проект | Стек | Ключевой результат |
|---|---|---|
| [Монетизация онлайн-игры «Секреты Тёмнолесья»](01-sql/darkwood-game-monetization) | PostgreSQL: CTE, подзапросы, агрегатные функции, JOIN | Метрики платящих игроков и покупок в разрезе рас |
| [Витрина данных маркетплейса «ВсёТут» и ad hoc-задачи](01-sql/marketplace-datamart) | PostgreSQL: CTE, CASE, оконные и агрегатные функции, DATE_TRUNC | Витрина + сегментация и когортный анализ |

## Анализ данных

| Проект | Стек | Ключевой результат |
|---|---|---|
| [Предобработка данных о продажах видеоигр](02-data-analysis/games-market-preprocessing) | Python, pandas | Sony и Nintendo — лидеры рынка 2000–2013 |
| [Исследование рынка общепита Москвы](02-data-analysis/moscow-restaurants-market) | Python, pandas, matplotlib, seaborn, phik | Рекомендации по округу и формату заведения |
| [Исследование возвратности пользователей Яндекс Афиши](02-data-analysis/yandex-afisha-eda) | Python, pandas, SQLAlchemy, PostgreSQL, matplotlib, seaborn, phik | 61,7% пользователей возвращаются; драйверы возврата |

## Статистика и A/B-тесты

| Проект | Стек | Ключевой результат |
|---|---|---|
| [A/B-тест алгоритма рекомендаций в приложении коротких видео](03-statistics-ab-testing/ab-test-video-feed) | Python, pandas, statsmodels, matplotlib | Эффекта нет (p = 0,58) — алгоритм не внедрять |

## Машинное обучение

| Проект | Стек | Ключевой результат |
|---|---|---|
| [Прогноз массы морских черепах по замерам](04-machine-learning/turtles-weight-regression) | Python, pandas, scikit-learn, matplotlib | R² = 0,981, MAPE = 4% |
| [Прогноз оттока клиентов сервиса доставки кофе](04-machine-learning/coffee-churn-prediction) | Python, pandas, scikit-learn, phik, joblib | Драйверы оттока: сбои приложения и подписка |
| [Предсказание вероятности клика по рекламному баннеру (CTR)](04-machine-learning/adtech-ctr-prediction) | Python, pandas, scikit-learn (SVM, LogisticRegression, калибровка), mlxtend, phik | Калиброванная модель CTR, топ-5 признаков |
| [Прогноз спроса на прокат велосипедов по погоде](04-machine-learning/bike-rental-weather-knn-trees) | Python, pandas, scikit-learn, Optuna, joblib | Решающее дерево лучше KNN и бейзлайна |
| [Поведенческий скоринг: просрочка 90+ дней на горизонте 12 месяцев](04-machine-learning/behavioral-credit-scoring) | Python, pandas, scikit-learn, Optuna, mlxtend | Калиброванный Random Forest с порогом под бизнес-метрику |
| [Экспресс-оценка стоимости подержанного автомобиля](04-machine-learning/car-price-gradient-boosting) | Python, pandas, CatBoost, LightGBM, XGBoost, Optuna, SHAP | CatBoost, R² = 0,874 |
| [Определение возрастной группы пользователя по цифровому следу](04-machine-learning/user-age-prediction) | Python, pandas, scikit-learn, phik, joblib | Многоклассовая модель по поведенческим признакам |
| [Прогноз отмен бронирования в сети отелей](04-machine-learning/hotel-cancellation-prediction) | Python, pandas, SQLAlchemy, LightGBM, CatBoost, Optuna, SHAP, scikit-learn | Доля отмен ≤ 10%, прирост IR ≥ 50% |
| [Прогноз риска задержки доставки заказа](04-machine-learning/delivery-delay-prediction) | Python, pandas, scikit-learn (KMeans), CatBoost, phik | ROC-AUC = 0,768 (цель > 0,75) |

## MLOps и Data Engineering

| Проект | Стек | Ключевой результат |
|---|---|---|
| [Витрины для книжного рекомендательного сервиса на PySpark](05-mlops-data-engineering/book-recs-pyspark) | PySpark 3.3, YARN, S3 (Parquet) | 5 витрин, оптимизация плана выполнения |
| [Выявление мошеннических транзакций с трекингом экспериментов в MLflow](05-mlops-data-engineering/fraud-detection-mlflow) | Python, XGBoost, MLflow (Tracking, Model Registry), S3, SHAP | Эксперименты и Model Registry в MLflow |
| [Пайплайн батч-признаков в Airflow](05-mlops-data-engineering/batch-features-airflow) | Airflow, PostgreSQL, S3, pandas | Воспроизводимые признаки без утечки из будущего |
| [Прогноз продаж дарксторов с батч-инференсом в Airflow](05-mlops-data-engineering/darkstore-sales-forecast) | CatBoost, Airflow, PostgreSQL, S3, PSI | R² = 0,977, модель в S3, DAG инференса |

## Как устроен репозиторий

- Каждый проект лежит в отдельной папке со своим README: задача, ход работы, результат, стек.
- Ноутбуки сохранены с выводами ячеек, так что графики и таблицы видны прямо на GitHub.
- Датасеты, тексты заданий и комментарии ревьюеров не публикуются — по правилам Яндекс Практикума.
- Доступы к базам данных и S3 в коде берутся из переменных окружения.
