# Data Science Portfolio

[Русская версия](README.md) · **English**

Projects completed during the **Data Scientist Plus** program at [Yandex Practicum](https://practicum.yandex.ru/) (2025–2026): from SQL and exploratory analysis to classical machine learning and putting models into production.

**Author:** Aleksandr Shmaruk

**Stack:** Python · pandas · NumPy · SQL (PostgreSQL) · scikit-learn · CatBoost · LightGBM · XGBoost · Optuna · SHAP · statsmodels · matplotlib · seaborn · PySpark · Apache Airflow · MLflow · S3

## SQL

| Project | Key result |
|---|---|
| [In-game currency monetization analysis ("Darkwood Secrets")](01-sql/darkwood-game-monetization) | Payer and purchase metrics by character race |
| [Marketplace data mart and ad hoc analytics ("VseTut")](01-sql/marketplace-datamart) | ~96% of users order once; installments used in ~50% of orders |

## Data Analysis

| Project | Key result |
|---|---|
| [Video game sales data preprocessing](02-data-analysis/games-market-preprocessing) | Sony and Nintendo led the 2000–2013 market |
| [Moscow restaurant market research](02-data-analysis/moscow-restaurants-market) | District and format recommendations for a new venue |
| [User retention analysis for Yandex Afisha (ticketing service)](02-data-analysis/yandex-afisha-eda) | 61.7% of users return; key retention drivers found |

## Statistics & A/B Testing

| Project | Key result |
|---|---|
| [A/B test of a recommendation algorithm in a short-video app](03-statistics-ab-testing/ab-test-video-feed) | No effect (31.57% vs 31.47%, p = 0.58) — do not ship |

## Machine Learning

| Project | Key result |
|---|---|
| [Sea turtle weight prediction from body measurements](04-machine-learning/turtles-weight-regression) | R² = 0.981, MAPE = 4% |
| [Customer churn prediction for a coffee delivery service](04-machine-learning/coffee-churn-prediction) | Main churn drivers: app failures and paid subscription |
| [Ad click-through rate (CTR) prediction](04-machine-learning/adtech-ctr-prediction) | Calibrated CTR model, top-5 features identified |
| [Bike rental demand forecasting from weather](04-machine-learning/bike-rental-weather-knn-trees) | Decision tree beats KNN and baseline |
| [Behavioral credit scoring: 90+ days past due within 12 months](04-machine-learning/behavioral-credit-scoring) | Calibrated Random Forest with a business-driven threshold |
| [Instant used-car buyout price estimation](04-machine-learning/car-price-gradient-boosting) | CatBoost, R² = 0.874 |
| [Predicting user age group from digital footprint](04-machine-learning/user-age-prediction) | Multiclass model on behavioral features |
| [Hotel booking cancellation prediction](04-machine-learning/hotel-cancellation-prediction) | Cancellations ≤ 10%, Incremental Revenue ≥ +50% |
| [Delivery delay risk prediction](04-machine-learning/delivery-delay-prediction) | ROC-AUC = 0.768 (target > 0.75) |

## MLOps & Data Engineering

| Project | Key result |
|---|---|
| [PySpark data marts for a book recommendation service](05-mlops-data-engineering/book-recs-pyspark) | 5 data marts; early filtering cut input from ~1M to 126K rows |
| [Fraud detection with MLflow experiment tracking](05-mlops-data-engineering/fraud-detection-mlflow) | Experiments and Model Registry in MLflow |
| [Batch feature pipeline in Airflow](05-mlops-data-engineering/batch-features-airflow) | Reproducible, leak-free features |
| [Dark store weekly sales forecast with batch inference in Airflow](05-mlops-data-engineering/darkstore-sales-forecast) | R² = 0.977, model in S3, inference DAG |

## About this repository

- Each project has its own folder with a README (Russian, plus a short English summary).
- Notebooks are saved with cell outputs, so charts and tables render directly on GitHub.
- Notebooks and code comments are in Russian.
- Datasets, assignment texts and reviewer comments are not published, per Yandex Practicum rules.
- Database and S3 credentials are read from environment variables.
