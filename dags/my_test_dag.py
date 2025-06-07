from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.http.sensors.http import HttpSensor
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import json
import logging
import random
import numpy as np

# Default arguments
default_args = {
    'owner': 'ml_analytics_team',
    'depends_on_past': False,
    'email': ['ml-alerts@company.com', 'data-science@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# === DATA EXTRACTION FUNCTIONS ===
def extract_crm_data(**context):
    """Extract customer data from CRM system"""
    logging.info("Extracting CRM customer data...")
    mock_data = {
        "customers": random.randint(1000, 1500),
        "new_leads": random.randint(50, 150),
        "conversion_rate": round(random.uniform(2.5, 4.5), 2),
        "customer_features": {
            "avg_deal_size": round(random.uniform(500, 2000), 2),
            "sales_cycle_days": random.randint(14, 45),
            "lead_score_avg": round(random.uniform(60, 85), 1)
        }
    }
    logging.info(f"CRM data extracted: {mock_data}")
    return mock_data

def extract_ecommerce_data(**context):
    """Extract e-commerce transaction data"""
    logging.info("Extracting e-commerce data...")
    mock_data = {
        "orders": random.randint(800, 1200),
        "revenue": round(random.uniform(25000, 45000), 2),
        "avg_order_value": round(random.uniform(35, 65), 2),
        "product_interactions": {
            "page_views": random.randint(50000, 80000),
            "cart_additions": random.randint(2000, 4000),
            "checkout_starts": random.randint(1200, 2000)
        }
    }
    logging.info(f"E-commerce data extracted: {mock_data}")
    return mock_data

def extract_behavioral_data(**context):
    """Extract user behavioral data for ML models"""
    logging.info("Extracting behavioral data...")
    mock_data = {
        "user_sessions": random.randint(15000, 25000),
        "click_patterns": {
            "avg_clicks_per_session": round(random.uniform(8, 15), 1),
            "time_on_page_avg": round(random.uniform(120, 300), 1),
            "scroll_depth_avg": round(random.uniform(0.6, 0.9), 2)
        },
        "feature_usage": {
            "search_usage": round(random.uniform(0.3, 0.7), 2),
            "filter_usage": round(random.uniform(0.2, 0.5), 2),
            "recommendation_clicks": round(random.uniform(0.1, 0.3), 2)
        }
    }
    logging.info(f"Behavioral data extracted: {mock_data}")
    return mock_data

def extract_social_media_data(**context):
    """Extract social media engagement data"""
    logging.info("Extracting social media data...")
    mock_data = {
        "instagram_followers": random.randint(15000, 18000),
        "facebook_engagement": round(random.uniform(3.2, 5.8), 2),
        "twitter_mentions": random.randint(200, 500),
        "sentiment_scores": {
            "positive": round(random.uniform(0.6, 0.8), 2),
            "neutral": round(random.uniform(0.15, 0.25), 2),
            "negative": round(random.uniform(0.05, 0.15), 2)
        }
    }
    logging.info(f"Social media data extracted: {mock_data}")
    return mock_data

def extract_email_campaign_data(**context):
    """Extract email marketing campaign data"""
    logging.info("Extracting email campaign data...")
    mock_data = {
        "emails_sent": random.randint(8000, 12000),
        "open_rate": round(random.uniform(18, 28), 2),
        "click_rate": round(random.uniform(2.5, 6.5), 2),
        "personalization_metrics": {
            "dynamic_content_ctr": round(random.uniform(4.5, 8.2), 2),
            "subject_line_variants": random.randint(3, 8),
            "optimal_send_time_score": round(random.uniform(0.7, 0.95), 2)
        }
    }
    logging.info(f"Email campaign data extracted: {mock_data}")
    return mock_data

def extract_web_analytics(**context):
    """Extract web analytics data"""
    logging.info("Extracting web analytics...")
    mock_data = {
        "sessions": random.randint(25000, 35000),
        "bounce_rate": round(random.uniform(35, 55), 2),
        "conversion_rate": round(random.uniform(1.8, 3.2), 2),
        "user_journey_data": {
            "avg_pages_per_session": round(random.uniform(3.2, 6.8), 1),
            "session_duration_avg": round(random.uniform(180, 420), 1),
            "exit_page_patterns": ["checkout", "pricing", "contact"]
        }
    }
    logging.info(f"Web analytics extracted: {mock_data}")
    return mock_data

# === ADVANCED ML FUNCTIONS ===
def train_churn_prediction_model(**context):
    """Train customer churn prediction model"""
    logging.info("Training churn prediction model...")
    
    # Simulate model training
    model_metrics = {
        "model_type": "XGBoost",
        "training_samples": random.randint(8000, 12000),
        "features_used": 45,
        "cross_validation_score": round(random.uniform(0.82, 0.91), 3),
        "precision": round(random.uniform(0.78, 0.88), 3),
        "recall": round(random.uniform(0.75, 0.85), 3),
        "f1_score": round(random.uniform(0.76, 0.86), 3),
        "feature_importance": {
            "days_since_last_purchase": 0.23,
            "total_spent": 0.18,
            "support_tickets": 0.15,
            "email_engagement": 0.12,
            "session_frequency": 0.10
        }
    }
    
    logging.info(f"Churn model trained: {model_metrics}")
    return model_metrics

def train_recommendation_model(**context):
    """Train product recommendation model"""
    logging.info("Training recommendation model...")
    
    model_metrics = {
        "model_type": "Collaborative Filtering + Content-Based",
        "algorithm": "Matrix Factorization (ALS)",
        "training_interactions": random.randint(50000, 80000),
        "unique_users": random.randint(8000, 12000),
        "unique_items": random.randint(1500, 2500),
        "map_at_10": round(random.uniform(0.15, 0.25), 3),
        "ndcg_at_10": round(random.uniform(0.22, 0.32), 3),
        "coverage": round(random.uniform(0.65, 0.85), 3),
        "diversity": round(random.uniform(0.45, 0.65), 3)
    }
    
    logging.info(f"Recommendation model trained: {model_metrics}")
    return model_metrics

def train_price_optimization_model(**context):
    """Train dynamic pricing optimization model"""
    logging.info("Training price optimization model...")
    
    model_metrics = {
        "model_type": "Multi-Armed Bandit + Regression",
        "price_elasticity_model": "Ridge Regression",
        "demand_forecasting_model": "LSTM",
        "training_period_days": 90,
        "products_analyzed": random.randint(200, 500),
        "price_sensitivity_score": round(random.uniform(0.3, 0.7), 2),
        "revenue_lift_potential": round(random.uniform(5.2, 12.8), 1),
        "confidence_interval": "95%"
    }
    
    logging.info(f"Price optimization model trained: {model_metrics}")
    return model_metrics

def train_ltv_prediction_model(**context):
    """Train Customer Lifetime Value prediction model"""
    logging.info("Training LTV prediction model...")
    
    model_metrics = {
        "model_type": "Gradient Boosting Regressor",
        "prediction_horizon_months": 12,
        "training_customers": random.randint(5000, 8000),
        "mae": round(random.uniform(45.2, 78.5), 2),
        "rmse": round(random.uniform(85.3, 125.7), 2),
        "r2_score": round(random.uniform(0.72, 0.86), 3),
        "feature_categories": {
            "transactional": 15,
            "behavioral": 12,
            "demographic": 8,
            "engagement": 10
        }
    }
    
    logging.info(f"LTV model trained: {model_metrics}")
    return model_metrics

def train_sentiment_analysis_model(**context):
    """Train sentiment analysis model for customer feedback"""
    logging.info("Training sentiment analysis model...")
    
    model_metrics = {
        "model_type": "BERT Fine-tuned",
        "training_samples": random.randint(15000, 25000),
        "languages_supported": ["en", "es", "fr"],
        "accuracy": round(random.uniform(0.88, 0.94), 3),
        "precision_positive": round(random.uniform(0.85, 0.92), 3),
        "precision_negative": round(random.uniform(0.82, 0.89), 3),
        "precision_neutral": round(random.uniform(0.78, 0.86), 3),
        "processing_speed_per_sec": random.randint(500, 1200)
    }
    
    logging.info(f"Sentiment analysis model trained: {model_metrics}")
    return model_metrics

def train_anomaly_detection_model(**context):
    """Train anomaly detection model for business metrics"""
    logging.info("Training anomaly detection model...")
    
    model_metrics = {
        "model_type": "Isolation Forest + LSTM Autoencoder",
        "metrics_monitored": 25,
        "training_period_days": 180,
        "anomaly_threshold": 0.05,
        "false_positive_rate": round(random.uniform(0.02, 0.08), 3),
        "detection_accuracy": round(random.uniform(0.89, 0.96), 3),
        "alert_categories": {
            "revenue_anomalies": "high_priority",
            "traffic_anomalies": "medium_priority", 
            "conversion_anomalies": "high_priority"
        }
    }
    
    logging.info(f"Anomaly detection model trained: {model_metrics}")
    return model_metrics

def generate_ml_predictions(**context):
    """Generate predictions using trained models"""
    logging.info("Generating ML predictions...")
    
    predictions = {
        "churn_predictions": {
            "high_risk_customers": random.randint(120, 200),
            "medium_risk_customers": random.randint(300, 500),
            "predicted_churn_rate": round(random.uniform(8.5, 15.2), 1)
        },
        "ltv_predictions": {
            "avg_predicted_ltv": round(random.uniform(180, 320), 2),
            "high_value_segment_size": random.randint(800, 1200),
            "ltv_growth_forecast": round(random.uniform(5.2, 12.8), 1)
        },
        "recommendation_stats": {
            "recommendations_generated": random.randint(25000, 40000),
            "expected_ctr": round(random.uniform(3.2, 6.8), 2),
            "personalization_coverage": round(random.uniform(0.75, 0.92), 2)
        },
        "price_recommendations": {
            "products_to_optimize": random.randint(45, 85),
            "avg_price_change_percent": round(random.uniform(-8.5, 12.3), 1),
            "expected_revenue_impact": round(random.uniform(15000, 35000), 2)
        }
    }
    
    logging.info(f"ML predictions generated: {predictions}")
    return predictions

def evaluate_model_performance(**context):
    """Evaluate and monitor ML model performance"""
    logging.info("Evaluating model performance...")
    
    performance_report = {
        "models_evaluated": 6,
        "performance_summary": {
            "churn_model": {
                "status": "healthy",
                "drift_score": round(random.uniform(0.02, 0.08), 3),
                "accuracy_change": round(random.uniform(-2.1, 1.8), 2)
            },
            "recommendation_model": {
                "status": "needs_retraining",
                "drift_score": round(random.uniform(0.12, 0.18), 3),
                "accuracy_change": round(random.uniform(-5.2, -2.8), 2)
            },
            "ltv_model": {
                "status": "healthy", 
                "drift_score": round(random.uniform(0.03, 0.07), 3),
                "accuracy_change": round(random.uniform(-1.2, 2.3), 2)
            }
        },
        "retraining_schedule": {
            "churn_model": "next_week",
            "recommendation_model": "urgent",
            "price_optimization": "next_month"
        }
    }
    
    logging.info(f"Model performance evaluation: {performance_report}")
    return performance_report

def deploy_ml_models(**context):
    """Deploy trained models to production"""
    logging.info("Deploying ML models to production...")
    
    deployment_status = {
        "models_deployed": 4,
        "deployment_details": {
            "churn_model": {
                "version": "v2.3.1",
                "endpoint": "https://ml-api.company.com/churn/predict",
                "latency_p95": f"{random.randint(45, 85)}ms",
                "throughput": f"{random.randint(800, 1500)}/sec"
            },
            "recommendation_model": {
                "version": "v1.8.2", 
                "endpoint": "https://ml-api.company.com/recommendations",
                "latency_p95": f"{random.randint(120, 200)}ms",
                "throughput": f"{random.randint(300, 600)}/sec"
            }
        },
        "monitoring_enabled": True,
        "a_b_testing_active": True
    }
    
    logging.info(f"Models deployed: {deployment_status}")
    return deployment_status

# === TRADITIONAL ANALYTICS FUNCTIONS ===
def calculate_customer_ltv(**context):
    """Calculate Customer Lifetime Value"""
    logging.info("Calculating Customer LTV...")
    ltv = round(random.uniform(150, 300), 2)
    logging.info(f"Average Customer LTV: ${ltv}")
    return ltv

def perform_cohort_analysis(**context):
    """Perform customer cohort analysis"""
    logging.info("Performing cohort analysis...")
    cohorts = {
        "month_1_retention": round(random.uniform(65, 85), 2),
        "month_3_retention": round(random.uniform(35, 55), 2),
        "month_6_retention": round(random.uniform(20, 35), 2)
    }
    logging.info(f"Cohort analysis results: {cohorts}")
    return cohorts

def calculate_marketing_attribution(**context):
    """Calculate multi-touch marketing attribution"""
    logging.info("Calculating marketing attribution...")
    attribution = {
        "first_touch": {"organic": 35, "paid_search": 25, "social": 20, "email": 20},
        "last_touch": {"organic": 30, "paid_search": 30, "social": 15, "email": 25}
    }
    logging.info(f"Attribution model results: {attribution}")
    return attribution

def segment_customers(**context):
    """Perform customer segmentation using RFM analysis"""
    logging.info("Performing customer segmentation...")
    segments = {
        "champions": random.randint(150, 250),
        "loyal_customers": random.randint(300, 450),
        "potential_loyalists": random.randint(200, 350),
        "at_risk": random.randint(100, 200),
        "hibernating": random.randint(80, 150)
    }
    logging.info(f"Customer segments: {segments}")
    return segments

def validate_data_quality(**context):
    """Comprehensive data quality validation"""
    logging.info("Validating data quality across all sources...")
    
    quality_score = random.uniform(85, 98)
    issues = []
    
    if quality_score < 90:
        issues.append("Minor data completeness issues in social media data")
    if quality_score < 85:
        issues.append("Data freshness warning in CRM system")
    
    result = {
        "overall_score": round(quality_score, 2),
        "issues": issues,
        "status": "PASS" if quality_score > 80 else "FAIL"
    }
    
    logging.info(f"Data quality validation: {result}")
    return result

# === REPORTING FUNCTIONS ===
def generate_ml_performance_report(**context):
    """Generate ML models performance report"""
    logging.info("Generating ML performance report...")
    
    report = {
        "report_date": context['ds'],
        "models_in_production": 6,
        "total_predictions_served": random.randint(150000, 250000),
        "model_performance": {
            "churn_model_accuracy": round(random.uniform(0.85, 0.92), 3),
            "recommendation_ctr": round(random.uniform(4.2, 7.8), 2),
            "ltv_prediction_mae": round(random.uniform(45, 75), 2)
        },
        "business_impact": {
            "churn_prevented_customers": random.randint(45, 85),
            "additional_revenue_from_recommendations": round(random.uniform(25000, 45000), 2),
            "pricing_optimization_lift": round(random.uniform(8.5, 15.2), 1)
        }
    }
    
    logging.info(f"ML performance report generated: {report}")
    return report

def generate_executive_dashboard(**context):
    """Generate executive dashboard"""
    logging.info("Generating executive dashboard...")
    dashboard = {
        "kpis": {
            "revenue_growth": f"{random.uniform(5, 15):.1f}%",
            "customer_acquisition_cost": f"${random.uniform(25, 45):.2f}",
            "monthly_recurring_revenue": f"${random.uniform(85000, 125000):.2f}",
            "ml_driven_revenue": f"${random.uniform(15000, 35000):.2f}"
        }
    }
    logging.info(f"Executive dashboard generated: {dashboard}")
    return dashboard

def generate_data_science_report(**context):
    """Generate comprehensive data science insights report"""
    logging.info("Generating data science report...")
    
    report = {
        "experiment_results": {
            "ab_tests_running": random.randint(8, 15),
            "significant_results": random.randint(3, 7),
            "recommendation_algorithm_winner": "collaborative_filtering_v2"
        },
        "model_insights": {
            "top_churn_risk_factors": ["low_engagement", "support_tickets", "price_sensitivity"],
            "highest_ltv_customer_traits": ["frequent_purchaser", "premium_features", "referrals"],
            "optimal_pricing_segments": random.randint(4, 8)
        },
        "data_quality_score": round(random.uniform(88, 96), 1),
        "feature_engineering_opportunities": random.randint(12, 25)
    }
    
    logging.info(f"Data science report generated: {report}")
    return report


# Create the DAG
with DAG(
    'advanced_ml_analytics_pipeline',
    default_args=default_args,
    description='Advanced ML-driven analytics pipeline with model training and deployment',
    schedule='0 6 * * *',  # Daily at 6 AM
    start_date=datetime(2025, 5, 1),
    catchup=False,
    tags=['ml', 'analytics', 'ai', 'data-science', 'production'],
    max_active_runs=1
) as dag:
    
    # === PIPELINE START ===
    pipeline_start = EmptyOperator(
        task_id='pipeline_start'
    )
    
    # === DATA AVAILABILITY CHECKS ===
    with TaskGroup(group_id='data_availability_checks') as availability_checks:
        check_crm_api = HttpSensor(
            task_id='check_crm_api',
            http_conn_id='crm_api',
            endpoint='health',
            request_params={},
            response_check=lambda response: response.status_code == 200,
            poke_interval=30,
            timeout=300,
            mode='reschedule'
        )
        
        check_ml_data_lake = GCSObjectExistenceSensor(
            task_id='check_ml_data_lake',
            bucket='ml-data-lake',
            object='features/daily/{{ds}}/feature_store.parquet',
            mode='reschedule'
        )
        
        check_model_registry = HttpSensor(
            task_id='check_model_registry',
            http_conn_id='mlflow_api',
            endpoint='health',
            request_params={},
            response_check=lambda response: response.status_code == 200,
            poke_interval=30,
            timeout=300,
            mode='reschedule'
        )
    
    # === PARALLEL DATA EXTRACTION ===
    with TaskGroup(group_id='data_extraction') as data_extraction:
        extract_crm = PythonOperator(
            task_id='extract_crm_data',
            python_callable=extract_crm_data
        )
        
        extract_ecommerce = PythonOperator(
            task_id='extract_ecommerce_data',
            python_callable=extract_ecommerce_data
        )
        
        extract_behavioral = PythonOperator(
            task_id='extract_behavioral_data',
            python_callable=extract_behavioral_data
        )
        
        extract_social = PythonOperator(
            task_id='extract_social_media_data',
            python_callable=extract_social_media_data
        )
        
        extract_email = PythonOperator(
            task_id='extract_email_campaign_data',
            python_callable=extract_email_campaign_data
        )
        
        extract_web = PythonOperator(
            task_id='extract_web_analytics',
            python_callable=extract_web_analytics
        )
    
    # === DATA QUALITY VALIDATION ===
    data_quality_check = PythonOperator(
        task_id='validate_data_quality',
        python_callable=validate_data_quality,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )
    
    # === FEATURE ENGINEERING ===
    with TaskGroup(group_id='feature_engineering') as feature_engineering:
        build_customer_features = BigQueryInsertJobOperator(
            task_id='build_customer_features',
            configuration={
                'query': {
                    'query': '''
                    -- Advanced customer features for ML
                    SELECT 
                        'customer_123' as customer_id,
                        45.50 as avg_order_value,
                        12 as total_orders,
                        5 as days_since_last_order,
                        0.85 as email_engagement_score,
                        3.2 as support_ticket_ratio,
                        0.65 as feature_usage_score,
                        'high_value' as customer_segment,
                        0.15 as churn_probability
                    UNION ALL
                    SELECT 
                        'customer_456' as customer_id,
                        67.25 as avg_order_value,
                        8 as total_orders,
                        2 as days_since_last_order,
                        0.92 as email_engagement_score,
                        1.1 as support_ticket_ratio,
                        0.78 as feature_usage_score,
                        'loyal' as customer_segment,
                        0.08 as churn_probability
                    ''',
                    'destinationTable': {
                        'projectId': 'ml-project',
                        'datasetId': 'features',
                        'tableId': 'customer_features'
                    },
                    'writeDisposition': 'WRITE_TRUNCATE',
                    'useLegacySql': False
                }
            }
        )
        
        build_behavioral_features = BigQueryInsertJobOperator(
            task_id='build_behavioral_features',
            configuration={
                'query': {
                    'query': '''
                    -- Behavioral features for recommendation system
                    SELECT 
                        'user_789' as user_id,
                        'product_123' as product_id,
                        8.5 as session_duration_minutes,
                        0.75 as scroll_depth,
                        3 as page_views,
                        1 as cart_additions,
                        0 as purchases,
                        0.65 as recommendation_click_rate
                    UNION ALL
                    SELECT 
                        'user_101' as user_id,
                        'product_456' as product_id,
                        12.3 as session_duration_minutes,
                        0.92 as scroll_depth,
                        5 as page_views,
                        2 as cart_additions,
                        1 as purchases,
                        0.82 as recommendation_click_rate
                    ''',
                    'destinationTable': {
                        'projectId': 'ml-project',
                        'datasetId': 'features',
                        'tableId': 'behavioral_features'
                    },
                    'writeDisposition': 'WRITE_TRUNCATE',
                    'useLegacySql': False
                }
            }
        )
        
        build_time_series_features = BigQueryInsertJobOperator(
            task_id='build_time_series_features',
            configuration={
                'query': {
                    'query': '''
                    -- Time series features for forecasting
                    SELECT 
                        '{{ds}}' as date,
                        'revenue' as metric,
                        45000.50 as value,
                        42000.25 as value_7d_ago,
                        38500.75 as value_30d_ago,
                        0.07 as growth_rate_7d,
                        0.17 as growth_rate_30d,
                        0.85 as seasonality_factor
                    UNION ALL
                    SELECT 
                        '{{ds}}' as date,
                        'conversion_rate' as metric,
                        3.2 as value,
                        3.0 as value_7d_ago,
                        2.8 as value_30d_ago,
                        0.067 as growth_rate_7d,
                        0.143 as growth_rate_30d,
                        1.05 as seasonality_factor
                    ''',
                    'destinationTable': {
                        'projectId': 'ml-project',
                        'datasetId': 'features',
                        'tableId': 'time_series_features'
                    },
                    'writeDisposition': 'WRITE_APPEND',
                    'useLegacySql': False
                }
            }
        )
    
    # === ML MODEL TRAINING (PARALLEL) ===
    with TaskGroup(group_id='ml_model_training') as ml_training:
        train_churn_model = PythonOperator(
            task_id='train_churn_prediction_model',
            python_callable=train_churn_prediction_model
        )
        
        train_recommendation_model = PythonOperator(
            task_id='train_recommendation_model',
            python_callable=train_recommendation_model
        )
        
        train_price_model = PythonOperator(
            task_id='train_price_optimization_model',
            python_callable=train_price_optimization_model
        )
        
        train_ltv_model = PythonOperator(
            task_id='train_ltv_prediction_model',
            python_callable=train_ltv_prediction_model
        )
        
        train_sentiment_model = PythonOperator(
            task_id='train_sentiment_analysis_model',
            python_callable=train_sentiment_analysis_model
        )
        
        train_anomaly_model = PythonOperator(
            task_id='train_anomaly_detection_model',
            python_callable=train_anomaly_detection_model
        )
    
    # === MODEL EVALUATION ===
    evaluate_models = PythonOperator(
        task_id='evaluate_model_performance',
        python_callable=evaluate_model_performance,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )
    
    # === MODEL DEPLOYMENT ===
    deploy_models = PythonOperator(
        task_id='deploy_ml_models',
        python_callable=deploy_ml_models
    )
    
    # === TRADITIONAL ANALYTICS (PARALLEL WITH ML) ===
    with TaskGroup(group_id='traditional_analytics') as traditional_analytics:
        calculate_ltv = PythonOperator(
            task_id='calculate_customer_ltv',
            python_callable=calculate_customer_ltv
        )
        
        cohort_analysis = PythonOperator(
            task_id='perform_cohort_analysis',
            python_callable=perform_cohort_analysis
        )
        
        attribution_analysis = PythonOperator(
            task_id='calculate_marketing_attribution',
            python_callable=calculate_marketing_attribution
        )
        
        customer_segmentation = PythonOperator(
            task_id='segment_customers',
            python_callable=segment_customers
        )
    
    # === ML PREDICTIONS GENERATION ===
    generate_predictions = PythonOperator(
        task_id='generate_ml_predictions',
        python_callable=generate_ml_predictions
    )
    
    # === REPORTING LAYER ===
    with TaskGroup(group_id='reporting') as reporting:
        ml_report = PythonOperator(
            task_id='generate_ml_performance_report',
            python_callable=generate_ml_performance_report
        )
        
        exec_dashboard = PythonOperator(
            task_id='generate_executive_dashboard',
            python_callable=generate_executive_dashboard
        )
        
        ds_report = PythonOperator(
            task_id='generate_data_science_report',
            python_callable=generate_data_science_report
        )
    
    # === FINAL STEPS ===
    consolidate_insights = EmptyOperator(
        task_id='consolidate_insights',
        trigger_rule=TriggerRule.ALL_SUCCESS
    )
    
    notify_ml_team = BashOperator(
        task_id='notify_ml_team',
        bash_command='echo "🤖 ML Pipeline completed! Models deployed and predictions generated. Dashboard: https://ml-dashboard.company.com/{{ds_nodash}}"'
    )
    
    notify_business_teams = BashOperator(
        task_id='notify_business_teams',
        bash_command='echo "📊 AI-powered insights ready! Churn predictions, recommendations, and pricing optimizations available."'
    )
    
    pipeline_end = EmptyOperator(
        task_id='pipeline_end'
    )
    
    # === COMPLEX ML DEPENDENCY GRAPH ===
    
    # Start pipeline
    pipeline_start >> availability_checks
    
    # Data extraction
    availability_checks >> data_extraction
    
    # Data quality and feature engineering
    data_extraction >> data_quality_check >> feature_engineering
    
    # Parallel ML training and traditional analytics
    feature_engineering >> ml_training
    data_quality_check >> traditional_analytics
    
    # Model evaluation and deployment
    ml_training >> evaluate_models >> deploy_models
    
    # Generate predictions after deployment
    deploy_models >> generate_predictions
    
    # Reporting needs both ML and traditional analytics
    generate_predictions >> ml_report
    traditional_analytics >> exec_dashboard
    ml_training >> ds_report
    traditional_analytics >> ds_report
    
    # Final consolidation
    reporting >> consolidate_insights
    
    # Notifications
    consolidate_insights >> notify_ml_team
    consolidate_insights >> notify_business_teams
    
    # Pipeline end
    notify_ml_team >> pipeline_end
    notify_business_teams >> pipeline_end