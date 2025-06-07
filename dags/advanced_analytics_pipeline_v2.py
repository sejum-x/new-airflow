from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.http.sensors.http import HttpSensor
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator  # ВИПРАВЛЕНО: замість DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import json
import logging
import random

# Default arguments
default_args = {
    'owner': 'data_analytics_team',
    'depends_on_past': False,
    'email': ['analytics-alerts@company.com'],
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
        "conversion_rate": round(random.uniform(2.5, 4.5), 2)
    }
    logging.info(f"CRM data extracted: {mock_data}")
    return mock_data

def extract_ecommerce_data(**context):
    """Extract e-commerce transaction data"""
    logging.info("Extracting e-commerce data...")
    mock_data = {
        "orders": random.randint(800, 1200),
        "revenue": round(random.uniform(25000, 45000), 2),
        "avg_order_value": round(random.uniform(35, 65), 2)
    }
    logging.info(f"E-commerce data extracted: {mock_data}")
    return mock_data

def extract_social_media_data(**context):
    """Extract social media engagement data"""
    logging.info("Extracting social media data...")
    mock_data = {
        "instagram_followers": random.randint(15000, 18000),
        "facebook_engagement": round(random.uniform(3.2, 5.8), 2),
        "twitter_mentions": random.randint(200, 500)
    }
    logging.info(f"Social media data extracted: {mock_data}")
    return mock_data

def extract_email_campaign_data(**context):
    """Extract email marketing campaign data"""
    logging.info("Extracting email campaign data...")
    mock_data = {
        "emails_sent": random.randint(8000, 12000),
        "open_rate": round(random.uniform(18, 28), 2),
        "click_rate": round(random.uniform(2.5, 6.5), 2)
    }
    logging.info(f"Email campaign data extracted: {mock_data}")
    return mock_data

def extract_web_analytics(**context):
    """Extract web analytics data"""
    logging.info("Extracting web analytics...")
    mock_data = {
        "sessions": random.randint(25000, 35000),
        "bounce_rate": round(random.uniform(35, 55), 2),
        "conversion_rate": round(random.uniform(1.8, 3.2), 2)
    }
    logging.info(f"Web analytics extracted: {mock_data}")
    return mock_data

# === DATA PROCESSING FUNCTIONS ===
def calculate_customer_ltv(**context):
    """Calculate Customer Lifetime Value"""
    logging.info("Calculating Customer LTV...")
    # Simulate complex LTV calculation
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

def detect_anomalies(**context):
    """Detect anomalies in key metrics"""
    logging.info("Running anomaly detection...")
    anomalies = []
    
    # Simulate anomaly detection
    if random.random() > 0.7:  # 30% chance of anomaly
        anomalies.append({
            "metric": "conversion_rate",
            "expected": 2.8,
            "actual": 1.2,
            "severity": "HIGH"
        })
    
    logging.info(f"Anomalies detected: {len(anomalies)}")
    return anomalies

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

def predict_churn(**context):
    """Predict customer churn using ML model"""
    logging.info("Running churn prediction model...")
    churn_prediction = {
        "high_risk_customers": random.randint(50, 120),
        "medium_risk_customers": random.randint(150, 250),
        "model_accuracy": round(random.uniform(82, 92), 2)
    }
    logging.info(f"Churn prediction results: {churn_prediction}")
    return churn_prediction

def optimize_pricing(**context):
    """Run price optimization analysis"""
    logging.info("Running price optimization...")
    optimization = {
        "recommended_price_changes": random.randint(5, 15),
        "potential_revenue_lift": round(random.uniform(3.5, 8.2), 2)
    }
    logging.info(f"Price optimization results: {optimization}")
    return optimization

# === REPORTING FUNCTIONS ===
def generate_executive_dashboard(**context):
    """Generate executive dashboard"""
    logging.info("Generating executive dashboard...")
    dashboard = {
        "kpis": {
            "revenue_growth": f"{random.uniform(5, 15):.1f}%",
            "customer_acquisition_cost": f"${random.uniform(25, 45):.2f}",
            "monthly_recurring_revenue": f"${random.uniform(85000, 125000):.2f}"
        }
    }
    logging.info(f"Executive dashboard generated: {dashboard}")
    return dashboard

def generate_marketing_report(**context):
    """Generate marketing performance report"""
    logging.info("Generating marketing report...")
    report = {
        "campaign_performance": "Generated",
        "channel_attribution": "Calculated",
        "roi_by_channel": "Analyzed"
    }
    logging.info(f"Marketing report generated: {report}")
    return report

def generate_customer_insights(**context):
    """Generate customer behavior insights"""
    logging.info("Generating customer insights...")
    insights = {
        "behavioral_patterns": "Identified",
        "segment_preferences": "Analyzed",
        "churn_risk_factors": "Documented"
    }
    logging.info(f"Customer insights generated: {insights}")
    return insights

def validate_data_quality(**context):
    """Comprehensive data quality validation"""
    logging.info("Validating data quality across all sources...")
    
    # Simulate data quality checks
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

# Create the DAG
with DAG(
    'advanced_analytics_pipeline_v2',
    default_args=default_args,
    description='Advanced multi-source analytics pipeline with parallel processing',
    schedule='0 6 * * *',  # Daily at 6 AM
    start_date=datetime(2025, 5, 1),
    catchup=False,
    tags=['analytics', 'marketing', 'customer-intelligence', 'production'],
    max_active_runs=1
) as dag:
    
    # === PIPELINE START ===
    pipeline_start = EmptyOperator(  # ВИПРАВЛЕНО: замість DummyOperator
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
        
        check_ecommerce_data = GCSObjectExistenceSensor(
            task_id='check_ecommerce_data',
            bucket='analytics-data-lake',
            object='ecommerce/daily/{{ds}}/transactions.csv',
            mode='reschedule'
        )
        
        check_social_api = HttpSensor(
            task_id='check_social_api',
            http_conn_id='social_api',
            endpoint='status',
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
    
    # === PARALLEL ANALYTICS PROCESSING ===
    with TaskGroup(group_id='customer_analytics') as customer_analytics:
        calculate_ltv = PythonOperator(
            task_id='calculate_customer_ltv',
            python_callable=calculate_customer_ltv
        )
        
        cohort_analysis = PythonOperator(
            task_id='perform_cohort_analysis',
            python_callable=perform_cohort_analysis
        )
        
        customer_segmentation = PythonOperator(
            task_id='segment_customers',
            python_callable=segment_customers
        )
        
        churn_prediction = PythonOperator(
            task_id='predict_churn',
            python_callable=predict_churn
        )
    
    with TaskGroup(group_id='marketing_analytics') as marketing_analytics:
        attribution_analysis = PythonOperator(
            task_id='calculate_marketing_attribution',
            python_callable=calculate_marketing_attribution
        )
        
        anomaly_detection = PythonOperator(
            task_id='detect_anomalies',
            python_callable=detect_anomalies
        )
        
        price_optimization = PythonOperator(
            task_id='optimize_pricing',
            python_callable=optimize_pricing
        )
    
    # === ADVANCED ML FEATURES ===
    with TaskGroup(group_id='ml_feature_engineering') as ml_features:
        build_customer_features = BigQueryInsertJobOperator(
            task_id='build_customer_features',
            configuration={
                'query': {
                    'query': '''
                    -- Customer behavior features
                    SELECT 
                        'customer_123' as customer_id,
                        45.50 as avg_order_value,
                        12 as total_orders,
                        '{{ds}}' as last_order_date,
                        5 as days_since_last_order
                    UNION ALL
                    SELECT 
                        'customer_456' as customer_id,
                        67.25 as avg_order_value,
                        8 as total_orders,
                        '{{ds}}' as last_order_date,
                        2 as days_since_last_order
                    ''',
                    'destinationTable': {
                        'projectId': 'analytics-project',
                        'datasetId': 'ml_features',
                        'tableId': 'customer_features'
                    },
                    'writeDisposition': 'WRITE_TRUNCATE',
                    'useLegacySql': False
                }
            }
        )
        
        build_marketing_features = BigQueryInsertJobOperator(
            task_id='build_marketing_features',
            configuration={
                'query': {
                    'query': '''
                    -- Marketing channel features
                    SELECT 
                        'google_ads' as channel,
                        1500 as total_clicks,
                        45000 as total_impressions,
                        2500.00 as total_cost,
                        3.2 as avg_conversion_rate
                    UNION ALL
                    SELECT 
                        'facebook' as channel,
                        1200 as total_clicks,
                        38000 as total_impressions,
                        1800.00 as total_cost,
                        2.8 as avg_conversion_rate
                    ''',
                    'destinationTable': {
                        'projectId': 'analytics-project',
                        'datasetId': 'ml_features',
                        'tableId': 'marketing_features'
                    },
                    'writeDisposition': 'WRITE_TRUNCATE',
                    'useLegacySql': False
                }
            }
        )
    
    # === REPORTING LAYER ===
    with TaskGroup(group_id='reporting') as reporting:
        exec_dashboard = PythonOperator(
            task_id='generate_executive_dashboard',
            python_callable=generate_executive_dashboard
        )
        
        marketing_report = PythonOperator(
            task_id='generate_marketing_report',
            python_callable=generate_marketing_report
        )
        
        customer_insights_report = PythonOperator(
            task_id='generate_customer_insights',
            python_callable=generate_customer_insights
        )
    
    # === FINAL CONSOLIDATION ===
    consolidate_insights = EmptyOperator(  # ВИПРАВЛЕНО: замість DummyOperator
        task_id='consolidate_insights',
        trigger_rule=TriggerRule.ALL_SUCCESS
    )
    
    # === NOTIFICATIONS ===
    notify_success = BashOperator(
        task_id='notify_success',
        bash_command='echo "🎉 Analytics pipeline completed successfully! Dashboard: https://analytics.company.com/dashboard/{{ds_nodash}}"'
    )
    
    notify_stakeholders = BashOperator(
        task_id='notify_stakeholders',
        bash_command='echo "📊 Daily analytics report ready. Key insights and recommendations available."'
    )
    
    pipeline_end = EmptyOperator(  # ВИПРАВЛЕНО: замість DummyOperator
        task_id='pipeline_end'
    )
    
    # === COMPLEX DEPENDENCY GRAPH ===
    
    # Start pipeline
    pipeline_start >> availability_checks
    
    # Data extraction depends on availability checks
    availability_checks >> data_extraction
    
    # Data quality check after extraction
    data_extraction >> data_quality_check
    
    # Parallel analytics processing after quality check
    data_quality_check >> customer_analytics
    data_quality_check >> marketing_analytics
    data_quality_check >> ml_features
    
    # Reporting depends on analytics completion
    customer_analytics >> reporting
    marketing_analytics >> reporting
    
    # Executive dashboard needs all analytics
    customer_analytics >> exec_dashboard
    marketing_analytics >> exec_dashboard
    
    # Customer insights need customer analytics and ML features
    customer_analytics >> customer_insights_report
    ml_features >> customer_insights_report
    
    # Marketing report needs marketing analytics and ML features
    marketing_analytics >> marketing_report
    ml_features >> marketing_report
    
    # Consolidation waits for all reports
    reporting >> consolidate_insights
    
    # Final notifications
    consolidate_insights >> notify_success
    consolidate_insights >> notify_stakeholders
    
    # Pipeline end
    notify_success >> pipeline_end
    notify_stakeholders >> pipeline_end