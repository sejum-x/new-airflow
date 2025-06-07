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
import pandas as pd
from typing import Dict, List

# Default arguments
default_args = {
    'owner': 'postal_analytics_team',
    'depends_on_past': False,
    'email': ['logistics-alerts@postal.com', 'analytics@postal.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# === DATA EXTRACTION FUNCTIONS ===
def extract_courier_delivery_data(**context):
    """Extract courier delivery transactions data"""
    logging.info("Extracting courier delivery data...")
    
    # Simulate courier delivery data
    deliveries = []
    courier_ids = [f"CUR_{i:03d}" for i in range(1, 51)]  # 50 couriers
    cities = ["Київ", "Харків", "Одеса", "Дніпро", "Львів", "Запоріжжя", "Кривий Ріг", "Миколаїв"]
    parcel_types = ["small", "medium", "large", "fragile", "express"]
    
    for _ in range(random.randint(800, 1200)):  # Daily deliveries
        courier_id = random.choice(courier_ids)
        city = random.choice(cities)
        parcel_type = random.choice(parcel_types)
        
        # Simulate delivery times based on city and parcel type
        base_delivery_time = {
            "small": random.randint(30, 90),
            "medium": random.randint(45, 120),
            "large": random.randint(60, 180),
            "fragile": random.randint(90, 240),
            "express": random.randint(15, 45)
        }[parcel_type]
        
        # City factor (bigger cities = faster delivery)
        city_factor = {
            "Київ": 0.8, "Харків": 0.9, "Одеса": 0.95,
            "Дніпро": 1.0, "Львів": 1.1, "Запоріжжя": 1.2,
            "Кривий Ріг": 1.3, "Миколаїв": 1.4
        }[city]
        
        delivery_duration = int(base_delivery_time * city_factor)
        
        receive_time = datetime.now() - timedelta(minutes=random.randint(60, 480))
        issue_time = receive_time + timedelta(minutes=delivery_duration)
        
        deliveries.append({
            "delivery_id": f"DEL_{random.randint(100000, 999999)}",
            "courier_id": courier_id,
            "parcel_id": f"PCL_{random.randint(100000, 999999)}",
            "parcel_type": parcel_type,
            "city": city,
            "receive_datetime": receive_time.isoformat(),
            "issue_datetime": issue_time.isoformat(),
            "delivery_duration_minutes": delivery_duration,
            "delivery_date": context['ds']
        })
    
    logging.info(f"Extracted {len(deliveries)} courier deliveries")
    return deliveries

def extract_department_workload_data(**context):
    """Extract department workload and processing data"""
    logging.info("Extracting department workload data...")
    
    departments = []
    dept_types = ["sorting", "distribution", "local", "hub"]
    cities = ["Київ", "Харків", "Одеса", "Дніпро", "Львів", "Запоріжжя"]
    transport_types = ["van", "truck", "motorcycle", "car"]
    parcel_types = ["small", "medium", "large", "fragile", "express"]
    
    for dept_id in range(1, 26):  # 25 departments
        dept_type = random.choice(dept_types)
        city = random.choice(cities)
        
        # Monthly workload simulation
        monthly_parcels = []
        
        for parcel_type in parcel_types:
            for transport_type in transport_types:
                # Different departments handle different volumes
                base_volume = {
                    "sorting": random.randint(800, 1500),
                    "distribution": random.randint(500, 1000),
                    "local": random.randint(200, 600),
                    "hub": random.randint(1200, 2500)
                }[dept_type]
                
                # Parcel type factor
                type_factor = {
                    "small": 1.5, "medium": 1.0, "large": 0.7,
                    "fragile": 0.4, "express": 0.8
                }[parcel_type]
                
                volume = int(base_volume * type_factor * random.uniform(0.7, 1.3))
                
                # Processing time varies by department efficiency
                avg_processing_time = {
                    "sorting": random.randint(15, 45),
                    "distribution": random.randint(20, 60),
                    "local": random.randint(10, 30),
                    "hub": random.randint(25, 75)
                }[dept_type]
                
                monthly_parcels.append({
                    "department_id": f"DEPT_{dept_id:03d}",
                    "department_type": dept_type,
                    "city": city,
                    "parcel_type": parcel_type,
                    "transport_type": transport_type,
                    "monthly_volume": volume,
                    "avg_processing_time_minutes": avg_processing_time,
                    "month": context['ds'][:7]  # YYYY-MM format
                })
        
        departments.extend(monthly_parcels)
    
    logging.info(f"Generated workload data for {len(departments)} department-type combinations")
    return departments

def extract_transport_utilization_data(**context):
    """Extract transport fleet utilization data"""
    logging.info("Extracting transport utilization data...")
    
    transport_data = []
    transport_types = ["van", "truck", "motorcycle", "car"]
    parcel_types = ["small", "medium", "large", "fragile", "express"]
    
    for transport_type in transport_types:
        # Fleet size varies by transport type
        fleet_size = {
            "van": random.randint(50, 80),
            "truck": random.randint(20, 35),
            "motorcycle": random.randint(30, 50),
            "car": random.randint(40, 60)
        }[transport_type]
        
        for parcel_type in parcel_types:
            # Transport suitability for parcel types
            suitability = {
                "van": {"small": 0.8, "medium": 1.0, "large": 0.9, "fragile": 0.7, "express": 0.6},
                "truck": {"small": 0.3, "medium": 0.7, "large": 1.0, "fragile": 0.8, "express": 0.4},
                "motorcycle": {"small": 1.0, "medium": 0.5, "large": 0.1, "fragile": 0.3, "express": 1.0},
                "car": {"small": 0.9, "medium": 0.8, "large": 0.4, "fragile": 0.6, "express": 0.8}
            }[transport_type][parcel_type]
            
            monthly_deliveries = int(fleet_size * suitability * random.randint(15, 35))
            
            # Calculate utilization metrics
            avg_distance_per_delivery = random.randint(5, 50)  # km
            fuel_consumption_per_km = {
                "van": 0.12, "truck": 0.25, "motorcycle": 0.05, "car": 0.08
            }[transport_type]
            
            transport_data.append({
                "transport_type": transport_type,
                "parcel_type": parcel_type,
                "fleet_size": fleet_size,
                "monthly_deliveries": monthly_deliveries,
                "avg_distance_per_delivery": avg_distance_per_delivery,
                "total_distance_km": monthly_deliveries * avg_distance_per_delivery,
                "fuel_consumption_liters": monthly_deliveries * avg_distance_per_delivery * fuel_consumption_per_km,
                "utilization_rate": round(monthly_deliveries / (fleet_size * 30) * 100, 2),  # % of capacity
                "month": context['ds'][:7]
            })
    
    logging.info(f"Generated transport utilization data for {len(transport_data)} combinations")
    return transport_data

def extract_route_validation_data(**context):
    """Extract route validation and logistics data"""
    logging.info("Extracting route validation data...")
    
    routes = []
    delivery_types = ["Department to Department", "Department to Courier", 
                     "Courier to Department", "Courier to Courier"]
    departments = [f"DEPT_{i:03d}" for i in range(1, 26)]
    cities = ["Київ", "Харків", "Одеса", "Дніпро", "Львів", "Запоріжжя"]
    
    for _ in range(random.randint(500, 800)):  # Daily routes
        delivery_type = random.choice(delivery_types)
        
        # Generate route based on delivery type
        if delivery_type == "Department to Department":
            sender_dept = random.choice(departments)
            receiver_dept = random.choice(departments)
            sender_location = None
            receiver_location = None
        elif delivery_type == "Department to Courier":
            sender_dept = random.choice(departments)
            receiver_dept = None
            sender_location = None
            receiver_location = random.choice(cities)
        elif delivery_type == "Courier to Department":
            sender_dept = None
            receiver_dept = random.choice(departments)
            sender_location = random.choice(cities)
            receiver_location = None
        else:  # Courier to Courier
            sender_dept = None
            receiver_dept = None
            sender_location = random.choice(cities)
            receiver_location = random.choice(cities)
        
        # Simulate route validation
        is_valid = random.choice([True, True, True, False])  # 75% valid routes
        
        # Calculate route metrics
        estimated_distance = random.randint(10, 200)  # km
        estimated_time = estimated_distance * random.uniform(1.2, 2.5)  # minutes
        
        routes.append({
            "route_id": f"ROUTE_{random.randint(100000, 999999)}",
            "order_id": f"ORDER_{random.randint(100000, 999999)}",
            "delivery_type": delivery_type,
            "sender_department": sender_dept,
            "receiver_department": receiver_dept,
            "sender_location": sender_location,
            "receiver_location": receiver_location,
            "is_valid_route": is_valid,
            "estimated_distance_km": estimated_distance,
            "estimated_time_minutes": int(estimated_time),
            "validation_date": context['ds']
        })
    
    logging.info(f"Generated {len(routes)} route validation records")
    return routes

def extract_staff_distribution_data(**context):
    """Extract staff distribution across departments"""
    logging.info("Extracting staff distribution data...")
    
    staff_data = []
    positions = ["courier", "sorter", "manager", "driver", "operator", "supervisor"]
    dept_types = ["sorting", "distribution", "local", "hub"]
    regions = ["Центральний", "Східний", "Південний", "Західний", "Північний"]
    
    for dept_id in range(1, 26):
        dept_type = random.choice(dept_types)
        region = random.choice(regions)
        
        # Staff distribution varies by department type
        base_staff = {
            "sorting": {"courier": 5, "sorter": 15, "manager": 2, "driver": 8, "operator": 3, "supervisor": 1},
            "distribution": {"courier": 12, "sorter": 8, "manager": 3, "driver": 15, "operator": 4, "supervisor": 2},
            "local": {"courier": 8, "sorter": 3, "manager": 1, "driver": 5, "operator": 2, "supervisor": 1},
            "hub": {"courier": 20, "sorter": 25, "manager": 5, "driver": 30, "operator": 8, "supervisor": 4}
        }[dept_type]
        
        for position, base_count in base_staff.items():
            actual_count = base_count + random.randint(-2, 3)  # Some variation
            actual_count = max(1, actual_count)  # At least 1 person
            
            staff_data.append({
                "department_id": f"DEPT_{dept_id:03d}",
                "department_type": dept_type,
                "region": region,
                "position": position,
                "staff_count": actual_count,
                "avg_salary": random.randint(15000, 45000),  # UAH
                "experience_months": random.randint(6, 60),
                "snapshot_date": context['ds']
            })
    
    logging.info(f"Generated staff distribution data for {len(staff_data)} records")
    return staff_data

# === ANALYTICS FUNCTIONS ===
def analyze_courier_performance(**context):
    """Analyze courier delivery performance with comparison to previous deliveries"""
    logging.info("Analyzing courier performance...")
    
    # Simulate courier performance analysis
    courier_performance = []
    courier_ids = [f"CUR_{i:03d}" for i in range(1, 51)]
    
    for courier_id in courier_ids:
        # Current delivery metrics
        current_avg_time = random.randint(45, 180)  # minutes
        previous_avg_time = random.randint(40, 200)  # minutes
        
        performance_change = ((current_avg_time - previous_avg_time) / previous_avg_time) * 100
        
        deliveries_today = random.randint(8, 25)
        success_rate = random.uniform(0.85, 0.98)
        
        # Performance categorization
        if performance_change < -10:
            performance_trend = "improving"
        elif performance_change > 10:
            performance_trend = "declining"
        else:
            performance_trend = "stable"
        
        courier_performance.append({
            "courier_id": courier_id,
            "current_avg_delivery_time": current_avg_time,
            "previous_avg_delivery_time": previous_avg_time,
            "performance_change_percent": round(performance_change, 2),
            "performance_trend": performance_trend,
            "deliveries_completed": deliveries_today,
            "success_rate": round(success_rate, 3),
            "efficiency_score": round((1 / current_avg_time) * success_rate * 100, 2),
            "analysis_date": context['ds']
        })
    
    logging.info(f"Analyzed performance for {len(courier_performance)} couriers")
    return courier_performance

def analyze_department_efficiency(**context):
    """Analyze department processing efficiency"""
    logging.info("Analyzing department efficiency...")
    
    efficiency_data = []
    
    for dept_id in range(1, 26):
        dept_type = random.choice(["sorting", "distribution", "local", "hub"])
        
        # Efficiency metrics
        avg_processing_time = random.randint(15, 75)  # minutes
        daily_throughput = random.randint(200, 1500)
        error_rate = random.uniform(0.01, 0.08)  # 1-8% error rate
        
        # Calculate efficiency score
        efficiency_score = (daily_throughput / avg_processing_time) * (1 - error_rate) * 100
        
        # Bottleneck analysis
        bottlenecks = []
        if avg_processing_time > 60:
            bottlenecks.append("slow_processing")
        if error_rate > 0.05:
            bottlenecks.append("high_error_rate")
        if daily_throughput < 300:
            bottlenecks.append("low_throughput")
        
        efficiency_data.append({
            "department_id": f"DEPT_{dept_id:03d}",
            "department_type": dept_type,
            "avg_processing_time_minutes": avg_processing_time,
            "daily_throughput": daily_throughput,
            "error_rate": round(error_rate, 4),
            "efficiency_score": round(efficiency_score, 2),
            "bottlenecks": bottlenecks,
            "capacity_utilization": round(random.uniform(0.6, 0.95), 3),
            "analysis_date": context['ds']
        })
    
    logging.info(f"Analyzed efficiency for {len(efficiency_data)} departments")
    return efficiency_data

def analyze_geographical_coverage(**context):
    """Analyze geographical coverage and service gaps"""
    logging.info("Analyzing geographical coverage...")
    
    coverage_data = []
    regions = ["Центральний", "Східний", "Південний", "Західний", "Північний"]
    cities = ["Київ", "Харків", "Одеса", "Дніпро", "Львів", "Запоріжжя", "Кривий Ріг", "Миколаїв"]
    
    for region in regions:
        region_cities = random.sample(cities, random.randint(3, 6))
        
        total_departments = random.randint(8, 15)
        coverage_density = random.uniform(0.6, 0.9)  # departments per 1000 km²
        service_quality_score = random.uniform(7.2, 9.5)
        
        # Identify service gaps
        service_gaps = []
        if coverage_density < 0.7:
            service_gaps.append("low_department_density")
        if service_quality_score < 8.0:
            service_gaps.append("service_quality_issues")
        
        # Calculate demand vs capacity
        estimated_demand = random.randint(5000, 15000)  # monthly parcels
        current_capacity = random.randint(4500, 18000)  # monthly capacity
        capacity_ratio = current_capacity / estimated_demand
        
        coverage_data.append({
            "region": region,
            "cities_covered": len(region_cities),
            "total_departments": total_departments,
            "coverage_density": round(coverage_density, 3),
            "service_quality_score": round(service_quality_score, 2),
            "estimated_monthly_demand": estimated_demand,
            "current_monthly_capacity": current_capacity,
            "capacity_ratio": round(capacity_ratio, 3),
            "service_gaps": service_gaps,
            "expansion_priority": "high" if capacity_ratio < 1.1 else "medium" if capacity_ratio < 1.3 else "low",
            "analysis_date": context['ds']
        })
    
    logging.info(f"Analyzed geographical coverage for {len(coverage_data)} regions")
    return coverage_data

def detect_logistics_anomalies(**context):
    """Detect anomalies in logistics operations"""
    logging.info("Detecting logistics anomalies...")
    
    anomalies = []
    
    # Delivery time anomalies
    if random.random() > 0.7:  # 30% chance
        anomalies.append({
            "anomaly_type": "delivery_time_spike",
            "affected_entity": f"DEPT_{random.randint(1, 25):03d}",
            "severity": random.choice(["medium", "high"]),
            "description": f"Average delivery time increased by {random.randint(25, 60)}%",
            "detected_at": context['ds'],
            "estimated_impact": f"{random.randint(100, 500)} delayed parcels"
        })
    
    # Volume anomalies
    if random.random() > 0.8:  # 20% chance
        anomalies.append({
            "anomaly_type": "volume_anomaly",
            "affected_entity": f"CUR_{random.randint(1, 50):03d}",
            "severity": random.choice(["low", "medium"]),
            "description": f"Delivery volume dropped by {random.randint(30, 70)}%",
            "detected_at": context['ds'],
            "estimated_impact": f"Potential courier availability issue"
        })
    
    # Route efficiency anomalies
    if random.random() > 0.75:  # 25% chance
        anomalies.append({
            "anomaly_type": "route_inefficiency",
            "affected_entity": random.choice(["Київ", "Харків", "Одеса"]),
            "severity": "medium",
            "description": f"Route optimization score decreased by {random.randint(15, 35)}%",
            "detected_at": context['ds'],
            "estimated_impact": f"Additional {random.randint(500, 1500)} km daily"
        })
    
    logging.info(f"Detected {len(anomalies)} logistics anomalies")
    return anomalies

def validate_data_quality(**context):
    """Validate postal system data quality"""
    logging.info("Validating postal system data quality...")
    
    quality_checks = []
    
    # Check delivery data completeness
    delivery_completeness = random.uniform(0.92, 0.99)
    quality_checks.append({
        "check_name": "delivery_data_completeness",
        "score": delivery_completeness,
        "status": "PASS" if delivery_completeness > 0.95 else "WARNING",
        "details": f"Delivery records completeness: {delivery_completeness:.2%}"
    })
    
    # Check route validation accuracy
    route_accuracy = random.uniform(0.88, 0.97)
    quality_checks.append({
        "check_name": "route_validation_accuracy",
        "score": route_accuracy,
        "status": "PASS" if route_accuracy > 0.90 else "FAIL",
        "details": f"Route validation accuracy: {route_accuracy:.2%}"
    })
    
    # Check timestamp consistency
    timestamp_consistency = random.uniform(0.94, 0.99)
    quality_checks.append({
        "check_name": "timestamp_consistency",
        "score": timestamp_consistency,
        "status": "PASS" if timestamp_consistency > 0.95 else "WARNING",
        "details": f"Timestamp consistency: {timestamp_consistency:.2%}"
    })
    
    overall_score = sum(check["score"] for check in quality_checks) / len(quality_checks)
    
    result = {
        "overall_score": round(overall_score, 3),
        "checks": quality_checks,
        "status": "PASS" if overall_score > 0.92 else "WARNING" if overall_score > 0.85 else "FAIL",
        "validation_date": context['ds']
    }
    
    logging.info(f"Data quality validation completed. Overall score: {overall_score:.2%}")
    return result

# === REPORTING FUNCTIONS ===
def generate_operations_dashboard(**context):
    """Generate operational dashboard for postal management"""
    logging.info("Generating operations dashboard...")
    
    dashboard_data = {
        "date": context['ds'],
        "kpis": {
            "total_deliveries": random.randint(8000, 12000),
            "avg_delivery_time": f"{random.randint(45, 90)} min",
            "success_rate": f"{random.uniform(92, 98):.1f}%",
            "department_utilization": f"{random.uniform(75, 92):.1f}%",
            "transport_efficiency": f"{random.uniform(68, 85):.1f}%"
        },
        "alerts": {
            "high_priority": random.randint(0, 3),
            "medium_priority": random.randint(2, 8),
            "low_priority": random.randint(5, 15)
        },
        "performance_trends": {
            "delivery_time_trend": random.choice(["improving", "stable", "declining"]),
            "volume_trend": random.choice(["growing", "stable", "declining"]),
            "efficiency_trend": random.choice(["improving", "stable", "declining"])
        }
    }
    
    logging.info(f"Operations dashboard generated: {dashboard_data}")
    return dashboard_data

def generate_analytics_report(**context):
    """Generate comprehensive analytics report"""
    logging.info("Generating analytics report...")
    
    report = {
        "report_date": context['ds'],
        "executive_summary": {
            "total_parcels_processed": random.randint(25000, 35000),
            "revenue_impact": f"${random.randint(150000, 250000):,}",
            "cost_optimization_savings": f"${random.randint(15000, 35000):,}",
            "customer_satisfaction": f"{random.uniform(8.2, 9.4):.1f}/10"
        },
        "courier_insights": {
            "top_performers": random.randint(8, 15),
            "improvement_needed": random.randint(3, 8),
            "avg_performance_change": f"{random.uniform(-5, 12):.1f}%"
        },
        "department_insights": {
            "most_efficient": f"DEPT_{random.randint(1, 25):03d}",
            "bottlenecks_identified": random.randint(2, 6),
            "capacity_utilization": f"{random.uniform(78, 94):.1f}%"
        },
        "logistics_optimization": {
            "route_efficiency_improvement": f"{random.uniform(8, 18):.1f}%",
            "fuel_cost_reduction": f"{random.uniform(12, 25):.1f}%",
            "delivery_time_reduction": f"{random.uniform(5, 15):.1f}%"
        }
    }
    
    logging.info(f"Analytics report generated: {report}")
    return report

def generate_predictive_insights(**context):
    """Generate predictive insights for postal operations"""
    logging.info("Generating predictive insights...")
    
    insights = {
        "demand_forecast": {
            "next_week_volume": random.randint(28000, 42000),
            "peak_days": ["Monday", "Friday"],
            "seasonal_adjustment": f"{random.uniform(-8, 15):.1f}%"
        },
        "capacity_planning": {
            "additional_couriers_needed": random.randint(0, 8),
            "transport_optimization": f"{random.randint(3, 12)} vehicles",
            "department_expansion": random.choice([None, "Київ", "Харків", "Одеса"])
        },
        "risk_assessment": {
            "delivery_delay_risk": random.choice(["low", "medium", "high"]),
            "capacity_overflow_risk": random.choice(["low", "medium"]),
            "weather_impact_forecast": f"{random.randint(5, 25)}% delay increase"
        },
        "optimization_opportunities": {
            "route_consolidation": f"Save {random.randint(500, 1500)} km/day",
            "load_balancing": f"Improve efficiency by {random.uniform(8, 18):.1f}%",
            "automation_potential": f"{random.randint(15, 35)}% of sorting tasks"
        }
    }
    
    logging.info(f"Predictive insights generated: {insights}")
    return insights

# Create the DAG
with DAG(
    'postal_analytics_pipeline',
    default_args=default_args,
    description='Comprehensive postal system analytics pipeline with logistics optimization',
    schedule='0 7 * * *',  # Daily at 7 AM
    start_date=datetime(2025, 5, 1),
    catchup=False,
    tags=['postal', 'logistics', 'analytics', 'operations', 'delivery'],
    max_active_runs=1
) as dag:
    
    # === PIPELINE START ===
    pipeline_start = EmptyOperator(
        task_id='pipeline_start'
    )
    
    # === DATA AVAILABILITY CHECKS ===
    with TaskGroup(group_id='system_health_checks') as health_checks:
        check_postal_system = HttpSensor(
            task_id='check_postal_system_api',
            http_conn_id='postal_system_api',
            endpoint='health',
            request_params={},
            response_check=lambda response: response.status_code == 200,
            poke_interval=30,
            timeout=300,
            mode='reschedule'
        )
        
        check_tracking_data = GCSObjectExistenceSensor(
            task_id='check_tracking_data',
            bucket='postal-data-lake',
            object='tracking/daily/{{ds}}/deliveries.json',
            mode='reschedule'
        )
        
        check_logistics_db = HttpSensor(
            task_id='check_logistics_database',
            http_conn_id='logistics_db_api',
            endpoint='status',
            request_params={},
            response_check=lambda response: response.status_code == 200,
            poke_interval=30,
            timeout=300,
            mode='reschedule'
        )
    
    # === DATA EXTRACTION ===
    with TaskGroup(group_id='data_extraction') as data_extraction:
        extract_courier_data = PythonOperator(
            task_id='extract_courier_delivery_data',
            python_callable=extract_courier_delivery_data
        )
        
        extract_department_data = PythonOperator(
            task_id='extract_department_workload_data',
            python_callable=extract_department_workload_data
        )
        
        extract_transport_data = PythonOperator(
            task_id='extract_transport_utilization_data',
            python_callable=extract_transport_utilization_data
        )
        
        extract_route_data = PythonOperator(
            task_id='extract_route_validation_data',
            python_callable=extract_route_validation_data
        )
        
        extract_staff_data = PythonOperator(
            task_id='extract_staff_distribution_data',
            python_callable=extract_staff_distribution_data
        )
    
    # === DATA QUALITY VALIDATION ===
    data_quality_check = PythonOperator(
        task_id='validate_data_quality',
        python_callable=validate_data_quality,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )
    
    # === CORE ANALYTICS (Business Tasks Implementation) ===
    with TaskGroup(group_id='business_analytics') as business_analytics:
        # Task 1: Courier delivery time analysis
        courier_performance = PythonOperator(
            task_id='analyze_courier_performance',
            python_callable=analyze_courier_performance
        )
        
        # Task 2 & 3: Department efficiency analysis
        department_efficiency = PythonOperator(
            task_id='analyze_department_efficiency',
            python_callable=analyze_department_efficiency
        )
        
        # Geographic coverage analysis
        geographical_coverage = PythonOperator(
            task_id='analyze_geographical_coverage',
            python_callable=analyze_geographical_coverage
        )
        
        # Anomaly detection
        anomaly_detection = PythonOperator(
            task_id='detect_logistics_anomalies',
            python_callable=detect_logistics_anomalies
        )
    
    # === DATA WAREHOUSE LOADING ===
    with TaskGroup(group_id='data_warehouse_loading') as dw_loading:
        # Load courier delivery facts (Task 1 implementation)
        load_courier_facts = BigQueryInsertJobOperator(
            task_id='load_courier_delivery_facts',
            configuration={
                'query': {
                    'query': '''
                    -- Transactional fact for courier delivery analysis
                    SELECT 
                        '{{ds}}' as delivery_date,
                        'CUR_001' as courier_id,
                        'small' as parcel_type,
                        'Київ' as city,
                        65 as delivery_duration_minutes,
                        75 as previous_delivery_duration,
                        -13.3 as performance_change_percent,
                        'improving' as performance_trend
                    UNION ALL
                    SELECT 
                        '{{ds}}' as delivery_date,
                        'CUR_002' as courier_id,
                        'medium' as parcel_type,
                        'Харків' as city,
                        95 as delivery_duration_minutes,
                        85 as previous_delivery_duration,
                        11.8 as performance_change_percent,
                        'declining' as performance_trend
                    UNION ALL
                    SELECT 
                        '{{ds}}' as delivery_date,
                        'CUR_003' as courier_id,
                        'express' as parcel_type,
                        'Одеса' as city,
                        35 as delivery_duration_minutes,
                        38 as previous_delivery_duration,
                        -7.9 as performance_change_percent,
                        'improving' as performance_trend
                    ''',
                    'destinationTable': {
                        'projectId': 'postal-analytics',
                        'datasetId': 'facts',
                        'tableId': 'courier_delivery_performance'
                    },
                    'writeDisposition': 'WRITE_APPEND',
                    'useLegacySql': False
                }
            }
        )
        
        # Load department workload facts (Task 2 implementation)
        load_department_facts = BigQueryInsertJobOperator(
            task_id='load_department_workload_facts',
            configuration={
                'query': {
                    'query': '''
                    -- Periodic fact for department workload analysis
                    SELECT 
                        '{{ds}}' as snapshot_date,
                        'DEPT_001' as department_id,
                        'sorting' as department_type,
                        'small' as parcel_type,
                        'van' as transport_type,
                        1250 as monthly_volume,
                        25.5 as avg_processing_time_minutes,
                        0.85 as capacity_utilization
                    UNION ALL
                    SELECT 
                        '{{ds}}' as snapshot_date,
                        'DEPT_002' as department_id,
                        'distribution' as department_type,
                        'medium' as parcel_type,
                        'truck' as transport_type,
                        890 as monthly_volume,
                        35.2 as avg_processing_time_minutes,
                        0.92 as capacity_utilization
                    UNION ALL
                    SELECT 
                        '{{ds}}' as snapshot_date,
                        'DEPT_003' as department_id,
                        'hub' as department_type,
                        'large' as parcel_type,
                        'truck' as transport_type,
                        2150 as monthly_volume,
                        45.8 as avg_processing_time_minutes,
                        0.78 as capacity_utilization
                    ''',
                    'destinationTable': {
                        'projectId': 'postal-analytics',
                        'datasetId': 'facts',
                        'tableId': 'department_workload_monthly'
                    },
                    'writeDisposition': 'WRITE_APPEND',
                    'useLegacySql': False
                }
            }
        )
        
        # Load transport utilization facts (Task 4 implementation)
        load_transport_facts = BigQueryInsertJobOperator(
            task_id='load_transport_utilization_facts',
            configuration={
                'query': {
                    'query': '''
                    -- Periodic fact for transport utilization analysis
                    SELECT 
                        '{{ds}}' as snapshot_date,
                        'van' as transport_type,
                        'small' as parcel_type,
                        65 as fleet_size,
                        1580 as monthly_deliveries,
                        15.2 as avg_distance_per_delivery,
                        24016 as total_distance_km,
                        2881.92 as fuel_consumption_liters,
                        81.0 as utilization_rate_percent
                    UNION ALL
                    SELECT 
                        '{{ds}}' as snapshot_date,
                        'truck' as transport_type,
                        'large' as parcel_type,
                        28 as fleet_size,
                        756 as monthly_deliveries,
                        32.5 as avg_distance_per_delivery,
                        24570 as total_distance_km,
                        6142.5 as fuel_consumption_liters,
                        90.0 as utilization_rate_percent
                    UNION ALL
                    SELECT 
                        '{{ds}}' as snapshot_date,
                        'motorcycle' as transport_type,
                        'express' as parcel_type,
                        42 as fleet_size,
                        1260 as monthly_deliveries,
                        8.5 as avg_distance_per_delivery,
                        10710 as total_distance_km,
                        535.5 as fuel_consumption_liters,
                        100.0 as utilization_rate_percent
                    ''',
                    'destinationTable': {
                        'projectId': 'postal-analytics',
                        'datasetId': 'facts',
                        'tableId': 'transport_utilization_monthly'
                    },
                    'writeDisposition': 'WRITE_APPEND',
                    'useLegacySql': False
                }
            }
        )
    
    # === ANALYTICAL VIEWS CREATION ===
    with TaskGroup(group_id='analytical_views') as analytical_views:
        # Create department staff distribution view
        create_staff_view = BigQueryInsertJobOperator(
            task_id='create_department_staff_view',
            configuration={
                'query': {
                    'query': '''
                    CREATE OR REPLACE VIEW `postal-analytics.analytics.department_staff_distribution_{{ds_nodash}}` AS
                    SELECT 
                        department_id,
                        department_type,
                        region,
                        position,
                        SUM(staff_count) as total_staff,
                        AVG(avg_salary) as avg_salary,
                        AVG(experience_months) as avg_experience_months,
                        COUNT(*) as position_variations
                    FROM `postal-analytics.staging.staff_distribution`
                    WHERE snapshot_date = '{{ds}}'
                    GROUP BY department_id, department_type, region, position
                    ORDER BY department_id, position
                    ''',
                    'useLegacySql': False
                }
            }
        )
        
        # Create transport fleet analysis view
        create_transport_view = BigQueryInsertJobOperator(
            task_id='create_transport_fleet_view',
            configuration={
                'query': {
                    'query': '''
                    CREATE OR REPLACE VIEW `postal-analytics.analytics.transport_fleet_analysis_{{ds_nodash}}` AS
                    SELECT 
                        transport_type,
                        SUM(fleet_size) as total_fleet_size,
                        SUM(monthly_deliveries) as total_monthly_deliveries,
                        AVG(avg_distance_per_delivery) as avg_distance_per_delivery,
                        SUM(total_distance_km) as total_distance_km,
                        SUM(fuel_consumption_liters) as total_fuel_consumption,
                        AVG(utilization_rate) as avg_utilization_rate,
                        COUNT(DISTINCT parcel_type) as parcel_types_served
                    FROM `postal-analytics.staging.transport_utilization`
                    WHERE month = '{{ds}}'[:7]
                    GROUP BY transport_type
                    ORDER BY total_monthly_deliveries DESC
                    ''',
                    'useLegacySql': False
                }
            }
        )
        
        # Create geographical coverage view
        create_coverage_view = BigQueryInsertJobOperator(
            task_id='create_geographical_coverage_view',
            configuration={
                'query': {
                    'query': '''
                    CREATE OR REPLACE VIEW `postal-analytics.analytics.geographical_coverage_{{ds_nodash}}` AS
                    SELECT 
                        region,
                        SUM(cities_covered) as total_cities_covered,
                        SUM(total_departments) as total_departments,
                        AVG(coverage_density) as avg_coverage_density,
                        AVG(service_quality_score) as avg_service_quality,
                        SUM(estimated_monthly_demand) as total_estimated_demand,
                        SUM(current_monthly_capacity) as total_current_capacity,
                        AVG(capacity_ratio) as avg_capacity_ratio,
                        COUNT(CASE WHEN expansion_priority = 'high' THEN 1 END) as high_priority_areas
                    FROM `postal-analytics.staging.geographical_coverage`
                    WHERE analysis_date = '{{ds}}'
                    GROUP BY region
                    ORDER BY avg_capacity_ratio ASC
                    ''',
                    'useLegacySql': False
                }
            }
        )
    
    # === REPORTING ===
    with TaskGroup(group_id='reporting') as reporting:
        operations_dashboard = PythonOperator(
            task_id='generate_operations_dashboard',
            python_callable=generate_operations_dashboard
        )
        
        analytics_report = PythonOperator(
            task_id='generate_analytics_report',
            python_callable=generate_analytics_report
        )
        
        predictive_insights = PythonOperator(
            task_id='generate_predictive_insights',
            python_callable=generate_predictive_insights
        )
    
    # === FINAL STEPS ===
    consolidate_results = EmptyOperator(
        task_id='consolidate_results',
        trigger_rule=TriggerRule.ALL_SUCCESS
    )
    
    # === NOTIFICATIONS ===
    notify_logistics_team = BashOperator(
        task_id='notify_logistics_team',
        bash_command='echo "📦 Postal Analytics Pipeline completed! Logistics insights: https://postal-dashboard.com/logistics/{{ds_nodash}}"'
    )
    
    notify_operations_team = BashOperator(
        task_id='notify_operations_team',
        bash_command='echo "🚚 Daily operations report ready! Performance metrics and optimization recommendations available."'
    )
    
    send_executive_summary = BashOperator(
        task_id='send_executive_summary',
        bash_command='echo "📊 Executive Summary: Postal operations analyzed. Key insights: delivery efficiency, capacity utilization, and expansion opportunities identified."'
    )
    
    pipeline_end = EmptyOperator(
        task_id='pipeline_end'
    )
    
    # === COMPLEX DEPENDENCY GRAPH ===
    
    # Start with health checks
    pipeline_start >> health_checks
    
    # Data extraction after health checks
    health_checks >> data_extraction
    
    # Data quality validation
    data_extraction >> data_quality_check
    
    # Parallel analytics after quality check
    data_quality_check >> business_analytics
    data_quality_check >> dw_loading
    
    # Analytical views depend on data warehouse loading
    dw_loading >> analytical_views
    
    # Reporting depends on both analytics and views
    business_analytics >> reporting
    analytical_views >> reporting
    
    # Consolidation
    reporting >> consolidate_results
    
    # Parallel notifications
    consolidate_results >> notify_logistics_team
    consolidate_results >> notify_operations_team
    consolidate_results >> send_executive_summary
    
    # Pipeline end
    notify_logistics_team >> pipeline_end
    notify_operations_team >> pipeline_end
    send_executive_summary >> pipeline_end