import os
from airflow import DAG
from airflow.operators.python_Operator import PythonOperator
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import pandas as pd
import sys


# Define DAG parameters
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 5, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'traffic_data_pipeline_local', default_args=default_args, schedule_interval=None
) as dag:

    # Load data to staging tables
    load_vehicle_data_task = CSVToPostgresOperator(
        task_id='load_vehicle_data',
        postgres_conn_id='traffic_db',
        table='vehicle_data',
        delimiter=',',
        # Select specific columns for vehicle_data table
        columns=[
            'id', 'type', 'traveled_d', 'avg_speed'
        ],
        # Update filepath to point to your local data file
        filepath='C:/Users/mesfi/OneDrive/Desktop/han AAU Training/10Academy/Week_2/Week_2_traffic_data_warehouse/data',
    )

    load_trajectory_data_task = CSVToPostgresOperator(
        task_id='load_trajectory_data',
        postgres_conn_id='traffic_db',
        table='trajectory_info',
        delimiter=',',
        # Select specific columns for trajectory_info table
        columns=[
            'id', 'lat', 'lon', 'speed', 'lon_acc', 'lat_acc', 'time'
        ],
        # Update filepath to point to your local data file
        filepath='C:/Users/mesfi/OneDrive/Desktop/han AAU Training/10Academy/Week_2/Week_2_traffic_data_warehouse/data',
    )

    # Define task dependencies
    load_vehicle_data_task >> load_trajectory_data_task

