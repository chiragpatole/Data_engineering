# Import necessary libraries
from airflow import models
from airflow import DAG
import airflow
from datetime import datetime, timedelta
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
import os
from airflow.operators.bash import BashOperator
from google.cloud import storage, bigquery
import pandas as pd
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator


# Default args_
default_args = {
    'owner': 'Airflow',
    'retries': 1,
    'retry_delay': timedelta(seconds=50),
    'dataflow_default_options': {
        'project': 'noble-airport-412610',
        'region': 'us-central1',
        'runner': 'DataflowRunner'
    }
}

PROJECT_ID = ''
STAGING_DATASET = "dataset_name"

# DAG definition
dag = DAG(
    dag_id='Titanic_DAG',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=airflow.utils.dates.days_ago(1),
    catchup=False,
    description="DAG for data ingestion and transformation"
)

# Tasks
start = DummyOperator(
    task_id="start_task_id",
    dag=dag
)

read_csv_file = GCSToBigQueryOperator(
    task_id='read_csv_file',
    bucket='degauravbk',
    source_objects=['titanic_dataset.csv'],
    destination_project_dataset_table=f'{PROJECT_ID}:{STAGING_DATASET}.titanic_data',
    write_disposition='WRITE_TRUNCATE',
    autodetect=True,
    source_format='csv',
    allow_quoted_newlines='true',
    skip_leading_rows=1,
    schema_fields=[
        {'name': 'PassengerId', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'Survived', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'Pclass', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'Sex', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Age', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'SibSp', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'Parch', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Ticket', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Fare', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Cabin', 'type': 'STRING', 'mode': 'NULLABLE'}
    ],
    dag=dag
)

def clean_data():
    # Load data from BigQuery into a Pandas DataFrame
    bq_client = bigquery.Client(project=PROJECT_ID)
    query = f"SELECT * FROM {PROJECT_ID}.{STAGING_DATASET}.titanic_data"
    df = bq_client.query(query).to_dataframe()

    # Remove duplicate rows
    df = df.drop_duplicates()

    # Save the cleaned DataFrame back to BigQuery
    df.to_gbq(destination_table=f'{PROJECT_ID}.{STAGING_DATASET}.titanic_data', if_exists='replace')

clean_data_task = PythonOperator(
    task_id='clean_data_task',
    python_callable=clean_data,
    dag=dag
)

def rename_columns():
    # Load data from BigQuery into a Pandas DataFrame
    bq_client = bigquery.Client(project=PROJECT_ID)
    query = f"SELECT * FROM {PROJECT_ID}.{STAGING_DATASET}.titanic_data"
    df = bq_client.query(query).to_dataframe()

    # Rename columns
    df = df.rename(columns={
        'PassengerId': 'Passenger_ID',
        'Survived': 'Survival_Status',
        'Pclass': 'Ticket_Class',
        'Sex': 'Gender',
        'Age': 'Passenger_Age',
        'SibSp': 'Siblings_Spouses_Aboard',
        'Parch': 'Parents_Children_Aboard',
        'Ticket': 'Ticket_Number',
        'Fare': 'Ticket_Fare',
        'Cabin': 'Cabin_Number'
    })
    df['Ticket_Fare'] = pd.to_numeric(df['Ticket_Fare'], errors='coerce')

    # Change data types
    df['Parents_Children_Aboard'] = df['Parents_Children_Aboard'].astype(int)
    df['Ticket_Fare'] = df['Ticket_Fare'].astype(int)
    # Save the renamed DataFrame back to BigQuery
    df.to_gbq(destination_table=f'{PROJECT_ID}.{STAGING_DATASET}.titanic_data', if_exists='replace')

rename_columns_task = PythonOperator(
    task_id='rename_columns_task',
    python_callable=rename_columns,
    dag=dag
)

def calculate_statistics():
    # Load data from BigQuery into a Pandas DataFrame
    bq_client = bigquery.Client(project=PROJECT_ID)
    query = f"SELECT * FROM {PROJECT_ID}.{STAGING_DATASET}.titanic_data"
    df = bq_client.query(query).to_dataframe()

    # Calculate statistics
    statistics = df.describe()

    # Save the statistics to BigQuery
    statistics.to_gbq(destination_table=f'{PROJECT_ID}.{STAGING_DATASET}.titanic_statistics', if_exists='replace')

statistics_task = PythonOperator(
    task_id='calculate_statistics_task',
    python_callable=calculate_statistics,
    dag=dag
)

# Task for data visualization
visualization_query_task = BigQueryExecuteQueryOperator(
    task_id='visualization_query_task',
    sql='''WITH PassengerCounts AS (
          SELECT
            Gender,
            COUNT(*) AS TotalPassengers
          FROM
            `noble-airport-412610.shashank.titanic_data`
          GROUP BY
            Gender
        ),

        SurvivalByClass AS (
          SELECT
            Ticket_Class,
            Survival_Status,
            COUNT(*) AS PassengerCount
          FROM
            `noble-airport-412610.shashank.titanic_data`
          GROUP BY
            Ticket_Class, Survival_Status
        ),

        AgeDistribution AS (
          SELECT
            Passenger_Age,
            COUNT(*) AS PassengerCount
          FROM
            `noble-airport-412610.shashank.titanic_data`
          WHERE
            Passenger_Age IS NOT NULL
          GROUP BY
            Passenger_Age
        ),

        SurvivalByGender AS (
          SELECT
            Gender,
            Survival_Status,
            COUNT(*) AS PassengerCount
          FROM
            `noble-airport-412610.shashank.titanic_data`
          GROUP BY
            Gender, Survival_Status
        )

        SELECT
          'Gender' AS ChartType,
          Gender AS Category,
          TotalPassengers AS Value
        FROM
          PassengerCounts

        UNION ALL

        SELECT
          'SurvivalByClass' AS ChartType,
          CONCAT('Class ', CAST(Ticket_Class AS STRING)) AS Category,
          PassengerCount AS Value
        FROM
          SurvivalByClass

        UNION ALL

        SELECT
          'AgeDistribution' AS ChartType,
          CASE
            WHEN Passenger_Age < 10 THEN '0-9'
            WHEN Passenger_Age BETWEEN 10 AND 19 THEN '10-19'
            WHEN Passenger_Age BETWEEN 20 AND 29 THEN '20-29'
            WHEN Passenger_Age BETWEEN 30 AND 39 THEN '30-39'
            WHEN Passenger_Age BETWEEN 40 AND 49 THEN '40-49'
            WHEN Passenger_Age BETWEEN 50 AND 59 THEN '50-59'
            WHEN Passenger_Age >= 60 THEN '60+'
            ELSE 'Unknown'
          END AS Category,
          PassengerCount AS Value
        FROM
          AgeDistribution

        UNION ALL

        SELECT
          'SurvivalByGender' AS ChartType,
          CONCAT(Gender, ' - ', Survival_Status) AS Category,
          PassengerCount AS Value
        FROM
          SurvivalByGender
    ''',
    use_legacy_sql=False,
    destination_dataset_table=f'{PROJECT_ID}.{STAGING_DATASET}.visualization_of_Survival',
    write_disposition='WRITE_TRUNCATE',
    dag=dag
)

def _data_validation_query():
    return f"""
        SELECT COUNT(*) as total_rows
        FROM `{PROJECT_ID}.{STAGING_DATASET}.titanic_data`
        WHERE Passenger_ID > 0  -- Adjust the condition as needed
    """

query = _data_validation_query()

bigquery_validation_task = BigQueryCheckOperator(
    task_id='bigquery_validation_task',
    sql=query,
    use_legacy_sql=False,
    dag=dag
)



def _data_validation_pass():
    print("Data validation passed!")

data_validation_pass = PythonOperator(
    task_id='data_validation_pass',
    python_callable=_data_validation_pass,
    dag=dag,
)

def _data_validation_fail():
    print("Data validation failed!")
    raise ValueError("Data validation failed")

data_validation_fail = PythonOperator(
    task_id='data_validation_fail',
    python_callable=_data_validation_fail,
    dag=dag,
)
end = DummyOperator(
    task_id="end_task_id",
    dag=dag
)

start >> read_csv_file >> clean_data_task >> rename_columns_task >> statistics_task >> visualization_query_task >> bigquery_validation_task

bigquery_validation_task >> data_validation_pass
bigquery_validation_task >> data_validation_fail

data_validation_pass >> end
data_validation_fail >> end

