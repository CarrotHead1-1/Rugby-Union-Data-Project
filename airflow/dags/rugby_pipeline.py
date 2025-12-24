from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG (
    dag_id = "rugby_pipeline",
    start_date = datetime(2024,1,1),
    schedule = "@daily",
    catchup = False,
    tags = ["rugby", "databricks"],

) as dag:
    
    start = BashOperator(
        task_id = "start_pipeline",
        bash_command = "echo 'Starting Rugby Data Pipeline'",
    )

    end = BashOperator(
        task_id = "end_pipeline",
        bash_command = "echo 'Pipeline Completed Successfully'",
    )

    start >> end

