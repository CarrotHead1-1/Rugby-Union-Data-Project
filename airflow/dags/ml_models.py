from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime

with DAG (
    dag_id = "ml_models",
    start_date = datetime(2024,1,1),
    schedule = "@daily",
    catchup = False,
    tags = ["rugby", "LogisticRegression"],

) as dag:
    
    # wait_for_prediction_features = ExternalTaskSensor(
    #     task_id = "wait_for_prediction_features",
    #     external_dag_id = "build_prediction_features",
    #     external_task_id = None,
    #     mode = "reschedule",
    #     poke_interval = 60,
    #     timeout = 60 * 60
    # )

    run_model = DatabricksRunNowOperator(
        task_id = "run_ml_models",
        databricks_conn_id = "databricks_default",
        job_id = 402563567448821
    )

    run_model