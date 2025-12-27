from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime

with DAG (
    dag_id = "match_features_pipeline",
    start_date = datetime(2024,1,1),
    schedule = "@daily",
    catchup = False,
    tags = ["databricks", "ml-predictions"],

) as dag:
    
    # wait_for_etl = ExternalTaskSensor(
    #     task_id = "wait_for_rugby_pipeline",
    #     external_dag_id = "rugby_pipeline",
    #     external_task_id = None,
    #     mode = "reschedule",
    #     poke_interval = 60,
    #     timeout = 60 * 60
    # )

    build_features = DatabricksRunNowOperator(
        task_id = "build_prediction_features",
        databricks_conn_id = "databricks_default",
        job_id = 35532112137397
    )

    trigger_ml_models = TriggerDagRunOperator(
        task_id = "trigger_ml_models",
        trigger_dag_id = "ml_models",
        wait_for_completion = True
    )

    build_features >> trigger_ml_models