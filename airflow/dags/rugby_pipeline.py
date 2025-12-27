from airflow import DAG
#from airflow.operators.bash import BashOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

with DAG (
    dag_id = "rugby_pipeline",
    start_date = datetime(2024,1,1),
    schedule = "@daily",
    catchup = False,
    tags = ["rugby", "databricks"],

) as dag:
    
    trigger_dlt = DatabricksRunNowOperator(
        task_id = "trigger_dlt_pipeline",
        databricks_conn_id = "databricks_default",
        job_id = 640017615260074 #databricks job id
    )

    trigger_features = TriggerDagRunOperator(
        task_id = "trigger_build_prediction_features",
        trigger_dag_id = "match_features_pipeline",
        wait_for_completion = True
    )

    trigger_dlt >> trigger_features
    
    #local setup 
    # start = BashOperator(
    #     task_id = "start_pipeline",
    #     bash_command = "echo 'Starting Rugby Data Pipeline'",
    # )

    # end = BashOperator(
    #     task_id = "end_pipeline",
    #     bash_command = "echo 'Pipeline Completed Successfully'",
    # )

    # start >> end

