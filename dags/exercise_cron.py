import datetime
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator

dag = DAG(
    schedule="45 13 * * MON,WED,FRI",
    dag_id="exercise_cron",
    start_date=datetime.datetime(2023, 5, 1, 4, 0, 0)
)

task = EmptyOperator(task_id="task", dag=dag)

task