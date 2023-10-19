import datetime
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator

dag = DAG(
    schedule=datetime.timedelta(days=3),
    dag_id="exercise_timedelta",
    start_date=datetime.datetime(2023, 5, 1, 5, 0)
)

task = EmptyOperator(task_id="task", dag=dag)

task