import airflow.utils.dates
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator

dag = DAG(
    dag_id="empty_operator_dag",
    start_date=airflow.utils.dates.days_ago(1),
    schedule=None
)

task1 = EmptyOperator(
    task_id="task1",
    dag=dag,
)
task2 = EmptyOperator(
    task_id="task2",
    dag=dag,
)
task3 = EmptyOperator(
    task_id="task3",
    dag=dag,
)
task4 = EmptyOperator(
    task_id="task4",
    dag=dag,
)
task5 = EmptyOperator(
    task_id="task5",
    dag=dag,
)
task1 >> task2 >> [task3, task4] >> task5
