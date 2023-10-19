import airflow.utils.dates
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

dag = DAG(
    dag_id="hello_world_evaluate",
    start_date=airflow.utils.dates.days_ago(3),
    description="This DAG will print 'hello' and 'world'. ",
    schedule_interval="@daily",
)
hello = BashOperator(task_id="hello", bash_command= "echo 'hello'", dag=dag )
world1 = PythonOperator(
    task_id="world1", python_callable=lambda: print("world"), dag=dag
)
world2 = PythonOperator(
    task_id="world2", python_callable=lambda: print("world"), dag=dag
)
world3 = PythonOperator(
    task_id="world3", python_callable=lambda: {}["test"], dag=dag
)
world4 = PythonOperator(
    task_id="world4", python_callable=lambda: {}["test"], dag=dag
)

evaluate = EmptyOperator(task_id="evaluate",trigger_rule=TriggerRule.ALL_DONE, dag=dag)
hello >> [world1, world2, world3] >> evaluate >> world4
