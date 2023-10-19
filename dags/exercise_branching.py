try:
    from typing import Literal
except ImportError:
    from typing_extensions import Literal

from airflow import DAG
import datetime
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule

day_to_email_mapping = {
    "Mon": "Bob",
    "Tue": "Joe",
    "Wed": "Alice",
    "Thu": "Joe",
    "Fri": "Alice",
    "Sat": "Alice",
    "Sun": "Alice",
}
WeekdayLiteral = Literal["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


def _print_weekday(execution_date: datetime, **_) -> str:
    """
    Gets the execution_date
    Returns the 3 letter version
    of the weekday, for example 'Mon' or 'Tue'
    """
    return execution_date.strftime("%a")


def choose_branch(**kwargs) -> WeekdayLiteral:
    """
    Takes the returned value from the check_weekday task from the xcom kwarg
    Returns the corresponding value from the mapping dict
    """
    xcom_value = kwargs["ti"].xcom_pull(task_ids="check_weekday")
    return day_to_email_mapping.get(xcom_value)


with DAG(
    dag_id="exercise_branching_4",
    start_date=datetime.datetime(2023, 5, 1, 0, 0),
    schedule="@daily",
) as dag:
    check_weekday = PythonOperator(
        task_id="check_weekday",
        python_callable=_print_weekday,
    )

    branching = BranchPythonOperator(task_id="branching", python_callable=choose_branch)

    email_user_tasks = [
        EmptyOperator(task_id=person) for person in set(day_to_email_mapping.values())
    ]

    final_task = EmptyOperator(
        task_id="final_task",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

check_weekday >> branching >> email_user_tasks >> final_task
