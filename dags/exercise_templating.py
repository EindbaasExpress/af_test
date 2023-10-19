import json
from pathlib import Path

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import DAG
import airflow.utils.dates
import requests
import time 

# Switch to the second URL (the dev endpoint) if you get rate-limited.
# The data will be old, but at least it will work.
# API_URL = "https://ll.thespacedevs.com/2.2.0/launch"
API_URL = "https://lldev.thespacedevs.com/2.2.0/launch"


with DAG(
    dag_id="exercise_templating_6",
    start_date=airflow.utils.dates.days_ago(7),
    schedule_interval="@daily",
) as dag:

    def _download_launches(templates_dict, **_):
        output_path = Path(templates_dict["output_path"])
        time.sleep(2)

        response = requests.get(
            API_URL,
            params={
                "window_start__gte": templates_dict["window_start"],
                "window_end__lt": templates_dict["window_end"],
            },
        )
        response.raise_for_status()

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w") as file_:
            json.dump(response.json(), file_)

    def _print_launch_count(templates_dict, **_):
        # TODO: Finish this task. Should load the launch JSON file
        # and print the 'count' field from it's contents.
        input_path = Path(templates_dict["input_path"])
        with open(input_path, "r") as file_:
            content = file_.read()
            json_data = json.loads(content)

        print(json_data)
        print(json_data.get("count", "count not found"))

    # TODO: Use templating to print the actual execution date. -> dag_run.logical_date
    print_date = BashOperator(task_id="print_date", bash_command="echo {{dag_run.logical_date}}")

    # TODO: Use current + next execution dates to define window start/end + file path.
    # Format for dates should be following: 2021-12-01T00:00:00Z"
    download_launches = PythonOperator(
        task_id="download_launches",
        python_callable=_download_launches,
        templates_dict={
            "output_path": "/tmp/launches/{{dag_run.logical_date | ts}}.json",
            "window_start": "{{dag_run.logical_date | ts}}",
            "window_end": "{{data_interval_end | ts}}",
        },
    )

    # TODO: Complete this task.
    check_for_launches = PythonOperator(
        task_id="check_for_launches",
        python_callable=_print_launch_count,
        templates_dict={
            "input_path": "/tmp/launches/{{dag_run.logical_date | ts}}.json",
        }
    )

    remove_tmp_file = BashOperator(
        task_id="remove_tmp_file", 
        bash_command="rm /tmp/launches/{{dag_run.logical_date | ts}}.json"
    )

    print_date >> download_launches >> check_for_launches >> remove_tmp_file


templates_dict = {
    "output_path": "./tmp/test.json",
    "window_start": "2023-05-20 00:00:00+00:00",
    "window_end": "2023-05-21 00:00:00+00:00"
}