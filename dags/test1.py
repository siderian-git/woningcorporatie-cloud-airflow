
import json

import pendulum

from airflow.sdk import dag, task
@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["test"],
)

def test():
    @task()
    def test_task():
        print("Hello world!")

    test_task()

test()