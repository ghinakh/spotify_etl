from airflow.decorators import dag, task
from datetime import datetime

@dag(dag_id="test_dag", start_date=datetime(2023, 1, 1), schedule=None, catchup=False)
def test_dag():
    @task
    def hello():
        print("Hello Airflow!")

    hello()

test_dag = test_dag()