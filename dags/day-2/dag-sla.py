import datetime
import logging
import pendulum
import time

from airflow.decorators import dag, task

def sla_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    logging.info("Triggering SLA.")
    logging.info(
        "The callback arguments are: ",
        {
            "dag": dag,
            "task_list": task_list,
            "blocking_task_list": blocking_task_list,
            "slas": slas,
            "blocking_tis": blocking_tis,
        },
    )

@dag(
    dag_id="dag_sla",
    schedule="*/2 * * * * ",
    start_date=pendulum.datetime(2025, 5, 1, tz="UCT"),
    catchup=False,
    sla_miss_callback=sla_callback,
    )
def dag_sla():
    
    @task(sla=datetime.timedelta(seconds=100))
    def sleep_5():
        time.sleep(5)
    
    @task(sla=datetime.timedelta(seconds=5))
    def sleep_9():
        time.sleep(9)

    @task
    def sleep_12():
        time.sleep(12)

    sleep_5() >> sleep_9() >> sleep_12()

dag_sla()
