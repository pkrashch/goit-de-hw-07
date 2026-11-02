"""ДЗ7. Apache Airflow: DAG з розгалуженням, затримкою та SQL-сенсором.

1. create_table      — створює таблицю (IF NOT EXISTS) для результатів
2. pick_medal        — випадково обирає Bronze / Silver / Gold
3. pick_medal_task   — розгалуження: запускає одне з трьох завдань
4. calc_Bronze / calc_Silver / calc_Gold — рахує медалі в
   olympic_dataset.athlete_event_results і записує результат у таблицю
5. generate_delay    — затримка (time.sleep), якщо одне з завдань виконано успішно
6. check_for_correctness — сенсор: найновіший запис не старший за 30 секунд
"""
import random
import time
from datetime import datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.sensors.sql import SqlSensor
from airflow.utils.trigger_rule import TriggerRule

MYSQL_CONN_ID = "goit_mysql_db_polina_krashchenko"
SCHEMA = "polina_krashchenko"
TABLE = f"{SCHEMA}.medal_counts"
SOURCE_TABLE = "olympic_dataset.athlete_event_results"
MEDALS = ["Bronze", "Silver", "Gold"]

# Затримка за замовчуванням (с). Змінюється при запуску через «Trigger DAG w/ config»:
# 20 — сенсор проходить, 35 — сенсор падає (запис старший за 30 с)
DEFAULT_DELAY_SECONDS = 20
MAX_RECORD_AGE_SECONDS = 30


def pick_medal():
    medal = random.choice(MEDALS)
    print(f"Picked medal: {medal}")
    return medal


def pick_medal_task(ti):
    medal = ti.xcom_pull(task_ids="pick_medal")
    return f"calc_{medal}"


def generate_delay(params):
    delay = int(params.get("delay_seconds", DEFAULT_DELAY_SECONDS))
    print(f"Sleeping for {delay} seconds...")
    time.sleep(delay)


default_args = {
    "owner": "polina_krashchenko",
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="polina_krashchenko_medals",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["polina_krashchenko"],
    params={"delay_seconds": Param(DEFAULT_DELAY_SECONDS, type="integer", minimum=0)},
) as dag:

    # 1. Створення схеми й таблиці
    create_table = MySqlOperator(
        task_id="create_table",
        mysql_conn_id=MYSQL_CONN_ID,
        sql=[
            f"CREATE DATABASE IF NOT EXISTS {SCHEMA};",
            f"""
            CREATE TABLE IF NOT EXISTS {TABLE} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                medal_type VARCHAR(10),
                count INT,
                created_at DATETIME
            );
            """,
        ],
    )

    # 2. Випадковий вибір медалі
    pick_medal_op = PythonOperator(
        task_id="pick_medal",
        python_callable=pick_medal,
    )

    # 3. Розгалуження
    pick_medal_task_op = BranchPythonOperator(
        task_id="pick_medal_task",
        python_callable=pick_medal_task,
    )

    # 4. Три завдання підрахунку
    calc_tasks = [
        MySqlOperator(
            task_id=f"calc_{medal}",
            mysql_conn_id=MYSQL_CONN_ID,
            sql=f"""
            INSERT INTO {TABLE} (medal_type, count, created_at)
            SELECT '{medal}', COUNT(*), NOW()
            FROM {SOURCE_TABLE}
            WHERE medal = '{medal}';
            """,
        )
        for medal in MEDALS
    ]

    # 5. Затримка — якщо хоча б одне з трьох завдань виконано успішно
    generate_delay_op = PythonOperator(
        task_id="generate_delay",
        python_callable=generate_delay,
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    # 6. Сенсор: найновіший запис не старший за 30 секунд
    check_for_correctness = SqlSensor(
        task_id="check_for_correctness",
        conn_id=MYSQL_CONN_ID,
        sql=f"""
        SELECT TIMESTAMPDIFF(SECOND, MAX(created_at), NOW()) <= {MAX_RECORD_AGE_SECONDS}
        FROM {TABLE};
        """,
        mode="poke",
        poke_interval=5,
        timeout=6,
    )

    create_table >> pick_medal_op >> pick_medal_task_op >> calc_tasks
    calc_tasks >> generate_delay_op >> check_for_correctness
