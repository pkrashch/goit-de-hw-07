# goit-de-hw-07 — Apache Airflow

DAG `polina_krashchenko_medals` (файл `polina_krashchenko_medals_dag.py`) працює з MySQL-базою
`olympic_dataset` через з'єднання `goit_mysql_db_polina_krashchenko`.

## Завдання DAG

| # | Task | Що робить |
|---|---|---|
| 1 | `create_table` | `CREATE DATABASE/TABLE IF NOT EXISTS polina_krashchenko.medal_counts` (`id` AUTO_INCREMENT PK, `medal_type`, `count`, `created_at`) |
| 2 | `pick_medal` | випадково обирає `Bronze`, `Silver` або `Gold` |
| 3 | `pick_medal_task` | `BranchPythonOperator`: за обраним значенням запускає одне з трьох завдань |
| 4 | `calc_Bronze` / `calc_Silver` / `calc_Gold` | рахує записи з відповідною медаллю в `olympic_dataset.athlete_event_results` і вставляє `medal_type`, `count`, `created_at = NOW()` |
| 5 | `generate_delay` | `PythonOperator` з `time.sleep(n)`, `trigger_rule=ONE_SUCCESS` — виконується, якщо одне з трьох завдань успішне |
| 6 | `check_for_correctness` | `SqlSensor`: перевіряє, що найновіший запис не старший за 30 секунд |

```
create_table → pick_medal → pick_medal_task → calc_Bronze | calc_Silver | calc_Gold
             → generate_delay → check_for_correctness
```

## Затримка

Затримка задається параметром DAG `delay_seconds` (за замовчуванням 20 с) і змінюється під час
запуску через **Trigger DAG w/ config**, без зміни коду:

- `delay_seconds = 20` → запис молодший за 30 с → сенсор **success**;
- `delay_seconds = 35` → запис старший за 30 с → сенсор **failed**.

## Розгортання

DAG розгортається через репозиторій курсу `goitacademy/airflow_sandbox` (GitHub Action),
у папці `dags/polina_krashchenko/`.
