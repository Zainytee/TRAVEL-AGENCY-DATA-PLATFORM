# scripts/operator.py
from airflow.operators.python import PythonOperator


def my_python_operator(dag, task_id, python_callable, **kwargs):
    """
    A wrapper function for the PythonOperator.
    """
    return PythonOperator(
        task_id=task_id,
        python_callable=python_callable,
        dag=dag,
        **kwargs
    )
