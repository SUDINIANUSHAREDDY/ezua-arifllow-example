from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago

default_args = {
    'start_date': days_ago(1),
}

with DAG(
    'example_kubernetes_pod_operator_xcom_resources_dict',
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
    tags=['example'],
) as dag:

    resources = {
        "cpus": 1,    # CPU cores as integer
        "ram": 512,   # RAM in MB
        "disk": 0,    # Disk in MB (0 if not used)
        "gpus": 0     # Number of GPUs
    }

    kpo_task = KubernetesPodOperator(
        namespace='default',
        image='python:3.9-slim',
        cmds=["python", "-c"],
        arguments=[
            "print('Hello from KubernetesPodOperator');"
            "import json; print(json.dumps({'result': 42}))"
        ],
        labels={"example": "true"},
        name="example-kpo",
        task_id="kpo_task",
        get_logs=True,
        do_xcom_push=True,
        resources=resources,
        is_delete_operator_pod=True,
    )

    kpo_task
