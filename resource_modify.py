from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from airflow.utils.operator_resources import Resources

default_args = {
    'start_date': days_ago(1),
}

with DAG(
    'example_kubernetes_pod_operator_xcom_resources',
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
    tags=['example'],
) as dag:

    resources = Resources(
        cpus=1,
        ram=512,
        disk=0,
        gpus=0,
    )

    task = KubernetesPodOperator(
        namespace='default',
        image='python:3.9-slim',
        cmds=["python", "-c"],
        arguments=[
            "print('Hello from KubernetesPodOperator');"
            "import json; print(json.dumps({'result': 42}))"
        ],
        labels={"foo": "bar"},
        name="example-kpo",
        task_id="kpo_task",
        get_logs=True,
        do_xcom_push=True,
        resources=resources,
        is_delete_operator_pod=True,
    )

    task
