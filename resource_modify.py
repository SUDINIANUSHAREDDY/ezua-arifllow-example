from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago

default_args = {
    'start_date': days_ago(1),
}

with DAG(
    'example_kubernetes_pod_operator_xcom_resources_dict_with_annotations',
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
    tags=['example'],
) as dag:

    pod_annotations = {
        "dag_id": "{{ dag.dag_id }}",
        "run_id": "{{ run_id }}",
    }

    resources = {
        "cpus": 1,
        "ram": 512,
        "disk": 0,
        "gpus": 0,
    }

    kpo_task = KubernetesPodOperator(
        namespace='admin-635a7131',  # <-- updated namespace here
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
        pod_annotations=pod_annotations,
    )

    kpo_task
