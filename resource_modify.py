from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from kubernetes.client import V1Pod, V1PodSpec, V1Container, V1ResourceRequirements, V1ObjectMeta

default_args = {
    'start_date': days_ago(1),
}

with DAG(
    'kubernetes_pod_with_resource_limits_fixed',
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
    tags=['example'],
) as dag:

    container = V1Container(
        name="base",
        image="python:3.9-slim",
        command=["python", "-c"],
        args=[
            "print('Hello from KubernetesPodOperator');"
            "import json; print(json.dumps({'result': 42}))"
        ],
        resources=V1ResourceRequirements(
            requests={"cpu": "500m", "memory": "512Mi"},
            limits={"cpu": "1", "memory": "1Gi"},
        ),
    )

    pod_spec = V1PodSpec(
        containers=[container],
        restart_policy="Never"
    )

    full_pod_spec = V1Pod(
        metadata=V1ObjectMeta(name="example-kpo"),
        spec=pod_spec
    )

    kpo_task = KubernetesPodOperator(
        task_id="kpo_task",
        namespace="admin-635a7131",
        full_pod_spec=full_pod_spec,
        get_logs=True,
        do_xcom_push=True,
        is_delete_operator_pod=True,
    )

    kpo_task
