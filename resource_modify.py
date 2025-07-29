from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago

from kubernetes.client import models as k8s  # Kubernetes Python client

default_args = {
    'start_date': days_ago(1),
}

with DAG(
    'example_kubernetes_pod_operator_with_resource_limits_object',
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
    tags=['example'],
) as dag:

    pod_override_object = k8s.V1Pod(
        spec=k8s.V1PodSpec(
            containers=[
                k8s.V1Container(
                    name="base",
                    resources=k8s.V1ResourceRequirements(
                        requests={"cpu": "500m", "memory": "512Mi"},
                        limits={"cpu": "1", "memory": "1Gi"}
                    )
                )
            ]
        )
    )

    kpo_task = KubernetesPodOperator(
        namespace='admin-635a7131',
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
        is_delete_operator_pod=True,
        pod_override_object=pod_override_object,  # <-- Correct approach
    )

    kpo_task
