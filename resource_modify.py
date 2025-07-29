from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago

default_args = {
    'start_date': days_ago(1),
}

with DAG(
    'example_kubernetes_pod_operator_with_full_pod_spec',
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
    tags=['example'],
) as dag:

    full_pod_spec = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "example-kpo"
        },
        "spec": {
            "containers": [
                {
                    "name": "base",
                    "image": "python:3.9-slim",
                    "command": ["python", "-c"],
                    "args": [
                        "print('Hello from KubernetesPodOperator');"
                        "import json; print(json.dumps({'result': 42}))"
                    ],
                    "resources": {
                        "requests": {
                            "cpu": "500m",
                            "memory": "512Mi"
                        },
                        "limits": {
                            "cpu": "1",
                            "memory": "1Gi"
                        }
                    }
                }
            ],
            "restartPolicy": "Never"
        }
    }

    kpo_task = KubernetesPodOperator(
        task_id="kpo_task",
        namespace="admin-635a7131",
        full_pod_spec=full_pod_spec,
        get_logs=True,
        do_xcom_push=True,
        is_delete_operator_pod=True,
    )

    kpo_task
