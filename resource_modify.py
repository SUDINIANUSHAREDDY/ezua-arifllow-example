from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.utils.dates import days_ago

namespace = 'admin-635a7131'

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 0,
}

def create_pod_override_dict(image_name):
    pod_spec = {
        "containers": [
            {
                "name": "base",
                "image": image_name,
                "resources": {
                    "requests": {"cpu": "100m", "memory": "128Mi"},
                    "limits": {"cpu": "200m", "memory": "256Mi"},
                },
            }
        ],
        "restartPolicy": "Never",
    }
    return {"spec": pod_spec}

with DAG(
    'test_vrealize_workflow',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    fetch_token = KubernetesPodOperator(
        namespace=namespace,
        image='python:3.8-slim',
        cmds=['python', '-c'],
        arguments=[
            """
print("Simulating token fetch...")
token = "dummy-token"
print(token)
            """
        ],
        name='fetch_token',
        task_id='get_token',
        get_logs=True,
        in_cluster=True,
        is_delete_operator_pod=True,
        do_xcom_push=True,
        pod_override=create_pod_override_dict('python:3.8-slim'),
    )

    fetch_data = KubernetesPodOperator(
        namespace=namespace,
        image='python:3.8-slim',
        cmds=['python', '-c'],
        arguments=[
            """
token = "dummy-token"
view_id = "test-view"
resource_id = "test-resource"
print(f"Fetching data with token: {token}")
data = {"sample": "data"}
print(f"Fetched data for view {view_id} and resource {resource_id}: {data}")
            """
        ],
        name='fetch_data',
        task_id='get_view',
        get_logs=True,
        in_cluster=True,
        is_delete_operator_pod=True,
        do_xcom_push=True,
        pod_override=create_pod_override_dict('python:3.8-slim'),
    )

    upload_data = KubernetesPodOperator(
        namespace=namespace,
        image='python:3.8-slim',
        cmds=['python', '-c'],
        arguments=[
            """
view_id = "test-view"
resource_id = "test-resource"
print(f"Uploading dummy file for {view_id}_{resource_id}.json to dummy S3 location...")
            """
        ],
        name='upload_data',
        task_id='put_view',
        get_logs=True,
        in_cluster=True,
        is_delete_operator_pod=True,
        do_xcom_push=True,
        pod_override=create_pod_override_dict('python:3.8-slim'),
    )

    fetch_token >> fetch_data >> upload_data
