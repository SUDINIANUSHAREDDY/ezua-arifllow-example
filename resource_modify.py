from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from kubernetes.client import models as k8s

namespace = 'admin-635a7131'

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 0,
}

resource_requirements = k8s.V1ResourceRequirements(
    requests={"memory": "128Mi", "cpu": "100m"},
    limits={"memory": "256Mi", "cpu": "200m"},
)

def create_pod_override(image_name):
    container = k8s.V1Container(
        name="base",
        image=image_name,
        resources=resource_requirements,
    )
    pod_spec = k8s.V1PodSpec(
        containers=[container],
        restart_policy="Never",
    )
    # Return only the pod spec as a dict
    return pod_spec.to_dict()

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
        pod_override=create_pod_override('python:3.8-slim'),
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
        pod_override=create_pod_override('python:3.8-slim'),
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
        pod_override=create_pod_override('python:3.8-slim'),
    )

    fetch_token >> fetch_data >> upload_data
