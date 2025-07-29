from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from kubernetes.client import V1Pod, V1PodSpec, V1Container, V1ResourceRequirements

# Namespace for Kubernetes
namespace = 'admin-635a7131'

# Default DAG args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 0,
}

# Resource requests and limits
resource_requirements = V1ResourceRequirements(
    requests={'memory': '128Mi', 'cpu': '100m'},
    limits={'memory': '256Mi', 'cpu': '200m'}
)

# Container with resources
container = V1Container(
    name='base',
    image='python:3.8-slim',
    resources=resource_requirements
)

# ✅ pod_override must be a V1Pod object
pod_override = V1Pod(
    spec=V1PodSpec(
        containers=[container],
        restart_policy='Never'
    )
)

# Define the DAG
with DAG(
    dag_id='test_vrealize_workflow',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='KubernetesPodOperator with proper pod_override'
) as dag:

    fetch_token = KubernetesPodOperator(
        task_id='get_token',
        name='fetch_token',
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
        get_logs=True,
        in_cluster=True,
        is_delete_operator_pod=True,
        do_xcom_push=True,
        pod_override=pod_override
    )

    fetch_data = KubernetesPodOperator(
        task_id='get_view',
        name='fetch_data',
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
        get_logs=True,
        in_cluster=True,
        is_delete_operator_pod=True,
        do_xcom_push=True,
        pod_override=pod_override
    )

    upload_data = KubernetesPodOperator(
        task_id='put_view',
        name='upload_data',
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
        get_logs=True,
        in_cluster=True,
        is_delete_operator_pod=True,
        do_xcom_push=True,
        pod_override=pod_override
    )

    # Set task dependencies
    fetch_token >> fetch_data >> upload_data
