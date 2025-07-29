from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from kubernetes.client import (
    V1Pod, V1PodSpec, V1Container, V1ResourceRequirements, V1ObjectMeta,
    V1Volume, V1VolumeMount, V1EmptyDirVolumeSource
)

default_args = {
    'start_date': days_ago(1),
}

with DAG(
    'kpo_with_xcom_working',
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
    tags=['example'],
) as dag:

    # XCom shared volume
    volume = V1Volume(
        name='xcom',
        empty_dir=V1EmptyDirVolumeSource()
    )

    volume_mount = V1VolumeMount(
        name='xcom',
        mount_path='/airflow/xcom'
    )

    # Main task container
    main_container = V1Container(
        name='base',
        image='python:3.9-slim',
        command=['python', '-c'],
        args=[
            "import json; "
            "result = {'xcom_value': 42}; "
            "with open('/airflow/xcom/return.json', 'w') as f: json.dump(result, f)"
        ],
        resources=V1ResourceRequirements(
            requests={"cpu": "500m", "memory": "512Mi"},
            limits={"cpu": "1", "memory": "1Gi"}
        ),
        volume_mounts=[volume_mount]
    )

    # Sidecar container for XCom
    xcom_sidecar = V1Container(
        name='airflow-xcom-sidecar',
        image='alpine',
        command=['sh', '-c', 'trap "exit 0" INT; while true; do sleep 1; done;'],
        resources=V1ResourceRequirements(
            requests={"cpu": "1m", "memory": "10Mi"}
        ),
        volume_mounts=[volume_mount]
    )

    # Full pod spec
    pod_spec = V1PodSpec(
        containers=[main_container, xcom_sidecar],
        restart_policy="Never",
        volumes=[volume]
    )

    full_pod_spec = V1Pod(
        metadata=V1ObjectMeta(name='example-kpo'),
        spec=pod_spec
    )

    kpo_task = KubernetesPodOperator(
        task_id='kpo_task',
        namespace='admin-635a7131',
        full_pod_spec=full_pod_spec,
        get_logs=True,
        do_xcom_push=True,  # <-- This triggers Airflow to pull from return.json
        is_delete_operator_pod=True,
    )
