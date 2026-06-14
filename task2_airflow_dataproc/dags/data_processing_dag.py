import datetime
import uuid

from airflow import DAG
from airflow.providers.yandex.operators.yandexcloud_dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreatePysparkJobOperator,
    DataprocDeleteClusterOperator,
)
from airflow.utils.trigger_rule import TriggerRule


YC_DP_AZ = "ru-central1-b"
YC_DP_SSH_PUBLIC_KEY = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAICKAi6OCjVnyCCOH9Sx4A91YASYh6DSOmADapxxpfC3X dataproc-homework"

YC_DP_SUBNET_ID = "e2lh2jodf9vqmqsfao72"
YC_DP_SA_ID = "ajeulk8t9l3pt2tc8ho8"

YC_BUCKET = "dataproc-bucket-kate-2026"

CLUSTER_NAME = f"airflow-dataproc-{uuid.uuid4().hex[:8]}"


with DAG(
        dag_id="DATA_PROCESSING_CREDIT_APPLICATIONS",
        schedule=None,
        start_date=datetime.datetime(2026, 6, 13),
        catchup=False,
        tags=["etl", "dataproc", "airflow", "module4"],
) as dag:

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        cluster_name=CLUSTER_NAME,
        cluster_description="Temporary Data Processing cluster for credit applications ETL",
        ssh_public_keys=[YC_DP_SSH_PUBLIC_KEY],
        service_account_id=YC_DP_SA_ID,
        subnet_id=YC_DP_SUBNET_ID,
        zone=YC_DP_AZ,
        s3_bucket=YC_BUCKET,
        cluster_image_version="2.1",
        masternode_resource_preset="s2.small",
        masternode_disk_type="network-hdd",
        masternode_disk_size=50,
        computenode_resource_preset="s2.small",
        computenode_disk_type="network-hdd",
        computenode_disk_size=50,
        computenode_count=1,
        datanode_count=0,
        services=["YARN", "SPARK"],
    )

    run_pyspark_job = DataprocCreatePysparkJobOperator(
        task_id="run_pyspark_job",
        cluster_id=create_cluster.output,
        main_python_file_uri=(
            f"s3a://{YC_BUCKET}/"
            "task2/scripts/process_credit_applications.py"
        ),
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_dataproc_cluster",
        cluster_id=create_cluster.output,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    create_cluster >> run_pyspark_job >> delete_cluster