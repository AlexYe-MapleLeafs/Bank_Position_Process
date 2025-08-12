import datetime
from datetime import datetime
import os
from airflow import models
from kubernetes.client import models as k8s
# from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.utils.dates import days_ago

docker_image="us-docker.pkg.dev/cida-tenant-deploy-vx9m/cidat-10040/positions_data_image:0.0.5"

# Instantiating a DAG object

with models.DAG(
    dag_id="positions_data_dag",
    description="positions_data",
    schedule_interval='30 11 * * 1-5',
    start_date=datetime(2025, 5, 15),
) as dag:

    task_01_dda = KubernetesPodOperator(
        task_id="task_01_dda",
        name="positions_source_dda",
        startup_timeout_seconds=600,
        namespace=os.environ["K8_NAMESPACE"],
        image=docker_image,
        cmds=["python"],
        arguments=["positions_source_dda.py"],
        get_logs=True,
        image_pull_policy='Always',
        config_file="/home/airflow/composer_kube_config",
        kubernetes_conn_id='kubernetes_default',
        )
    
    task_02_fal = KubernetesPodOperator(
        task_id="task_02_fal",
        name="positions_source_fal",
        startup_timeout_seconds=600,
        namespace=os.environ["K8_NAMESPACE"],
        image=docker_image,
        cmds=["python"],
        arguments=["positions_source_fal.py"],
        get_logs=True,
        image_pull_policy='Always',
        config_file="/home/airflow/composer_kube_config",
        kubernetes_conn_id='kubernetes_default',
        )
    
    task_03_oll = KubernetesPodOperator(
        task_id="task_03_oll",
        name="positions_source_oll",
        startup_timeout_seconds=600,
        namespace=os.environ["K8_NAMESPACE"],
        image=docker_image,
        cmds=["python"],
        arguments=["positions_source_oll.py"],
        get_logs=True,
        image_pull_policy='Always',
        config_file="/home/airflow/composer_kube_config",
        kubernetes_conn_id='kubernetes_default',
        )
    
    task_04_tls = KubernetesPodOperator(
        task_id="task_04_tls",
        name="positions_source_tls",
        startup_timeout_seconds=600,
        namespace=os.environ["K8_NAMESPACE"],
        image=docker_image,
        cmds=["python"],
        arguments=["positions_source_tls.py"],
        get_logs=True,
        image_pull_policy='Always',
        config_file="/home/airflow/composer_kube_config",
        kubernetes_conn_id='kubernetes_default',
        )

    task_05_ufdhs = KubernetesPodOperator(
        task_id="task_05_ufdhs",
        name="positions_source_ufdhs",
        startup_timeout_seconds=600,
        namespace=os.environ["K8_NAMESPACE"],
        image=docker_image,
        cmds=["python"],
        arguments=["positions_source_ufdhs.py"],
        get_logs=True,
        image_pull_policy='Always',
        config_file="/home/airflow/composer_kube_config",
        kubernetes_conn_id='kubernetes_default',
        )

    task_06_uftas = KubernetesPodOperator(
        task_id="task_06_uftas",
        name="positions_source_uftas",
        startup_timeout_seconds=600,
        namespace=os.environ["K8_NAMESPACE"],
        image=docker_image,
        cmds=["python"],
        arguments=["positions_source_uftas.py"],
        get_logs=True,
        image_pull_policy='Always',
        config_file="/home/airflow/composer_kube_config",
        kubernetes_conn_id='kubernetes_default',
        )
    
    task_07_wssmm = KubernetesPodOperator(
        task_id="task_07_wssmm",
        name="positions_source_wssmm",
        startup_timeout_seconds=600,
        namespace=os.environ["K8_NAMESPACE"],
        image=docker_image,
        cmds=["python"],
        arguments=["positions_source_wssmm.py"],
        get_logs=True,
        image_pull_policy='Always',
        config_file="/home/airflow/composer_kube_config",
        kubernetes_conn_id='kubernetes_default',
        )

    task_13_infolease = KubernetesPodOperator(
        task_id="task_13_infolease",
        name="positions_source_infolease",
        startup_timeout_seconds=600,
        namespace=os.environ["K8_NAMESPACE"],
        image=docker_image,
        cmds=["python"],
        arguments=["positions_source_infolease.py"],
        get_logs=True,
        image_pull_policy='Always',
        config_file="/home/airflow/composer_kube_config",
        kubernetes_conn_id='kubernetes_default',
        )

    task_14_mtroy = KubernetesPodOperator(
        task_id="task_14_mtroy",
        name="positions_source_mtroy",
        startup_timeout_seconds=600,
        namespace=os.environ["K8_NAMESPACE"],
        image=docker_image,
        cmds=["python"],
        arguments=["positions_source_mtroy.py"],
        get_logs=True,
        image_pull_policy='Always',
        config_file="/home/airflow/composer_kube_config",
        kubernetes_conn_id='kubernetes_default',
        )

    task_08_all = KubernetesPodOperator(
        task_id="task_08_all",
        name="task_08_all",
        startup_timeout_seconds=600,
        namespace=os.environ["K8_NAMESPACE"],
        image=docker_image,
        cmds=["python"],
        arguments=["positions_stage_all.py"],
        get_logs=True,
        image_pull_policy='Always',
        config_file="/home/airflow/composer_kube_config",
        kubernetes_conn_id='kubernetes_default',
        )

    task_09_daily = KubernetesPodOperator(
        task_id="task_09_daily",
        name="positions_15day",
        startup_timeout_seconds=600,
        namespace=os.environ["K8_NAMESPACE"],
        image=docker_image,
        cmds=["python"],
        arguments=["positions_15day.py"],
        get_logs=True,
        image_pull_policy='Always',
        config_file="/home/airflow/composer_kube_config",
        kubernetes_conn_id='kubernetes_default',
        )

    task_10_monthly = KubernetesPodOperator(
        task_id="task_10_monthly",
        name="positions_monthly",
        startup_timeout_seconds=600,
        namespace=os.environ["K8_NAMESPACE"],
        image=docker_image,
        cmds=["python"],
        arguments=["positions_monthly.py"],
        get_logs=True,
        image_pull_policy='Always',
        config_file="/home/airflow/composer_kube_config",
        kubernetes_conn_id='kubernetes_default',
        )

    task_11_quarterly = KubernetesPodOperator(
        task_id="task_11_quarterly",
        name="positions_quarterly",
        startup_timeout_seconds=600,
        namespace=os.environ["K8_NAMESPACE"],
        image=docker_image,
        cmds=["python"],
        arguments=["positions_quarterly.py"],
        get_logs=True,
        image_pull_policy='Always',
        config_file="/home/airflow/composer_kube_config",
        kubernetes_conn_id='kubernetes_default',
        )

    task_12_history = KubernetesPodOperator(
        task_id="task_12_history",
        name="positions_history",
        startup_timeout_seconds=600,
        namespace=os.environ["K8_NAMESPACE"],
        image=docker_image,
        cmds=["python"],
        arguments=["positions_history.py"],
        get_logs=True,
        image_pull_policy='Always',
        config_file="/home/airflow/composer_kube_config",
        kubernetes_conn_id='kubernetes_default',
        )

# order for triggering linked DAGs
[task_01_dda, task_02_fal, task_03_oll, task_04_tls, task_05_ufdhs, task_06_uftas, task_07_wssmm, task_13_infolease, task_14_mtroy] >> task_08_all >> [task_09_daily, task_10_monthly, task_11_quarterly, task_12_history]
