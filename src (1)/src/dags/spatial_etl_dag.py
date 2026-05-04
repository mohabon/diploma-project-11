from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="tartus_spatial_etl",
    description="ETL-пайплайн пространственного хранилища данных мухафазы Тартус",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["tartus", "etl", "raw", "data-vault", "data-marts"],
    doc_md="""
    ### ETL-пайплайн пространственного хранилища данных мухафазы Тартус

    Последовательность обработки данных в прототипе:

    `Raw Layer → Data Vault → Data Marts → Data Quality → Streamlit Dashboard`

    Основные этапы:
    1. Загрузка исходных данных в слой `raw`
    2. Формирование слоя `Data Vault` и аналитических витрин `Data Marts`
    3. Контроль качества данных
    """,
) as dag:

    load_raw_data = BashOperator(
        task_id="load_raw_data",
        task_display_name="Загрузка исходных данных в Raw Layer",
        bash_command="cd /app && python etl/load_raw.py",
    )

    transform_to_vault_and_marts = BashOperator(
        task_id="transform_to_vault_and_marts",
        task_display_name="Формирование Data Vault и Data Marts",
        bash_command="cd /app && python etl/transform_business.py",
    )

    run_data_quality_checks = BashOperator(
        task_id="run_data_quality_checks",
        task_display_name="Контроль качества данных",
        bash_command="cd /app && python etl/dq/test_cadastre.py",
    )

    load_raw_data >> transform_to_vault_and_marts >> run_data_quality_checks