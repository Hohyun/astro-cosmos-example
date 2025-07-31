from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

import os
from datetime import datetime

airflow_home = os.environ["AIRFLOW_HOME"]

profile_config = ProfileConfig(
    profile_name="dbt_demo",
    target_name="dev",
    profiles_yml_filepath=f"{airflow_home}/dags/dbt/dbt_demo/profiles.yml"
)

my_cosmos_dag = DbtDag(
    project_config=ProjectConfig(
        f"{airflow_home}/dags/dbt/dbt_demo",
    ),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path=f"{airflow_home}/.venv/bin/dbt",
    ),
    operator_args={"install_deps": True},
    # normal dag parameters
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    dag_id="my_cosmos_dag",
    default_args={"retries": 0},
)
