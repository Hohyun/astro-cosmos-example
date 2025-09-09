from airflow.sdk import dag, DAG
from datetime import datetime
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator

with DAG (
    dag_id="postgres_example_dag",
    start_date=datetime(2025, 8, 27),
    schedule='0 13 * * 3',
    catchup=False,
    tags=["datalake", "sales"],
) as dag:

    query = "SELECT * from users"

    sql_task = SQLExecuteQueryOperator(
        task_id="postgres_query_task",
        conn_id="aprs_default",
        sql=query,
    )
      
    sql_to_s3_task = SqlToS3Operator(
        task_id="sql_to_s3_aprs_users_task",
        sql_conn_id="aprs_default",
        query=query,
        aws_conn_id="minio_default",
        s3_bucket="datalake",
        s3_key="aprs_users",
        file_format="csv",
        replace=True,
    )

    sql_task >> sql_to_s3_task

