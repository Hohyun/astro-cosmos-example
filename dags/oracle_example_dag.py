from airflow.sdk import dag, DAG
from datetime import datetime
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

with DAG (
    dag_id="oracle_test_dag",
    start_date=datetime(2025, 8, 27),
    schedule='0 13 * * 3',
    catchup=False,
    tags=["datalake", "sales"],
) as dag:
    
    query = "SELECT * FROM EMIS e FETCH FIRST 2 ROWS only"

    sql_task = SQLExecuteQueryOperator(
        task_id="oracle_query_task",
        conn_id="oracle_default",
        sql=query,
    )
      
    sql_task

