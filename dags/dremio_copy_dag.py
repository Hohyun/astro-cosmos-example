from airflow.sdk import dag, DAG
from datetime import datetime, timedelta
import pendulum
from airflow.operators.python import PythonOperator
from dremio_simple_query.connect import get_token, DremioConnection
import os
import logging
import dotenv

dotenv.load_dotenv()
logging.basicConfig(level=logging.INFO)
local_tz = pendulum.timezone("Asia/Seoul")

def copy_data_to_dremio(s3_file_key:str):

    # login_endpoint = "http://host.docker.internal:9047/apiv2/login"

    # payload = {
    #     "userName": "hohyunkim",
    #     "password": "8HsVT83AdZNc2h3TkYrd"
    # }
    # token = get_token(uri = login_endpoint, payload=payload)

    token = "dg11j87s165ecljmsb4qclthct"
    uri = "grpc://host.docker.internal:32010"
    dremio = DremioConnection(token, uri)
    sql = f"""
COPY INTO icerberg.sales_refund
FROM '@minio/datalake/{s3_file_key}'
FILE_FORMAT 'parquet' 
"""
    _ = dremio.toArrow(sql)


with DAG (
    dag_id="dremio_copy_dag",
    start_date=datetime(2025, 8, 27, tzinfo=local_tz),
    schedule='0 13 * * 3',
    catchup=False,
    tags=["datalake", "sales"],
) as dag:

    # set environment variables for start_date and end_date if needed
    os.environ["START_DATE"] = "2025-09-01"
    os.environ["END_DATE"] = "2025-09-02"

    # Wednesday of the last week ~ Tuesday of this week 
    week_delta = 1
    last_wed = (datetime.now() - timedelta(days=(datetime.now().weekday() - 2) % 7 + 7 * week_delta)).date()
    this_tue = last_wed + timedelta(days=6)
    start_date = os.getenv("START_DATE", last_wed.strftime('%Y-%m-%d'))
    end_date = os.getenv("END_DATE", this_tue.strftime('%Y-%m-%d'))

    logging.info(f"Start Date: {start_date}, End Date: {end_date}")

    s3_bucket_name = "datalake"
    s3_file_key = f"sales/sales_refund_{start_date.replace('-', '')}_{end_date.replace('-', '')}.parquet"

    run_dremio_copy_task = PythonOperator(
        task_id="run_dremio_copy_data_task",
        python_callable=copy_data_to_dremio,
        op_args=[s3_file_key],
    )

    run_dremio_copy_task


