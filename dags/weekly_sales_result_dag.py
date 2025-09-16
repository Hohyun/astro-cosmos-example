from datetime import datetime, timedelta
import pendulum
from dremio_simple_query.connect import DremioConnection
import os
import logging
import dotenv
from airflow.decorators import dag, task

dotenv.load_dotenv()
logging.basicConfig(level=logging.INFO)
local_tz = pendulum.timezone("Asia/Seoul")

@dag (
    dag_id="weekly_sales_result_dag",
    start_date=datetime(2025, 8, 27, tzinfo=local_tz),
    schedule='30 13 * * 3',
    catchup=False,
    tags=["datalake", "sales"],
) 
def weekly_sales_result_dag():

    # set environment variables for start_date and end_date if needed
    # os.environ["START_DATE"] = ""
    # os.environ["END_DATE"] = ""

    # Wednesday of the last week ~ Tuesday of this week 
    last_wed = (datetime.now() - timedelta(days=(datetime.now().weekday() - 2) % 7 + 7)).date()
    this_tue = last_wed + timedelta(days=6)
    start_date = os.getenv("START_DATE", last_wed.strftime('%Y-%m-%d'))
    end_date = os.getenv("END_DATE", this_tue.strftime('%Y-%m-%d'))

    s3_file_key = f"sales/sales_result_{start_date.replace('-', '')}_{end_date.replace('-', '')}.csv"
    logging.info(f"Start Date: {start_date}, End Date: {end_date}")

    @task
    def copy_data_to_dremio(s3_file_key:str):
        token = "dg11j87s165ecljmsb4qclthct"
        uri = "grpc://host.docker.internal:32010"
        dremio = DremioConnection(token, uri)
        sql = f"""
    COPY INTO icerberg.sales_temp
    FROM '@minio/datalake/{s3_file_key}'
    FILE_FORMAT 'parquet' 
    """
        _ = dremio.toArrow(sql)
        logging.info(f"Copied data from {s3_file_key} to sales_refund table.")

    copy_data_to_dremio(s3_file_key)


