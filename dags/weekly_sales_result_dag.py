from datetime import datetime, timedelta
import pendulum
from dremio_simple_query.connect import get_token, DremioConnection
import os
import logging
import dotenv
from airflow.decorators import dag, task

dotenv.load_dotenv()
logging.basicConfig(level=logging.INFO)
local_tz = pendulum.timezone("Asia/Seoul")

@dag (
    dag_id="weekly_sales_result_dag",
    start_date=datetime(2025, 9, 10, tzinfo=local_tz),
    schedule='30 13 * * 3',
    catchup=False,
    tags=["datalake", "sales"],
) 
def weekly_sales_result_dag():

    # set environment variables for start_date and end_date if needed
    os.environ["START_DATE"] = "2025-09-01"
    os.environ["END_DATE"] = "2025-09-09"

    # Wednesday of the last week ~ Tuesday of this week 
    last_wed = (datetime.now() - timedelta(days=(datetime.now().weekday() - 2) % 7 + 7)).date()
    this_tue = last_wed + timedelta(days=6)
    start_date = os.getenv("START_DATE", last_wed.strftime('%Y-%m-%d'))
    end_date = os.getenv("END_DATE", this_tue.strftime('%Y-%m-%d'))

    s3_file_key = f"sales/sales_result_{start_date.replace('-', '')}_{end_date.replace('-', '')}.csv"
    logging.info(f"Start Date: {start_date}, End Date: {end_date}")
    logging.info(f"S3 File Key: {s3_file_key}")

    @task
    def copy_data_to_dremio(s3_file_key:str):
        login_endpoint = "http://host.docker.internal:9047/apiv2/login"

        payload = {
            "userName": "hohyunkim",
            "password": "8HsVT83AdZNc2h3TkYrd"
        }

        token = get_token(uri = login_endpoint, payload=payload)
        print(token)

        uri = "grpc://host.docker.internal:32010"
        dremio = DremioConnection(token, uri)
        sql = f"""
    COPY INTO icerberg.sales_temp
    FROM '@minio/datalake/{s3_file_key}'
    FILE_FORMAT 'csv'
    ( FIELD_DELIMITER ';', DATE_FORMAT 'YYYY-MM-DD' )
    """
        logging.info(sql)

        _ = dremio.toArrow(sql)
        logging.info(f"Copied data from {s3_file_key} to sales_temp table.")

    copy_data_to_dremio(s3_file_key)

weekly_sales_result_dag()

