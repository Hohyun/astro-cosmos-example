from airflow.sdk import DAG
from datetime import datetime, timedelta
import pendulum
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from airflow.operators.python import PythonOperator
from dremio_simple_query.connect import get_token, DremioConnection
import os
import logging
import dotenv
from airflow.decorators import dag, task

dotenv.load_dotenv()
logging.basicConfig(level=logging.INFO)
local_tz = pendulum.timezone("Asia/Seoul")

def get_dremio_connection() -> DremioConnection:
    login_endpoint = "http://host.docker.internal:9047/apiv2/login"

    payload = {
        "userName": "hohyunkim",
        "password": "8HsVT83AdZNc2h3TkYrd"
    }

    token = get_token(uri = login_endpoint, payload=payload)
    print(token)

    uri = "grpc://host.docker.internal:32010"
    dremio = DremioConnection(token, uri)
    return dremio


@dag (
    dag_id="sales_refund_final_dag",
    start_date=datetime(2025, 8, 27, tzinfo=local_tz),
    schedule='0 1 6 * *',  # At 01:00 on day-of-month 8
    catchup=False,
    tags=["datalake", "sales"],
)
def sales_refund_final_dag():

    # set environment variables for start_date and end_date if needed
    # os.environ["START_DATE"] = ""
    # os.environ["END_DATE"] = ""

    # First day of the last month ~ Last day of the last month
    first_day_of_last_month = (datetime.now().replace(day=1) - timedelta(days=1)).replace(day=1).date()
    last_day_of_last_month = (datetime.now().replace(day=1) - timedelta(days=1)).date()

    start_date = os.getenv("START_DATE", first_day_of_last_month.strftime('%Y-%m-%d'))
    end_date = os.getenv("END_DATE", last_day_of_last_month.strftime('%Y-%m-%d'))

    logging.info(f"Start Date: {start_date}, End Date: {end_date}")

    s3_bucket_name = "datalake"
    s3_file_key = f"sales/sales_refund_{start_date.replace('-', '')}_{end_date.replace('-', '')}.parquet"

    query = f"""
SELECT 'Sale' "Type",
        e.srci "Source",
        p.peri "RptPeriod",
        TO_CHAR(e.dais, 'YYYY-MM-DD') "TrxDate",
        frck.dom_int_uu (p.tdnr, t.isoc, 'Y') "DomInt",
        p.fptp "FOP",
        p.cuop "CCY",
        SUM (NVL (fpam, 0)) "Fare",
        SUM (NVL (p.cort_coam, 0)) "StdCom",
        SUM (NVL (p.cort_spam, 0)) "SupplCom",
        SUM (NVL (p.cort_coam, 0)) + SUM (NVL (p.cort_spam, 0)) "TotalCom",
        SUM(NVL(p.vatc,0)) "VatCom",
        SUM (NVL (fptx, 0)) "Tax",
        SUM (NVL(YR.tmfa, 0)) "YR",
        SUM (NVL(v.vatcf, 0)) "Vatf",
        SUM (NVL (v.vatcyr, 0)) "VatYR",
        (  SUM (NVL (fpam, 0))
          - SUM (NVL (p.cort_spam, 0))
          - SUM (NVL(v.vatcf, 0))) "DiscFareNetVAT",
        SUM (NVL(YR.tmfa, 0)) -  SUM (NVL (v.vatcyr, 0)) "YRNetVAT",
        (  SUM (NVL (fpam, 0))
         + SUM (NVL (fptx, 0))
         - SUM (NVL (p.cort_coam, 0))
         - SUM (NVL (p.cort_spam, 0))
         - SUM(NVL(p.vatc,0))) "Net",
        t.cutp "COA",
        SUM (NVL (fpaf, 0)) "Fare_COA",
        SUM (NVL (p.cort_coaf, 0)) "StdCom_COA",
        SUM (NVL (p.cort_spaf, 0)) "SupplCom_COA",
        SUM (NVL (p.cort_coaf, 0)) + SUM (NVL (p.cort_spaf, 0)) "TotalCom_COA",
        SUM(NVL(p.vatf,0)) "VatCom_COA",
        SUM (NVL (fptf, 0)) "Tax_COA",
        SUM (NVL(YR.TMFF, 0)) "YR_COA",
        SUM (NVL(v.vatff, 0)) "VatFare_COA",
        SUM (NVL(v.vatfyr, 0)) "VatYR_COA",
        (  SUM (NVL (fpaf, 0))
         - SUM (NVL (p.cort_spaf, 0))
         - SUM (NVL(v.vatff, 0))) "DiscFareNetVAT_COA",
        SUM (NVL(YR.TMFF, 0)) -  SUM (NVL(v.vatfyr, 0)) "YRNetVAT_COA",
        (  SUM (NVL (fpaf, 0))
         + SUM (NVL (fptf, 0))
         - SUM (NVL (p.cort_coaf, 0))
         - SUM (NVL (p.cort_spaf, 0))
         - SUM(NVL(p.vatf,0))) "Net_COA"
FROM payt p
	LEFT JOIN para t ON 1=1
    LEFT JOIN emis e ON e.tdnr = p.tdnr
    LEFT JOIN 
    	( SELECT payt_sqnu, 
           		sum(case when vat_type = 'FA' then vatc else 0 end) vatcf,
                sum(case when vat_type = 'FA' then vatf else 0 end) vatff,
                sum(case when vat_type = 'TA' AND INSTR (vat_type_deta, 'ITYR') > 0 then vatc else 0 end) vatcyr,
                sum(case when vat_type = 'TA' AND INSTR (vat_type_deta, 'ITYR') > 0 then vatf else 0 end) vatfyr
            FROM payt_vatc                       
            GROUP BY payt_sqnu ) v ON v.payt_sqnu = p.sqnu        
    LEFT JOIN
    	( SELECT payt_sqnu, sum(nvl(tmfa, 0)) tmfa,  sum(nvl(tmff, 0)) tmff
           FROM payt_txca
           WHERE tmft = 'YR'
           GROUP BY payt_sqnu) YR ON YR.payt_sqnu = p.sqnu        
WHERE e.dais BETWEEN TO_DATE ('{start_date}', 'YYYY-MM-DD') AND TO_DATE ('{end_date}', 'YYYY-MM-DD')
GROUP BY e.srci,
         p.peri,
         e.dais,
         t.isoc,
         t.cutp,
         frck.dom_int_uu (p.tdnr, t.isoc, 'Y'), 
         p.fptp,
         p.cuop        
UNION
SELECT 'Refund' "Type",
        srci "Source",
        p.peri "Period",
        TO_CHAR(p.daut, 'YYYY-MM-DD') "TrxDate",
        frck.dom_int_uu (p.utnr, t.isoc , 'Y') "DomInt",
        p.fptp "FOP",
        p.cutp "CCY",
        -SUM (NVL (cpvl, 0)) "Fare",
        -SUM (NVL (p.cort_coam, 0)) "StdCom",
        -SUM (NVL (p.cort_spam, 0)) "SupplCom",
        - (SUM (NVL (p.cort_coam, 0)) + SUM (NVL (p.cort_spam, 0))) "Total_Com",
        -SUM(NVL(p.vatc,0)) "VatCom",
        -SUM (NVL (p.tmfa, 0)) "Tax",
        -SUM (NVL (YR.tmfa, 0)) "YR",
        -SUM (NVL(v.vatcf, 0)) "VatF",
        -SUM (NVL(v.vatcyr, 0)) "VatYR",
        - ( SUM (NVL (cpvl, 0))
          - SUM (NVL (p.cort_spam, 0))
          - SUM (NVL(v.vatcf, 0))) "DiscFareNetVAT",
        - (SUM (NVL (YR.tmfa, 0)) - SUM (NVL(v.vatcyr, 0))) "YRNetVAT",
        - ( (  SUM (NVL (cpvl, 0))
             + SUM (NVL (p.tmfa, 0))
             - SUM (NVL (p.cort_coam, 0))
             - SUM (NVL (p.cort_spam, 0)))
             - SUM(NVL(p.vatc,0))) "Net",
        t.cutp "COA",
        -SUM (NVL (cpvf, 0)) "Fare_COA",
        -SUM (NVL (p.cort_coaf, 0)) "StdCom_COA",
        -SUM (NVL (p.cort_spaf, 0)) "SupplCom_COA",
        - (SUM (NVL (p.cort_coaf, 0)) + SUM (NVL (p.cort_spaf, 0))) "TotalCom_COA",
        -SUM(NVL(p.vatf,0)) "VatCom_COA",
        -SUM (NVL (p.tmff, 0)) "Tax_COA",
        -SUM (NVL(YR.TMFF, 0)) "YR_COA",
        -SUM (NVL(v.vatff, 0)) "VatFare_COA",
        -SUM (NVL(v.vatfyr, 0)) "VatYR_COA",
        - (  SUM (NVL (cpvf, 0))
           - SUM (NVL (p.cort_spaf, 0))
           - SUM (NVL(v.vatff, 0))) "DiscFareNetVAT_COA",
        - (SUM (NVL(YR.TMFF, 0)) - SUM (NVL(v.vatfyr, 0))) "YRNetVAT_COA",
        - (  SUM (NVL (cpvf, 0))
           + SUM (NVL (p.tmff, 0))
           - SUM (NVL (p.cort_coaf, 0))
           - SUM (NVL (p.cort_spaf, 0))
           - SUM(NVL(p.vatf,0))) "Net_COA"
FROM rmbt p
	LEFT JOIN para t on 1=1
    LEFT JOIN 
    (  SELECT rmbt_id, 
              SUM(case when vat_type = 'FA' then vatc else 0 end) vatcf, 
              SUM(case when vat_type = 'FA' then vatf else 0 end) vatff,
              SUM(case when vat_type = 'TA' AND INSTR (vat_type_deta, 'ITYR') > 0 then vatc else 0 end) vatcYR,
              SUM(case when vat_type = 'TA' AND INSTR (vat_type_deta, 'ITYR') > 0 then vatf else 0 end) vatfYR
       FROM rmbt_vatc                     
       GROUP BY rmbt_id) V ON v.rmbt_id = p.sqnu         
    LEFT JOIN
    (  SELECT rmbt_id, SUM (NVL (tmfa, 0)) tmfa, SUM (NVL (tmff, 0)) tmff
       FROM rmbt_txca
       WHERE tmft = 'YR'
       GROUP BY rmbt_id) YR ON YR.rmbt_id = p.sqnu        
WHERE p.daut BETWEEN TO_DATE ('{start_date}', 'YYYY-MM-DD') AND TO_DATE ('{end_date}', 'YYYY-MM-DD')
GROUP BY p.srci,
         p.peri,
         p.daut,
         t.isoc,
         t.cutp,
         frck.dom_int_uu (p.utnr, t.isoc, 'Y'),
         p.fptp,
         p.cutp
order by 1 DESC
		"""
    sql_to_s3_task = SqlToS3Operator(
        task_id="sql_to_s3_sales_refund_task",
        sql_conn_id="oracle_default",
        query=query,
        aws_conn_id="minio_default",
        s3_bucket=s3_bucket_name,
        s3_key=s3_file_key,
        file_format="parquet",
        replace=True,
    )

    @task
    def delete_exist_data():
        dremio = get_dremio_connection()

        sql = f"""
        DELETE FROM icerberg.sales_refund
        WHERE TrxDate BETWEEN '{start_date}' AND '{end_date}'
        """
        logging.info(sql)
        dremio.query(sql)
        logging.info(f"Deleted existing data for {start_date} ~ {end_date} from sales_refund table.")   

        sql = f"""
        DELETE FROM icerberg.sales_temp
        WHERE TrxDate BETWEEN '{start_date}' AND '{end_date}'
        """
        logging.info(sql)
        dremio.query(sql)
        logging.info(f"Deleted existing data for {start_date} ~ {end_date} from sales_temp table.")

    # run_dremio_delete_task = PythonOperator(
    #     task_id="run_dremio_delete_data_task",
    #     python_callable=delete_exist_data,
    #     op_args=[start_date, end_date],
    # )

    # run_dremio_copy_task = PythonOperator(
    #     task_id="run_dremio_copy_data_task",
    #     python_callable=copy_data_to_dremio,
    #     op_args=[s3_file_key],
    # )

    @task
    def copy_data_to_dremio():
        dremio = get_dremio_connection()
        sql = f"""
        COPY INTO icerberg.sales_refund
        FROM '@minio/datalake/{s3_file_key}'
        FILE_FORMAT 'parquet' 
        """
        _ = dremio.toArrow(sql)
        logging.info(f"Copied data from {s3_file_key} to sales_refund table.")

    # run_dremio_delete_task >> sql_to_s3_task >> run_dremio_copy_task
    delete_exist_data() >> sql_to_s3_task >> copy_data_to_dremio()

sales_refund_final_dag()