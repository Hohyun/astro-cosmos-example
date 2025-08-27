# import pandas as pd
from datetime import datetime, timedelta
import os
import polars as pl
import sqlalchemy
from minio import Minio
import io
import logging
import dotenv

dotenv.load_dotenv()

logging.basicConfig(level=logging.INFO)

## Prerequisites
# - Install the required packages:
#   - polars
#   - pandas
#   - sqlalchemy
#   - oracledb
#   - minio

def main():
	# check running time
	start_time = datetime.now()
	logging.info(f"Start Time: {start_time}")

	# Wednesday of the last week ~ Tuesday of this week 
	last_wed = (datetime.now() - timedelta(days=(datetime.now().weekday() - 2) % 7 + 7)).date()
	this_tue = last_wed + timedelta(days=6)
	start_date = os.getenv("START_DATE", last_wed.strftime('%Y-%m-%d'))
	end_date = os.getenv("END_DATE", this_tue.strftime('%Y-%m-%d'))
	logging.info(f"Start Date: {start_date}, End Date: {end_date}")

	try:
		conn_string = 'oracle+oracledb://jinair_read:hDtxZgzrfgXCtPv2QmEH@lj.db.rep.mrva.io:1521/ORCL'
		engine = sqlalchemy.create_engine(conn_string)
		query = f"""
SELECT 'Sale' "Type",
		e.tdnr "TDNR",
        e.srci "Source",
        p.peri "RptPeriod",
        e.dais "TrxDate",
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
GROUP BY e.tdnr, 
		 e.srci,
         p.peri,
         e.dais,
         t.isoc,
         t.cutp,
         frck.dom_int_uu (p.tdnr, t.isoc, 'Y'), 
         p.fptp,
         p.cuop        
UNION
SELECT 'Refund' "Type",
		p.utnr "TDNR",
        srci "Source",
        p.peri "Period",
        p.daut "TrxDate",
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
GROUP BY p.utnr, 
         p.srci,
         p.peri,
         p.daut,
         t.isoc,
         t.cutp,
         frck.dom_int_uu (p.utnr, t.isoc, 'Y'),
         p.fptp,
         p.cutp
order by 1 DESC
		"""
		# df = pd.read_sql(query, con=engine)	
		df = pl.read_database(query, connection=engine.connect())
		print(df)
		df.write_parquet("/tmp/sales_refund.parquet")

		upload_file_to_minio("datalake", f"sales/sales_refund_{start_date.replace('-', '')}_{end_date.replace('-', '')}.parquet", "/tmp/sales_refund.parquet")

	except Exception as e:
		print(f"Error occurred:, {e}")

	end_time = datetime.now()
	logging.info(f"End Time: {end_time}, Elapsed Time: {end_time - start_time}")


def read_parquet_from_minio(bucket_name, object_name):
	"""
	Read a Parquet file from MinIO and return a Polars DataFrame.
	"""
	client = Minio(
	"10.90.65.61:9000",
	access_key="X6I698S1TZ4N791O9PK2",
	secret_key="nSd6SEEMxVrI5IdD06itUMGt+44StxTiz5i7uVpa",
	secure=False,  # Set to True if using HTTPS
)
	response = client.get_object(bucket_name, object_name)

	# Read the file content into a Polars DataFrame
	data = io.BytesIO(response.read())
	df = pl.read_parquet(data)
	return df

def upload_file_to_minio(bucket_name, object_name, file_name):
	client = Minio(
		"10.90.65.61:9000",
		access_key="X6I698S1TZ4N791O9PK2",
		secret_key="nSd6SEEMxVrI5IdD06itUMGt+44StxTiz5i7uVpa",
		secure=False,  # Set to True if using HTTPS
	)

	with open(file_name, "rb") as file_data:
		result = client.put_object(bucket_name, object_name, file_data, os.path.getsize(file_name))
		logging.info(
			"created {0} object; etag: {1}, version-id: {2}".format(
				result.object_name, result.etag, result.version_id,
    	),
)


def test():
	# upload_file_to_minio("datalake", "oracle_sales_refund.parquet", "oracle_sales_refund.parquet")
	# df = read_parquet_from_minio("datalake", "sales/sales_refund_20250820_20250826.parquet")
	# print(df)
	pass

if __name__ == "__main__":
	main()	
	# test()