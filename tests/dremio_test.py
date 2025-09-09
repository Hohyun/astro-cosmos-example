from dremio_simple_query.connect import get_token, DremioConnection


def copy_data_to_dremio(s3_file_key:str):

    login_endpoint = "http://localhost:9047/apiv2/login"

    payload = {
        "userName": "hohyunkim",
        "password": "8HsVT83AdZNc2h3TkYrd"
    }

    token = "dg11j87s165ecljmsb4qclthct"
    # token = get_token(uri = login_endpoint, payload=payload)
    # print(token)
    uri = "grpc://localhost:32010"
    dremio = DremioConnection(token, uri)
    sql = f"""
COPY INTO icerberg.sales_refund
FROM '@minio/datalake/{s3_file_key}'
FILE_FORMAT 'parquet' 
"""

    _ = dremio.toArrow(sql)
    # table = stream.read_all().to_pydict()
   
    
if __name__ == "__main__":
    copy_data_to_dremio("sales/sales_refund_20250901_20250902.parquet")    