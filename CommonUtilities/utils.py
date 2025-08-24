import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle
from Configuration.config import *


# Create database connection strings

oracle_conn = create_engine(f"oracle+cx_oracle://{ORACLE_USER}:{ORACLE_PASSWORD}@{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}")

mysql_conn = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}")


def read_data_from_file_write_to_stag(file_path,file_type,table_name,db_conn):
    try:
        if file_type =='csv':
            df = pd.read_csv(file_path)
        elif file_type =='json':
            df = pd.read_json(file_path)
        elif file_type =='xml':
            df = pd.read_xml(file_path,xpath='.//item')
        else:
            raise ValueError(f"Usupported file type passed {file_type}")
        df.to_sql(table_name,db_conn, if_exists='replace', index=False)
    except Exception as e:
        print("error encountered while reading or writing..",e)


