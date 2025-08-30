import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle
from Configuration.config import *
import paramiko
import logging

# Logging configuration
logging.basicConfig(
    filename="LogFile/etljob.log",
    filemode='a', # a for append , w = overwrite
    format = '%(asctime)s-%(levelname)s-%(message)s',
    level = logging.INFO
)
logger = logging.getLogger(__name__)



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


def download_sales_file_from_Linux():
    logger.info("Sales file download started....")
    try:
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(hostname,username=user_name,password=pass_word)
        sftp = ssh_client.open_sftp()
        sftp.get(linux_file_path,project_file_path)
        sftp.close()
        ssh_client.close()
        logger.info("Sales file download completed....")
    except Exception as e:
        logger.error(f"download of sales file failed {e}",exc_info=True)



    logger.info("Sales file download completed....")

