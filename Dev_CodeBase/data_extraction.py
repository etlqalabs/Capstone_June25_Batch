import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle
from CommonUtilities.utils import read_data_from_file_write_to_stag
from Configuration.config import *
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


# data extraction scripts

def extract_sales_data_from_file_load_to_staging_table(file_path,file_type,table_name,db_conn):
    logger.info("Sales data extaction started....")
    try:
        read_data_from_file_write_to_stag(file_path,file_type,table_name,db_conn)
    except Exception as e:
        logger.error("error while reading or writting sales data in extraction process.",e,exc_info=True)
    logger.info("Sales data extaction completed....")

def extract_product_data_from_file_load_to_staging_table(file_path,file_type,table_name,db_conn):
    print("Product data extaction started....")
    read_data_from_file_write_to_stag(file_path, file_type, table_name, db_conn)
    print("Product data stag loading completed....")

def extract_supplier_data_from_file_load_to_staging_table(file_path,file_type,table_name,db_conn):
    print("Supplier data extaction started....")
    read_data_from_file_write_to_stag(file_path, file_type, table_name, db_conn)
    print("Supplier data stag loading completed....")

def extract_inventory_data_from_file_load_to_staging_table(file_path,file_type,table_name,db_conn):
    print("Inventory data extaction started....")
    read_data_from_file_write_to_stag(file_path,file_type,table_name,db_conn)
    print("Inventory data stag loading completed....")

def extract_stores_data_from_file_load_to_staging_table():
    print("Stores data extaction started....")
    query = """select * from stores"""
    df_stores = pd.read_sql(query,oracle_conn)
    df_stores.to_sql("stag_stores",mysql_conn,if_exists='replace',index=False)
    print("Stores data stag loading completed....")


if __name__ == "__main__":
    extract_sales_data_from_file_load_to_staging_table("SourceSystems/sales_data.csv","csv","stag_sales",mysql_conn)
    extract_product_data_from_file_load_to_staging_table("SourceSystems/product_data.csv","csv","stag_product",mysql_conn)
    extract_supplier_data_from_file_load_to_staging_table("SourceSystems/supplier_data.json","json","stag_supplier",mysql_conn)
    extract_inventory_data_from_file_load_to_staging_table("SourceSystems/inventory_data.xml","xml","stag_inventory",mysql_conn)
    extract_stores_data_from_file_load_to_staging_table()