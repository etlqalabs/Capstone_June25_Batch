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


# data transformation scripts

def transform_filter_sales_data():
    logger.info("Sales data transformation for sales data  started....")
    query = """select * from stag_sales where sale_date>='2024-09-10'"""
    df = pd.read_sql(query,mysql_conn)
    df.to_sql("filtered_sales",mysql_conn,if_exists='replace',index=False)
    logger.info("Sales data transfromation for sales data compeleted....")

def transform_router_low_region_sales_data():
    logger.info(" Router transformation for low region on sales data started....")
    query = """select * from filtered_sales where region='Low'"""
    df = pd.read_sql(query,mysql_conn)
    df.to_sql("low_sales",mysql_conn,if_exists='replace',index=False)
    logger.info(" Router transformation for low region on sales data completed....")

def transform_router_high_region_sales_data():
    logger.info(" Router transformation for high region on sales data started....")
    query = """select * from filtered_sales where region='High'"""
    df = pd.read_sql(query,mysql_conn)
    df.to_sql("high_sales",mysql_conn,if_exists='replace',index=False)
    logger.info(" Router transformation for high region on sales data completed....")

def transform_sales_aggregator():
    logger.info(" sales Aggregator transformation started....")
    query = """select product_id,month(sale_date) as month,year(sale_date) as year,sum(price*quantity) as total_sales from filtered_sales
                group by product_id,month(sale_date),year(sale_date) """
    df = pd.read_sql(query,mysql_conn)
    df.to_sql("monthly_sales_summary_source",mysql_conn,if_exists='replace',index=False)
    logger.info("sales Aggregator transformation completed....")


def transform_inventory_aggregator():
    logger.info(" Inventory Aggregator transformation started....")
    query = """select store_id, sum(quantity_on_hand) as total_inventory from stag_inventory group by store_id """
    df = pd.read_sql(query,mysql_conn)
    df.to_sql("aggregated_inventory_level",mysql_conn,if_exists='replace',index=False)
    logger.info("Inventory Aggregator transformation completed....")

def transform_Join_sales_stores_product():
    logger.info(" Inventory Aggregator transformation started....")
    query = """select fs.sales_id,fs.quantity,fs.price,fs.quantity*fs.price as total_sales_amount,fs.sale_date,
                p.product_id,p.product_name,
                s.store_id,s.store_name
                from filtered_sales as fs 
                inner join stag_product as p on fs.product_id = p.product_id
                inner join stag_stores as s on s.store_id = fs.store_id"""
    df = pd.read_sql(query,mysql_conn)
    df.to_sql("sales_with_details",mysql_conn,if_exists='replace',index=False)
    logger.info("Inventory Aggregator transformation completed....")


if __name__ == "__main__":
    transform_filter_sales_data()
    transform_router_low_region_sales_data()
    transform_router_high_region_sales_data()
    transform_sales_aggregator()
    transform_inventory_aggregator()
    transform_Join_sales_stores_product()