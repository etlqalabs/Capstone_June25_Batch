import pandas as pd
from sqlalchemy import create_engine, text
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

def load_fact_sales_table():
    query = text(""" insert into fact_sales(sales_id,product_id,store_id,quantity,total_sales,sale_date)
                 select sales_id,product_id,store_id,quantity,total_sales_amount,sale_date from sales_with_details""")
    try:
        with mysql_conn.connect() as conn:
            logger.info("Loading to fact-sales table  started....")
            logger.info(query)
            conn.execute(query)
            conn.commit()
            logger.info("Loading to fact-sales table completed....")
    except Exception as e:
        logger.error("error while loading in to target table fact_sales.",e,exc_info=True)


if __name__ == "__main__":
    load_fact_sales_table()
