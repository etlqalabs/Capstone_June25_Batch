import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle


# Create database connection strings
oracle_conn = create_engine("oracle+cx_oracle://system:admin@localhost:1521/xe")
mysql_conn = create_engine("mysql+pymysql://root:Admin%40143@localhost:3308/retaildwh")

# data extraction scripts

def extract_sales_data_from_file_load_to_staging_table():
    print("Sales data extaction started....")
    df_sales = pd.read_csv("SourceSystems/sales_data.csv")
    df_sales.to_sql("stag_sales",mysql_conn,if_exists='replace',index=False)
    print("Sales data stag loading completed....")

def extract_product_data_from_file_load_to_staging_table():
    print("Product data extaction started....")
    df_product = pd.read_csv("SourceSystems/product_data.csv")
    df_product.to_sql("stag_product",mysql_conn,if_exists='replace',index=False)
    print("Product data stag loading completed....")

def extract_supplier_data_from_file_load_to_staging_table():
    print("Supplier data extaction started....")
    df_supplier = pd.read_json("SourceSystems/supplier_data.json")
    df_supplier.to_sql("stag_supplier",mysql_conn,if_exists='replace',index=False)
    print("Supplier data stag loading completed....")

def extract_inventory_data_from_file_load_to_staging_table():
    print("Inventory data extaction started....")
    df_inventory = pd.read_xml("SourceSystems/inventory_data.xml",xpath=".//item")
    df_inventory.to_sql("stag_inventory",mysql_conn,if_exists='replace',index=False)
    print("Inventory data stag loading completed....")

def extract_stores_data_from_file_load_to_staging_table():
    print("Stores data extaction started....")
    query = """select * from stores"""
    df_stores = pd.read_sql(query,oracle_conn)
    df_stores.to_sql("stag_stores",mysql_conn,if_exists='replace',index=False)
    print("Stores data stag loading completed....")


if __name__ == "__main__":
    extract_sales_data_from_file_load_to_staging_table()
    extract_product_data_from_file_load_to_staging_table()
    extract_supplier_data_from_file_load_to_staging_table()
    extract_inventory_data_from_file_load_to_staging_table()
    extract_stores_data_from_file_load_to_staging_table()