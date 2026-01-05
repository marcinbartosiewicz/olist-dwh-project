import os
import pandas as pd
import holidays
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.pandas_tools import write_pandas

default_args = {
    'owner': 'idh_projekt_mb_az',
    'start_date': datetime(2026, 1, 1),
}

def etl_and_load():
    snf_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    conn = snf_hook.get_conn()
    data_path = '/opt/airflow/data/'

    
    cust = pd.read_csv(f'{data_path}olist_customers_dataset.csv')
    items = pd.read_csv(f'{data_path}olist_order_items_dataset.csv')
    orders = pd.read_csv(f'{data_path}olist_orders_dataset.csv')
    prod = pd.read_csv(f'{data_path}olist_products_dataset.csv')
    trans = pd.read_csv(f'{data_path}product_category_name_translation.csv')
    rev = pd.read_csv(f'{data_path}olist_order_reviews_dataset.csv')
    sellers = pd.read_csv(f'{data_path}olist_sellers_dataset.csv')

    
    
    dim_prod = prod.merge(trans, on='product_category_name', how='left')[['product_id', 'product_category_name_english']]
    dim_prod.columns = [c.upper() for c in dim_prod.columns]

    
    orders['order_purchase_timestamp'] = pd.to_datetime(orders['order_purchase_timestamp'])
    avg_rev = rev.groupby('order_id')['review_score'].mean().reset_index()
    fact = items.merge(orders, on='order_id').merge(sellers, on='seller_id').merge(avg_rev, on='order_id', how='left')
    fact['order_purchase_timestamp'] = fact['order_purchase_timestamp'].dt.strftime('%Y-%m-%d')
    min_date = fact['order_purchase_timestamp'].min()
    max_date = fact['order_purchase_timestamp'].max()
    dr = pd.date_range(start=min_date, end=max_date)
    
    
    dim_date = pd.DataFrame({'DATE_KEY': dr})
    
    
    dim_date['DAY_OF_WEEK'] = dim_date['DATE_KEY'].dt.dayofweek
    dim_date['MONTH'] = dim_date['DATE_KEY'].dt.month
    dim_date['YEAR'] = dim_date['DATE_KEY'].dt.year
    
    br_holidays = holidays.Brazil()
    dim_date['IS_HOLIDAY'] = dim_date['DATE_KEY'].apply(lambda x: x in br_holidays)
    
    
    dim_date['DATE_KEY'] = dim_date['DATE_KEY'].dt.strftime('%Y-%m-%d')
    
    
    dim_date.columns = [c.upper() for c in dim_date.columns]

   
    fact['DATE_KEY'] = fact['order_purchase_timestamp']
    
    
    fact_final = fact[['order_item_id', 'order_id', 'product_id', 'customer_id', 'seller_id',
    'price', 'freight_value', 'order_purchase_timestamp', 
    'order_delivered_customer_date', 'order_estimated_delivery_date', 
    'review_score', 'DATE_KEY']]
    fact_final.columns = [c.upper() for c in fact_final.columns]

    write_pandas(conn, sellers.rename(columns=str.upper), 'DIM_SELLERS')
    write_pandas(conn, cust.rename(columns=str.upper), 'DIM_CUSTOMERS')
    write_pandas(conn, dim_prod, 'DIM_PRODUCTS')
    write_pandas(conn, dim_date, 'DIM_DATE')
    write_pandas(conn, fact_final, 'FACT_ORDER_ITEMS')

with DAG('olist_snowflake_pipeline', default_args=default_args, schedule_interval=None,template_searchpath=['/opt/airflow'], 
    catchup=False) as dag:
    
    task_setup = SnowflakeOperator(
        task_id='setup_tables',
        snowflake_conn_id='snowflake_default',
        sql='sql/snowflake_setup.sql'
    )

    task_etl = PythonOperator(
        task_id='run_etl_load',
        python_callable=etl_and_load
    )

    task_setup >> task_etl