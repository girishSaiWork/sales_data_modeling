import sys
import logging,os
from snowflake.snowpark import Session, DataFrame
from snowflake.snowpark.functions import col, lit, rank
from snowflake.snowpark import Window

# Setup logging
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%I:%M:%S'
)

def get_snowpark_session() -> Session:
    connection_parameters = {
        "ACCOUNT": os.getenv("ACCOUNT_ID"),
        "USER": os.getenv("USER"),
        "PASSWORD": os.getenv("PASSWORD"),
        "ROLE": os.getenv("ROLE"),
        "DATABASE": os.getenv("DATABASE"),
        "SCHEMA": os.getenv("SCHEMA"),
        "WAREHOUSE": os.getenv("WAREHOUSE")
    }
    logging.info("Creating Snowflake session...")
    return Session.builder.configs(connection_parameters).create()

def filter_dataset(df, column_name, filter_criterian) -> DataFrame:
    logging.info(f"Filtering data where {column_name} = {filter_criterian}")
    return df.filter(col(column_name) == filter_criterian)

def main():
    try:
        session = get_snowpark_session()
        logging.info("Snowflake session created successfully.")

        sales_df = session.sql("SELECT * FROM in_sales_order")
        logging.info("Fetched data from in_sales_order.")

        paid_sales_df = filter_dataset(sales_df, 'PAYMENT_STATUS', 'Paid')
        shipped_sales_df = filter_dataset(paid_sales_df, 'SHIPPING_STATUS', 'Delivered')
        logging.info("Filtered for Paid and Delivered orders.")

        country_sales_df = shipped_sales_df.with_column('Country', lit('IN')).with_column('Region', lit('APAC'))
        logging.info("Added Country and Region columns.")

        forex_df = session.sql("SELECT * FROM sales_dwh.common.exchange_rate")
        forex_df.show(2)
        logging.info("Fetched data from exchange_rate table.")

        sales_with_forext_df = country_sales_df.join(
            forex_df,
            country_sales_df['order_dt'] == forex_df['date'],
            join_type='outer'
        )
        logging.info("Joined sales with exchange rate data.")

        unique_orders = sales_with_forext_df.with_column(
            'order_rank',
            rank().over(Window.partitionBy(col("order_dt")).order_by(col('_metadata_last_modified').desc()))
        ).filter(col("order_rank") == 1).select(col('SALES_ORDER_KEY').alias('unique_sales_order_key'))
        logging.info("Performed de-duplication based on order_dt.")

        final_sales_df = unique_orders.join(
            sales_with_forext_df,
            unique_orders['unique_sales_order_key'] == sales_with_forext_df['SALES_ORDER_KEY'],
            join_type='inner'
        )

        final_sales_df = final_sales_df.select(
            col('SALES_ORDER_KEY'),
            col('ORDER_ID'),
            col('ORDER_DT'),
            col('CUSTOMER_NAME'),
            col('MOBILE_KEY'),
            col('Country'),
            col('Region'),
            col('ORDER_QUANTITY'),
            lit('INR').alias('LOCAL_CURRENCY'),
            col('UNIT_PRICE').alias('LOCAL_UNIT_PRICE'),
            col('PROMOTION_CODE'),
            col('FINAL_ORDER_AMOUNT').alias('LOCAL_TOTAL_ORDER_AMT'),
            col('TAX_AMOUNT').alias('local_tax_amt'),
            col('USD2INR').alias("Exhchange_Rate"),
            (col('FINAL_ORDER_AMOUNT') / col('USD2INR')).alias('US_TOTAL_ORDER_AMT'),
            (col('TAX_AMOUNT') / col('USD2INR')).alias('USD_TAX_AMT'),
            col('payment_status'),
            col('shipping_status'),
            col('payment_method'),
            col('payment_provider'),
            col('mobile').alias('conctact_no'),
            col('shipping_address')
        )

        logging.info("Final dataframe created. Preparing to write to curated table...")

        final_sales_df.write.save_as_table("sales_dwh.curated.in_sales_order", mode="append")
        logging.info("Data successfully ingested into sales_dwh.curated.in_sales_order")
        print("Ingestion completed successfully.")

    except Exception as e:
        logging.error(f"Error occurred during execution: {e}")
        print("An error occurred. Check logs for more details.")

if __name__ == '__main__':
    main()
