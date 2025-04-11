import sys
import logging,os
from snowflake.snowpark import Session

# Set up logging
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Snowpark session
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
    return Session.builder.configs(connection_parameters).create()

def ingest_in_sales(session):
    try:
        session.sql("""
            COPY INTO SALES_DWH.SOURCE.IN_SALES_ORDER FROM (
                SELECT 
                    SALES_DWH.SOURCE.IN_SALES_ORDER_SEQ.NEXTVAL,
                    t.$1::TEXT AS order_id,
                    t.$2::TEXT AS customer_name,
                    t.$3::TEXT AS mobile_key,
                    t.$4::NUMBER AS order_quantity,
                    t.$5::NUMBER AS unit_price,
                    t.$6::NUMBER AS order_value,
                    t.$7::TEXT AS promotion_code,
                    t.$8::NUMBER(10,2) AS final_order_amount,
                    t.$9::NUMBER(10,2) AS tax_amount,
                    t.$10::DATE AS order_dt,
                    t.$11::TEXT AS payment_status,
                    t.$12::TEXT AS shipping_status,
                    t.$13::TEXT AS payment_method,
                    t.$14::TEXT AS payment_provider,
                    t.$15::TEXT AS mobile,
                    t.$16::TEXT AS shipping_address,
                    METADATA$FILENAME AS stg_file_name,
                    METADATA$FILE_ROW_NUMBER AS stg_row_number,
                    METADATA$FILE_LAST_MODIFIED AS stg_last_modified
                FROM @SALES_DWH.SOURCE.MY_INTERNAL_STG/csv/sales/source=IN/format=csv/
                (FILE_FORMAT => 'SALES_DWH.COMMON.MY_CSV_FORMAT') t
            )
            ON_ERROR = 'CONTINUE'
        """).collect()
        logging.info("✅ IN sales data ingested successfully.")
    except Exception as e:
        logging.error(f"❌ Failed to ingest IN sales data: {e}")

def ingest_us_sales(session):
    try:
        session.sql("""
            COPY INTO SALES_DWH.SOURCE.US_SALES_ORDER FROM (
                SELECT 
                    SALES_DWH.SOURCE.US_SALES_ORDER_SEQ.NEXTVAL,
                    $1:"Order ID"::TEXT AS order_id,
                    $1:"Customer Name"::TEXT AS customer_name,
                    $1:"Mobile Model"::TEXT AS mobile_key,
                    TO_NUMBER($1:"Quantity") AS quantity,
                    TO_NUMBER($1:"Price per Unit") AS unit_price,
                    TO_DECIMAL($1:"Total Price") AS total_price,
                    $1:"Promotion Code"::TEXT AS promotion_code,
                    $1:"Order Amount"::NUMBER(10,2) AS order_amount,
                    TO_DECIMAL($1:"Tax") AS tax,
                    $1:"Order Date"::DATE AS order_dt,
                    $1:"Payment Status"::TEXT AS payment_status,
                    $1:"Shipping Status"::TEXT AS shipping_status,
                    $1:"Payment Method"::TEXT AS payment_method,
                    $1:"Payment Provider"::TEXT AS payment_provider,
                    $1:"Phone"::TEXT AS phone,
                    $1:"Delivery Address"::TEXT AS shipping_address,
                    METADATA$FILENAME AS stg_file_name,
                    METADATA$FILE_ROW_NUMBER AS stg_row_number,
                    METADATA$FILE_LAST_MODIFIED AS stg_last_modified
                FROM @SALES_DWH.SOURCE.MY_INTERNAL_STG/parquet/sales/source=US/format=parquet/
                (FILE_FORMAT => SALES_DWH.COMMON.MY_PARQUET_FORMAT)
            )
            ON_ERROR = CONTINUE
        """).collect()
        logging.info("✅ US sales data ingested successfully.")
    except Exception as e:
        logging.error(f"❌ Failed to ingest US sales data: {e}")

def ingest_fr_sales(session):
    try:
        session.sql("""
            COPY INTO SALES_DWH.SOURCE.FR_SALES_ORDER FROM (
                SELECT 
                    SALES_DWH.SOURCE.FR_SALES_ORDER_SEQ.NEXTVAL,
                    $1:"Order ID"::TEXT AS order_id,
                    $1:"Customer Name"::TEXT AS customer_name,
                    $1:"Mobile Model"::TEXT AS mobile_key,
                    TO_NUMBER($1:"Quantity") AS quantity,
                    TO_NUMBER($1:"Price per Unit") AS unit_price,
                    TO_DECIMAL($1:"Total Price") AS total_price,
                    $1:"Promotion Code"::TEXT AS promotion_code,
                    $1:"Order Amount"::NUMBER(10,2) AS order_amount,
                    TO_DECIMAL($1:"Tax") AS tax,
                    $1:"Order Date"::DATE AS order_dt,
                    $1:"Payment Status"::TEXT AS payment_status,
                    $1:"Shipping Status"::TEXT AS shipping_status,
                    $1:"Payment Method"::TEXT AS payment_method,
                    $1:"Payment Provider"::TEXT AS payment_provider,
                    $1:"Phone"::TEXT AS phone,
                    $1:"Delivery Address"::TEXT AS shipping_address,
                    METADATA$FILENAME AS stg_file_name,
                    METADATA$FILE_ROW_NUMBER AS stg_row_number,
                    METADATA$FILE_LAST_MODIFIED AS stg_last_modified
                FROM @SALES_DWH.SOURCE.MY_INTERNAL_STG/json/sales/source=FR/format=json/
                (FILE_FORMAT => SALES_DWH.COMMON.MY_JSON_FORMAT)
            )
            ON_ERROR = CONTINUE
        """).collect()
        logging.info("✅ FR sales data ingested successfully.")
    except Exception as e:
        logging.error(f"❌ Failed to ingest FR sales data: {e}")

def main():
    session = None
    try:
        session = get_snowpark_session()
        logging.info("🔗 Snowpark session created.")

        ingest_in_sales(session)
        ingest_us_sales(session)
        ingest_fr_sales(session)

    except Exception as e:
        logging.critical(f"🔥 Critical failure: {e}")
    finally:
        if session:
            session.close()
            logging.info("🔒 Snowpark session closed.")

if __name__ == '__main__':
    main()
