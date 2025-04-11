from snowflake.snowpark import Session
import sys
import logging
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# initiate logging at info level
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%I:%M:%S')

# snowpark session
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
    # creating snowflake session object
    snowpark_session = Session.builder.configs(connection_parameters).create()
    return snowpark_session

def main():
    session = get_snowpark_session()

    context_df = session.sql("select current_role(), current_database(), current_schema(), current_warehouse()")
    context_df.show(2)

    customer_df = session.sql("select c_custkey,c_name,c_phone,c_mktsegment from snowflake_sample_data.tpch_sf1.customer limit 10")
    customer_df.show(5)

if __name__ == '__main__':
    main()