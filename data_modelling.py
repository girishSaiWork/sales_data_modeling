import sys
import logging
import pandas as pd

from snowflake.snowpark import Session, DataFrame, CaseExpr
from snowflake.snowpark.functions import col,lit,row_number, rank, split,cast, when, expr,min, max,sql_expr, current_timestamp, concat, lit, substring, date_part
from snowflake.snowpark.types import StructType, StringType, StructField, StringType,LongType,DecimalType,DateType,TimestampType
from snowflake.snowpark import Window

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
    return Session.builder.configs(connection_parameters).create()   

# This is a simple dim table having nation and region.
# fields are 'Country','Region'
def create_region_dim(all_sales_df, session) -> None:
    print("\n=== Creating Region Dimension Table ===")
    try:
        # Step 1: Get distinct region-country pairs
        region_dim_df = all_sales_df.select("REGION", "COUNTRY").distinct()
        region_dim_df = region_dim_df.with_column("isActive", lit('Y'))

        # Step 2: Format timestamp into yyyyMMddHHmmss
        timestamp_str = concat(
            date_part("year", current_timestamp()),
            date_part("month", current_timestamp()),
            date_part("day", current_timestamp())
        )

        # Step 3: Build REGION_ID_PK
        region_dim_df = region_dim_df.with_column(
            "REGION_ID_PK",
            concat(
                lit("REG_"),
                substring(col("REGION"), -2, 2),
                lit("_"),
                col("COUNTRY"),
                lit("_"),
                timestamp_str
            )
        )

        region_dim_df = region_dim_df.selectExpr("REGION_ID_PK", "REGION", "COUNTRY", "isActive")
        print("\n▶ New candidate records for Region dimension:")
        region_dim_df.show()
        
        existing_region_dim_df = session.sql("select COUNTRY, REGION from sales_dwh.consumption.region_dim")
        existing_count = int(existing_region_dim_df.count())
        print(f"\n▶ Found {existing_count} existing records in Region dimension")
        if existing_count > 0:
            print("\n▶ Current records in Region dimension:")
            existing_region_dim_df.show()
            
            print("\n▶ Identifying new records through anti-join...")
            region_dim_df = region_dim_df.join(
                existing_region_dim_df,
                [region_dim_df['COUNTRY'] == existing_region_dim_df['COUNTRY'],
                 region_dim_df['REGION'] == existing_region_dim_df['REGION']],
                join_type='leftanti'
            )
            print("\n▶ Records identified for insertion:")
            region_dim_df.show()
        
        insert_cnt = int(region_dim_df.count())
        if insert_cnt > 0:
            region_dim_df.write.save_as_table("sales_dwh.consumption.region_dim", mode="append")
            print(f"\n✓ Successfully inserted {insert_cnt} new records into Region dimension")
        else:
            print("\n○ No new records to insert into Region dimension")
            
    except Exception as e:
        print(f"\n× Failed to create/update Region dimension: {str(e)}")

def create_product_dim(all_sales_df, session) -> None:
    print("\n=== Creating Product Dimension Table ===")
    try:
        product_dim_df = all_sales_df.with_column("Brand", split(col('MOBILE_KEY'), lit('/'))[0]) \
                                   .with_column("Model", split(col('MOBILE_KEY'), lit('/'))[1]) \
                                   .with_column("Color", split(col('MOBILE_KEY'), lit('/'))[2]) \
                                   .with_column("Memory", split(col('MOBILE_KEY'), lit('/'))[3]) \
                                   .select(col('mobile_key'), col('Brand'), col('Model'), col('Color'), col('Memory'))
    
        product_dim_df = product_dim_df.select(
            col('mobile_key'),
            cast(col('Brand'), StringType()).as_("Brand"),
            cast(col('Model'), StringType()).as_("Model"),
            cast(col('Color'), StringType()).as_("Color"),
            cast(col('Memory'), StringType()).as_("Memory")
        )
    
        product_dim_df = product_dim_df.groupBy(col('mobile_key'), col("Brand"), col("Model"), col("Color"), col("Memory")).count()
        product_dim_df = product_dim_df.with_column("isActive", lit('Y'))

        print("\n▶ New candidate records for Product dimension:")
        product_dim_df.show()

        existing_product_dim_df = session.sql("select mobile_key, Brand, Model, Color, Memory from sales_dwh.consumption.product_dim")
        existing_count = int(existing_product_dim_df.count())
        print(f"\n▶ Found {existing_count} existing records in Product dimension")
        
        if existing_count > 0:
            print("\n▶ Current records in Product dimension:")
            existing_product_dim_df.show()
            
            print("\n▶ Identifying new records through anti-join...")
            product_dim_df = product_dim_df.join(
                existing_product_dim_df,
                ["mobile_key", "Brand", "Model", "Color", "Memory"],
                join_type='leftanti'
            )
            print("\n▶ Records identified for insertion:")
            product_dim_df.show()

        product_dim_df = product_dim_df.selectExpr(
            "sales_dwh.consumption.product_dim_seq.nextval as product_id_pk",
            "mobile_key", "Brand", "Model", "Color", "Memory", "isActive"
        )

        insert_cnt = int(product_dim_df.count())
        if insert_cnt > 0:
            product_dim_df.write.save_as_table("sales_dwh.consumption.product_dim", mode="append")
            print(f"\n✓ Successfully inserted {insert_cnt} new records into Product dimension")
        else:
            print("\n○ No new records to insert into Product dimension")
    except Exception as e:
        print(f"\n× Failed to create/update Product dimension: {str(e)}")

def create_promocode_dim(all_sales_df,session)-> None:
    print("\n=== Creating Promo Code Dimension Table ===")
    try:
        promo_code_dim_df = all_sales_df.with_column( "promotion_code", expr("case when promotion_code is null then 'NA' else promotion_code end"))
        promo_code_dim_df = promo_code_dim_df.groupBy(col("promotion_code"),col("country"),col("region")).count()
        promo_code_dim_df = promo_code_dim_df.with_column("isActive",lit('Y'))

        #fetch existing product dim records.
        existing_promo_code_dim_df = session.sql("select promotion_code, country, region from sales_dwh.consumption.promo_code_dim")

        promo_code_dim_df = promo_code_dim_df.join(existing_promo_code_dim_df,["promotion_code", "country", "region"],join_type='leftanti')

        promo_code_dim_df = promo_code_dim_df.selectExpr("sales_dwh.consumption.promo_code_dim_seq.nextval as promo_code_id_pk","promotion_code", "country","region","isActive") 


        intsert_cnt = int(promo_code_dim_df.count())
        if intsert_cnt>0:
            promo_code_dim_df.write.save_as_table("sales_dwh.consumption.promo_code_dim",mode="append")
            print(f"✓ Successfully inserted {intsert_cnt} new records into Promo Code dimension")
        else:
            print("× No new records to insert into Promo Code dimension")
    except Exception as e:
        print(f"× Failed to create/update Promo Code dimension: {str(e)}")
    
def create_customer_dim(all_sales_df, session) -> None:
    print("\n=== Creating Customer Dimension Table ===")
    try:
        customer_dim_df = all_sales_df.groupBy(col("COUNTRY"),col("REGION"),col("CUSTOMER_NAME"),col("CONCTACT_NO"),col("SHIPPING_ADDRESS")).count()
        customer_dim_df = customer_dim_df.with_column("isActive",lit('Y'))
        customer_dim_df = customer_dim_df.selectExpr("customer_name", "conctact_no","shipping_address","country","region" ,"isactive")
        
        existing_customer_dim_df = session.sql("select customer_name,conctact_no,shipping_address,country, region from sales_dwh.consumption.customer_dim")

        customer_dim_df = customer_dim_df.join(existing_customer_dim_df,["customer_name","conctact_no","shipping_address","country", "region"],join_type='leftanti')

        customer_dim_df = customer_dim_df.selectExpr("sales_dwh.consumption.customer_dim_seq.nextval as customer_id_pk","customer_name", "conctact_no","shipping_address","country","region", "isActive") 

        intsert_cnt = int(customer_dim_df.count())
        if intsert_cnt>0:
            customer_dim_df.write.save_as_table("sales_dwh.consumption.customer_dim",mode="append")
            print(f"✓ Successfully inserted {intsert_cnt} new records into Customer dimension")
        else:
            print("× No new records to insert into Customer dimension")
    except Exception as e:
        print(f"× Failed to create/update Customer dimension: {str(e)}")
    
def create_payment_dim(all_sales_df, session) -> None:
    print("\n=== Creating Payment Dimension Table ===")
    try:
        payment_dim_df = all_sales_df.groupBy(col("COUNTRY"),col("REGION"),col("payment_method"),col("payment_provider")).count()
        payment_dim_df = payment_dim_df.with_column("isActive",lit('Y'))
        
        existing_payment_dim_df = session.sql("select payment_method,payment_provider,country, region from sales_dwh.consumption.payment_dim")

        payment_dim_df = payment_dim_df.join(existing_payment_dim_df,
                                             ["payment_method","payment_provider","country", "region"],
                                             join_type='leftanti')

        payment_dim_df = payment_dim_df.selectExpr("sales_dwh.consumption.payment_dim_seq.nextval as payment_id_pk","payment_method", "payment_provider","country","region", "isActive") 


        intsert_cnt = int(payment_dim_df.count())
        if intsert_cnt>0:
            payment_dim_df.write.save_as_table("sales_dwh.consumption.payment_dim",mode="append")
            print(f"✓ Successfully inserted {intsert_cnt} new records into Payment dimension")
        else:
            print("× No new records to insert into Payment dimension")
    except Exception as e:
        print(f"× Failed to create/update Payment dimension: {str(e)}")

def create_date_dim(all_sales_df, session) -> None:
    print("\n=== Creating Date Dimension Table ===")
    try:
        start_date = all_sales_df.select(min("order_dt").alias("min_order_dt")).collect()[0].as_dict()['MIN_ORDER_DT']
        end_date = all_sales_df.select(max("order_dt").alias("max_order_dt")).collect()[0].as_dict()['MAX_ORDER_DT']
        date_range = pd.date_range(start=start_date, end=end_date, freq='D')
        #print(date_range)
        date_dim = pd.DataFrame()
        date_dim['order_dt'] = date_range.date
        date_dim['Year'] = date_range.year
        # Calculate day counter
        start_day_of_year = pd.to_datetime(start_date).dayofyear
        date_dim['DayCounter'] = date_range.dayofyear - start_day_of_year + 1

        date_dim['Month'] = date_range.month
        date_dim['Quarter'] = date_range.quarter
        date_dim['Day'] = date_range.day
        date_dim['DayOfWeek'] = date_range.dayofweek
        date_dim['DayName'] = date_range.strftime('%A')
        date_dim['DayOfMonth'] = date_range.day
        date_dim['Weekday'] = date_dim['DayOfWeek'].map({0: 'Weekday', 1: 'Weekday', 2: 'Weekday', 3: 'Weekday', 4: 'Weekday', 5: 'Weekend', 6: 'Weekend'})


        date_dim_df = session.create_dataframe(date_dim)

        existing_date_dim_df = session.sql("select order_dt from sales_dwh.consumption.date_dim ") 
        date_dim_df = date_dim_df.join(existing_date_dim_df,existing_date_dim_df['order_dt']==date_dim_df['"order_dt"'],join_type='leftanti')

        date_dim_df = date_dim_df.selectExpr(' \
                                           sales_dwh.consumption.date_dim_seq.nextval, \
                                           "order_dt" as order_dt, \
                                           "Year" as order_year, \
                                           "Month" as order_month, \
                                           "Quarter" as order_quarter, \
                                           "Day" as order_day, \
                                           "DayOfWeek" as order_dayofweek, \
                                           "DayName" as order_dayname, \
                                           "DayOfMonth" as order_dayofmonth, \
                                           "Weekday" as order_weekday,\
                                           "DayCounter" as day_counter\
                                           ')

        intsert_cnt = int(date_dim_df.count())
        if intsert_cnt>0:
            date_dim_df.write.save_as_table("sales_dwh.consumption.date_dim",mode="append")
            print(f"✓ Successfully inserted {intsert_cnt} new records into Date dimension")
        else:
            print("× No new records to insert into Date dimension")
    except Exception as e:
        print(f"× Failed to create/update Date dimension: {str(e)}")

def main():
    print("\n=== Starting Data Modeling Process ===")
    try:
        #get the session object and get dataframe
        session = get_snowpark_session()
        print("✓ Successfully connected to Snowflake")

        # Create sequence if it doesn't exist
        session.sql("CREATE SEQUENCE IF NOT EXISTS sales_dwh.consumption.sales_fact_seq START = 1 INCREMENT = 1").collect()
        print("✓ Successfully created/verified sales_fact_seq")

        print("\n=== Loading Source Data ===")
        in_sales_df = session.sql("select * from sales_dwh.curated.in_sales_order")
        us_sales_df = session.sql("select * from sales_dwh.curated.us_sales_order")
        fr_sales_df = session.sql("select * from sales_dwh.curated.fr_sales_order")
        print("✓ Successfully loaded source data")

        all_sales_df = in_sales_df.union(us_sales_df).union(fr_sales_df)

        # Create dimensions
        create_date_dim(all_sales_df,session)
        create_region_dim(all_sales_df,session)
        create_product_dim(all_sales_df,session)
        create_promocode_dim(all_sales_df,session)
        create_customer_dim(all_sales_df,session)
        create_payment_dim(all_sales_df,session)

        print("\n=== Creating Sales Fact Table ===")
        try:
            date_dim_df = session.sql("select date_id_pk, order_dt from sales_dwh.consumption.date_dim")
            customer_dim_df = session.sql("select customer_id_pk, customer_name, country, region from sales_dwh.consumption.CUSTOMER_DIM")
            payment_dim_df = session.sql("select payment_id_pk, payment_method, payment_provider, country, region from sales_dwh.consumption.PAYMENT_DIM")
            product_dim_df = session.sql("select product_id_pk, mobile_key from sales_dwh.consumption.PRODUCT_DIM")
            promo_code_dim_df = session.sql("select promo_code_id_pk,promotion_code,country, region from sales_dwh.consumption.PROMO_CODE_DIM")
            region_dim_df = session.sql("select region_id_pk,country, region from sales_dwh.consumption.REGION_DIM")

            all_sales_df = all_sales_df.with_column( "promotion_code", expr("case when promotion_code is null then 'NA' else promotion_code end"))
            all_sales_df = all_sales_df.join(date_dim_df, ["order_dt"],join_type='inner')
            all_sales_df = all_sales_df.join(customer_dim_df, ["customer_name","region","country"],join_type='inner')
            all_sales_df = all_sales_df.join(payment_dim_df, ["payment_method", "payment_provider", "country", "region"],join_type='inner')
            #all_sales_df = all_sales_df.join(product_dim_df, ["brand","model","color","Memory"],join_type='inner')
            all_sales_df = all_sales_df.join(product_dim_df, ["mobile_key"],join_type='inner')
            all_sales_df = all_sales_df.join(promo_code_dim_df, ["promotion_code","country", "region"],join_type='inner')
            all_sales_df = all_sales_df.join(region_dim_df, ["country", "region"],join_type='inner')
            all_sales_df = all_sales_df.selectExpr("sales_dwh.consumption.sales_fact_seq.nextval as order_id_pk, \
                                                   order_id as order_code,                               \
                                                   date_id_pk as date_id_fk,          \
                                                   region_id_pk as region_id_fk,            \
                                                   customer_id_pk as customer_id_fk,        \
                                                   payment_id_pk as payment_id_fk,          \
                                                   product_id_pk as product_id_fk,          \
                                                   promo_code_id_pk as promo_code_id_fk,    \
                                                   order_quantity,                          \
                                                   local_total_order_amt,                   \
                                                   local_tax_amt,                           \
                                                   exhchange_rate,                          \
                                                   us_total_order_amt,                      \
                                                   usd_tax_amt                              \
                                                   ")
            all_sales_df.write.save_as_table("sales_dwh.consumption.sales_fact",mode="append")
            print("✓ Successfully created Sales Fact table")
        except Exception as e:
            print(f"× Failed to create Sales Fact table: {str(e)}")

        print("\n=== Data Modeling Process Completed ===")
    except Exception as e:
        print(f"\n× Data Modeling Process Failed: {str(e)}")

if __name__ == '__main__':
    main()
