-- creating internal stage within source schema.
use schema source;
create or replace stage my_internal_stg;

-- following put command can be executed
/*
-- csv example
put file:///tmp/sales/source=IN/format=csv/date=2022-02-22/order-20220222.csv @sales_dwh.source.my_internal_stg/sales/source=IN/format=csv/date=2022-02-22 auto_compress=False overwrite=True, parallel=3 ;
put file:///tmp/sales/source=IN/format=csv/date=2021-04-26/order-20210426.csv @sales_dwh.source.my_internal_stg/sales/source=IN/format=csv/date=2021-04-26 auto_compress=False overwrite=True, parallel=3 ;

-- json example
put file:///tmp/sales/source=FR/format=json/date=2022-02-22/order-20220222.json @sales_dwh.source.my_internal_stg/sales/source=FR/format=json/date=2022-02-22 auto_compress=False overwrite=True, parallel=3 ;
put file:///tmp/sales/source=FR/format=json/date=2021-04-26/order-20210426.json @sales_dwh.source.my_internal_stg/sales/source=FR/format=json/date=2021-04-26 auto_compress=False overwrite=True, parallel=3 ;

-- parquet example
put file:///tmp/sales/source=US/format=parquet/date=2022-02-22/order-20220222.snappy.parquet @sales_dwh.source.my_internal_stg/sales/source=US/format=parquet/date=2022-02-22 auto_compress=False overwrite=True, parallel=3 ;
put file:///tmp/sales/source=US/format=parquet/date=2021-04-26/order-20210426.snappy.parquet @sales_dwh.source.my_internal_stg/sales/source=US/format=parquet/date=2021-04-26 auto_compress=False overwrite=True, parallel=3 ;
*/