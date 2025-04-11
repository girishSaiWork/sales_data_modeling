use role sysadmin;

use warehouse compute_wh;

create database if not exists cricket;

create or replace schema cricket.land;
create or replace schema cricket.raw;
create or replace schema cricket.clean;
create or replace schema cricket.consumption;


show schemas in database cricket;

use schema cricket.land;

-- json file format
create or replace file format my_json_format
 type = json
 null_if = ('\\n', 'null', '')
    strip_outer_array = true
    comment = 'Json File Format with outer stip array flag true'; 

-- creating an internal stage
create or replace stage my_stg; 

-- lets list the internal stage
list @my_stg;

-- check if data is being loaded or not
list @my_stg/cricket/json/;