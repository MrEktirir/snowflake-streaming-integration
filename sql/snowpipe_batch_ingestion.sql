CREATE OR REPLACE DATABASE hol_streaming;

USE DATABASE hol_streaming;

CREATE OR REPLACE WAREHOUSE hol_streaming_wh WITH WAREHOUSE_SIZE='XSMALL' MIN_CLUSTER_COUNT = 1 MAX_CLUSTER_COUNT=1 AUTO_SUSPEND=60;


CREATE OR REPLACE TABLE stg_customer (
    raw_json        VARIANT,
    file_name       STRING NOT NULL,
    file_row_seq    NUMBER NOT NULL,
    ldts            STRING NOT NULL
);

CREATE OR REPLACE TABLE stg_orders(
    o_orderkey      NUMBER,
    o_custkey       NUMBER,
    o_orderstatus   STRING,
    o_totalprice    NUMBER,
    o_orderdate     DATE,
    o_orderpriority STRING,
    o_clerk         STRING,
    o_shippriority  NUMBER,
    o_comment       STRING,
    filename        STRING  NOT NULL,
    file_row_seq    NUMBER  NOT NULL,
    ldts            STRING  NOT NULL
)

CREATE OR REPLACE STAGE customer_data FILE_FORMAT = (TYPE = JSON);
CREATE OR REPLACE STAGE orders_data   FILE_FORMAT = (TYPE = CSV);


COPY INTO @customer_data 
FROM
(SELECT object_construct(*)
  FROM snowflake_sample_data.tpch_sf10.customer limit 10
) 
INCLUDE_QUERY_ID=TRUE;

COPY INTO @orders_data 
FROM
(SELECT *
  FROM snowflake_sample_data.tpch_sf10.orders limit 1000
) 
INCLUDE_QUERY_ID=TRUE;

list@customer_data;
SELECT METADATA$FILENAME,$1 FROM @customer_data;

CREATE OR REPLACE PIPE stg_orders_pp 
AS 
COPY INTO stg_orders 
FROM
(
SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9 
     , metadata$filename
     , metadata$file_row_number
     , CURRENT_TIMESTAMP()
  FROM @orders_data
);

CREATE OR REPLACE PIPE stg_customer_pp 
AS 
COPY INTO stg_customer
FROM 
(
SELECT $1
     , metadata$filename
     , metadata$file_row_number
     , CURRENT_TIMESTAMP()
  FROM @customer_data
);

ALTER PIPE stg_customer_pp REFRESH;

ALTER PIPE stg_orders_pp   REFRESH;

SELECT 'stg_customer', count(1) FROM stg_customer
UNION ALL
SELECT 'stg_orders', count(1) FROM stg_orders;

select pipe_received_time, last_load_time,src.*
from table(information_schema.copy_history(table_name=>'STG_CUSTOMER', start_time=> dateadd(hours, -1, current_timestamp()))) src;

select *
  from table(information_schema.pipe_usage_history(
    date_range_start=>dateadd('hour',-12,current_timestamp()),
    pipe_name=>'STG_CUSTOMER_PP'))
union all
select *
  from table(information_schema.pipe_usage_history(
    date_range_start=>dateadd('hour',-12,current_timestamp()),
    pipe_name=>'STG_ORDERS_PP'));


-- COPY COMMAND -- 

CREATE OR REPLACE DATABASE hol_streaming;

CREATE OR REPLACE TABLE stg_customer
(
    raw_json            VARIANT
, filename            STRING   NOT NULL
, file_row_seq        NUMBER   NOT NULL
, ldts                STRING   NOT NULL
);

-- Hedef tablo sütunlarını büyük harfle (Snowflake standartlarına uygun) tanımlıyoruz
CREATE OR REPLACE TABLE customer_target 
( 
  C_ACCTBAL         NUMBER
, C_NAME              STRING
, C_MKTSEGMENT        STRING
);

CREATE OR REPLACE STAGE customer_data 
FILE_FORMAT = (TYPE = JSON) 
DIRECTORY = (ENABLE = TRUE);

CREATE OR REPLACE STREAM customer_data_files_stream ON STAGE customer_data;


-- =====================================================================
-- BÖLÜM 2: ÖRNEK VERİYİ STAGE ALANINA YÜKLEME (COPY INTO STAGE)
-- =====================================================================

COPY INTO @customer_data 
FROM
(
  SELECT OBJECT_CONSTRUCT(*)
  FROM snowflake_sample_data.tpch_sf10.customer
) 
INCLUDE_QUERY_ID = TRUE;




BEGIN TRANSACTION;

-- 1. Adım: Stage'deki ham verileri stg_customer tablosuna çek
COPY INTO stg_customer
FROM 
(
  SELECT $1
       , METADATA$FILENAME
       , METADATA$FILE_ROW_NUMBER
       , CURRENT_TIMESTAMP()
  FROM @customer_data
);

-- 2. Adım: Ham verileri parse edip hedef tabloya MERGE et
MERGE INTO customer_target AS t
USING 
(
  SELECT raw_json:C_ACCTBAL::number    AS c_acctbal
       , raw_json:C_NAME::string       AS c_name
       , raw_json:C_MKTSEGMENT::string AS c_mktsegment
  FROM stg_customer
) AS s
ON t.C_NAME = s.c_name 
   AND t.C_MKTSEGMENT = s.c_mktsegment
WHEN MATCHED THEN 
    UPDATE SET t.C_ACCTBAL = s.c_acctbal
WHEN NOT MATCHED THEN 
    INSERT (C_ACCTBAL, C_NAME, C_MKTSEGMENT) 
    VALUES (s.c_acctbal, s.c_name, s.c_mktsegment)
;

COMMIT;



--  Automation using Snowflake tasks

ALTER STAGE customer_data REFRESH;
SELECT * FROM DIRECTORY(@customer_data);
SELECT * FROM customer_data_files_stream;

CREATE OR REPLACE TASK tsk_customer_data_load
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    SCHEDULE = '1 minute'
WHEN
    SYSTEM$STREAM_HAS_DATA('CUSTOMER_DATA_FILES_STREAM')
AS
COPY INTO stg_customer
FROM
(
SELECT $1
    , metadata$filename
    , metadata$file_row_number
    , CURRENT_TIMESTAMP()
    FROM @customer_data
);

ALTER TASK tsk_customer_data_load RESUME;
EXECUTE TASK tsk_customer_data_load;

-- Observability --
USE DATABASE HOL_STREAMING;
USE SCHEMA PUBLIC;
SHOW TASKS;

SELECT *
  FROM TABLE(information_schema.serverless_task_history(
    date_range_start=>dateadd(d, -7, current_date),
    date_range_end=>current_date,
    task_name=>'TSK_CUSTOMER_DATA_LOAD'));
