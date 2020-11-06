------------------------ CREDENTIAL ------------------------
CREATE DATABASE SCOPED CREDENTIAL MaxPylypovychAzureStorageCredential
WITH
  IDENTITY = 'SHARED ACCESS SIGNATURE',
  SECRET = 'REMOVED';
GO

------------------------ EXTERNAL DATA SOURCE ------------------------
CREATE EXTERNAL DATA SOURCE MaxPylypovychAzureStorage
WITH
  ( LOCATION = 'wasbs://homework-2-11-6-11@maxpylypovychstr.blob.core.windows.net/' ,
    CREDENTIAL = MaxPylypovychAzureStorageCredential ,
    TYPE = HADOOP
  );

------------------------ FILE FORMAT ------------------------
 CREATE EXTERNAL FILE FORMAT MaxPylypovychTextFileFormat
 WITH
 (
     FORMAT_TYPE = DELIMITEDTEXT
     , FORMAT_OPTIONS ( FIELD_TERMINATOR = ','
        , STRING_DELIMITER = ''
       , DATE_FORMAT = 'yyyy-MM-dd HH:mm:ss'
       , USE_TYPE_DEFAULT = FALSE
       , First_Row = 2
       )
 )
------------------------ EXTERNAL TABLE ------------------------
CREATE EXTERNAL TABLE [max_pylypovych_schema].[external]
( [VendorID] [int] NULL,
  [tpep_pickup_datetime] DATETIME NULL,
  [tpep_dropoff_datetime] DATETIME NULL,
  [passenger_count] [int] NULL,
  [trip_distance] [float]  NULL,
  [RatecodeID] [int] NULL,
  [store_and_fwd_flag] char(1) NULL,
  [PULocationID] [int]  NULL,
  [DOLocationID] [int]  NULL,
  [payment_type] [int]  NULL,
  [fare_amount] [float]  NULL,
  [extra] [float] NULL,
  [mta_tax] [float]  NULL,
  [tip_amount] [float]  NULL,
  [tolls_amount] [float]  NULL,
  [improvement_surcharge] [float]  NULL,
  [total_amount] [float]  NULL,
  [congestion_surcharge] [float]  NULL )
WITH
(
    LOCATION='/yellow_tripdata_2020-01.csv' ,
    DATA_SOURCE = MaxPylypovychAzureStorage ,
    FILE_FORMAT = MaxPylypovychTextFileFormat ,
    REJECT_TYPE = VALUE ,
    REJECT_VALUE = 0
) ;
GO


------------------------ fact_tripdata ------------------------
CREATE TABLE [max_pylypovych_schema].[fact_tripdata]
WITH
(
	DISTRIBUTION = HASH ( [tpep_pickup_datetime] )
)
AS (SELECT * FROM [max_pylypovych_schema].[external])
GO

------------------------ payment_type ------------------------
CREATE TABLE [max_pylypovych_schema].[Payment_type]
WITH
(
	DISTRIBUTION = REPLICATE
)
AS (SELECT DISTINCT [payment_type] as [ID],
    CASE
        WHEN [payment_type] = 1 THEN 'Credit card'
        WHEN [payment_type] = 2 THEN 'Cash'
        WHEN [payment_type] = 3 THEN 'No charge'
        WHEN [payment_type] = 4 THEN 'Dispute'
        WHEN [payment_type] = 5 THEN 'Unknown'
        WHEN [payment_type] = 6 THEN 'Voided trip'
    END AS [Name]
FROM [max_pylypovych_schema].[fact_tripdata]
    -- ORDER BY [id]
    WHERE [payment_type] IS NOT NULL
)
GO


------------------------ rate_code ------------------------
CREATE TABLE [max_pylypovych_schema].[RateCode]
WITH
(
	DISTRIBUTION = REPLICATE
)
AS (SELECT DISTINCT [RatecodeID] as [ID],
    CASE
        WHEN [RatecodeID] = 1 THEN 'Standard rate'
        WHEN [RatecodeID] = 2 THEN 'JFK'
        WHEN [RatecodeID] = 3 THEN 'Newark'
        WHEN [RatecodeID] = 4 THEN 'Nassau or Westchester'
        WHEN [RatecodeID] = 5 THEN 'Negotiated fare'
        WHEN [RatecodeID] = 6 THEN 'Group ride'
        ELSE 'undefined'
    END AS [Name]
FROM [max_pylypovych_schema].[fact_tripdata]
    -- ORDER BY [RatecodeID]
    WHERE [RatecodeID] IS NOT NULL
)
GO


------------------------ vendors ------------------------
CREATE TABLE [max_pylypovych_schema].[Vendor]
WITH
(
	DISTRIBUTION = REPLICATE
)
AS (SELECT DISTINCT [VendorID] as [ID],
    CASE
        WHEN [VendorID] = 1 THEN 'Creative Mobile Technologies'
        WHEN [VendorID] = 2 THEN 'VeriFone Inc.'
        ELSE 'undefined'
    END AS [Name]
FROM [max_pylypovych_schema].[fact_tripdata]
    -- ORDER BY [VendorID]
    WHERE [VendorID] IS NOT NULL
)
