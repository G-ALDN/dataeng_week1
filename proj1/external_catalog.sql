-- ============================================================
-- Snowflake -> Amazon S3 using Apache Iceberg
-- External Catalog: AWS Glue
-- Student Demo
-- ============================================================

---------------------------------------------------------------
-- 1. Create Warehouse
---------------------------------------------------------------

CREATE OR REPLACE WAREHOUSE demo_wh
    WAREHOUSE_SIZE = XSMALL
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

USE WAREHOUSE demo_wh;

---------------------------------------------------------------
-- 2. Create Database
---------------------------------------------------------------

CREATE OR REPLACE DATABASE iceberg_demo;

USE DATABASE iceberg_demo;

---------------------------------------------------------------
-- 3. Create Schema
---------------------------------------------------------------

CREATE OR REPLACE SCHEMA demo;

USE SCHEMA demo;

SELECT CURRENT_VERSION();
SELECT CURRENT_REGION();
---------------------------------------------------------------
-- 4. Create External Volume + AWS Role: SnowflakeIceberg
---------------------------------------------------------------
-- Create a new AWS role in IAM called 'SnowflakeIceBerg'
-- under trusted entities use (we will change later, use your own root account ID):
-- {
--   "Version": "2012-10-17",
--   "Statement": [
--     {
--       "Effect": "Allow",
--       "Principal": {
--         "AWS": "arn:aws:iam::454497087304:root"
--       },
--       "Action": "sts:AssumeRole"
--     }
--   ]
-- }
-- for the permissions, create one inline and point to your own bucket:
-- {
-- 	"Version": "2012-10-17",
-- 	"Statement": [
-- 		{
-- 			"Effect": "Allow",
-- 			"Action": [
-- 				"s3:GetBucketLocation",
-- 				"s3:ListBucket"
-- 			],
-- 			"Resource": "arn:aws:s3:::spark-rev-571600835123-us-east-2-an"
-- 		},
-- 		{
-- 			"Effect": "Allow",
-- 			"Action": [
-- 				"s3:GetObject",
-- 				"s3:PutObject",
-- 				"s3:DeleteObject"
-- 			],
-- 			"Resource": "arn:aws:s3:::spark-rev-571600835123-us-east-2-an/iceberg/*"
-- 		}
-- 	]
-- }

CREATE OR REPLACE EXTERNAL VOLUME my_external_volume
STORAGE_LOCATIONS =
(
    (
        NAME='s3_location'
        STORAGE_PROVIDER='S3'
        STORAGE_BASE_URL='s3://spark-rev-571600835123-us-east-2-an/iceberg/'
        STORAGE_AWS_ROLE_ARN='arn:aws:iam::571600835123:role/SnowflakeIceberg'
    )
)
ALLOW_WRITES = TRUE;

-- get the STORAGE_AWS_IAM_USER_ARN & STORAGE_AWS_EXTERNAL_ID
-- we will use these to update our trust policy in the role
-- this would be a best practice in prod
-- we will have to create a role with a generic trust policy first, 
-- then we can update with the information from this describe call 
DESC EXTERNAL VOLUME my_external_volume;

-- trusted entities for the SnowflakeIceberg role is updated with the new information for best security:
-- {
--     "Version": "2012-10-17",
--     "Statement": [
--         {
--             "Effect": "Allow",
--             "Principal": {
--                 "AWS": "arn:aws:iam::006116051983:user/fbkz1000-s"
--             },
--             "Action": "sts:AssumeRole",
--             "Condition": {
--                 "StringEquals": {
--                     "sts:ExternalId": "XV79844_SFCRole=4_fkvzwu+OoTfEarkds9ybFJTNxzM="
--                 }
--             }
--         }
--     ]
-- }


---------------------------------------------------------------
-- 5. Create Glue Catalog Integration + AWS Role: SnowflakeGlueRole
---------------------------------------------------------------

-- Create a new AWS role in IAM called 'SnowflakeGlueRole'
-- under trusted entities use (we will change later, use your own root account ID):
-- {
--   "Version": "2012-10-17",
--   "Statement": [
--     {
--       "Effect": "Allow",
--       "Principal": {
--         "AWS": "arn:aws:iam::454497087304:root"
--       },
--       "Action": "sts:AssumeRole"
--     }
--   ]
-- }
-- for permissions, create an inline policy:
-- {
-- 	"Version": "2012-10-17",
-- 	"Statement": [
-- 		{
-- 			"Effect": "Allow",
-- 			"Action": [
-- 				"glue:GetDatabase",
-- 				"glue:GetDatabases",
-- 				"glue:CreateDatabase",
-- 				"glue:GetTable",
-- 				"glue:GetTables",
-- 				"glue:CreateTable",
-- 				"glue:UpdateTable",
-- 				"glue:DeleteTable"
-- 			],
-- 			"Resource": "*"
-- 		}
-- 	]
-- }

CREATE OR REPLACE CATALOG INTEGRATION my_catalog
CATALOG_SOURCE = GLUE
TABLE_FORMAT = ICEBERG
CATALOG_NAMESPACE = 'iceberg_catalog_db'
GLUE_AWS_ROLE_ARN = 'arn:aws:iam::571600835123:role/SnowflakeGlueRole'
GLUE_CATALOG_ID = '571600835123'
GLUE_REGION = 'us-east-2'
ENABLED = TRUE;

-- much like with our external volume, we will get values 
-- from this so that we can update our trust policy in our relevant AWS role 
-- output like this will be used in our trust policy:
-- GLUE_AWS_IAM_USER_ARN	String	arn:aws:iam::389656351827:user/6yqy1000-s
-- GLUE_AWS_EXTERNAL_ID	String	YO33549_SFCRole=4_Vdh7RHz9mLExrFBUECCfdyoj1aI=
DESC CATALOG INTEGRATION my_catalog;
-- update the trust policy on the SnowflakeGlueRole with the values from our describe call:
-- {
--     "Version": "2012-10-17",
--     "Statement": [
--         {
--             "Effect": "Allow",
--             "Principal": {
--                 "AWS": "arn:aws:iam::006116051983:user/fbkz1000-s"
--             },
--             "Action": "sts:AssumeRole",
--             "Condition": {
--                 "StringEquals": {
--                     "sts:ExternalId": "XV79844_SFCRole=4_hw6j5WQwIDObfvngh9lhh8552T4="
--                 }
--             }
--         }
--     ]
-- }

---------------------------------------------------------------
-- 6. Create Iceberg Table
---------------------------------------------------------------
SELECT SYSTEM$VERIFY_EXTERNAL_VOLUME('my_external_volume');
CREATE OR REPLACE ICEBERG TABLE customers
    EXTERNAL_VOLUME = 'my_external_volume'
    CATALOG = 'my_catalog'
    CATALOG_TABLE_NAME = 'customers';
CREATE OR REPLACE ICEBERG TABLE products
    EXTERNAL_VOLUME = 'my_external_volume'
    CATALOG = 'my_catalog'
    CATALOG_TABLE_NAME = 'products';
CREATE OR REPLACE ICEBERG TABLE orders
    EXTERNAL_VOLUME = 'my_external_volume'
    CATALOG = 'my_catalog'
    CATALOG_TABLE_NAME = 'orders';


---------------------------------------------------------------
-- 7. Query Data
---------------------------------------------------------------

SELECT *
FROM customers;
SELECT *
FROM products;
SELECT *
FROM orders;



---------------------------------------------------------------
-- 8. Verify Metadata
---------------------------------------------------------------

SHOW ICEBERG TABLES;

DESCRIBE ICEBERG TABLE customers;

SELECT COUNT(*)
FROM orders;

---------------------------------------------------------------
-- 9. Cleanup
---------------------------------------------------------------

DROP TABLE titanic;
DROP SCHEMA demo;
DROP DATABASE iceberg_demo;
DROP EXTERNAL VOLUME my_external_volume;
DROP CATALOG INTEGRATION my_catalog;