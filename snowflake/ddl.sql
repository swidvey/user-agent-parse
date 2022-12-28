---------------------------------------------------
---------------------------------------------------
-- User Agent Parse Tables

-- Table creation
--      Step 1) Create Data Base and Schema
--      Step 2) Create stage table
--      Step 3) Create main table
--      Step 4) Create data table

-- note: there is no copy statment. 
-- The python program writes into snowflake.
-- Main Tables need to be created first.

-- It is assumed you have a snowflake table with UA data
-- See example on USERSTACK 
---------------------------------------------------
------------         Step 1            ------------
---------------------------------------------------
USE WAREHOUSE LOAD_WH;
CREATE DATABASE USER_DATA;
CREATE SCHEMA USERSTACK;

---------------------------------------------------
------------         Step 2            ------------
---------------------------------------------------

CREATE OR REPLACE TABLE USERSTACK.STAGE.USER_AGENT_MAPPER(
      user_agent                VARCHAR(5000)
    , ua_type                   VARCHAR(100)
    , ua_brand                  VARCHAR(100)
    , ua_name                   VARCHAR(100)
    , ua_url                    VARCHAR(5000)
    , os_name                   VARCHAR(5000)  
    , os_code                   VARCHAR(5000)  
    , os_url                    VARCHAR(5000)  
    , os_family                 VARCHAR(5000)  
    , os_family_code            VARCHAR(5000)
    , os_family_vendor          VARCHAR(5000)    
    , os_icon                   VARCHAR(5000)    
    , os_icon_large             VARCHAR(5000)
    , is_mobile_device          VARCHAR(10)
    , device_type               VARCHAR(50)
    , device_brand              VARCHAR(100)
    , device_brand_code         VARCHAR(50)
    , device_brand_url          VARCHAR(5000)
    , device_name               VARCHAR(50)
    , browser_name              VARCHAR(50)
    , browser_version           VARCHAR(50)
    , browser_version_major     VARCHAR(500)
    , browser_engine            VARCHAR(500)
    , is_crawler                VARCHAR(10)
    , crawler_category          VARCHAR(50)
    , crawler_last_seen         VARCHAR(50)
    , LOAD_TIMESTAMP            DATETIME
);


---------------------------------------------------
------------         Step 3            ------------
---------------------------------------------------
CREATE OR REPLACE TABLE USER_DATA.USERSTACK.USER_AGENT_MAPPER(
      user_agent                VARCHAR(5000)
    , ua_type                   VARCHAR(100)
    , ua_brand                  VARCHAR(100)
    , ua_name                   VARCHAR(100)
    , ua_url                    VARCHAR(5000)
    , os_name                   VARCHAR(5000)  
    , os_code                   VARCHAR(5000)  
    , os_url                    VARCHAR(5000)  
    , os_family                 VARCHAR(5000)  
    , os_family_code            VARCHAR(5000)
    , os_family_vendor          VARCHAR(5000)    
    , os_icon                   VARCHAR(5000)    
    , os_icon_large             VARCHAR(5000)
    , is_mobile_device          VARCHAR(10)
    , device_type               VARCHAR(50)
    , device_brand              VARCHAR(100)
    , device_brand_code         VARCHAR(50)
    , device_brand_url          VARCHAR(5000)
    , device_name               VARCHAR(50)
    , browser_name              VARCHAR(50)
    , browser_version           VARCHAR(50)
    , browser_version_major     VARCHAR(500)
    , browser_engine            VARCHAR(500)
    , is_crawler                VARCHAR(10)
    , crawler_category          VARCHAR(50)
    , crawler_last_seen         VARCHAR(50)
    , LOAD_TIMESTAMP            DATETIME
);
