
-- to Do: ADD VARCHAR size. These are max value

---------------------------------------------------
---------------------------------------------------
-- User Agent Parse Tables

-- Table creation
--      Step 1) Create stage table
--      Step 2) Create main table

-- note: there is no copy statment. 
-- The python program writes into snowflake.
-- Main Tables need to be created first 
---------------------------------------------------
------------         Step 1            ------------
---------------------------------------------------

USE WAREHOUSE LOAD_WH;

CREATE OR REPLACE TABLE USER_DATA.STAGE.USER_AGENT_MAPPER(
      user_agent                VARCHAR()
    , ua_type                   VARCHAR()
    , ua_brand                  VARCHAR()
    , ua_name                   VARCHAR()
    , ua_url                    VARCHAR()
    , os_name                   VARCHAR()  
    , os_code                   VARCHAR()  
    , os_url                    VARCHAR()  
    , os_family                 VARCHAR()  
    , os_family_code            VARCHAR()
    , os_family_vendor          VARCHAR()    
    , os_icon                   VARCHAR()    
    , os_icon_large             VARCHAR()
    , is_mobile_device          VARCHAR()
    , device_type               VARCHAR()
    , device_brand              VARCHAR()
    , device_brand_code         VARCHAR()
    , device_brand_url          VARCHAR()
    , device_name               VARCHAR()
    , browser_name              VARCHAR()
    , browser_version           VARCHAR()
    , browser_version_major     VARCHAR()
    , browser_engine            VARCHAR()
    , is_crawler                VARCHAR()
    , crawler_category          VARCHAR()
    , crawler_last_seen         VARCHAR()
    , LOAD_TIMESTAMP            VARCHAR()
);


---------------------------------------------------
------------         Step 2            ------------
---------------------------------------------------
CREATE OR REPLACE TABLE USER_DATA.PUBLIC.USER_AGENT_MAPPER(
      user_agent                VARCHAR()
    , ua_type                   VARCHAR()
    , ua_brand                  VARCHAR()
    , ua_name                   VARCHAR()
    , ua_url                    VARCHAR()
    , os_name                   VARCHAR()  
    , os_code                   VARCHAR()  
    , os_url                    VARCHAR()  
    , os_family                 VARCHAR()  
    , os_family_code            VARCHAR()
    , os_family_vendor          VARCHAR()    
    , os_icon                   VARCHAR()    
    , os_icon_large             VARCHAR()
    , is_mobile_device          VARCHAR()
    , device_type               VARCHAR()
    , device_brand              VARCHAR()
    , device_brand_code         VARCHAR()
    , device_brand_url          VARCHAR()
    , device_name               VARCHAR()
    , browser_name              VARCHAR()
    , browser_version           VARCHAR()
    , browser_version_major     VARCHAR()
    , browser_engine            VARCHAR()
    , is_crawler                VARCHAR()
    , crawler_category          VARCHAR()
    , crawler_last_seen         VARCHAR()
    , LOAD_TIMESTAMP            VARCHAR() 
);
	