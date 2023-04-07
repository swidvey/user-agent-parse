---------------------------------------------------
---------------------------------------------------
-- User Agent Parse Tables

-- Table creation
--      Step 1) Create Data Base and Schema
--      Step 2) Create stage table
--      Step 3) Create main table
--      Step 4) Create user data table with UA to parse
--      Step 4) Create table View after program runs

-- note: there is no copy statment. 
-- The python program writes into snowflake.
-- Main Tables need to be created first.
--
-- NOTE: user data table acts regualar updated user data

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

---------------------------------------------------
------------         Step 4            ------------
---------------------------------------------------
CREATE OR REPLACE TABLE USER_DATA.USER_INFO.USER_INFORMATION(
      user_name                 VARCHAR(5000)
    , email_user                VARCHAR(100)
    , age_user                  VARCHAR(100)
    , user_agent                VARCHAR(5000)
);

---------------------------------------------------
------------         Step 5            ------------
---------------------------------------------------
CREATE OR REPLACE VIEW USER_DATA.USERSTACK.USER_AGENT_MAPPER_VW                          
comment='USER AGENT MASTER Mapper' as                                         
SELECT                                                                        
    ua_json:ua::string                               as user_agent                
    ,ua_json:type::string                             as ua_type                  
    ,ua_json:brand::string                            as ua_brand                 
    ,ua_json:name::string                             as ua_name                  
    ,ua_json:url::string                              as ua_url                   
    ,ua_json:os:name::string                          as os_name                  
    ,ua_json:os:code::string                          as os_code                  
    ,ua_json:os:url::string                           as os_url                   
    ,ua_json:os:family::string                        as os_family                
    ,ua_json:os:family_code::string                   as os_family_code           
    ,ua_json:os:family_vendor::string                 as os_family_vendor        
    ,ua_json:os:icon::string                          as icon                     
    ,ua_json:os:icon_large::string                    as icon_large               
    ,ua_json:device:is_mobile_device::string          as is_mobile_device         
    ,ua_json:device:type::string                      as device_type              
    ,ua_json:device:brand::string                     as device_brand             
    ,ua_json:device:brand_code::string                as device_brand_code        
    ,ua_json:device:brand_url::string                 as device_brand_url         
    ,ua_json:device:name::string                      as device_name              
    ,ua_json:browser:name::string                     as browser_name             
    ,ua_json:browser:version::string                  as browser_version          
    ,ua_json:browser:version_major::string            as browser_version_major    
    ,ua_json:browser:engine::string                   as browser_engine           
    ,ua_json:crawler:is_crawler::string               as is_crawler               
    ,ua_json:crawler:category::string                 as crawler_category         
    ,ua_json:crawler:last_seen::string                as crawler_last_seen       
    ,LOAD_DATE                                                                  
from UTIL_DB.PUBLIC.USER_AGENT_JSON ;