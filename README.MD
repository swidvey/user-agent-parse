<p align="center">
<img src="https://external-content.duckduckgo.com/iu/?u=https%3A%2F%2Fwww.netguru.com%2Fhs-fs%2Fhubfs%2FSnowflake%2520logo.jpg%3Fwidth%3D653%26name%3DSnowflake%2520logo.jpg&f=1&nofb=1" alt="Snowflake ETL" title="Snowflake ETL" width="400" style="display: block; margin: 0 auto">
</p>
<br>
<p align="center">
<img src="https://designshack.net/wp-content/uploads/userstack-368x246.png" alt="USERSTACK" title="USERSTACK image" width="400" style="display: block; margin: 0 auto">
</p>
______________________________________________________________________
______________________________________________________________________

### Summary: 
```
This repo is an end-to-end example on how to parse user agent strings
using the USERSTACK API, automation through Cron and AWS EC2, and data insertion to 
a Snowflake table. 


About USERSTACK:
USERSTACK.com is a great API if you are looking to parse out user agent
information. Userstack offers a real-time, easy-to-use REST API interface 
capable of parsing User-Agent strings to accurately detect device, browser 
and operating system information.

Documetation can be found: https://userstack.com/documentation




Limitations: 
The process requires user to create an USERSTACK account. Paid account For large parse 
This example requires snowflake access. 

Special note:
Paths were removed. To utilize this code in production you will need to point
to your cron path

It is assumed you have a data table with User agents already avaliable. 
As a test you can use the User Agent data example given by USERSTACK

"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36"

```
______________________________________________________________________
______________________________________________________________________
### Design

```
Snowflake data -> ec2 -> API call -> clean parsed data in ec2 -> load to SNowflake 

```


______________________________________________________________________
______________________________________________________________________

### Files Name Descriptions: 
```
user-agent-parse 
    config.yaml:
        This is a yaml configuration file

    initiate_app.sh:
        Preps program and creates venv and installs needed packages in EC2.
    
    main.py:
        This is the main parse script that turns data to dataframe and writes to snowflake
    
    requirements.txt:
        python lib requieremnts
    
    user_agent_bash.sh:
        This script is what is called in the cron task, and preps venv and runs main.py
    
    log_file.log:
        error log file

snowflake folder
    ddl.sql:
        Shows how to create a table in snowflake and defines table values    

cron folder:
    crontab.txt:
        Shows cron schedule and path to run .sh program
```
______________________________________________________________________
______________________________________________________________________
### Steps: 
```
to do: add steps to run program
to do: add steps to run program
```
______________________________________________________________________
______________________________________________________________________
