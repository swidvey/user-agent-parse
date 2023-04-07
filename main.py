#!/usr/bin/env python3 
import os
import yaml         #pip install PyYAML==5.3
import smtplib
import calendar
import logging
import argparse
import requests
import pandas as pd
import snowflake.connector
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import date, timedelta
from snowflake.connector.pandas_tools import write_pandas


def cron_email(subject, sender, recipients, message_body):
      """ Sends email from cron. Mesage body is used to report differnt issues.
      """
      try:
            message = MIMEMultipart()
            message['Subject'] = subject
            message['From'] = sender
            message['To'] = recipients

            message.attach(MIMEText(f"Hello,<br><br> This email was sent to inform you that your automated program had an issues.<br><br> \
            Program: User Agent Parser <br> \
            Issue: <br> {message_body} <br> \
            <br> This is an automated email. <br><br> Regards<br>", "html"))
            msg_body = message.as_string()

            server = smtplib.SMTP('localhost')
            server.set_debuglevel(0)

            server.sendmail(message['From'], message['To'], msg_body)
            server.quit()
      except:
            logging.error("ERROR cron_email failed")


def user_agent_snowflake(userid, pw, ac, wh, start_date, end_date, table, yaml_dic):
      ''' Returns df of user agents from mapper data from snowflake
      '''
      try:
            conn=snowflake.connector.connect(
                  user        = userid
                  , password  = pw
                  , account   = ac
                  , warehouse = wh
                  )
            curs=conn.cursor()
 
            if table == 'MAPPER':
                  #execute SQL statement
                  curs.execute(f"SELECT DISTINCT UA \
                              from Util_db.PUBLIC.USER_AGENT_JSON \
                              where LOAD_DATE between \'{start_date}\' and \'{end_date}\'  \
                              ; ")
                  rows = curs.fetchall()
                  colnames = [desc[0] for desc in curs.description]
                  df = pd.DataFrame(data=rows, columns=colnames)
            
            elif table == 'MAIN': 
                  # Main table - execute SQL statement
                  curs.execute(f"SELECT DISTINCT user_agent \
                              from USER_DATA.USER_INFO.USER_INFORMATION \
                              where CAST(dateadd(S, UNIX_TIME, '1970-01-01') AS DATE) between \'{start_date}\' and \'{end_date}\'  \
                              ; ")
                  rows = curs.fetchall()
                  colnames = [desc[0] for desc in curs.description]
                  df_stream = pd.DataFrame(data=rows, columns=colnames)
            
            conn.close()
      except:
            message_body = f'ERROR user_agent_snowflake failed'
            cron_email(yaml_dic['main']['subject'], yaml_dic['main']['sender'], yaml_dic['main']['recipients'], message_body )
            logging.error("ERROR user_agent_snowflake failed")
      return df


def call_userstack_api(user_agent, access_key, yaml_dic):
      ''' Checks single USER AGENT element in API. 
      FAILED API JSON = {'success': False, 'error': {'code': 301, 'type': 'missing_user_agent'}}
      Failed API call is colleted in dataframe_parsed_ua function and emailed as a complete list. 
      '''
      try:
            params = {
            'access_key': access_key,
            'ua': user_agent
            }
            api_result = requests.get('http://api.userstack.com/detect', params)
      except:
            message_body = f'ERROR call_userstack_api failed, {user_agent}'
            cron_email(yaml_dic['main']['subject'], yaml_dic['main']['sender'], yaml_dic['main']['recipients'], message_body )
            logging.error("ERROR user_agent_snowflake failed")
      
      return api_result.json()


def unparsed_user_agents(df_main, df_mapper):
      ''' Differance between MAIN and Mapper. 
      '''
      dif = set(df_main['USER_AGENT']).difference(set(df_mapper['UA']))
      return list(dif)


def write_df_to_snowflake(df, userid, pw, ac, wh, db, sc, table_name, yaml_dic):
      ''' Writes dataframe to snowflake table
      '''
      # csv chuck and write to chunk
      try:
            conn=snowflake.connector.connect(
                          user      = userid
                        , password  = pw
                        , account   = ac
                        , warehouse = wh
                        , database  = db
                        , schema    = sc
                        )
            success, nchunks, nrows, _ = write_pandas(conn=conn, df= df , table_name =table_name, quote_identifiers=False)
            conn.close()
      except:
            message_body = f'ERROR write_df_to_snowflake failed'
            cron_email(yaml_dic['main']['subject'], yaml_dic['main']['sender'], yaml_dic['main']['recipients'], message_body )
            logging.error("ERROR write_df_to_snowflake failed") 


def dataframe_json(yaml_dic, ua_unparsed_list, load_date):
      '''Take User Agent JSON and parses it dataframe. Collects all failed UA and emails list
      '''
      un_parsed = []
      json_list = []
      us_list   = []

      for element in ua_unparsed_list:
            try:
                  api_result_json = call_userstack_api(element, yaml_dic['access']['userstack_key'], yaml_dic)
                  # ua only appears id parse was successful
                  if 'ua' in api_result_json.keys():
                        json_list.append(api_result_json)
                        us_list.append(element)
            except:
                  #element did not parse
                  un_parsed.append(element)


      # email list of unparsed values for inspection
      if len(un_parsed) > 0:
            message_body = f'User Agent has unparsed values:  {un_parsed}'
            cron_email(yaml_dic['main']['subject'], yaml_dic['main']['sender'], yaml_dic['main']['recipients'], message_body )
      
      df = pd.DataFrame(us_list, columns =['ua'])
      df['ua_json'] = json_list
      df['load_date'] = load_date
      return df


def insert_stage_to_main(userid, pw, ac, wh, yaml_dic):
      ''' Inserts stage data to next stage with parsed JSON
      '''
      try: 
            conn=snowflake.connector.connect(
                  user        = userid
                  , password  = pw
                  , account   = ac
                  , warehouse = wh
                  )
            curs=conn.cursor()
            curs.execute(" INSERT INTO  USER_DATA.USERSTACK.USER_AGENT_MAPPER    \
                              SELECT                                      \
                              UA                                          \
                              , parse_json(ua_json) as        ua_json     \
                              , LOAD_DATE                                 \
                              from USER_DATA.STAGE.USER_AGENT_MAPPER; \
                              ")
            conn.close()
      except:
            message_body = f'ERROR insert_stage_to_main failed'
            cron_email(yaml_dic['main']['subject'], yaml_dic['main']['sender'], yaml_dic['main']['recipients'], message_body )
            logging.error("ERROR insert_stage_to_main failed") 


def delete_stage_rows(userid, pw, ac, wh, yaml_dic):
      ''' delets row in stage table
      '''
      try:
            conn=snowflake.connector.connect(
                  user        = userid
                  , password  = pw
                  , account   = ac
                  , warehouse = wh
                  )
            curs=conn.cursor()
            curs.execute(" DELETE FROM USER_DATA.STAGE.USER_AGENT_MAPPER; ")
            conn.close()
      except:
            message_body = f'ERROR delete_stage_rows failed'
            cron_email(yaml_dic['main']['subject'], yaml_dic['main']['sender'], yaml_dic['main']['recipients'], message_body )
            logging.error("ERROR delete_stage_rows failed") 


def search_parse_load(yaml_dic, mapper_from_date, mapper_to_date, main_from_date, main_to_date, load_date, backlog, **kwargs):
      ''' Calls major functions. Searches Mapper & MAIN tables, and parses function. Load function. Checks backlog at end of month
      Emails to let us know if back log is complete
      '''
      try:
            # get data from mapper and MAIN
            mapper = user_agent_snowflake(yaml_dic['access']['user'], yaml_dic['access']['password'], 
                                          yaml_dic['access']['account'], yaml_dic['access']['warehouse'], mapper_from_date, mapper_to_date , 'MAPPER', yaml_dic)

            user_table = user_agent_snowflake(yaml_dic['access']['user'], yaml_dic['access']['password'], 
                                          yaml_dic['access']['account'], yaml_dic['access']['warehouse'], main_from_date , main_to_date ,'main', yaml_dic)
            
            # List of user agents to parse
            ua_unparsed_list = unparsed_user_agents(user_table, mapper)
            parse_n = kwargs.get('parse_limit', len(ua_unparsed_list))
            ua_unparsed_list = ua_unparsed_list[:parse_n]

            # clean tables
            del mapper
            del user_table

            # make this a function
            if len(ua_unparsed_list) > 0:
                  df = dataframe_json(yaml_dic, ua_unparsed_list, load_date)

                  # if data frame is 0 we have nothing to write
                  if len(df)>0:
                        write_df_to_snowflake(df, yaml_dic['access']['user'], yaml_dic['access']['password'], yaml_dic['access']['account']
                                    ,yaml_dic['access']['warehouse'], yaml_dic['access']['database'], yaml_dic['access']['schema'], 'USER_AGENT_MAPPER_STAGE', yaml_dic) 
                  del df
            
            # backlog = Y only during backlog run
            elif (len(ua_unparsed_list) == 0) & (backlog == 'Y'):
                  message_body = 'User Agent Backlog is completed. Please turn of back log search'
                  cron_email(yaml_dic['main']['subject'], yaml_dic['main']['sender'], yaml_dic['main']['recipients'], message_body )
      except:
            message_body = f'ERROR search_parse_load failed'
            cron_email(yaml_dic['main']['subject'], yaml_dic['main']['sender'], yaml_dic['main']['recipients'], message_body )
            logging.error("ERROR search_parse_load failed") 
         

def main():
      """ Main Function call. Loads YAML, LOGGER, Calls functions
      """
      # Grabs YAMAL file
      try:
          # YAMAL
          yaml_file = os.path.abspath('config.yaml')
          yaml_open = open(yaml_file)
          yaml_dic  = yaml.load(yaml_open, Loader=yaml.FullLoader)
          #yaml_dat  = yaml_dic[args.program]
      except Exception as error:
          print(f'Failed to load YAMAL file {yaml_file}: {error}')

      # Grabs log file
      home = yaml_dic['main']['home_directory']
      try:
          # Prepare a logger. 
          log_file    = home + yaml_dic['main']['logfile_path']
          logging.basicConfig(level=logging.ERROR, filename=log_file, format='%(asctime)s %(levelname)s %(threadName)-10s %(message)s')
      except Exception as error:
          print(f' Failed to load Logger file {log_file}: {error}')
      
      # todays date to check if already loaded 
      today = date.today()
      load_date = today.strftime("%Y-%m-%d")

      # search and load
      search_parse_load(yaml_dic, '1970-01-01', load_date , args.date_lookback, args.date_yesterday, load_date, 'N', parse_limit = args.parse_limit)

      # Backlog - only runs at end of the month for cost reasons
      if args.backlog == 'Y':
            # prep last day check
            todays_day = load_date[-2:]
            year = load_date[0:4]
            month = load_date[5:7]
            last_day_of_month = calendar.monthrange(int(year), int(month))[1]
            
            end_of_month = str(year) + '-' + str(month) + '-' + str(last_day_of_month)  
            begining_of_month = str(year) + '-' + str(month) + '-' + '01'

            if todays_day == last_day_of_month:
                  # check number of quieres completed for the month
                  mapper_len = len(user_agent_snowflake(yaml_dic['access']['user'], yaml_dic['access']['password'], 
                                              yaml_dic['access']['account'], yaml_dic['access']['warehouse'], begining_of_month, end_of_month , 'MAPPER', yaml_dic))
                  
                  if (mapper_len < 500_000) & (mapper_len != 0):
                        n_to_process = 500_000 - mapper_len
                        # queiries full back log of data and processed our limit
                        search_parse_load(yaml_dic, '1970-01-01', args.date_yesterday , '1970-01-01', args.date_yesterday, 'Y', parse_limit = n_to_process)
                  

      # Final Snowflake movements Stage to Public, and create new view with updated data        
      insert_stage_to_main(yaml_dic['access']['user'], yaml_dic['access']['password'],  yaml_dic['access']['account'], yaml_dic['access']['warehouse'], yaml_dic)
      delete_stage_rows(yaml_dic['access']['user'], yaml_dic['access']['password'],  yaml_dic['access']['account'], yaml_dic['access']['warehouse'], yaml_dic)


# Start main processing
if __name__ == "__main__":
    today         = date.today() 
    yesterday     = today - timedelta(days = 1) 
    yesterday_str = yesterday.strftime("%Y-%m-%d")

    lookback = today - timedelta(days = 7)
    lookback_str = lookback.strftime("%Y-%m-%d")

    opts = argparse.ArgumentParser(description='Change values for process run')
    opts.add_argument('-d', '--date_yesterday',   type = str, required = False,  dest = 'date_yesterday', default = yesterday_str,  help = 'Change date value in file name. Must be YYYY-MM-DD. Default is yesterday',   )
    opts.add_argument('-dl', '--date_lookback',   type = str, required = False,  dest = 'date_lookback',  default = lookback_str,   help = 'Change look back date value in file name. Must be YYYY-MM-DD. Default is 7 days',   )
    opts.add_argument('-b', '--backlog',          type = str, required = False,  dest = 'backlog',                                  help = 'Must be Y to run. Check backlog user_agents at end of month',   )
    opts.add_argument('-pl', '--parse_limit',     type = int, required = False,  dest = 'parse_limit',  default = 500_000,        help = 'Sets parse_limit, default is 500_000 the max monthly limit, ',   )

    args = opts.parse_args()
    main()