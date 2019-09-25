#!/usr/bin/python
# encoding:utf-8
 
import logging
import pymysql
import datetime
import passwd
from apscheduler.schedulers.blocking import BlockingScheduler  
from FinDataNowAPI import FinDataNowAPI

if __name__ == "__main__":
    try:
        # debug switcher
        debug_mode = True
        # start MySQL
        conn = pymysql.connect(host = 'localhost', user = 'root', password = passwd.passwd, db = 'mysql', charset = 'utf8', port=3308)
        cur = conn.cursor()

        # build up AuTD structure for shgold
        pdData_list = []  #Mysql + urltrieved 
        mysqlSavedNum_list = []  #data saved in mysql num
        url = "http://api.k780.com/?app=finance.gzgold&appkey=29115&sign=51ab5331f653425bced95c234149cc88&format=json"
        fin_data_nowapi = FinDataNowAPI(debug_mode, url, "nowapi", conn)
        fin_data_nowapi.pdDataListInit()

        # execute the program at 8:00 am, excute duration is 4 days and 20 hours
        start_time = fin_data_nowapi.genStartEndTime()[0]
        end_time = fin_data_nowapi.genStartEndTime()[1]

        logging.basicConfig()
        scheduler = BlockingScheduler()
        if debug_mode == False:
            scheduler = BlockingScheduler()
            scheduler.add_job(fin_data_nowapi.pdDataListUpdate, "cron", args=[], minute="*/3",start_date = start_time, end_date=end_time)
            scheduler.add_job(fin_data_nowapi.DBUpdate, "cron", args=[], minute ="*/29", start_date = start_time, end_date=end_time)
            scheduler.add_job(fin_data_nowapi.reportNormalByMail, "cron", args=[""], hour ="*/2", start_date = start_time, end_date=end_time)
            scheduler.add_job(fin_data_nowapi.sendAlertMail, "cron", args=[], minute ="*/5", start_date = start_time, end_date=end_time)
            scheduler.add_job(fin_data_nowapi.sendWeeklyMail, "date", args=[], run_date = end_time)
            scheduler.start()
        else:
            fin_data_nowapi.pdDataListUpdate()
            fin_data_nowapi.DBUpdate()
            fin_data_nowapi.reportNormalByMail()
            fin_data_nowapi.sendAlertMail()
            fin_data_nowapi.sendWeeklyMail()

    except pymysql.Error as e:
        print('Got error {!r}, errno is {}'.format(e, e.args[0]))
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        cur.close()
        conn.close()
    finally:
        if debug_mode == False:
            scheduler.shutdown()  
        cur.close()
        conn.close()
