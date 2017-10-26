#!/usr/bin/python
# encoding:utf-8
 
import logging
import pymysql
from apscheduler.schedulers.blocking import BlockingScheduler  
from FinDataNowAPI import FinDataNowAPI

if __name__ == "__main__":
    try:
        # start MySQL
        conn = pymysql.connect(host = 'localhost', user = 'root', password = 'glg1117kai', db = 'mysql', charset = 'utf8')
        cur = conn.cursor()

        #build up AuTD structure for shgold
        pdData_list = []  #Mysql + urltrieved 
        mysqlSavedNum_list = []  #data saved in mysql num
        url = "http://api.k780.com/?app=finance.gzgold&appkey=29115&sign=51ab5331f653425bced95c234149cc88&format=json"
        fin_data_nowapi = FinDataNowAPI(url, "nowapi", conn)
        #list[0]: AuTD in shgold
        fin_data_nowapi.pdBankDataPushBack(pdData_list, mysqlSavedNum_list)

        #the data needs to be analyzed circularly, now the cycle is 3s
        logging.basicConfig()
        scheduler = BlockingScheduler()
        scheduler.add_job(fin_data_nowapi.dataAnalyzed, "cron", args=[pdData_list], minute="*/1")
        scheduler.add_job(fin_data_nowapi.writeBankDataToMySQL, "cron", args=[pdData_list, mysqlSavedNum_list], minute ="*/5")
        scheduler.start()
        #fin_data_bank.dataAnalyzed(pdData_list)
        #fin_data_bank.writeBankDataToMySQL(pdData_list, mysqlSavedNum_list)

    except pymysql.Error as e:
        print('Got error {!r}, errno is {}'.format(e, e.args[0]))
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
    finally:
        #scheduler.shutdown()  
        cur.close()
        conn.close()
