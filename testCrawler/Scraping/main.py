#!/usr/bin/python
# encoding:utf-8
 
import logging
import pymysql
from apscheduler.schedulers.blocking import BlockingScheduler  
from FinDataBank import FinDataBank

if __name__ == "__main__":
    try:
        # start MySQL
        conn = pymysql.connect(host = 'localhost', user = 'root', password = 'glg1117kai', db = 'mysql', charset = 'utf8')
        cur = conn.cursor()

        #build up AuTD structure for shgold
        pdData_list = []  #Mysql + urltrieved 
        mysqlSavedNum_list = []  #data saved in mysql num
        fin_data_bank = FinDataBank("bank", conn)
        #list[0]: AuTD in shgold
        fin_data_bank.pdBankDataPushBack(pdData_list, mysqlSavedNum_list)

        #the data needs to be analyzed circularly, now the cycle is 3s
        logging.basicConfig()
        scheduler = BlockingScheduler()
        scheduler.add_job(fin_data_bank.dataAnalyzed, "cron", args=[pdData_list], second="*/10")
        scheduler.add_job(fin_data_bank.writeBankDataToMySQL, "cron", args=[pdData_list, mysqlSavedNum_list], second ="*/35")
        scheduler.start()
        #fin_data_bank.dataAnalyzed(pdData_list)
        #fin_data_bank.writeBankDataToMySQL(pdData_list, mysqlSavedNum_list)

    except pymysql.Error as e:
        print('Got error {!r}, errno is {}'.format(e, e.args[0]))
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
    finally:
        scheduler.shutdown()  
        cur.close()
        conn.close()
