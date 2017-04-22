#!/usr/bin/python
# encoding:utf-8
 
import urllib2, json, urllib
import datetime
from apscheduler.schedulers.blocking import BlockingScheduler  
import numpy as np
import pandas as pd
import pymysql
from collect_data import *
from save_data import *

#  jisuapi.com require category
def dataAnalyzed(product_name, table_name, pdData_list):
    json_data = getDataInJson(table_name)
    pdData_list[0] = appendPdDataForShgold(pdData_list[0], json_data, product_name)
    print(len(pdData_list[0].index))

# only execute once, dump the MySQL data into pdData_list
def pdDataPushBack(pdData_list, mysqlSavedNum_list, table_name, product_name, conn):
    shgold_AuTD = loadMysqlToPandas(conn, table_name, product_name)
    pdData_list.append(shgold_AuTD)
    mysqlSavedNum_list.append(len(pdData_list[0].index))

# save the new url trieved data into mysql, and update the saved num flag
def writeBackToMySQL(pdData_list, mysqlSavedNum_list,table_name, product_name, cur):
    saveDataIntoMysql(cur, pdData_list[0], product_name, table_name, mysqlSavedNum_list[0])
    #TO DO: multi-thread need locks
    mysqlSavedNum_list[0] = len(pdData_list[0].index)
    print(np.array(mysqlSavedNum_list[0]))

if __name__ == "__main__":
    try:
        # start MySQL
        conn = pymysql.connect(host = 'localhost', user = 'root', password = 'glg1117kai', db = 'mysql', charset = 'utf8')
        cur = conn.cursor()

        #build up AuTD structure for shgold
        pdData_list = []  #Mysql + urltrieved 
        mysqlSavedNum_list = []  #data saved in mysql num
        #list[0]: AuTD in shgold
        pdDataPushBack(pdData_list, mysqlSavedNum_list, "shgold", "AuTD", conn)

        #the data needs to be analyzed circularly, now the cycle is 3s
        scheduler = BlockingScheduler()
        scheduler.add_job(dataAnalyzed, "cron", args=["AuTD", "shgold", pdData_list], second="*/3")
        scheduler.add_job(writeBackToMySQL, "cron", args=[pdData_list, mysqlSavedNum_list, "shgold", "AuTD", cur], second="*/10")
        scheduler.start()

    except pymysql.Error as e: 
        print('Got error {!r}, errno is {}'.format(e, e.args[0]))
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()  
    finally:
        scheduler.shutdown()  
        cur.close()
        conn.close()
