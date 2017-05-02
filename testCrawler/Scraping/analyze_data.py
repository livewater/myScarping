#!/usr/bin/python
# encoding:utf-8

import sys
sys.path.append("../Notifications/")
from send_mail import *
from collect_data import *
import numpy as np
import pandas as pd

#  jisuapi.com require category
def dataAnalyzed(product_name, table_name, pdData_list):
    json_data = getDataInJson(table_name)
    pdData_list[0] = appendPdData(pdData_list[0], json_data, product_name, table_name)
    #if(pdData_list[0]["Price"][0] > 225.0):
    #    sendMail()
    print(len(pdData_list[0].index))

# only execute once, dump the MySQL data into pdData_list
def pdDataPushBack(pdData_list, mysqlSavedNum_list, table_name, product_name, conn):
    pdData = loadMysqlToPandas(conn, table_name, product_name)
    pdData_list.append(pdData)
    mysqlSavedNum_list.append(len(pdData_list[0].index))

# save the new url trieved data into mysql, and update the saved num flag
def writeBackToMySQL(pdData_list, mysqlSavedNum_list,table_name, product_name, cur):
    saveDataIntoMysql(cur, pdData_list[0], product_name, table_name, mysqlSavedNum_list[0])
    #TO DO: multi-thread need locks
    mysqlSavedNum_list[0] = len(pdData_list[0].index)
    print(np.array(mysqlSavedNum_list[0]))

