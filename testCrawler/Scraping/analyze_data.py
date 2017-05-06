#!/usr/bin/python
# encoding:utf-8

import sys
sys.path.append("../Notifications/")
from send_mail import *
from collect_data import *
import numpy as np
import pandas as pd

req_list_bank = ["AuUSD", "PtUSD", "PdUSD", "AuRMB", "PtRMB", "PdRMB"]
#  jisuapi.com require category
def dataAnalyzed(table_name, pdData_list):
    json_data = getDataInJson(table_name)
    for product_idx in range(0, len(req_list_bank)):
        product_name = req_list_bank[product_idx]
        pdData_list[product_idx] = appendPdData(pdData_list[product_idx], json_data, product_name, table_name)
    #if(pdData_list[0]["Price"][0] > 225.0):
    #    sendMail()
    #print(len(pdData_list[1].index))

# only execute once, dump the MySQL data into pdData_list
def pdSingleDataPushBack(pdData_list, mysqlSavedNum_list, table_name, product_name, conn):
    pdData = loadMysqlToPandas(conn, table_name, product_name)
    pdData_list.append(pdData)
    mysqlSavedNum_list.append(len(pdData_list[0].index))
    
def pdBankDataPushBack(pdData_list, mysqlSavedNum_list, conn):
    for product_idx in range(0, len(req_list_bank)):
        product_name = req_list_bank[product_idx]
        pdData = loadMysqlToPandas(conn, "bank", product_name)
        pdData_list.append(pdData)
        mysqlSavedNum_list.append(len(pdData_list[product_idx].index))

# save the new url trieved data into mysql, and update the saved num flag
def writeSingleDataBackToMySQL(pdData_list, mysqlSavedNum_list,table_name, product_name, cur):
    saveDataIntoMysql(cur, pdData_list[0], product_name, table_name, mysqlSavedNum_list[0])
    mysqlSavedNum_list[0] = len(pdData_list[0].index)

def writeBankDataToMySQL(pdData_list, mysqlSavedNum_list,cur):
    for product_idx in range(0, len(req_list_bank)):
        product_name = req_list_bank[product_idx]
        saveDataIntoMysql(cur, pdData_list[product_idx], product_name, "bank", mysqlSavedNum_list[product_idx])
        #TO DO: multi-thread need locks
        mysqlSavedNum_list[product_idx] = len(pdData_list[product_idx].index)
        #print(np.array(mysqlSavedNum_list[4]))

