#!/usr/bin/python
# encoding:utf-8
import numpy as np
import pandas as pd
from FinData import FinData

class FinDataNowAPI(FinData):
    """processing the metal data from bank"""
    def __init__(self, url, req, conn):
        FinData.__init__(self, url, req, conn)
        self.cate_map = {"USDAAU": "1151", "CNYAAU": "1152", "USDAAG": "1153", "CNYAAG": "1154", "USDAPT": "1155", "CNYAPT": "1156", "USDAPD": "1157", "CNYAPD": "1158"}
        self.req_list = self.cate_map.keys()

    def appendPdData(self, pdData, json_data, product_name):
        item = json_data[self.cate_map[product_name]]
        pdData = pdData.append({'last_price':item["last_price"], 'high_price':item["high_price"], 'low_price':item["low_price"], 'buy_price':item["buy_price"], 'sell_price':item["sell_price"], 'update_time':item["uptime"], 'create_time':super(FinDataNowAPI, self).genCurrentTime()}, ignore_index=True) 
        return pdData

    #pdData_list: store the mysql data
    #mysqlSavedNum_list: calc the mysql data for every category
    def pdBankDataPushBack(self, pdData_list, mysqlSavedNum_list):
        for product_idx in range(0, len(self.req_list)):
            product_name = self.req_list[product_idx]
            pdData = super(FinDataNowAPI, self).loadMysqlToPandas(self.req, product_name)
            pdData_list.append(pdData)
            mysqlSavedNum_list.append(len(pdData_list[product_idx].index))

    def dataAnalyzed(self, pdData_list):
        json_data = super(FinDataNowAPI, self).getDataInJson()
        for product_idx in range(0, len(self.req_list)):
            product_name = self.req_list[product_idx]
            pdData_list[product_idx] = self.appendPdData(pdData_list[product_idx], json_data, product_name)

    def saveDataIntoMysql(self, pdData, product_name, init_data_size):
        pdNewData = pdData.ix[pdData.index[init_data_size:]]
        pdNewArray = np.array(pdNewData)
        sql_command = "USE " + self.req
        self.cur.execute(sql_command)
        mysql_command = "INSERT INTO "+product_name+" (last_price, high_price, low_price, buy_price, sell_price, update_time) VALUES (%s, %s, %s, %s, %s, %s)"
        for row in pdNewArray:
            self.cur.execute(mysql_command, (row[0], row[1], row[2], row[3], row[4], row[5]))
        self.cur.connection.commit()

    def writeBankDataToMySQL(self, pdData_list, mysqlSavedNum_list):
        for product_idx in range(0, len(self.req_list)):
            product_name = self.req_list[product_idx]
            self.saveDataIntoMysql(pdData_list[product_idx], product_name, mysqlSavedNum_list[product_idx])
            #TO DO: multi-thread need locks
            mysqlSavedNum_list[product_idx] = len(pdData_list[product_idx].index)
        #print "Success!"

