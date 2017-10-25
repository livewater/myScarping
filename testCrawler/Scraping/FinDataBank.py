#!/usr/bin/python
# encoding:utf-8
import numpy as np
import pandas as pd
from FinData import FinData

class FinDataBank(FinData):
    """processing the metal data from bank"""
    def __init__(self, req, conn):
        FinData.__init__(self, req, conn)
        self.cate_map = {"AuUSD":0, "PtUSD":1, "PdUSD": 2, "AuRMB": 3, "PtRMB": 4, "PdRMB": 5}
        self.req_list = ["AuUSD", "PtUSD", "PdUSD", "AuRMB", "PtRMB", "PdRMB"]
        self.check_list = [u"美元账户黄金", u"美元账户铂金", u"美元账户钯金", u"人民币账户黄金", u"人民币账户铂金", u"人民币账户钯金"]

    def appendPdData(self, pdData, json_data, product_name):
        item = json_data[self.cate_map[product_name]] 
        pdData = pdData.append({'MidPrice':item["midprice"], 'BuyPrice':item["buyprice"], 'SellPrice':item["sellprice"], 'MaxPrice':item["maxprice"], 'MinPrice':item["minprice"], 'OpeningPrice':item["openingprice"], 'LastClosingPrice':item["lastclosingprice"], 'ChangeQuantity':item["changequantity"],'UpdateTime':item["updatetime"], 'CreatedTime':super(FinDataBank, self).genCurrentTime()}, ignore_index=True) 
        return pdData

    #pdData_list: store the mysql data
    #mysqlSavedNum_list: calc the mysql data for every category
    def pdBankDataPushBack(self, pdData_list, mysqlSavedNum_list):
        for product_idx in range(0, len(self.req_list)):
            product_name = self.req_list[product_idx]
            pdData = super(FinDataBank, self).loadMysqlToPandas(self.req, product_name)
            pdData_list.append(pdData)
            mysqlSavedNum_list.append(len(pdData_list[product_idx].index))

    def dataAnalyzed(self, pdData_list):
        json_data = super(FinDataBank, self).getDataInJson()
        for product_idx in range(0, len(self.req_list)):
            product_name = self.req_list[product_idx]
            pdData_list[product_idx] = self.appendPdData(pdData_list[product_idx], json_data, product_name)

    def saveDataIntoMysql(self, pdData, product_name, init_data_size):
        pdNewData = pdData.ix[pdData.index[init_data_size:]]
        pdNewArray = np.array(pdNewData)
        sql_command = "USE " + self.req
        self.cur.execute(sql_command)
        mysql_command = "INSERT INTO "+product_name+" (MidPrice, BuyPrice, SellPrice, MaxPrice, MinPrice, OpeningPrice, LastClosingPrice, ChangeQuantity, UpdateTime) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
        for row in pdNewArray:
            self.cur.execute(mysql_command, (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8]))
        self.cur.connection.commit()

    def writeBankDataToMySQL(self, pdData_list, mysqlSavedNum_list):
        for product_idx in range(0, len(self.req_list)):
            product_name = self.req_list[product_idx]
            self.saveDataIntoMysql(pdData_list[product_idx], product_name, mysqlSavedNum_list[product_idx])
            #TO DO: multi-thread need locks
            mysqlSavedNum_list[product_idx] = len(pdData_list[product_idx].index)
        print "Success!"

