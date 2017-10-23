#!/usr/bin/python
# encoding:utf-8
from FinData import *

class FinDataBank(FinData):
    """processing the metal data from bank"""
    def __init__(self, req, conn):
        FinData.__init__(self, req, conn)
        self.cate_map = {"AuUSD":0, "PtUSD":1, "PdUSD": 2, "AuRMB": 3, "PtRMB": 4, "PdRMB": 5}
        self.req_list = ["AuUSD", "PtUSD", "PdUSD", "AuRMB", "PtRMB", "PdRMB"]
        self.check_list = [u"美元账户黄金", u"美元账户铂金", u"美元账户钯金", u"人民币账户黄金", u"人民币账户铂金", u"人民币账户钯金"]

    def saveDataBank():
        cur = self.conn.cursor()
        cur.execute("USE bank")
        result = self.getDataInJson("bank")
        index = 3
        for req in self.req_list:
            sql_command = "INSERT INTO " + self.req_list[index] + " (MidPrice, BuyPrice, SellPrice, MaxPrice, MinPrice, OpeningPrice, LastClosingPrice, ChangeQuantity, UpdateTime) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            if(result[self.cate_map[req]]["typename"] == check_list_bank[self.cate_map[req]]):
                cur.execute(sql_command, ( result[self.cate_map[req]]["midprice"], result[self.cate_map[req]]["buyprice"], result[self.cate_map[req]]["sellprice"],result[self.cate_map[req]]["maxprice"], result[self.cate_map[req]]["minprice"], result[self.cate_map[req]]["openingprice"], result[self.cate_map[req]]["lastclosingprice"], result[self.cate_map[req]]["changequantity"], result[self.cate_map[req]]["updatetime"]))
                index += 1
            else:
                print("API changed, need to check!\n")
                super(FinDataBank, self).closeDB()
                exit()
        cur.connection.commit()

    def appendPdData(self, pdData, json_data, product_name):
        if self.req == "bank":
            item = json_data[self.cate_map[product_name]] 
            pdData = pdData.append({'MidPrice':item["midprice"], 'BuyPrice':item["buyprice"], 'SellPrice':item["sellprice"], 'MaxPrice':item["maxprice"], 'MinPrice':item["minprice"], 'OpeningPrice':item["openingprice"], 'LastClosingPrice':item["lastclosingprice"], 'ChangeQuantity':item["changequantity"],'UpdateTime':item["updatetime"], 'CreatedTime':self.genCurrentTime()}, ignore_index=True) 
        else:
            print("Unknow table name!\n")
        return pdData

    #pdData_list: store the mysql data
    #mysqlSavedNum_list: calc the mysql data for every category
    def pdBankDataPushBack(self, pdData_list, mysqlSavedNum_list):
        for product_idx in range(0, len(self.req_list)):
            product_name = self.req_list[product_idx]
            pdData = super(FinDataBank, self).loadMysqlToPandas("bank", product_name)
            pdData_list.append(pdData)
            mysqlSavedNum_list.append(len(pdData_list[product_idx].index))

    def dataAnalyzed(self, pdData_list):
        json_data = self.getDataInJson()
        for product_idx in range(0, len(self.req_list)):
            product_name = self.req_list[product_idx]
            pdData_list[product_idx] = self.appendPdData(pdData_list[product_idx], json_data, product_name)

    def saveDataIntoMysql(self, pdData, product_name, init_data_size):
        cur = self.conn.cursor()
        pdNewData = pdData.ix[pdData.index[init_data_size:]]
        pdNewArray = np.array(pdNewData)
        sql_command = "USE " + self.req
        cur.execute(sql_command)
        if self.req == "bank":
            mysql_command = "INSERT INTO "+product_name+" (MidPrice, BuyPrice, SellPrice, MaxPrice, MinPrice, OpeningPrice, LastClosingPrice, ChangeQuantity, UpdateTime) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            for row in pdNewArray:
                cur.execute(mysql_command, (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8]))
        else:
            print("Unknow table name!\n")
        cur.connection.commit()

    def writeBankDataToMySQL(self, pdData_list, mysqlSavedNum_list):
        cur = self.conn.cursor()
        for product_idx in range(0, len(self.req_list)):
            product_name = self.req_list[product_idx]
            self.saveDataIntoMysql(pdData_list[product_idx], product_name, mysqlSavedNum_list[product_idx])
            #TO DO: multi-thread need locks
            mysqlSavedNum_list[product_idx] = len(pdData_list[product_idx].index)
        print "Success!"

