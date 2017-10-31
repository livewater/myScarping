#!/usr/bin/python
# encoding:utf-8
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
import time
import datetime
import threading
import numpy as np
import pandas as pd
from FinData import FinData

class FinDataNowAPI(FinData):
    """processing the metal data from bank"""
    def __init__(self, url, req, conn):
        FinData.__init__(self, url, req, conn)
        self.cate_map = {"USDAAU": "1151", "CNYAAU": "1152", "USDAAG": "1153", "CNYAAG": "1154", "USDAPT": "1155", "CNYAPT": "1156", "USDAPD": "1157", "CNYAPD": "1158"}
        self.req_list = ["USDAAU", "CNYAAU", "USDAAG", "CNYAAG", "USDAPT", "CNYAPT", "USDAPD", "CNYAPD"]
        self.pdData_list = []
        self.mysqlSavedNum_list = []
        self.report_hour = 1
        self.figure_hour = 6
        self.lock = threading.Lock()

    '''def appendPdData(self, json_data, product_name):
        item = json_data[self.cate_map[product_name]]
        self.pdData.append({'last_price':item["last_price"], 'high_price':item["high_price"], 'low_price':item["low_price"], 'buy_price':item["buy_price"], 'sell_price':item["sell_price"], 'update_time':item["uptime"], 'create_time':super(FinDataNowAPI, self).genCurrentTime()}, ignore_index=True)''' 

    #pdData_list: store the mysql data
    #mysqlSavedNum_list: calc the mysql data for every category
    def pdDataListInit(self):
        for product_idx in range(0, len(self.req_list)):
            product_name = self.req_list[product_idx]
            pdData = super(FinDataNowAPI, self).loadMysqlToPandas(self.req, product_name)
            self.pdData_list.append(pdData)
            self.mysqlSavedNum_list.append(len(self.pdData_list[product_idx].index))

    def checkJsonData(self, jsonarr):
        if jsonarr["success"] != u"1": 
            print jsonarr["msg"]
            exit()
        result = jsonarr["result"]
        return result

    def pdDataListUpdate(self):
        json_arr = super(FinDataNowAPI, self).getDataInJson()
        json_data = self.checkJsonData(json_arr) 
        for product_idx in range(0, len(self.req_list)):
            product_name = self.req_list[product_idx]
            item = json_data[self.cate_map[product_name]]
            self.pdData_list[product_idx] = self.pdData_list[product_idx].append({'last_price':item["last_price"], 'high_price':item["high_price"], 'low_price':item["low_price"], 'buy_price':item["buy_price"], 'sell_price':item["sell_price"], 'update_time':item["uptime"], 'create_time':super(FinDataNowAPI, self).genCurrentTime()}, ignore_index=True) 

    def appendPdDataToDB(self, pdData, product_name, init_data_size):
        pdNewData = pdData.ix[pdData.index[init_data_size:]]
        pdNewArray = np.array(pdNewData)
        #sql_command = "USE " + self.req
        #self.cur.execute(sql_command)
        mysql_command = "INSERT INTO "+product_name+" (last_price, high_price, low_price, buy_price, sell_price, update_time) VALUES (%s, %s, %s, %s, %s, %s)"
        for row in pdNewArray:
            self.lock.acquire()
            self.cur.execute(mysql_command, (row[0], row[1], row[2], row[3], row[4], row[5]))
            self.lock.release()
        self.lock.acquire()
        self.cur.connection.commit()
        self.lock.release()

    def DBUpdate(self):
        for product_idx in range(0, len(self.req_list)):
            product_name = self.req_list[product_idx]
            self.appendPdDataToDB(self.pdData_list[product_idx], product_name, self.mysqlSavedNum_list[product_idx])
            #TO DO: multi-thread need locks
            self.mysqlSavedNum_list[product_idx] = len(self.pdData_list[product_idx].index)

    def pdDataExtract(self, date_range):
        extract_result = []
        for product_idx in range(0, len(self.req_list)):
            mysql_command = "SELECT * FROM "+ self.req_list[product_idx] +" WHERE create_time BETWEEN '"+ date_range[0] + "' AND '"+ date_range[1] + "'"
            self.lock.acquire()
            self.cur.execute(mysql_command)
            self.lock.release()
            result = np.array(self.cur.fetchall())
            extract_result.append(result)
            self.lock.acquire()
            self.cur.connection.commit()
            self.lock.release()
            self.cur.close()
            self.cur = self.conn.cursor()
        return extract_result
    
    def drawSellPriceFigure(self, date_range):
        sell_price_list = []
        for product_idx in range(0, len(self.req_list)):
            mysql_command = "SELECT sell_price FROM "+ self.req_list[product_idx] +" WHERE create_time BETWEEN '"+ date_range[0] + "' AND '"+ date_range[1] + "'"
            self.lock.acquire()
            self.cur.execute(mysql_command)
            self.lock.release()
            result = np.array(self.cur.fetchall())
            sell_price_list.append(result)
            self.lock.acquire()
            self.cur.connection.commit()
            self.lock.release()

        fig = plt.figure(figsize = (15, 15))
        for product_idx in range(0, len(self.req_list)):
            plt.subplot(4, 2, product_idx+1)
            plt.title(self.req_list[product_idx], {'fontsize': 8})
            plt.plot(range(len(sell_price_list[product_idx])), sell_price_list[product_idx], lw=1.0, color='r')
            #ax.set_xticklabels([], fontsize = 6)
        #TO DO: consider the picture name
        plt.savefig("/home/livewater/Projects/myScarping/testCrawler/Scraping/test.png")
        self.cur.close()
        self.cur = self.conn.cursor()

    def reportByMail(self):
        mail_msg = ""
        end_tick = int(time.time()) 
        #Just for test
        #end_tick = time.mktime(datetime.datetime(2017,10,27,23,0,0).timetuple())
        report_date_range = super(FinDataNowAPI, self).genDateRange(end_tick, self.report_hour)
        figure_date_range = super(FinDataNowAPI, self).genDateRange(end_tick, self.figure_hour)
        extract_result = self.pdDataExtract(report_date_range)
        self.drawSellPriceFigure(figure_date_range)

        for product_idx in range(0, len(self.req_list)):
            mail_msg += '<h4>' + self.req_list[product_idx] + "</h4>"
            mail_msg += '<table border="1"><tr><th>last_price</th><th>high_price</th><th>low_price</th><th>buy_price</th><th>sell_price</th><th>update_time</th></tr>'
            for item_idx in range(0, len(extract_result[product_idx])):
                mail_msg += "<tr><td>" + str(extract_result[product_idx][item_idx][1]).ljust(10) + "</td><td>" + str(extract_result[product_idx][item_idx][2]).ljust(10) + "</td><td>" + str(extract_result[product_idx][item_idx][3]).ljust(10) + "</td><td>" + str(extract_result[product_idx][item_idx][4]).ljust(10) + "</td><td>" + str(extract_result[product_idx][item_idx][5]).ljust(10) + "</td><td>" + str(extract_result[product_idx][item_idx][6]).ljust(10) + "</td></tr>"
            mail_msg += "</table><br />"
        super(FinDataNowAPI, self).sendMail(mail_msg, "/home/livewater/Projects/myScarping/testCrawler/Scraping/test.png")
