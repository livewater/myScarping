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
import decimal as de
from FinData import FinData

class FinDataNowAPI(FinData):
    """processing the metal data from bank"""
    def __init__(self, debug, url, req, conn):
        FinData.__init__(self, debug, url, req, conn)
        self.cate_map = {"USDAAU": "1151", "CNYAAU": "1152", "USDAAG": "1153", "CNYAAG": "1154", "USDAPT": "1155", "CNYAPT": "1156", "USDAPD": "1157", "CNYAPD": "1158"}
        self.req_list = ["USDAAU", "CNYAAU", "USDAAG", "CNYAAG", "USDAPT", "CNYAPT", "USDAPD", "CNYAPD"]
        self.pdData_list = []
        self.mysqlSavedNum_list = []  # the record num in mysql
        self.savedNum_list = []  # the record num now = saved + not saved
        self.report_hour = 0.1   #just for data verify
        self.figure_hour = 6
        self.lock = threading.Lock()
        self.window = 10
        self.alert_timer = 0
        self.alert_msg_head = "<h2>Alert Product: <br /></h2>"

    #pdData_list: store the mysql data
    #mysqlSavedNum_list: calc the mysql data for every category
    def pdDataListInit(self):
        for product_idx in range(0, len(self.req_list)):
            product_name = self.req_list[product_idx]
            pdData = super(FinDataNowAPI, self).loadMysqlToPandas(self.req, product_name)
            self.pdData_list.append(pdData)
            self.mysqlSavedNum_list.append(len(self.pdData_list[product_idx].index))
            self.savedNum_list.append(len(self.pdData_list[product_idx].index))

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
            self.lock.acquire()
            self.pdData_list[product_idx] = self.pdData_list[product_idx].append({'last_price':de.Decimal(item["last_price"]), 'high_price':de.Decimal(item["high_price"]), 'low_price':de.Decimal(item["low_price"]), 'buy_price':de.Decimal(item["buy_price"]), 'sell_price':de.Decimal(item["sell_price"]), 'update_time':item["uptime"], 'create_time':super(FinDataNowAPI, self).genCurrentTime()}, ignore_index=True) 
            self.savedNum_list[product_idx] += 1
            self.lock.release()

    def appendPdDataToDB(self, pdData, product_name, init_data_size):
        pdNewData = pdData.ix[pdData.index[init_data_size:]]
        pdNewArray = np.array(pdNewData)
        mysql_command = "INSERT INTO "+product_name+" (last_price, high_price, low_price, buy_price, sell_price, update_time) VALUES (%s, %s, %s, %s, %s, %s)"
        self.lock.acquire()
        pdNewArray = (pdNewArray.take(range(6), axis=1)).tolist()
        self.cur.executemany(mysql_command, pdNewArray)
        #for row in pdNewArray:
            #self.cur.execute(mysql_command, (row[0], row[1], row[2], row[3], row[4], row[5]))

        if self.debug == False:
            self.cur.connection.commit()
        self.lock.release()

    def DBUpdate(self):
        for product_idx in range(0, len(self.req_list)):
            product_name = self.req_list[product_idx]
            self.appendPdDataToDB(self.pdData_list[product_idx], product_name, self.mysqlSavedNum_list[product_idx])
            self.mysqlSavedNum_list[product_idx] = self.savedNum_list[product_idx]

            #TO DO: multi-thread need locks
            #self.mysqlSavedNum_list[product_idx] = len(self.pdData_list[product_idx].index)

    def pdDataExtract(self, date_range):
        extract_result = []
        for product_idx in range(0, len(self.req_list)):
            mysql_command = "SELECT * FROM "+ self.req_list[product_idx] +" WHERE create_time BETWEEN '"+ date_range[0] + "' AND '"+ date_range[1] + "'"
            self.lock.acquire()
            self.cur.execute(mysql_command)
            result = np.array(self.cur.fetchall())
            extract_result.append(result)
            if self.debug == False:
                self.cur.connection.commit()
            self.cur.close()
            self.cur = self.conn.cursor()
            self.lock.release()
        return extract_result
    
    def drawSellPriceFigure(self, date_range, figure_name):
        sell_price_list = []
        for product_idx in range(0, len(self.req_list)):
            mysql_command = "SELECT sell_price FROM "+ self.req_list[product_idx] +" WHERE create_time BETWEEN '"+ date_range[0] + "' AND '"+ date_range[1] + "'"
            self.lock.acquire()
            self.cur.execute(mysql_command)
            result = np.array(self.cur.fetchall())
            sell_price_list.append(result)
            if self.debug == False:
                self.cur.connection.commit()
            self.lock.release()

        fig = plt.figure(figsize = (15, 15))
        for product_idx in range(0, len(self.req_list)):
            plt.subplot(4, 2, product_idx+1)
            plt.title(self.req_list[product_idx], {'fontsize': 8})
            plt.plot(range(len(sell_price_list[product_idx])), sell_price_list[product_idx], lw=1.0, color='r')
            #ax.set_xticklabels([], fontsize = 6)
        plt.savefig(figure_name)
        self.lock.acquire()
        self.cur.close()
        self.cur = self.conn.cursor()
        self.lock.release()

    def genMailData(self, alert_msg):
        #Just for test
        if self.debug == False:
            end_tick = int(time.time()) 
        else:
            end_tick = time.mktime(datetime.datetime(2017,11,1,11,0,0).timetuple())

        report_date_range = super(FinDataNowAPI, self).genDateRange(end_tick, self.report_hour)
        extract_result = self.pdDataExtract(report_date_range)
        mail_msg = '<h3><font color="#FF0000">' + alert_msg + "</font></h3>"
        for product_idx in range(0, len(self.req_list)):
            mail_msg += '<h4>' + self.req_list[product_idx] + "</h4>"
            mail_msg += '<table border="1"><tr><th>last_price</th><th>high_price</th><th>low_price</th><th>buy_price</th><th>sell_price</th><th>update_time</th></tr>'
            for item_idx in range(0, len(extract_result[product_idx])):
                mail_msg += "<tr><td>" + str(extract_result[product_idx][item_idx][1]).ljust(10) + "</td><td>" + str(extract_result[product_idx][item_idx][2]).ljust(10) + "</td><td>" + str(extract_result[product_idx][item_idx][3]).ljust(10) + "</td><td>" + str(extract_result[product_idx][item_idx][4]).ljust(10) + "</td><td>" + str(extract_result[product_idx][item_idx][5]).ljust(10) + "</td><td>" + str(extract_result[product_idx][item_idx][6]).ljust(10) + "</td></tr>"
            mail_msg += "</table><br />"
        return mail_msg

    def reportByMail(self, alert_msg, fig_name="report.png"):
        if self.debug == False:
            end_tick = int(time.time()) 
        else:
            end_tick = time.mktime(datetime.datetime(2017,11,1,11,0,0).timetuple())

        figure_date_range = super(FinDataNowAPI, self).genDateRange(end_tick, self.figure_hour)
        self.drawSellPriceFigure(figure_date_range, fig_name)
        #Now turn off data tranferring 
        mail_msg = self.genMailData(alert_msg)
        super(FinDataNowAPI, self).sendMail(mail_msg, fig_name)
        #print "Success!"

    def checkAlert(self):
        alert_msg = self.alert_msg_head
        for product_idx in range(0, len(self.req_list)):
            neg_flag = 0
            pos_flag = 0
            self.lock.acquire()
            pdData = self.pdData_list[product_idx]['sell_price']
            base = self.savedNum_list[product_idx]
            self.lock.release()
            for check_idx in range(-self.window, 0):
                delta = pdData[base+check_idx]-pdData[base+check_idx-1]
                if delta > 0:
                    pos_flag += 1
                elif delta < 0:
                    neg_flag += 1 
            data_in_window = np.array(pdData[base-self.window-1:])
            max_price = np.max(data_in_window)
            min_price = np.min(data_in_window)
            mean_price = np.mean(data_in_window)
            #print pos_flag, neg_flag, data_in_window, np.abs(max_price - min_price), de.Decimal(0.005)*mean_price
            if ((np.abs(max_price - min_price) > de.Decimal(0.005)*mean_price) or (neg_flag >=de.Decimal(0.2)*self.window or pos_flag >= de.Decimal(0.2)*self.window)):
                alert_msg += (self.req_list[product_idx] + ", max_price=" + str(max_price) +" , min_price=" + str(min_price) \
                        +" , mean_price=" + str(mean_price.quantize(de.Decimal('0.0000'))) + ", pos_flag=" + str(pos_flag) + ", neg_flag=" + str(neg_flag) + "<br />")

        return alert_msg

    def sendAlertMail(self, fig_name = "alert.png"):
        alert_msg = self.checkAlert()
        if(alert_msg != self.alert_msg_head):
            if (self.alert_timer == 0)
                self.reportByMail(alert_msg, fig_name)
                self.alert_timer = 4

        # the alert mail will delivered after 3*cycles
        self.alert_timer = self.alert_timer-1
