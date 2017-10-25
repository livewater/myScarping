#!/usr/bin/python
# encoding:utf-8
import urllib2, json, urllib
import pymysql
import pandas as pd
import time

class FinData(object):
    '''the base class from jisuapi'''
    def __init__(self, req, conn):
        self.req = req
        self.conn = conn
        self.cur = conn.cursor()

    def getDataInJson(self):
        url = "http://api.jisuapi.com/gold/"+ self.req + "?appkey=0fb7150dc4ce3494"
        request = urllib2.Request(url)
        result = urllib2.urlopen(request)
        jsonarr = json.loads(result.read())
    #    jsonarr = json.load(open('../Test/bank.json',"r"))
         
        if jsonarr["status"] != u"0": 
            print jsonarr["msg"]
            exit()
        result = jsonarr["result"]
        return result

    def loadMysqlToPandas(self, product_name, table_name):
        sql_command = "USE " + product_name
        self.cur.execute(sql_command)
        sql_command = "SELECT * from "+ table_name
        data = pd.io.sql.read_sql(sql_command, self.conn) 
        data = data.drop(['ID'], axis=1)
        return data

    def genCurrentTime(self):
        now = int(time.time()) 
        localtime = time.localtime(now)
        time_format = "%Y-%m-%d %H:%M:%S"
        res = time.strftime(time_format, localtime)
        return res

    def closeDB(self):
        self.cur.close()
        self.conn.close()

