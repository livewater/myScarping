#!/usr/bin/python
# encoding:utf-8
 
import urllib2, json, urllib
import numpy as np
import pandas as pd
import pymysql
from collect_data import *
from save_data import *

#  jisuapi.com require category
mysql_secure_path = "/tmp/"
if __name__ == "__main__":
    try:
        conn = pymysql.connect(host = 'localhost', user = 'root', password = 'glg1117kai', db = 'mysql', charset = 'utf8')
        cur = conn.cursor()

        #build up AuTD structure for shgold
        shgold_AuTD = loadMysqlToPandas(conn, "shgold", "AuTD")
        init_data_size = shgold_AuTD.index.size
        json_data = getDataInJson("shgold")
        shgold_AuTD = appendPdDataForShgold(shgold_AuTD,  json_data, "AuTD")
        print(shgold_AuTD.index.size)

#每隔x小时需要将数据存入DB中
        #saveDataIntoMysql(cur, shgold_AuTD, "AuTD", init_data_size)
        #pdNewData = shgold_AuTD.ix[shgold_AuTD.index[init_data_size:]]
        #print(np.pdNewData['Price'])
        #print(np.array(pdNewData)[0][0])
        #index = index+1

        '''saveDataShGold(cur, conn)
        dumpTableIntoCSV(cur, "AuTD", mysql_secure_path+"./test5.csv")'''

    except pymysql.Error as e: 
        print('Got error {!r}, errno is {}'.format(e, e.args[0]))
    finally:
        cur.close()
        conn.close()
