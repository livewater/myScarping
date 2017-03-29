#!/usr/bin/python
# encoding:utf-8
 
import urllib2, json, urllib
import pymysql
from collect_data import *
from save_data import *

#  jisuapi.com require category
mysql_secure_path = "/tmp/mysql_file/"
if __name__ == "__main__":
    try:
        conn = pymysql.connect(host = 'localhost', user = 'root', password = 'glg1117kai', db = 'mysql', charset = 'utf8')
        cur = conn.cursor()
        saveDataShGold(cur, conn)
        dumpTableIntoCSV(cur, "AuTD", mysql_secure_path+"./test5.csv")
    except pymysql.Error as e: 
        print('Got error {!r}, errno is {}'.format(e, e.args[0]))
    finally:
        cur.close()
        conn.close()
