#!/usr/bin/python
# encoding:utf-8
 
import urllib2, json, urllib
import pymysql
import sys
from collect_data import *

#  jisuapi.com require category

if __name__ == "__main__":
    conn = pymysql.connect(host = 'localhost', user = 'root', password = 'glg1117kai', db = 'mysql', charset = 'utf8')
    cur = conn.cursor()
    saveDataShGold(cur, conn)


