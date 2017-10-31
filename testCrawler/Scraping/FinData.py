#!/usr/bin/python
# encoding:utf-8
import urllib2, json, urllib
import os, sys
import smtplib
import pymysql
import time
import threading
import pandas as pd
from smtplib import SMTP_SSL
from email.header import Header
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart

class FinData(object):
    '''the base class from jisuapi'''
    def __init__(self, url, req, conn):
        self.url = url
        self.req = req
        self.conn = conn
        self.cur = conn.cursor()
        self.mail_info = {
            "from": "549121944@qq.com",
            "to": "gvineyard@163.com",
            #"to": "lingwang@zju.edu.cn",
            "hostname": "smtp.qq.com",
            "username": "549121944@qq.com",
            "password": "zxfyzmmltwfsbbii",
            "mailsubject": "FinData Report",
            #"mailtext": "hello, this is send mail test.",
            "mailencoding": "utf-8"
        }

    def getDataInJson(self):
        #url = "http://api.jisuapi.com/gold/"+ self.req + "?appkey=0fb7150dc4ce3494"
        #url = "http://api.k780.com/?app=finance.gzgold&appkey=29115&sign=51ab5331f653425bced95c234149cc88&format=json"
        request = urllib2.Request(self.url)
        result = urllib2.urlopen(request)
        jsonarr = json.loads(result.read())
    #    jsonarr = json.load(open('../Test/bank.json',"r"))
         
        return jsonarr

    def loadMysqlToPandas(self, product_name, table_name):
        lock = threading.Lock()
        sql_command = "USE " + product_name
        lock.acquire()
        self.cur.execute(sql_command)
        lock.release()
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

    def genDateRange(self, end_tick, hours):
        time_format = "%Y-%m-%d %H:%M:%S"
        start_tick = end_tick - 3600 * hours
        end_time = time.localtime(end_tick)
        start_time = time.localtime(start_tick)
        return [time.strftime(time_format, start_time), time.strftime(time_format, end_time)
]

    def closeDB(self):
        self.cur.close()
        self.conn.close()

    def sendMail(self, mail_msg, mail_pic_path):
        smtp = SMTP_SSL(self.mail_info["hostname"])
        smtp.set_debuglevel(1)
        smtp.ehlo(self.mail_info["hostname"])
        smtp.login(self.mail_info["username"],self.mail_info["password"])
        
        msg = MIMEMultipart()
        msg.attach(MIMEText('<html><body><h1>'+ mail_msg+'</h1>' + '<p><img src="cid:image1"></p>' + '</body></html>', 'html', 'utf-8'))
        fp = open(mail_pic_path, 'rb')
        msgImage = MIMEImage(fp.read())
        fp.close
        msgImage.add_header('Content-ID', '<image1>')

        msg.attach(msgImage)
        #msg = MIMEText(mail_msg,"plain",self.mail_info["mailencoding"])
        msg["Subject"] = Header(self.mail_info["mailsubject"] + "--" + self.genCurrentTime(), self.mail_info["mailencoding"])
        msg["from"] = self.mail_info["from"]
        msg["to"] = self.mail_info["to"]
        smtp.sendmail(self.mail_info["from"], self.mail_info["to"], msg.as_string())
        smtp.quit()
