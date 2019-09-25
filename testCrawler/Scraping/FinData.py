#!/usr/bin/python
# encoding:utf-8
import urllib2, json, urllib
import os, sys
import smtplib
import pymysql
import time
import datetime
import threading
import pandas as pd
from smtplib import SMTP_SSL
from email.header import Header
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart

class FinData(object):
    '''the base class from jisuapi'''
    def __init__(self, debug, url, req, conn):
        self.debug = debug
        self.url = url
        self.req = req
        self.conn = conn
        self.cur = conn.cursor()
        self.time_format = "%Y-%m-%d %H:%M:%S"
        self.mail_info = {
            "from": "549121944@qq.com",
            "to": "gvineyard@163.com",
            "hostname": "smtp.qq.com",
            "username": "549121944@qq.com",
            "password": "kzjzwtazwfykbchb",
            "mailsubject": "FinData Report",
            #"mailtext": "hello, this is send mail test.",
            "mailencoding": "utf-8"
        }

    def getDataInJson(self):
        #url = "http://api.jisuapi.com/gold/"+ self.req + "?appkey=0fb7150dc4ce3494"
        #url = "http://api.k780.com/?app=finance.gzgold&appkey=29115&sign=51ab5331f653425bced95c234149cc88&format=json"
        if self.debug == False:
            request = urllib2.Request(self.url)
            try:
                result = urllib2.urlopen(request)
            except:
                print("##### Open the URL failed, try again!")
                result = urllib2.urlopen(request)
            jsonarr = json.loads(result.read())
        else:
            try:
                with open("../Test/nowapi_gzgold.json", "r") as f:
                    jsonarr = json.load(f)
            except:
                print("##### Cannot open sample json file!")
                sys.exit(-1)
                
        return jsonarr
    #    jsonarr = json.load(open('../Test/bank.json',"r"))

    def loadMysqlToPandas(self, product_name, table_name):
        lock = threading.Lock()
        sql_command = "USE " + product_name
        lock.acquire()
        self.cur.execute(sql_command)
        lock.release()
        sql_command = "SELECT * from "+ table_name
        data = pd.io.sql.read_sql(sql_command, self.conn, coerce_float=False) 
        data = data.drop(['ID'], axis=1)
        return data

    def genCurrentTime(self):
        now = int(time.time()) 
        localtime = time.localtime(now)
        res = time.strftime(self.time_format, localtime)
        return res

    def genDateRange(self, end_tick, hours):
        start_tick = end_tick - 3600 * hours
        end_time = time.localtime(end_tick)
        start_time = time.localtime(start_tick)
        return [time.strftime(self.time_format, start_time), time.strftime(self.time_format, end_time)]

    def genStartEndTime(self):
        now = datetime.datetime.now()
        if self.debug == False:
            self.start_time = now.replace(hour=8, minute=0, second=0)
        else:
            self.start_time = datetime.datetime(2017, 11, 13, 8, 0, 0, 0)
        self.end_time = self.start_time + datetime.timedelta(days=4, hours=20)
        return [self.start_time.strftime(self.time_format), self.end_time.strftime(self.time_format)]

    def closeDB(self):
        self.cur.close()
        self.conn.close()

    def sendMail(self, mail_msg, mail_pic_path, mail_category=''):
        smtp = SMTP_SSL(self.mail_info["hostname"])
        smtp.set_debuglevel(1)
        smtp.ehlo(self.mail_info["hostname"])
        try:
            smtp.login(self.mail_info["username"],self.mail_info["password"])
        except:
            print("##### Mail login failed!")
            sys.exit(-1)
        
        msg = MIMEMultipart()
        msg.attach(MIMEText('<html><body>' + '<p><img src="cid:image1"></p>' + '<h1>'+ mail_msg+'</h1>' + '</body></html>', 'html', 'utf-8'))
        with open(mail_pic_path, 'rb') as fp:
            msgImage = MIMEImage(fp.read())
            fp.close
            
        msgImage.add_header('Content-ID', '<image1>')

        msg.attach(msgImage)
        #msg = MIMEText(mail_msg,"plain",self.mail_info["mailencoding"])
        msg["Subject"] = Header(mail_category +self.mail_info["mailsubject"] + "--" + self.genCurrentTime(), self.mail_info["mailencoding"])
        msg["from"] = self.mail_info["from"]
        msg["to"] = self.mail_info["to"]
        try:
            smtp.sendmail(self.mail_info["from"], self.mail_info["to"], msg.as_string())
        except:
            print("##### Mail sending failed, try again!")
            smtp.sendmail(self.mail_info["from"], self.mail_info["to"], msg.as_string())

        smtp.quit()
