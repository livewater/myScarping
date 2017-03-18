#!/usr/bin/python
# encoding:utf-8
 
import urllib2, json, urllib
import pymysql

#  jisuapi.com require category
data = {}
data["appkey"] = "0fb7150dc4ce3494"
category = ["shgold", "bank"]
cateIndex = {"AuTD":0, "Pt9995":3}

conn = pymysql.connect(host = 'localhost', user = 'root', password = 'glg1117kai', db = 'mysql', charset = 'utf8')
cur = conn.cursor()
cur.execute("USE shgold")
#cur.execute("DESCRIBE AuTD")

def getDataInJson(req):
    #url_values = urllib.urlencode(data)
    #category_values = urllib.urlencode(req)
    #url = "http://api.jisuapi.com/gold/"+ category_values + "?" + url_values
    #request = urllib2.Request(url)
    #result = urllib2.urlopen(request)
    jsonarr = json.load(open('../Test/shgold.json',"r"))
     
    if jsonarr["status"] != u"0":
        print jsonarr["msg"]
        exit()
    result = jsonarr["result"]
    return result

def saveDataAuTD():
    result = getDataInJson("shgold")
    sql_command = "INSERT INTO AuTD (Price, OpenningPrice, MaxPrice, MinPrice, LastClosingPrice, TradeAmount, UpdateTime) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    cur.execute(sql_command, (result[0]["price"], result[0]["openingprice"], result[0]["maxprice"],result[0]["minprice"], result[0]["lastclosingprice"], result[0]["tradeamount"], result[0]["updatetime"]))
    cur.connection.commit()

saveDataAuTD()
#for val in result:
#    print val["type"],val["typename"],val["price"],val["openingprice"],val["maxprice"],val["minprice"],val["changepercent"],val["lastclosingprice"],val["tradeamount"],val["updatetime"]

cur.close()
conn.close()
