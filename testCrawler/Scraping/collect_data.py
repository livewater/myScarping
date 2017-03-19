#!/usr/bin/python
# encoding:utf-8
 
import urllib2, json, urllib
import pymysql

#  jisuapi.com require category
category = ["shgold", "bank"]

#shgold 上海黄金交易所
req_list_shgold = ["AuTD", "Pt9995"]
cate_map_shgold = {"AuTD":0, "Pt9995":3}
check_list_shgold = [u"黄金延期", u"迷你黄金延期", u"沪金99", u"沪铂95", u"延期双金", u"沪金100G", u"延期单金", u"IAU100G", u"IAU99.5", u"IAU99.99", u"沪金95"]

#bank 银行账户贵金属价格
req_list_bank = ["AuUSD", "PtUSD", "PdUSD", "AuRMB", "PtRMB", "PdRMB"]
cate_map_bank = {"AuUSD":0, "PtUSD":1, "PdUSD": 2, "AuRMB": 3, "PtRMB": 4, "PdRMB": 5}
check_list_bank = [u"美元账户黄金", u"美元账户铂金", u"美元账户钯金", u"人民币账户黄金", u"人民币账户铂金", u"人民币账户钯金"]

#data = {}
#data["appkey"] = "0fb7150dc4ce3494"
#url_values = urllib.urlencode(data)
def getDataInJson(req):
    #category_values = urllib.urlencode(req)
    #url = "http://api.jisuapi.com/gold/"+ category_values + "?appkey=0fb7150dc4ce3494"
    #request = urllib2.Request(url)
    #result = urllib2.urlopen(request)
    jsonarr = json.load(open('../Test/shgold.json',"r"))
     
    if jsonarr["status"] != u"0": 
        print jsonarr["msg"]
        exit()
    result = jsonarr["result"]
    return result

def saveDataShGold(cur, conn):
    cur.execute("USE shgold")
    result = getDataInJson("shgold")
    index = 0
    for req in req_list_shgold:
        sql_command = "INSERT INTO " + req_list_shgold[index] +" (Price, OpenningPrice, MaxPrice, MinPrice, LastClosingPrice, TradeAmount, UpdateTime) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        if(result[cate_map_shgold[req]]["typename"] == check_list_shgold[cate_map_shgold[req]]):
            cur.execute(sql_command, ( result[cate_map_shgold[req]]["price"], result[cate_map_shgold[req]]["openingprice"], result[cate_map_shgold[req]]["maxprice"], result[cate_map_shgold[req]]["minprice"], result[cate_map_shgold[req]]["lastclosingprice"], result[cate_map_shgold[req]]["tradeamount"], result[cate_map_shgold[req]]["updatetime"]))
            index += 1
        else:
            print("API changed, need to check!\n")
            closeDB(cur, conn)
            exit()
    cur.connection.commit()

'''def saveDataBank(cur, conn):
    cur.execute("USE bank")
    result = getDataInJson("bank")
    index = 0
    for req in req_list_bank:
        sql_command = "INSERT INTO " + req_list_bank[index] + " (MidPrice, BuyPrice, SellPrice, MaxPrice, MinPrice, OpenningPrice, LastClosingPrice, ChangeQuantity, UpdateTime) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
        if(result[cate_map_bank[req]]["typename"] == check_list_bank[cate_map_bank[req]]):
            cur.execute(sql_command, ( result[cate_map_bank[req]]["midprice"], result[cate_map_bank[req]]["buyprice"], result[cate_map_bank[req]]["sellprice"],result[cate_map_bank[req]]["maxprice"], result[cate_map_bank[req]]["minprice"], result[cate_map_bank[req]]["openningprice"], result[cate_map_bank[req]]["lastclosingprice"], result[cate_map_bank[req]]["changequantity"], result[cate_map_bank[req]]["updatetime"]))
            index += 1
        else:
            print("API changed, need to check!\n")
            closeDB(cur, conn)
            exit()
    cur.connection.commit()
'''

#for val in result:
#    print val["type"],val["typename"],val["price"],val["openingprice"],val["maxprice"],val["minprice"],val["changepercent"],val["lastclosingprice"],val["tradeamount"],val["updatetime"]

def closeDB(cur, conn):
    cur.close()
    conn.close()
