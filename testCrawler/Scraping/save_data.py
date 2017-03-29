#!/usr/bin/python
# encoding:utf-8
import pymysql
import commands

#外部保证在正确的db中
def dumpTableIntoCSV(cur, table_name, dump_file_name):
    sql_command = "SELECT * from " + table_name + " INTO outfile \'"+ dump_file_name + "\' FIELDS TERMINATED BY \',\' OPTIONALLY ENCLOSED BY \'\"\' ESCAPED BY \'\"\' LINES TERMINATED BY \'\r\n\'"
    shell_command = "rm %s" % (dump_file_name)
    commands.getstatusoutput(shell_command)
    cur.execute(sql_command)

#外部保证在正确的db中
def loadCSVIntoTable(cur, table_name, csv_name):
    sql_command = "LOAD data infile " + csv_name + " INTO TABLE \'"+ table_name + "\' FIELDS TERMINATED BY \',\' OPTIONALLY ENCLOSED BY \'\"\' ESCAPED BY \'\"\' LINES TERMINATED BY \'\r\n\'"
    cur.execute(sql_command)
