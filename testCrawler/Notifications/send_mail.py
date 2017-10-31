import os, sys
import smtplib
from smtplib import SMTP_SSL
from email.header import Header
from email.mime.text import MIMEText

FinDataMailInfo = {
    "from": "549121944@qq.com",
    "to": "gjk282@126.com",
    "hostname": "smtp.qq.com",
    "username": "549121944@qq.com",
    "password": "zxfyzmmltwfsbbii",
    "mailsubject": "FinData Report",
    "mailtext": "hello, this is send mail test.",
    "mailencoding": "utf-8"
}

if __name__ == '__main__':
#def sendMail():
    smtp = SMTP_SSL(FinDataMailInfo["hostname"])
    smtp.set_debuglevel(1)
    smtp.ehlo(FinDataMailInfo["hostname"])
    smtp.login(FinDataMailInfo["username"],FinDataMailInfo["password"])
    
    msg = MIMEText(FinDataMailInfo["mailtext"],"plain",FinDataMailInfo["mailencoding"])
    msg["Subject"] = Header(FinDataMailInfo["mailsubject"],FinDataMailInfo["mailencoding"])
    msg["from"] = FinDataMailInfo["from"]
    msg["to"] = FinDataMailInfo["to"]
    smtp.sendmail(FinDataMailInfo["from"], FinDataMailInfo["to"], msg.as_string())
    smtp.quit()
