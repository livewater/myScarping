import os, sys
import smtplib
from smtplib import SMTP_SSL
from email.header import Header
from email.mime.text import MIMEText

mailInfo = {
    "from": "549121944@qq.com",
    "to": "gjk282@126.com",
    "hostname": "smtp.qq.com",
    "username": "549121944@qq.com",
    #"password": "glg@#568033kai",
    "password": "razatykjngqwbffh",
    "mailsubject": "this is test",
    "mailtext": "hello, this is send mail test.",
    "mailencoding": "utf-8"
}

#if __name__ == '__main__':
def sendMail():
    smtp = SMTP_SSL(mailInfo["hostname"])
    smtp.set_debuglevel(1)
    smtp.ehlo(mailInfo["hostname"])
    smtp.login(mailInfo["username"],mailInfo["password"])
    
    msg = MIMEText(mailInfo["mailtext"],"plain",mailInfo["mailencoding"])
    msg["Subject"] = Header(mailInfo["mailsubject"],mailInfo["mailencoding"])
    msg["from"] = mailInfo["from"]
    msg["to"] = mailInfo["to"]
    smtp.sendmail(mailInfo["from"], mailInfo["to"], msg.as_string())
    smtp.quit()
