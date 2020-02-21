import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
import os

smtpserver = 'smtp.163.com'
smtpport = 465
sender = 'gwac_yw@163.com'
sender_pwd = 'gwacyw1234'
# rece = '876177803@qq.com'
# rece = 'ytzheng@nao.cas.cn'
rece = 'gwac_yw@163.com'
message = MIMEMultipart()

mail_title = 'Observed'
mail_inside = MIMEText(r'This is the goal that has been observed today!','plain','utf-8')

message['FROM'] = sender
message['To'] = rece
message['Subject'] = Header(mail_title,'utf-8')
message.attach(mail_inside)

attr1 = MIMEText(open(r'GWAC_observation_log_20200131.txt','rb').read(),'base64','utf-8')
attr1["content_Type"] = 'application/octet-stream'
attr1["Content-Disposition"] = 'attachment; filename="GWAC_observation_log_20200131.txt"'  # 表示这是附件，名字是啥
message.attach(attr1)

smtpobj = smtplib.SMTP_SSL(smtpserver, port=smtpport)
smtpobj.login(sender, sender_pwd)
smtpobj.sendmail(sender,rece,message.as_string())
print ('send success')
smtpobj.quit
