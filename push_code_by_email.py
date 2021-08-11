#!/usr/bin/python
# -*- coding:utf-8 -*-
import jqdatasdk
from jqdatasdk import *
import numpy as np
import pandas as pd
import datetime, schedule
import itertools, time
import smtplib
from email.mime.text import MIMEText
from email.utils import formataddr

import requests

my_sender = "r6260329@qq.com"
my_pass = "robert1491491"
my_user = "r6260329@qq.com"
def send_sms(context):
    account_sid = "ACec03c506c5e910cccb5ebabe395d34a8"
    auth_token = "e3da9b6ef5b8da5306c77bdba13bc637"
    client = Client(account_sid, auth_token)

    message = client.messages.create(
                        body="菜鸟教程发送邮件测试",
                        from_='+16105954774',
                        to='+8613601685504'
                    )
    return [True, message]
def mail(context):
    string = "https://email.us-west-2.amazonaws.com?Action=SendEmail&Source=r6260329%40qq.com&Destination.ToAddresses.member.1=r6260329v2%40qq.com&Message.Subject.Data=This%20is%20the%20subject%20line.&Message.Body.Text.Data=Hello.%20I%20hope%20you%20are%20having%20a%20good%20day"
    r = requests.get(string)
    print(r.content)
    # ret=True
    # try:
    #     msg=MIMEText(context,'plain','utf-8')
    #     msg['From']=formataddr(["FromRunoob",my_sender])  # 括号里的对应发件人邮箱昵称、发件人邮箱账号
    #     msg['To']=formataddr(["FK",my_user])              # 括号里的对应收件人邮箱昵称、收件人邮箱账号
    #     msg['Subject']="菜鸟教程发送邮件测试"                # 邮件的主题，也可以说是标题
 
    #     server=smtplib.SMTP_SSL("smtp.qq.com", 465)  # 发件人邮箱中的SMTP服务器，端口是25
    #     server.login(my_sender, my_pass)  # 括号中对应的是发件人邮箱账号、邮箱密码
    #     server.sendmail(my_sender,[my_user,],msg.as_string())  # 括号中对应的是发件人邮箱账号、收件人邮箱账号、发送邮件
    #     server.quit()  # 关闭连接
    # except Exception as e:  # 如果 try 中的语句没有执行，则会执行下面的 ret=False
    #     ret=False
    #     print(e)
    # return ret
 


def run():
    auth("13601685504","685504")
    all_stock = get_all_securities(types=['stock'], date=None).index
    today = datetime.datetime.now()
    
    print(today)
    print(all_stock)
    for n_day in range(30):
        push_code = "符合條件如下：\n"
        focus_day = today - datetime.timedelta(days=n_day)
        today_string = today.strftime("%Y-%m-%d")
        for code in all_stock:
            report   = finance.run_query(query(finance.STK_FIN_FORCAST).filter(finance.STK_FIN_FORCAST.code==code, finance.STK_FIN_FORCAST.pub_date >=today_string))
            report2  = finance.run_query(query(finance.STK_INCOME_STATEMENT).filter(finance.STK_INCOME_STATEMENT.code==code, finance.STK_INCOME_STATEMENT.pub_date >=today_string))
            cashflow_report  = finance.run_query(query(finance.STK_CASHFLOW_STATEMENT).filter(finance.STK_CASHFLOW_STATEMENT.code==code, finance.STK_CASHFLOW_STATEMENT.pub_date >=today_string))
            if len(report) or len(report2) or len(cashflow_report):
                print(focus_day ,code, " has report")
                pre_date = today - datetime.timedelta(days=12)
                info = get_price(code, start_date=pre_date, end_date=today, frequency='daily', fields=['open', 'close', 'volume'], skip_paused=False, fq='pre', panel=True)
                #         print(date)
                preclose = info.values[-2][1]
                prevolume = info.values[-2][-1]
                report_date_open, report_date_close, report_date_volume = info.values[-1]
                if report_date_open/preclose > 1.03 and report_date_close/preclose > 1.06 and report_date_volume/prevolume > 2:
                    string = "{}: 收盤: {}, 上漲：{}%, 成交量比率: {}%\n".format(code,report_date_close, (report_date_close-preclose)*100/preclose, (report_date_volume-prevolume)*100/prevolume)
                    push_code+=string
                    print(string)
        print("++++++++++++:{}".format(focus_day))
        print(push_code)
    ret=send_sms(push_code)

    if ret[0]:
        print(ret[1])
    else:
        print("邮件发送失败")

    print(push_code)
    print("vvvvvvv"*20)

if __name__=='__main__':
    run()
    # schedule.every().hour.do(run, "121321321213")
    #mail("123")
    # schedule.every(10).seconds.do(run)
    # schedule.every().hour.do(run, "121321321213")
    # while True:
    #     schedule.run_pending()
    #     time.sleep(1)