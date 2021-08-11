#!/usr/bin/python
# -*- coding:utf-8 -*-
import jqdatasdk, os ,json, time, pickle
import jqdatasdk as jq
from jqdatasdk import *
import numpy as np
import pandas as pd
import datetime, schedule
import itertools, time
import threading

import requests
import sys
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from corelation.corelation_download import (get_all_report_data, 
price_volumn_hk_big_buy, allStockGroupIndex, get_one_technical, get_one_finance_rate)
from corelation.benefit_with_xgb import deal_hk_flow
from corelation.specialist import SpecialForPushCode

def get_date_info():
    today = datetime.datetime.now()
    if today.weekday() == 0:
        date_from = (today - datetime.timedelta(days=2)).date()
    else:
        date_from = today.date()
    return today, date_from
auth("13601685504","685504")
driver = webdriver.Chrome(executable_path="../chromedriver_linux64/chromedriver")

fix_url = "http://www.cninfo.com.cn/new/fulltextSearch?notautosubmit=&keyWord=%E4%B8%9A%E7%BB%A9%E9%A2%84%E5%91%8A%E4%BF%AE%E6%AD%A3"
quick_url = "http://www.cninfo.com.cn/new/fulltextSearch?notautosubmit=&keyWord=%E4%B8%9A%E7%BB%A9%E5%BF%AB%E6%8A%A5"
report_url = "http://www.cninfo.com.cn/new/fulltextSearch?notautosubmit=&keyWord=%E4%B8%9A%E7%BB%A9%E5%85%AC%E5%91%8A"
pre_url = "http://www.cninfo.com.cn/new/fulltextSearch?notautosubmit=&keyWord=%E4%B8%9A%E7%BB%A9%E9%A2%84%E5%91%8A"
finance_url = "http://www.cninfo.com.cn/new/fulltextSearch?notautosubmit=&keyWord=财务报告"
finance2_url = "http://www.cninfo.com.cn/new/fulltextSearch?notautosubmit=&keyWord=财务报表"
finance3_url = "http://www.cninfo.com.cn/new/fulltextSearch?notautosubmit=&keyWord=半年度"

industry = "zjw"
industry_list = get_industries(name=industry)
print(industry_list.index)
industry_list = list(industry_list.index)

def get_industry_one_hot(index):
    r = np.zeros((len(industry_list)))
    r[index] = 1
    return list(r)

class myThread (threading.Thread):
    def __init__(self, threadID, code_list, date_from):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.code_list = code_list
        self.date_from = date_from
    def run(self):
        print ("开始线程：{}".format(self.threadID))
        self.result = morning_run_threading(self.code_list, self.date_from)
        print ("退出线程：{}".format(self.threadID))
    def get(self):
        return self.result



def get_code_total(code):
    if code[:3] in ["600","601", "603", "605", "900", "688"]:
        return code + ".XSHG"
    elif len(code)==6 and code[:2] in ["00", "20", "30"]:
        return code + ".XSHE"
    else:
        return False

def check_pre_report(code, date):
    report  = finance.run_query(query(finance.STK_FIN_FORCAST).filter(finance.STK_FIN_FORCAST.code==code, finance.STK_FIN_FORCAST.pub_date ==date))
    if len(report.values) > 0 and report.values[0][-2] != None and int(report.values[0][-2])<20 and report.values[0][-6] > 0:
        return False
    return True

def get_web_info(URL,today):
    today, date_from = get_date_info()
    f = open("./today_stock.json", "r")
    result = json.load(f)
    f.close()
    driver.get(URL)
    time.sleep(1)
    page = 0
    
    while True:
        page += 1
        continue_bool = False
        time.sleep(1)
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        # soup=BeautifulSoup(html,'html.parser')

        tables = soup.findAll('table', class_="el-table__body")
        tab = tables[-1]
        number = len(tab.tbody.findAll('tr'))
        print("get data number: {}".format(number))
        for index, tr in enumerate(tab.tbody.findAll('tr')):
            td_list = tr.findAll('td')
            org_code = str(td_list[0].find("div").find("span").getText())
            code = get_code_total(org_code)
            
            string_date = str(td_list[2].find("div").find("span").getText())
            string_date = string_date.replace("\n", "").replace("  ", "")
            if len(string_date.split(" ")) != 1:
                y,m,d = string_date.split(" ")[0].split("-")
                date = datetime.date(int(y),int(m),int(d)) + datetime.timedelta(days=1)
            else:
                y,m,d = string_date.split("-")
                date = datetime.date(int(y),int(m),int(d))
            
            print(code, string_date, today.date())
            if date >= date_from and date <= today.date():
                if code:
                    if check_pre_report(code, today.date()):
                        result.append(code)
                else:
                    print("**********{} 沒取得對應交易市場".format(org_code))
            if number == index+1 and date >= date_from:
                print("will click next page :{}".format(page+1))
                all_li = driver.find_element_by_class_name("el-pager").find_elements_by_tag_name('li')
                for li in all_li:
                    print(li.text)
                    if li.text == str(page+1):
                        li.click()
                        continue_bool = True
                        break
        if continue_bool:
            continue
        else:
            break
    result = list(np.unique(np.array(result)))
    with open("./today_stock.json", "w") as f:
        json.dump(result, f)
    return result

def do_xgb(code_list, today):
    path = "./corelation/not_ready_weight/"
    model_list, conf_list = list(), list()
    model_name_list = os.listdir(path)
    dat_name_list, txt_name_list = list(),list()
    for model_name in model_name_list:
        if "txt" in model_name:
            txt_name_list.append(model_name)
        else:
            dat_name_list.append(model_name)
    model_name_list = list()
    for model_name in dat_name_list:
        if model_name + ".txt" in txt_name_list:
            with open("{}{}.txt".format(path, model_name), "r") as f:
                try:
                    conf = float(f.readline().split(":")[1])
                except:
                    continue
            model = pickle.load(open("{}{}".format(path, model_name), "rb"))
            model_list.append(model)
            conf_list.append(conf)
            model_name_list.append(model_name)
    stock_weight_class =  allStockGroupIndex(today)
    sfpc = SpecialForPushCode(driver)
    x_list = list()
    after_code_list = list()
    if today.date().weekday() == 0:
        pre_days = 3
    else:
        pre_days = 1
    for days_num in range(pre_days):
        report_date = today.date() - datetime.timedelta(days=days_num)
        for code in code_list:
            industry_type = get_industry(code, date=today)[code]
            if industry in industry_type:
                code_industry_key = industry_type[industry]['industry_code']
                industry_one_hot = get_industry_one_hot(industry_list.index(code_industry_key))
                
                report_data = get_all_report_data(code, report_date)
                
                if report_data:
                    print("xgb deal: {}".format(code))
                    hk_and_m = deal_hk_flow(price_volumn_hk_big_buy(code, report_date))
                    specialist_data =  sfpc.one_code_specialist(code, report_date)
                    print(specialist_data)
                    X = report_data+hk_and_m+stock_weight_class.get_stock_weight_info(code)+industry_one_hot+get_one_technical(code, report_date)+specialist_data#+get_one_finance_rate(code, report_date)
                    X[8] = 0
                    X[9] = 0
                    X[10] = 0
                    x_list.append(X)
                    print("data len : {}".format(len(X)))
                    print(code, report_date, X)
                    after_code_list.append(code)
                
    after_code_list = np.array(after_code_list)
    if len(x_list) ==0: return 
    x_list = np.array(x_list)
    # x_list[np.isnan(x_list)] = 0
    
    for index, model in enumerate(model_list):
        result = model.predict(x_list)
        print("======機器學習成果======")
        code_result = after_code_list[result > conf_list[index]] #
        print(model_name_list[index],code_result) #
        if len(code_result)>=1:
            for code in code_result:
                print("[avg, close_price, v,(sum(info[40:50])/10),hk]")
                print(deal_hk_flow(price_volumn_hk_big_buy(code, report_date)))

def afternoon_run():
    today, date_from = get_date_info()
    f = open("./today_stock.json", "r")
    code_list = json.load(f)
    do_xgb(code_list, today)
    for code in code_list:
        print(get_query_count())
        pre_date = today - datetime.timedelta(days=12)
        pre_one_date = today - datetime.timedelta(days=1)
        try:
            info =  get_price(code, start_date=pre_date, end_date=today, frequency='daily', fields=['open', 'close', 'volume'], skip_paused=False, fq='pre', panel=True)
        except:
            print("{} 可能已退市".format(code))
            continue
        # print(date)
        preclose = info.values[-2][1]
        prevolume = info.values[-2][-1]
        report_date_open, report_date_close, report_date_volume = info.values[-1]
        if report_date_open/preclose > 1.03 and report_date_close/preclose > 1.06 and report_date_volume/prevolume > 2:
            hk_hold_info = finance.run_query(  query( finance.STK_HK_HOLD_INFO).filter( finance.STK_HK_HOLD_INFO.code==code,  finance.STK_HK_HOLD_INFO.day == pre_one_date.strftime("%Y-%m-%d")))
            if len(hk_hold_info.values) != 0:
                hk_hold_percent = hk_hold_info.values[0][-1]
            else:
                hk_hold_percent = 0
            price_rate = (report_date_close-preclose)*100/preclose
            volumn_rate = (report_date_volume-prevolume)*100/prevolume
            string = "{}: 收盤: {}, 上漲：{}%, 成交量比率: {}%, 外資持股比例: {}%\n".format(code,report_date_close, price_rate, volumn_rate, hk_hold_percent)
            print(string)
    f.close()
    print("afternoon_run ok")

def get_report(code,date_from):
    while True:
        try:
            report =  finance.run_query( query( finance.STK_FIN_FORCAST).filter( finance.STK_FIN_FORCAST.code==code,  finance.STK_FIN_FORCAST.pub_date >=date_from))
            report2 =  finance.run_query( query( finance.STK_INCOME_STATEMENT).filter( finance.STK_INCOME_STATEMENT.code==code,  finance.STK_INCOME_STATEMENT.pub_date >=date_from))
            cashflow_report =  finance.run_query( query( finance.STK_CASHFLOW_STATEMENT).filter( finance.STK_CASHFLOW_STATEMENT.code==code,  finance.STK_CASHFLOW_STATEMENT.pub_date >=date_from))
            return report, report2, cashflow_report
        except:
            print("error")
            time.sleep(1)
def morning_run_threading(code_list, date_from):
    date_from = date_from.strftime("%Y-%m-%d")
    push_code = "符合條件如下：\n"
    focus_stock_list = list()
    for code in code_list:
        print(code)
        report, report2, cashflow_report = get_report(code,date_from)
        if len(report) or len(report2) or len(cashflow_report):
            focus_stock_list.append(code)
            
    
    print(focus_stock_list)
    return focus_stock_list

def morning_run():
    today = datetime.datetime.now()
    hours = datetime.datetime.now().strftime("%H")
    print(f"hours: {hours}")
    # all_stock =  get_all_securities(types=['stock'], date=None).index
    # all_stock = list(all_stock)
    # code_num = len(all_stock)
    # focus_stock_list = list()
    # process_list = list()
    # batch_size = 10
    # for i in range(batch_size):
    #     batch_num = int(code_num/batch_size)
    #     if i == 4:
    #         code_list = all_stock[i*batch_num:]
    #     else:
    #         code_list = all_stock[i*batch_num:(i+1)*batch_num]
        
    #     process_list.append(myThread(i, code_list,date_from))
    #     process_list[-1].start()
    # for p in process_list:
    #     p.join()
    # for p in process_list:
    #     focus_stock_list += p.get()
    # with open("./today_stock.json", "a+") as f:
    #     json.dump(focus_stock_list, f)
    with open("./today_stock.json", "w") as f:
        json.dump([], f)
    get_web_info(finance_url,today)
    get_web_info(finance2_url,today)
    get_web_info(finance3_url,today)
    
    print("開始業績公告查詢")
    get_web_info(report_url,today)
    time.sleep(1)
    print("開始業績修正公告查詢")
    get_web_info(fix_url,today)
    print("開始業績快報查詢")
    time.sleep(1)
    get_web_info(quick_url,today)
    print("開始業績預告查詢")
    time.sleep(1)
    get_web_info(pre_url,today)
    
if __name__=='__main__':
    hours = datetime.datetime.now().strftime("%H")
    print(f"hours: {hours}")
    # get_web_info(fix_url)
    # get_web_info(quick_url)
    morning_run()
    afternoon_run()
    
    # if int(hours) > 15: 
    #     afternoon_run()

    schedule.every().day.at("06:50").do(morning_run)
    schedule.every().day.at("15:00").do(afternoon_run)
    while True:
        schedule.run_pending()
        time.sleep(1)