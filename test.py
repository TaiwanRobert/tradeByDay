#!/usr/bin/env python 
# -*- coding:utf-8 -*-
import datetime
from selenium import webdriver
from bs4 import BeautifulSoup
 # 要想调用键盘按键操作需要引入keys包
from selenium.webdriver.common.keys import Keys
 #创建浏览器对象
driver = webdriver.Chrome(executable_path="../chromedriver_linux64/chromedriver")
URL = "http://www.cninfo.com.cn/new/fulltextSearch?notautosubmit=&keyWord=%E4%B8%9A%E7%BB%A9%E5%BF%AB%E6%8A%A5"
driver.get(URL) 
# 获取当前页面Cookie
soup = BeautifulSoup(driver.page_source, 'html.parser')
tables = soup.findAll('table')
result = list()
tab = tables[-1]
for tr in tab.tbody.findAll('tr'):
    td_list = tr.findAll('td')
    code = str(td_list[0].find("div").find("span").getText())
    string_date = str(td_list[2].find("div").find("span").getText())
    y,m,d = string_date.split("-")
    date = datetime.date(int(y),int(m),int(d))
    print(code, date)
    # result.append([code, date])
# 关闭浏览器
driver.quit()