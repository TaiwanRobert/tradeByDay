#!/usr/bin/python
# -*- coding:utf-8 -*-
import os ,json, time, pickle, sys, xlrd, datetime
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.keys import Keys

URL = "http://webapi.cninfo.com.cn/#/dataBrowse"
def get_web_info():
    driver = webdriver.Chrome(executable_path="../../chromedriver_linux64/chromedriver")
    driver.get(URL)
    time.sleep(3)
    stock_info = open("./corelation_income.json", "r")
    stock_info = json.load(stock_info)
    result = {}
    while True:
        try:
            click1_list = driver.find_element_by_class_name("ul-container").find_elements_by_tag_name('li')
            break
        except:
            time.sleep(0.2)
    for c in click1_list:
        a = c.find_elements_by_tag_name('a')[0]
        if a.text == "投资评级":
            a.click()
            break

    for index, code in enumerate(stock_info.keys()):
        print(code)
        driver.find_element_by_class_name("stock-input").clear()
        driver.find_element_by_class_name("stock-input").send_keys(code.split(".")[0])
        time.sleep(0.1)
        start_date = driver.find_element_by_class_name("start")
        end_date = driver.find_element_by_class_name("end")
        start_date.click()
        start_date.send_keys(Keys.ENTER)
        start_date.send_keys("2015-01-01")
        
        end_date.click()
        end_date.send_keys(Keys.ENTER)
        end_date.send_keys("2021-08-01")
        while True:
            try:
                driver.find_element_by_class_name("stock-search").click()
                time.sleep(0.1)
                driver.find_element_by_class_name("download-btn").click()
                break
            except:
                time.sleep(0.2)
def arrange_download_data():
    path = "/home/robert/quant/tradeByday/specialist_data/"
    for index, name in enumerate(os.listdir(path)):
        if "xls" not in name: continue
        # print("mv {}{} {}{}.csv".format(path,name,path,name.split(".")[0]))
        # os.system("mv {}{} {}{}.csv".format(path,name,path,name.split(".")[0]))
        with open(path+name, 'r') as f:
            data = f.readline()
            soup = BeautifulSoup(data, 'html.parser')
            print(path, name, index)
            if soup.find('table') == None: continue
            for index, tr in enumerate(soup.find('table').findAll('tr')[:2]):
                if index == 0: continue
                td_list = tr.findAll('td')
                # print(td_list)
                code = td_list[0].getText()
                pub_date = td_list[2].getText()
            
            os.system("mv {}{} {}{}.xls".format(path, name, path, code))

class Arrange(object):
    def __init__(self):
        self.recommend = list()
        self.pre_recommend = list()
        self.is_first = list()

    def set_all_status(self, table):
        for index, tr in enumerate(table.findAll('tr')):
            if index == 0: continue
            td_list = tr.findAll('td')
            pre_recommend = td_list[9].getText()
            recommend = td_list[6].getText()
            is_first = td_list[7].getText()
            if recommend not in self.recommend:
                self.recommend.append(recommend)
            
            if pre_recommend not in self.pre_recommend:
                self.pre_recommend.append(pre_recommend)
            
            if is_first not in self.is_first:
                self.is_first.append(is_first)
        
        print(self.recommend, self.pre_recommend, self.is_first)
    
    def get_industry_one_hot(self, index, type_list):
        r = np.zeros((len(type_list)))
        r[index] = 1
        return r

    def get_arrange_data(self,table,date):
        if type(date) == type(""):
            y,m,d = date.split("-")
            edate = datetime.date(int(y),int(m),int(d))
        else:
            edate = date
        
        sdate = edate - datetime.timedelta(days=180)
        result = list()
        recommend_one_hot, pre_recommend_one_hot, is_first_one_hot =\
            np.zeros((len(self.recommend))), np.zeros((len(self.pre_recommend))), np.zeros((len(self.is_first)))
        for index, tr in enumerate(table.findAll('tr')):
            if index == 0: continue
            td_list = tr.findAll('td')
            y,m,d = td_list[2].getText().split("-")
            pre_recommend = td_list[9].getText()
            recommend = td_list[6].getText()
            is_first = td_list[7].getText()

            real_pub_date = datetime.date(int(y),int(m),int(d))
            if real_pub_date >= sdate and real_pub_date < edate:
                recommend_index = self.recommend.index(recommend)
                pre_recommend_index = self.pre_recommend.index(pre_recommend)
                is_first_index = self.is_first.index(is_first)
                recommend_one_hot += self.get_industry_one_hot(recommend_index, self.recommend)
                pre_recommend_one_hot += self.get_industry_one_hot(pre_recommend_index, self.pre_recommend)
                is_first_one_hot += self.get_industry_one_hot(is_first_index, self.is_first)
        return list(recommend_one_hot)+list(pre_recommend_one_hot)+list(is_first_one_hot)

def arrange_class_init(stock_info_path = "./corelation_income.json"):
    stock_info = open(stock_info_path, "r")
    path = "/home/robert/quant/tradeByday/specialist_data/"
    stock_info = json.load(stock_info)
    arr = Arrange()
    for index, code in enumerate(list(stock_info.keys())[:500]):
        file_path = path+code.split(".")[0]+".xls"
        print(file_path)
        if not os.path.isfile(file_path): continue
        with open(file_path, 'r') as f:
            table = BeautifulSoup(f.readline(), 'html.parser').find('table')
            if table:
                arr.set_all_status(table)
    return arr

def main_specialist():
    stock_info = open("./corelation/corelation_income.json", "r")
    path = "/home/robert/quant/tradeByday/specialist/"
    stock_info = json.load(stock_info)
    result = {}
    arr = arrange_class_init()
    
    # arr.recommend.append('')
    # arr.pre_recommend.append('')
    print(arr.recommend)
    print(arr.pre_recommend)
    print(arr.is_first)
    for index, code in enumerate(stock_info.keys()):
        print(code)
        result[code] = {}
        file_path = path+code.split(".")[0]+".xls"
        if os.path.isfile(file_path):
            with open(path+code.split(".")[0]+".xls", 'r') as f:
                data = f.readline()
                soup = BeautifulSoup(data, 'html.parser')
                table = soup.find('table')
                for date in stock_info[code].keys():
                    
                    if table == None:
                        result[code][date] = [None]*(len(arr.pre_recommend)+len(arr.recommend)+len(arr.is_first))
                    else:
                        result[code][date] = arr.get_arrange_data(table, date)
        else:
            for date in stock_info[code].keys():
                result[code][date] = [None]*(len(arr.pre_recommend)+len(arr.recommend)+len(arr.is_first))
    with open("./specialist.json", "w") as f:
        json.dump(result, f)

class SpecialForPushCode(object):
    def __init__(self, driver):
        self.arr = arrange_class_init(stock_info_path = "./corelation/corelation_income.json")
        self.driver = driver
        self.driver.get(URL)
        time.sleep(2)
        
        while True:
            try:
                self.click1_list = driver.find_element_by_class_name("ul-container").find_elements_by_tag_name('li')
                break
            except:
                time.sleep(0.5)
        self.click_one_key(self.click1_list)
    def click_one_key(self,click1_list):
        for c in click1_list:
            a = c.find_elements_by_tag_name('a')[0]
            if a.text == "投资评级":
                a.click()
                break

    def one_code_specialist(self, code, date):
        print(code, date)
        driver = self.driver
        
        
        time.sleep(0.5)
        sdate = date - datetime.timedelta(days=180)
        string_sdate = sdate.strftime("%Y-%m-%d")
        string_edate = date.strftime("%Y-%m-%d")
        while True:
            try:
                driver.find_element_by_class_name("stock-input").clear()
                driver.find_element_by_class_name("stock-input").send_keys(code.split(".")[0])
                time.sleep(0.1)
                start_date = driver.find_element_by_class_name("start")
                time.sleep(0.1)
                end_date = driver.find_element_by_class_name("end")
                time.sleep(0.2)
                break
            except:
                time.sleep(1)
                # self.click_one_key(self.click1_list)
                return self.one_code_specialist(code, date)
                time.sleep(1)
        while True:
            try:
                start_date.click()
                start_date.send_keys(Keys.ENTER)
                time.sleep(0.1)
                start_date.send_keys(string_sdate)
                time.sleep(0.1)
                break
            except:
                time.sleep(0.5)
                self.click_one_key(self.click1_list)
                time.sleep(1)
        
        while True:
            try:
                end_date.click()
                time.sleep(0.1)
                end_date.send_keys(Keys.ENTER)
                time.sleep(0.1)
                end_date.send_keys(string_edate)
                break
            except:
                time.sleep(0.5)
        while True:
            try:
                time.sleep(0.1)
                driver.find_element_by_class_name("stock-search").click()
                time.sleep(0.1)
                break
            except:
                print("stock-search error")
                time.sleep(0.3)
        result = np.array([0]*(len(self.arr.pre_recommend)+len(self.arr.recommend)+len(self.arr.is_first)))
        error_num = 0
        while True:
            try:
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                table = soup.find('table',id="contentTable").find('tbody')
                if len(table.findAll('tr')) ==1 and table.findAll('tr', class_="no-records-found") == 1:
                    print("no found any ============")
                    return list(result)
                else:
                    result += np.array(self.arr.get_arrange_data(table, date), dtype = np.int64)
                    time.sleep(1.5)
                    error_num = 0
                    while True:
                        try:
                            time.sleep(0.5)
                            driver.find_element_by_class_name("page-next").click()
                            break
                        except:
                            error_num+=1
                            if error_num > 10:
                                print("no page-next ============")
                                return list(result)
                    time.sleep(1)
                    soup = BeautifulSoup(driver.page_source, 'html.parser')
                    if soup.find('ul', class_="pagination").find('li', class_="active").getText() == "1":
                        print("done one range ============")
                        return list(result)
            except Exception as e:
                time.sleep(0.2)
                print(e)
                error_num+=1
                if error_num > 10:
                    return self.one_code_specialist(code, date)
        
        

if __name__ == "__main__":
    # get_web_info()
    # arrange_download_data()
    main_specialist()

    