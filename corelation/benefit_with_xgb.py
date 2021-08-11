import json, math, pickle, os, datetime
from collections import  Counter
import numpy as np
import pandas as pd 
import matplotlib.pyplot as plt
from sklearn.datasets import load_boston
from sklearn.model_selection import train_test_split
import xgboost as xgb,numpy as np
from sklearn.metrics import mean_squared_error  
from xgboost.sklearn import XGBClassifier
from sklearn import metrics   #Additional scklearn functions
from sklearn.preprocessing import OneHotEncoder
from jqdatasdk import *

auth("13601685504","685504")
industry = "zjw"
industry_list = get_industries(name=industry)
print(industry_list.index)
industry_list = list(industry_list.index)
class_num=None

def get_trade_price(code, date):
    for i in range(10):
        try:
            if date.weekday()==5:
                date = date + datetime.timedelta(days=2)
            elif date.weekday()==6:
                date = date + datetime.timedelta(days=1)

            price = get_price(code, start_date=date, end_date=date, frequency='daily', fields=['open', 'close', 'volume'], skip_paused=False, fq='pre', panel=True).values[0][0]
            print(code, date, "use {}".format(i+1))
            return price
        except:
            pass
        date = date + datetime.timedelta(days=1)

def get_industry_one_hot(index):
    r = np.zeros((len(industry_list)))
    r[index] = 1
    return list(r)

def deal_hk_flow(info):
    if info[9] == 0:
        avg=0
    else:
        avg=info[9]/(sum(info[:10])/10)
    if info[29] == 0:
        close_price=0
    else:
        close_price=info[29]/(sum(info[20:30])/10)
    if info[39] == 0:
        v=0
    else:
        v=info[39]/(sum(info[30:40])/10)
    if info[59] == 0:
        hk=0
    else:
        hk=info[59]/(sum(info[50:60])/10)
    return [avg, close_price, v,(sum(info[40:50])/10),hk]
    

def main_deal(cashflow_data, income_data, industry_info, price_vol_hk_hold, stock_weight, future, technical_and_rateBy300, specialist,finance_rate):
    X_list = list()
    Y_list = list()
    price_index,name_index = list(), list()
    real_earn_rate = list()
    for code_key in cashflow_data.keys():
        date_list = list(cashflow_data[code_key].keys())
        date_list.sort()
        for date in date_list:
            y,m,d = date.split("-")
            date_datetime = datetime.date(int(y),int(m),int(d))
            if future:
                if date_datetime < datetime.date(2021, 1, 1) or date_datetime > datetime.date(2021, 3, 31): continue
                # if date_datetime < datetime.date(2021, 4, 1) or date_datetime > datetime.date(2021, 4, 30): continue
                # if date_datetime < datetime.date(2020, 4, 1) or date_datetime > datetime.date(2020, 9, 30): continue
                # if date_datetime < datetime.date(2020, 10, 1) or date_datetime > datetime.date(2020, 12, 31): continue
            else:
                if date_datetime > datetime.date(2020, 3, 31): continue
            # if "2021" not in date and "2020" not in date: continue
            if "price" in cashflow_data[code_key][date] and not math.isnan(cashflow_data[code_key][date]["price"]):
                try:
                    cashflow_key_list = list(cashflow_data[code_key][date].keys())
                    income_key_list = list(income_data[code_key][date].keys())
                except:
                    print("error: {}, {}".format(code_key, date))
                    continue
                if len(cashflow_key_list) <= 1 or len(income_key_list) <= 1: continue
                values = list(cashflow_data[code_key][date].values())
                income_values = list(income_data[code_key][date].values())
                del values[cashflow_key_list.index("price")]
                del income_values[income_key_list.index("price")]
                try:
                    industry_code = industry_info[code_key][date][code_key][industry]["industry_code"]
                except:
                    print(code_key, date, "industry_info error")
                    continue
                industry_one_hot = get_industry_one_hot(industry_list.index(industry_code))
                hk_and_m = deal_hk_flow(price_vol_hk_hold[code_key][date])
                # values + income_values + 
                finance_rate_data = finance_rate[code_key][date]
                X_list.append(hk_and_m + stock_weight[code_key][date]+industry_one_hot+technical_and_rateBy300[code_key][date][:-2]+specialist[code_key][date]+finance_rate_data)
                price_index.append(income_data[code_key][date]["price"])
                name_index.append("{}_{}".format(code_key, date))
                if class_num ==None:
                    Y_list.append(cashflow_data[code_key][date]["price"])
                else:
                    for i in range(class_num):
                        if cashflow_data[code_key][date]["price"] < 0.7+0.05*i:
                            Y_list.append(i)
                            break
                        elif i == class_num-1:
                            Y_list.append(class_num)
                    
                # if cashflow_data[code_key][date]["price"] > 1.5:
                #     Y_list.append(3)
                # elif cashflow_data[code_key][date]["price"] > 1.2:
                #     Y_list.append(2)
                # elif cashflow_data[code_key][date]["price"] > 0.85:
                #     Y_list.append(1)
                # else:
                #     Y_list.append(0)
    
    Y_list = np.array(Y_list)
    return np.array(X_list), Y_list, np.array(price_index), np.array(name_index)

def get_all_data(future=False):
    cashflow = open("./corelation_cashflow.json", "r")
    income = open("./corelation_income.json", "r")
    industry_info = open("./stock_industry.json", "r")
    price_vol_hk_hold = open("./price_volumn_hk.json", "r")
    stock_weight = open("./stock_weight.json", "r")
    technical_and_rateBy300 = open("./technical_and_rateBy300_new.json", "r")
    specialist = open("./specialist.json", "r")
    finance_rate = open("./finance_rate_acceleration.json", "r")
    income_data = json.load(income)
    cashflow_data = json.load(cashflow)
    industry_info = json.load(industry_info)
    price_vol_hk_hold = json.load(price_vol_hk_hold)
    stock_weight = json.load(stock_weight)
    technical_and_rateBy300 = json.load(technical_and_rateBy300)
    specialist = json.load(specialist)
    finance_rate = json.load(finance_rate)
    # X,Y = main_deal(income_data)
    X,Y, price_index, name_index = main_deal(
        cashflow_data, income_data, industry_info, price_vol_hk_hold, 
        stock_weight,future, technical_and_rateBy300, specialist,finance_rate
    )
    return X,Y, price_index, name_index

def relation_filter(X, Y, conf):
    new_X = np.zeros((len(X), 0))
    select_index_list = list()
    for index in range(len(X[0])):
        item_list = X[:,index].reshape((len(X)))
        
        if float(abs(np.corrcoef(item_list,Y)[0][1])) > conf:
            select_index_list.append(index)

    return X[:, select_index_list]

def add_rate(json_obj, normal_deal_rate, key):
    if key in json_obj:
        json_obj[key] += normal_deal_rate
    else:
        json_obj[key] = normal_deal_rate
    # print(len(json_obj[key]), key)
    return json_obj

def print_all_key_rate(json_obj, name):
    for key in json_obj.keys():
        print(name, key, len(json_obj[key]), sum(json_obj[key])/len(json_obj[key]))

if __name__ == "__main__":
    X, Y, price_index, name_index = get_all_data(future=True)# 
    # futureX, futureY, future_price_index, future_name_index = get_all_data(future=True)
    # futureX[futureX == np.nan] = 0
    # X[X] = 0
    # futureX[futureX == -100] = 0
    X[X == -100] = 0
    # 划分数据集，80% 训练数据和 20% 测试数据
    test_size = 0.999
    X_train, X_test, y_train, y_test = train_test_split(X, Y, test_size=test_size, random_state=42)#
    
    train_price_index, test_price_index, train_name_index, test_name_index  = train_test_split(price_index, name_index, test_size=test_size, random_state=42)
    try:
        X_test = np.array(list(X_test)+list(futureX))
        y_test = np.append(y_test, futureY)
        test_price_index = np.append(test_price_index, future_price_index)
        test_name_index = np.append(test_name_index, future_name_index)
    except:
        pass
    print("X_train:{}, X_test:{}, y_train:{}, y_test:{}".format(len(X_train), len(X_test), len(y_train), len(y_test)))
    big_deal_list, normal_deal_list, normal_deal_rate = list(), list(), list()
    neg_big_deal_list, neg_normal_deal_list, neg_normal_deal_rate = list(), list(), list()
    max_depth = {}
    gamma = {}
    colsample_bytree = {}
    reg_lambda = {}
    objective_name = {}
    min_child_weight = {}
    
    # for rm_index in range(len(X_train[0])):
    for md in [-6, -4, -2, 0]: #   0,1
        for ga in [0.05, 0.1, 0.2]: # 0.01,0.03, 0.1, 0.2
            for la in [0.2, 0.5, 1, 2]: # 2,4
                for objective in ["reg:gamma", "reg:tweedie"]: # "reg:gamma", 'reg:squarederror'
                    for min_child in [0.2, 0.5, 0.8]: # 0.8
                        for bytree in [0.5, 0.75, 1]: # 0.5 add 0.8
                            model = xgb.XGBRegressor( #XGBRegressor XGBClassifier
                                silent=1,
                                learning_rate= 0.1,
                                min_child_weight=min_child,
                                max_depth=15+md,
                                gamma=ga,  # 树的叶子节点上作进一步分区所需的最小损失减少,越大越保守，一般0.1、0.2这样子。
                                subsample=1, # 随机采样训练样本 训练实例的子采样比
                                max_delta_step=0,#最大增量步长，我们允许每个树的权重估计。
                                colsample_bytree=bytree, # 生成树时进行的列采样 
                                reg_lambda=la,  # 控制模型复杂度的权重值的L2正则化项参数，参数越大，模型越不容易过拟合。
                                reg_alpha=0, # L1 正则项参数
                                scale_pos_weight=100, #如果取值大于0的话，在类别样本不平衡的情况下有助于快速收敛。平衡正负权重
                                objective= objective,# reg:logistic reg:pseudohubererror 'multi:softmax', #多分类的问题 指定学习任务和相应的学习目标
                                num_class=1, # 类别数，多分类与 multisoftmax 并用
                                n_estimators=10000, #树的个数
                                seed=1, #随机种子
                                gpu_id=0,
                                tree_method='gpu_hist'
                            )
                            # X_train[:, 8] = 0
                            # X_train[:, 9] = 0
                            # X_train[:, 10] = 0
                            # X_test[:, 8] = 0
                            # X_test[:, 9] = 0
                            # X_test[:, 10] = 0

                            model_name = "./add_finance_rate/{}_{}_{}_{}_{}_{}.dat".format(15+md, ga, la, objective.split(":")[-1], min_child, bytree)
                            # if model_name.split("/")[-1] != "23_0.1_2_tweedie_0.2_0.1.dat": continue
                            print("max_depth: {}, gamma: {}, reg_lambda: {}, objective: {}, min_child: {}, colsample_bytree: {}".format(15+md, ga, la, objective, min_child, bytree))
                            print(model_name)
                            if test_size > 0.9 and os.path.isfile(model_name+".txt"):
                                with open(model_name+".txt","r") as f:
                                    try:
                                        conf = float(f.readline().split(":")[1])
                                        f.readline()
                                        if len(f.readline().split(",")) < 10: 
                                            conf = None
                                            continue
                                    except:
                                        continue
                                        # conf = None
                            elif test_size > 0.9:
                                continue
                            if os.path.isfile(model_name):
                                try:
                                    model = pickle.load(open(model_name, "rb"))
                                except:
                                    print("load ", model_name, "error")
                                    continue
                            elif test_size > 0.9 or os.path.isfile(model_name+".txt"):
                                # model = pickle.load(open(model_name, "rb"))
                                continue
                            else:
                                # X_train[:, rm_index] = 0
                                # X_test[:, rm_index] = 0
                                f = open(model_name+".txt","w")
                                model.fit(X_train, y_train)
                                f.close()
                                pickle.dump(model, open(model_name, "wb"))
                            
                            # y_test[y_test>1]=1
                            # y_test[y_test<=1]=0
                            fit_pred = model.predict(X_test)
                            model = None
                            # if n <= 6: continue
                            
                            if class_num:
                                lenOfAll = Counter(fit_pred)
                                for i in range(class_num+1):
                                    if lenOfAll[i]==0:
                                        print(0)
                                    else:
                                        print(sum(test_price_index[fit_pred==i])/lenOfAll[i], lenOfAll[i])
                                print(metrics.classification_report(y_test, fit_pred, labels=None, target_names=None, sample_weight=None, digits=2))
                            else:
                                for i in [1.2,1.3,1.4,1.5,1.6,2]:
                                    lenOfAll = Counter(fit_pred > i)
                                    trueOfAll = Counter(y_test > i)
                                    normal_deal = list(test_price_index[fit_pred > i])
                                    normal_deal_name = list(test_name_index[fit_pred > i])
                                    
                                    if lenOfAll[True]!=0:
                                        rate = round(sum(y_test[fit_pred > i])/lenOfAll[True], 3)
                                        print(rate, lenOfAll[True], trueOfAll[True])
                                        if test_size > 0.9 and i == conf:# 
                                            max_depth =        add_rate(max_depth, normal_deal, 10+md)
                                            gamma =            add_rate(gamma, normal_deal, ga)
                                            colsample_bytree = add_rate(colsample_bytree, normal_deal, bytree)
                                            reg_lambda =       add_rate(reg_lambda, normal_deal, la)
                                            objective_name =   add_rate(objective_name, normal_deal, objective.split(":")[-1])
                                            min_child_weight = add_rate(min_child_weight, normal_deal, min_child)
                                            print(model_name)
                                            with open(model_name+".txt","r") as f:
                                                recode_code_list = f.readlines()[2].split(":")[1].replace("\n", "").split(",")
                                                # for i in recode_code_list:
                                                #     index = list(test_name_index).index(i)
                                                #     print(list(test_price_index)[index], list(fit_pred)[index], list(test_name_index)[index])
                                            big_deal_list += list(test_name_index[np.multiply(y_test > 2, fit_pred > i)])
                                            normal_deal_list += normal_deal_name
                                            normal_deal_rate += normal_deal
                                            print("{} org: {}".format(conf, recode_code_list))
                                        elif test_size <0.9:
                                            if rate > 1.3 and lenOfAll[True]>10:
                                                big_deal_list += list(test_name_index[np.multiply(y_test > 2, fit_pred > i)])
                                                normal_deal_list += normal_deal_name
                                                normal_deal_rate += normal_deal
                                                with open(model_name+".txt","w") as f:
                                                    f.write(f"get more  :{i}:\n")
                                                    f.write(f"big_deal_list:{','.join(test_name_index[np.multiply(y_test > 2, fit_pred > i)])}:\n")
                                                    f.write(f"normal_deal_list:{','.join(list(test_name_index[fit_pred > i]))}:\n")
                                                break
                                            else:
                                                open(model_name+".txt","w")
                                
                                mse = mean_squared_error(y_test, fit_pred)
                                print(mse)
    
    print_all_key_rate(max_depth,"max_depth")
    print_all_key_rate(gamma,"gamma")
    print_all_key_rate(colsample_bytree,"colsample_bytree")
    print_all_key_rate(reg_lambda,"reg_lambda")
    print_all_key_rate(objective_name,"objective")
    print_all_key_rate(min_child_weight,"min_child_weight")
    
    big_deal_list = np.unique(np.array(big_deal_list))
    normal_deal_list = np.unique(np.array(normal_deal_list))
    # normal_deal_rate = np.unique(np.array(normal_deal_rate))
    if len(normal_deal_rate) != 0:
        rate = round(sum(normal_deal_rate)/len(normal_deal_rate), 3)
        print(rate)
    print(big_deal_list, len(big_deal_list))
    print(normal_deal_list, len(normal_deal_list))
    normal_deal_rate = Counter(normal_deal_rate)
    total = list()
    more_than = 10
    for key in normal_deal_rate.keys():
        if normal_deal_rate[key] >= more_than:
            print(key, normal_deal_rate[key])
            total+=[key]*int(normal_deal_rate[key]/more_than)
    print(sum(total)/len(total))

    # for index, name_date in enumerate(test_name_index):
    #     if fit_pred[index] > 1.3:
    #         print(name_date, y_test[index])

    # plt.bar(range(len(model.feature_importances_)), model.feature_importances_)
    # plt.show()
    # np_pre = np.array(fit_pred)
    # np_test_y = np.array(y_test)
    # print(np_pre[np_pre == 2])
    # print(np_test_y[np_test_y == 2])
    # 


    # model = pickle.load(open("model.dat", "rb"))
    # pickle.dump(model, open("model.dat", "wb"))

    # fit_pred = model.predict(X_test[:,:-5])
    # last_p = list()
    # last_y = list()
    # for index, p in enumerate(fit_pred):
    #     if p > 0:
    #         if X_test[index][-5] > 0.04 and X_test[index][-4] > 0.06 and X_test[index][-3] > 2 and (X_test[index][-2] > 3 or X_test[index][-1] > 8):
    #             print(y_test[index])
    #             last_p.append(p)
    #             last_y.append(y_test[index])
    # # np_pre = np.array(fit_pred)
    # # np_test_y = np.array(y_test)
    # # print(np_pre[np_pre == 2])
    # # print(np_test_y[np_test_y == 2])
    # last_y = np.array(last_y)
    # last_y[last_y>1] = 1
    # last_y[last_y<1] = 0
    # print(metrics.classification_report(np.array(last_y), np.array(last_p), labels=[0,1], target_names=["n","p"], sample_weight=None, digits=2))
    