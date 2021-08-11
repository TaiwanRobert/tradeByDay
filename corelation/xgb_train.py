import json, math, pickle
import xgboost as xgb
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

def get_industry_one_hot(index):
    r = np.zeros((len(industry_list)))
    r[index] = 1
    return list(r)

def main_deal(cashflow_data, income_data, industry_info):
    X_list = list()
    Y_list = list()
    for code_key in cashflow_data.keys():
        date_list = list(cashflow_data[code_key].keys())
        date_list.sort()
        for date in date_list:
            if "price" in cashflow_data[code_key][date] and not math.isnan(cashflow_data[code_key][date]["price"]):
                cashflow_key_list = list(cashflow_data[code_key][date].keys())
                income_key_list = list(income_data[code_key][date].keys())
                if len(cashflow_key_list) == 1 or len(income_key_list) == 1: continue
                values = list(cashflow_data[code_key][date].values())
                income_values = list(income_data[code_key][date].values())
                del values[cashflow_key_list.index("price")]
                del income_values[income_key_list.index("price")]
                insustry_code = industry_info[code_key][date][code_key][industry]["industry_code"]
                industry_one_hot = get_industry_one_hot(industry_list.index(insustry_code))
                X_list.append(values+income_values+industry_one_hot)
                if cashflow_data[code_key][date]["price"] > 1 and income_data[code_key][date]["price"] > 1:
                    Y_list.append(1)
                elif cashflow_data[code_key][date]["price"] <= 1 and income_data[code_key][date]["price"] <= 1:
                    Y_list.append(0)
                else:
                    aaaa
    Y_list = np.array(Y_list)
    return np.array(X_list), Y_list

def get_all_data():
    cashflow = open("./corelation_cashflow.json", "r")
    income = open("./corelation_income.json", "r")
    industry_info = open("./stock_industry.json", "r")
    income_data = json.load(income)
    cashflow_data = json.load(cashflow)
    industry_info = json.load(industry_info)
    # X,Y = main_deal(income_data)
    X,Y = main_deal(cashflow_data, income_data, industry_info)
    return X,Y

def relation_filter(X, Y, conf):
    new_X = np.zeros((len(X), 0))
    select_index_list = list()
    for index in range(len(X[0])):
        item_list = X[:,index].reshape((len(X)))
        if abs(np.corrcoef(item_list,Y)) > 0.02:
            select_index_list.append(index)

    print(select_index_list)
    return X[:, select_index_list]

if __name__ == "__main__":
    X, Y = get_all_data()
    X[np.isnan(X)] = 0
    X = relation_filter(X, Y, 0.01)
    # 划分数据集，80% 训练数据和 20% 测试数据
    X_train, X_test, y_train, y_test = train_test_split(X, Y, test_size=0.2, random_state=42)
    print("X_train:{}, X_test:{}, y_train:{}, y_test:{}".format(len(X_train), len(X_test), len(y_train), len(y_test)))

    model = xgb.XGBClassifier(
        silent=0,
        learning_rate= 0.1,
        min_child_weight=0.1,
        max_depth=10,
        gamma=0.01,  # 树的叶子节点上作进一步分区所需的最小损失减少,越大越保守，一般0.1、0.2这样子。
        subsample=1, # 随机采样训练样本 训练实例的子采样比
        max_delta_step=1,#最大增量步长，我们允许每个树的权重估计。
        colsample_bytree=1, # 生成树时进行的列采样 
        reg_lambda=0.01,  # 控制模型复杂度的权重值的L2正则化项参数，参数越大，模型越不容易过拟合。
        reg_alpha=0.01, # L1 正则项参数
        scale_pos_weight=100, #如果取值大于0的话，在类别样本不平衡的情况下有助于快速收敛。平衡正负权重
        objective= 'multi:softmax', #多分类的问题 指定学习任务和相应的学习目标
        num_class=2, # 类别数，多分类与 multisoftmax 并用
        n_estimators=200, #树的个数
        seed=1000 #随机种子

    )
    model.fit(X_train, y_train)
    pickle.dump(model, open("model.dat", "wb"))

    fit_pred = model.predict(X_test)
    # np_pre = np.array(fit_pred)
    # np_test_y = np.array(y_test)
    # print(np_pre[np_pre == 2])
    # print(np_test_y[np_test_y == 2])
    print(metrics.classification_report(y_test, fit_pred, labels=[0,1], target_names=["n","p"], sample_weight=None, digits=2))
    # mse = mean_squared_error(y_test, fit_pred)
    acc = metrics.accuracy_score(y_test, fit_pred)
    print(acc)