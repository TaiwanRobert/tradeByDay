import json
import xgboost as xgb
import numpy as np
import pandas as pd 
import matplotlib.pyplot as plt

from sklearn.datasets import load_boston
from sklearn.model_selection import train_test_split
import xgboost as xgb,numpy as np
from sklearn.metrics import mean_squared_error  
from corelation_download import income_key_list, cashflow_key_list

def get_all_relation():
    cashflow = open("./corelation_cashflow.json", "r")
    income = open("./corelation_income.json", "r")
    income_data = json.load(income)
    cashflow_data = json.load(cashflow)
    
    for key in income_key_list:
        X_list = list()
        Y_list = list()
        for code_key in income_data.keys():
            date_list = list(income_data[code_key].keys())
            date_list.sort()
            for date in date_list:
                if "price" in income_data[code_key][date] and type(income_data[code_key][date]["price"]) == type(1.5):
                    
                    report_key_list = list(income_data[code_key][date].keys())
                    if len(report_key_list) == 1: continue
                    X_list.append(income_data[code_key][date][key])
                    Y_list.append(income_data[code_key][date]["price"])
        X_list = np.array(X_list)
        X_list[np.isnan(X_list)] = 0
        Y_list = np.array(Y_list)
        Y_list[np.isnan(Y_list)] = 1
        print("-----------{} with data len: {}-------------".format(key, X_list.shape))
        print(np.corrcoef(X_list,Y_list))
    for key in cashflow_key_list:
        X_list = list()
        Y_list = list()
        for code_key in cashflow_data.keys():
            date_list = list(cashflow_data[code_key].keys())
            date_list.sort()
            for date in date_list:
                if "price" in cashflow_data[code_key][date] and type(cashflow_data[code_key][date]["price"]) == type(1.5):
                    
                    report_key_list = list(cashflow_data[code_key][date].keys())
                    if len(report_key_list) == 1: continue
                    X_list.append(cashflow_data[code_key][date][key])
                    Y_list.append(cashflow_data[code_key][date]["price"])
        X_list = np.array(X_list)
        X_list[np.isnan(X_list)] = 0
        Y_list = np.array(Y_list)
        Y_list[np.isnan(Y_list)] = 1
        print("-----------{} with data len: {}-------------".format(key, X_list.shape))
        print(np.corrcoef(X_list,Y_list))
    # return np.array(X_list), np.array(Y_list)

if __name__ == "__main__":
    get_all_relation()
    