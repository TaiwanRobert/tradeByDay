from jqdatasdk import *

from jqdatasdk.technical_analysis import *
# from jqfactor import get_factor_values, get_all_factors
import os ,json, time

import numpy as np
import pandas as pd
import datetime, schedule
import itertools, time
import threading

import requests

auth("13601685504","685504")
print(get_query_count())
income_key_list = ["total_operating_revenue", "operating_revenue", "total_operating_cost", "operating_cost", 
                    "sale_expense", "administration_expense", "investment_income", "invest_income_associates", 
                    "operating_profit", "subsidy_income", 
                    "non_operating_revenue", "non_operating_expense", "disposal_loss_non_current_liability", 
                    "total_profit",
                    "net_profit", "rd_expenses", "credit_impairment_loss"]
cashflow_key_list = [
    "goods_sale_and_service_render_cash", "subtotal_operate_cash_inflow", "goods_and_services_cash_paid", "staff_behalf_paid",
    "subtotal_operate_cash_outflow", "invest_withdrawal_cash",
    "fix_intan_other_asset_dispo_cash", "net_cash_deal_subcompany", "subtotal_invest_cash_inflow",
    "impawned_loan_net_increase", "net_cash_from_sub_company", "net_invest_cash_flow", "cash_from_invest",
    "cash_from_borrowing", "cash_from_bonds_issue", "subtotal_finance_cash_inflow", "borrowing_repayment",
    "subtotal_finance_cash_outflow", "cash_equivalent_increase", 
    "assets_depreciation_reserves", "fixed_assets_depreciation", "invest_loss", 
    "inventory_decrease", "operate_receivables_decrease", "operate_payable_increase", 
    "debt_to_capital", "cbs_expiring_in_one_year", "cash_at_end",
    "investment_property_depreciation"
]
balance_key_list = [
    "total_current_assets", "longterm_equity_invest", "fixed_assets", "total_assets"
]
exchange_dict = {
    "investment_income": "total_current_assets",
    "invest_income_associates": "total_current_assets",
    "operating_profit": "total_assets",
    "subsidy_income": "total_assets",
    "non_operating_revenue": "total_assets",
    "non_operating_expense": "total_assets",
    "disposal_loss_non_current_liability": "total_assets",
    "total_profit": "total_assets",
    "net_profit": "total_assets",
    "rd_expenses": "total_assets",
    "credit_impairment_loss": "total_assets",
    "invest_withdrawal_cash": "total_current_assets",
    "fix_intan_other_asset_dispo_cash": "total_assets",
    "net_cash_deal_subcompany": "total_assets",
    "subtotal_invest_cash_inflow": "total_assets",
    "impawned_loan_net_increase": "total_assets",
    "net_invest_cash_flow": "total_assets",
    "cash_from_borrowing": "total_assets",
    "subtotal_finance_cash_inflow": "total_assets",
    "borrowing_repayment": "total_current_assets",
    "subtotal_finance_cash_outflow": "total_assets",
    "cash_equivalent_increase": "total_assets",
    "assets_depreciation_reserves": "total_assets",
    "fixed_assets_depreciation": "total_assets",
    "invest_loss": "total_assets",
    "inventory_decrease": "total_assets",
    "operate_receivables_decrease": "total_assets",
    "operate_payable_increase": "total_assets",
    "debt_to_capital": "total_assets",
    "cbs_expiring_in_one_year": "total_assets"

}
def get_true_value(the_value, pre_value, key, base_info, pre_base_info):
    if key not in exchange_dict.keys():
        if pre_value ==0: return 1
        
        return the_value/abs(pre_value)
    else:
        return (the_value-pre_value)/base_info[exchange_dict[key]][0]

def get_pre_trade_price(code, date):
    for i in range(10):
        date = date - datetime.timedelta(days=1)
        try:
            if date.weekday()==6:
                date = date - datetime.timedelta(days=2)
            elif date.weekday()==5:
                date = date - datetime.timedelta(days=1)

            price, p_close, volume, avg = get_price(code, start_date=date, end_date=date, frequency='daily', fields=['open', 'close', 'volume', 'avg'], skip_paused=False, fq='pre', panel=True).values[0]
            return price, p_close, volume, avg, i+1
        except:
            pass
    aaa
def get_trade_price(code, date, just_the_date = False):
    for i in range(10):
        try:
            if not just_the_date:
                if date.weekday()==5:
                    date = date + datetime.timedelta(days=2)
                elif date.weekday()==6:
                    date = date + datetime.timedelta(days=1)

            price, p_close, volume, avg = get_price(code, start_date=date, end_date=date, frequency='daily', fields=['open','close', 'volume', 'avg'], skip_paused=False, fq='pre', panel=True).values[0]
            # print(code, date, "use {}".format(i+1))
            return price, p_close, volume, avg
        except:
            if just_the_date:
                return None,None,None,None
            else:
                pass
        date = date + datetime.timedelta(days=1)
    aaa

def query_report(code, std_end_date, query_from, date=None):
    if date:
        return finance.run_query(
                    query(query_from).filter(
                        query_from.code==code, 
                        query_from.report_type==0,
                        query_from.pub_date==date,
                        query_from.end_date == std_end_date
                        ).limit(40)
                    )
    else:
        return finance.run_query(
                    query(query_from).filter(
                        query_from.code==code, 
                        query_from.report_type==0,
                        query_from.end_date == std_end_date
                        ).limit(40)
                    )
def get_std_date_and_pre(date):
    if date.month <= 3:
        std_end_date = datetime.date(date.year-1, 12, 31)
    elif date.month <= 6:
        std_end_date = datetime.date(date.year, 3, 31)
    elif date.month <= 9:
        std_end_date = datetime.date(date.year, 6, 30)
    else:
        std_end_date = datetime.date(date.year, 9, 30)
    pre_std_end_date = datetime.date(std_end_date.year-1,std_end_date.month, std_end_date.day)
    return std_end_date, pre_std_end_date

def get_all_report_data(code, date):
    # finance.STK_CASHFLOW_STATEMENT, finance.STK_INCOME_STATEMENT
    
    data = list()
    std_end_date, pre_std_end_date = get_std_date_and_pre(date)
    
    base_info = query_report(code, std_end_date, finance.STK_BALANCE_SHEET, date)
    pre_base_info = query_report(code, pre_std_end_date, finance.STK_BALANCE_SHEET)
    print(std_end_date, pre_std_end_date)
    for key_list, query_from in [(cashflow_key_list, finance.STK_CASHFLOW_STATEMENT), (income_key_list, finance.STK_INCOME_STATEMENT)]:
        report = query_report(code, std_end_date, query_from, date)
        if len(report.values) == 0: return None
        prereport = query_report(code, pre_std_end_date, query_from)
        if len(prereport.values) == 0: return None
        for key in key_list:
            try:
                data.append(get_true_value(report[key][0], prereport[key][0], key, base_info, pre_base_info))
            except:
                data.append(1)
        
    return data

def create_finance_data(all_stock, data_type, query_from, key_list):
    code_data, the_standard_report = {}, {}
    for code in all_stock:
        print(get_query_count())
        # if code not in code_date_relation:
        #     code_date_relation[code] = {}
        code_data[code] = {}
        print(code)
        # try:
        for key in  key_list:
            the_standard_report[key] = {}
        report = finance.run_query(
            query(query_from).filter(
                query_from.code==code, 
                query_from.report_type==0, 
                query_from.pub_date >= "2015-01-01"
                ).limit(40)
            )
        for index, one_report in enumerate(report.values):
            pub_date = one_report[7].strftime("%Y-%m-%d")
            
            date_key = one_report[9].strftime("%m-%d")
            
            print("start: {}, end: {}, pub: {}".format(one_report[8],one_report[9],one_report[7]))
            code_data[code][pub_date] = {}
            if len(the_standard_report[key_list[0]]) == 0 and one_report[9]-one_report[8] > datetime.timedelta(days=360): continue
            
            std_end_date, pre_std_end_date = get_std_date_and_pre(one_report[7])
            base_info = query_report(code, std_end_date, finance.STK_BALANCE_SHEET, pub_date)
            pre_base_info = query_report(code, pre_std_end_date, finance.STK_BALANCE_SHEET)
            
            if date_key in the_standard_report[key_list[0]]:
                for key in key_list:
                    try:
                        code_data[code][pub_date][key] = get_true_value(report[key][index], the_standard_report[key][date_key], key, base_info, pre_base_info)
                    except:
                        code_data[code][pub_date][key] = 1
                    # if report[key][index] and the_standard_report[key][date_key] and the_standard_report[key][date_key] != 0:
                    #     code_data[code][pub_date][key] = report[key][index] / abs(the_standard_report[key][date_key])
                    # else:
                    #     code_data[code][pub_date][key] = 1
            _, start_price, _, _ = get_trade_price(code, one_report[7])
            sell_date = one_report[7] + datetime.timedelta(days=90)
            _, end_price, _, _ = get_trade_price(code, sell_date)
            if start_price and end_price:
                code_data[code][pub_date]["price"] = end_price/start_price
            else:
                code_data[code][pub_date]["price"] = 1
            for key in  key_list:
                the_standard_report[key][date_key] = report[key][index]
        # except:
        #     print(code, " get error")
    with open("./corelation_{}.json".format(data_type), "w") as f:
        json.dump(code_data, f)

def get_stock_industry():
    stock_info = open("./corelation_income.json", "r")
    stock_info = json.load(stock_info)
    result = {}
    for code in stock_info.keys():
        print(code, get_query_count())
        result[code]={}
        for date in stock_info[code].keys():
            result[code][date] = get_industry(code, date=date)
    with open("./stock_industry.json", "w") as f:
        json.dump(result, f)
    # return code_date_relation

def price_volumn_hk_big_buy(code, date):
    if type(date) == type("132"):
        y,m,d = date.split(" ")[0].split("-")
        real_date = datetime.date(int(y),int(m),int(d))
    else:
        real_date = date
    hk_hold_info =finance.run_query( 
        query(finance.STK_HK_HOLD_INFO).filter(
            finance.STK_HK_HOLD_INFO.code==code, 
            finance.STK_HK_HOLD_INFO.day ==date
        ).order_by(finance.STK_HK_HOLD_INFO.day.desc())
    )
    pre_open, pre_close, pre_volume = None,None,None
    day_change = 1
    hk_hold_list, money_flow_list,v_list,open_rate_list,close_rate_list = list(), list(), list(), list(), list()
    avg_list = list()
    while True:
        pre_date = real_date - datetime.timedelta(days=1)
        if pre_open:
            the_open, the_close, the_volume, avg = pre_open, pre_close, pre_volume, pre_avg
        else:
            the_open, the_close, the_volume, avg = get_trade_price(code, real_date, just_the_date=True)
        # print(the_open, the_close, the_volume, avg, real_date)
        if not the_open: 
            real_date -= datetime.timedelta(days=1)
            continue
        
        pre_open, pre_close, pre_volume, pre_avg, day_change = get_pre_trade_price(code, real_date)
        money_flow = get_money_flow(code, start_date=real_date, end_date=real_date)
        
        hk_hold_info =finance.run_query( 
            query(finance.STK_HK_HOLD_INFO).filter(
                finance.STK_HK_HOLD_INFO.code== code, 
                finance.STK_HK_HOLD_INFO.day >= pre_date,
                finance.STK_HK_HOLD_INFO.day <= date
            ).order_by(finance.STK_HK_HOLD_INFO.day.desc())
        )
        real_date -= datetime.timedelta(days=day_change)
        if len(hk_hold_info.values) == 0:
            hk_hold_list.append(0)
        else:
            hk_hold_list.append(hk_hold_info.values[0][-1])

        if len(money_flow.values):
            money_flow_list.append(money_flow.values[0][4])
        else:
            money_flow_list.append(0)
        
        if pre_volume == 0:
            v_list.append(1)
        else:
            v_list.append(the_volume/pre_volume)
        open_rate_list.append((the_open-pre_close)/pre_close)
        close_rate_list.append((the_close-pre_close)/pre_close)
        avg_list.append((avg-pre_avg)/pre_avg)
        if len(avg_list) == 10: break
        # print(avg_list[-1], close_rate_list[-1], open_rate_list[-1], v_list[-1], money_flow_list[-1], hk_hold_list[-1])
    data = avg_list+close_rate_list+open_rate_list+v_list+money_flow_list+hk_hold_list
    print(len(data))
    return data
    # return [(the_open-pre_close)/pre_close, (the_close-pre_close)/pre_close, v, hk_hold, big_buy]

def get_price_volumn_and_hk_hold():
    stock_info = open("./corelation_income.json", "r")
    stock_info = json.load(stock_info)
    result = {}

    for index, code in enumerate(stock_info.keys()):
        if get_query_count()['spare'] < 5000: break
        print(code, get_query_count(), index/len(stock_info.keys()))
        result[code]={}
        for date in stock_info[code]:
            try:
                data = price_volumn_hk_big_buy(code, date)
            except Exception as e:
                print(e)
                data = [1]*60
            result[code][date] = data
    
    with open("./price_volumn_hk.json", "w") as f:
        json.dump(result, f)

class allStockGroupIndex(object):
    def __init__(self, date):
        self.HG_weight = get_index_weights('000001.XSHG', date=date)
        self.HE_weight = get_index_weights('399001.XSHE', date=date)
        self.weight300 = get_index_stocks('000300.XSHG', date=date)
        self.startup = get_index_stocks('399006.XSHE', date=date)
        self.middle_small = get_index_stocks('399005.XSHE', date=date)
        self.middle500 = get_index_stocks('399905.XSHE', date=date)

        self.hg_list, self.he_list = list(), list()
        for value in self.HG_weight.values:
            self.hg_list.append(value[1])
        for value in self.HE_weight.values:
            self.he_list.append(value[1])
        self.hg_list.sort()
        self.he_list.sort()
    def get_stock_weight_info(self, code):
        data = list()
        try:
            data.append(self.hg_list.index(self.HG_weight.loc[self.HG_weight.index == code].values[0][1]))
        except:
            data.append(0)
        try:
            data.append(self.he_list.index(self.HE_weight.loc[self.HE_weight.index == code].values[0][1]))
        except:
            data.append(0)
        if code in self.weight300:
            data.append(1)
        else:
            data.append(0)
        if code in self.startup:
            data.append(1)
        else:
            data.append(0)
        if code in self.middle_small:
            data.append(1)
        else:
            data.append(0)
        if code in self.middle500:
            data.append(1)
        else:
            data.append(0)
        return data

def get_stock_weight():
    stock_info = open("./corelation_income.json", "r")
    stock_info = json.load(stock_info)
    try:
        with open("./stock_weight.json", "r") as f:
            stock_weight = json.load(f)
    except:
        stock_weight = None
    result = {}
    
    date_list = list()
    for code in stock_info.keys():
        result[code] = {}
        for date in stock_info[code]:
            if len(stock_info[code][date].keys()) >=2:
                date_list.append(date)
    date_list.sort()
    the_month = "2016-12"
    for date in date_list:
        if date[:7] != the_month:
            the_month = date[:7]
            all_stock_group = allStockGroupIndex(date)
            
            # "399802.XSHE" "399905.XSHE"
        
        print(date, get_query_count())
        code_list = list()
        for code in stock_info.keys():
            if date in stock_info[code]:
                data = all_stock_group.get_stock_weight_info(code)
                
                if stock_weight:
                    result[code][date] = stock_weight[code][date]+data
                else:
                    result[code][date] = data
                # print(result[code][date], code)
            
    with open("./stock_weight.json", "w") as f:
        json.dump(result, f)

index300_date_json = {}
def get300index(date):
    global index300_date_json
    y,m,d = date.split(" ")[0].split("-")
    real_date = datetime.date(int(y),int(m),int(d))
    print(real_date)
    if date not in index300_date_json:
        _, close,_,_ = get_trade_price('000300.XSHG', real_date)
        _, pre_close,_,_,_ = get_pre_trade_price('000300.XSHG', real_date)
        index300_date_json[date] = close/pre_close
    return index300_date_json[date]

def get_one_technical(code, date):
    result = list()
    if type(date) == type("132"):
        y,m,d = date.split(" ")[0].split("-")
        real_date = datetime.date(int(y),int(m),int(d))
    else:
        real_date = date
        date = date.strftime("%Y-%m-%d")
    
    rate_300 = get300index(date)
    _, close,_,_ = get_trade_price(code, real_date)
    _, pre_close,_,_,_ = get_pre_trade_price(code, real_date)
    rateBy300 = (close/pre_close)/rate_300
    result.append(rateBy300)
    bias = BIAS(code,date, N1=6, N2=12, N3=24, unit = '1d', include_now = True)
    for b in bias:
        result.append(b[code])
    
    macd = MACD(code, date, SHORT = 12, LONG = 26, MID = 9, unit = '1d', include_now = True)
    for m in macd:
        result.append(m[code])

    # mfi = MFI(code, date, timeperiod=14, unit = '1d', include_now = True)
    # result.append(mfi[code])

    # rsi = RSI(code, date, N1=10, unit = '1d', include_now = True)
    # result.append(rsi[code])
    print(result)
    return result

def get_technical_and_rateBy300():
    stock_info = open("./corelation_income.json", "r")
    stock_info = json.load(stock_info)
    result = {}

    for index, code in enumerate(list(stock_info.keys())):
        if get_query_count()['spare'] < 5000: break
        print(code, get_query_count(), index/len(stock_info.keys()))
        result[code]={}
        
        for date in stock_info[code].keys():
            if len(list(stock_info[code][date].keys())) == 0: continue
            
            result[code][date] = get_one_technical(code, date)
            
        
    with open("./technical_and_rateBy300_new.json", "w") as f:
        json.dump(result, f)

def get_one_finance_rate(code, date):
    if type(date) != type("132"):
        date = date.strftime("%Y-%m-%d")
    query_object = query(
        indicator.statDate,
        indicator.roe,
        indicator.roa,
        indicator.net_profit_margin,
        indicator.gross_profit_margin,
        indicator.expense_to_total_revenue,
        indicator.operation_profit_to_total_revenue,
        indicator.net_profit_to_total_revenue,
        indicator.operating_expense_to_total_revenue,
        indicator.inc_total_revenue_year_on_year,
        indicator.inc_total_revenue_annual,
        indicator.inc_revenue_year_on_year,
        indicator.inc_revenue_annual,
        indicator.inc_operation_profit_year_on_year,
        indicator.inc_operation_profit_annual,
        indicator.inc_net_profit_year_on_year,
        indicator.inc_net_profit_annual,
        valuation.turnover_ratio,
        valuation.pe_ratio,
        valuation.ps_ratio
      ).filter(
          indicator.code == code,
      )
    result = get_fundamentals(query_object, date=date)
    if len(result.values) == 0:
        return [None]*18
    else:    
        result = result.values[0]
    result[0] = int(result[0].split("-")[1])
    result[1] = result[1]/(result[0])*3
    return list(result)

def get_finance_rate():
    stock_info = open("./corelation_income.json", "r")
    stock_info = json.load(stock_info)
    result = {}

    for index, code in enumerate(list(stock_info.keys())):
        if get_query_count()['spare'] < 5000: break
        print(code, get_query_count(), index/len(stock_info.keys()))
        result[code]={}
        
        for date in stock_info[code].keys():
            if len(list(stock_info[code][date].keys())) == 0: continue
            
            result[code][date] = get_one_finance_rate(code, date)
            
    with open("./finance_rate.json", "w") as f:
        json.dump(result, f)
def finance_rate_acceleration():
    finance_rate = open("./finance_rate.json", "r")
    finance_rate = json.load(finance_rate)
    pre_finance_rate = {}
    result = {}
    for index, code in enumerate(list(finance_rate.keys())):
        print(code)
        pre_finance_rate[code] = {}
        result[code] = {}
        for date in finance_rate[code].keys():
            
            the_season = finance_rate[code][date][0]
            print(date, the_season)
            if the_season in pre_finance_rate[code] and pre_finance_rate[code][the_season][0] != None:
                print(finance_rate[code][date])
                print(pre_finance_rate[code][the_season])
                add_data = np.array(finance_rate[code][date])[1:] - np.array(pre_finance_rate[code][the_season])[1:]
                add_data = list(add_data)
                result[code][date] = finance_rate[code][date] + add_data
                pre_finance_rate[code][the_season] = finance_rate[code][date].copy()
                print(add_data)
                
            else:
                pre_finance_rate[code][the_season] = finance_rate[code][date].copy()
                result[code][date] = finance_rate[code][date] + [None]*len(finance_rate[code][date][1:])

            
    with open("./finance_rate_acceleration.json", "w") as f:
        json.dump(result, f)
if __name__ == "__main__":
    # all_stock =  get_all_securities(types=['stock'], date=None).index
    # all_stock = list(all_stock)
    # code_date_relation = {}
    # create_finance_data(all_stock, "cashflow", finance.STK_CASHFLOW_STATEMENT, cashflow_key_list)
    # #print(code_date_relation)
    # create_finance_data(all_stock, "income", finance.STK_INCOME_STATEMENT, income_key_list)
    # print(code_date_relation)
    # get_price_volumn_and_hk_hold()
    # securities = get_all_securities(["index"])
    # for index, value in enumerate(securities.values):
    #     print(list(securities.index)[index], value)
    # 399984.XSHE 399980.XSHE 399972.XSHE 399306.XSHE 399300.XSHE 000057.XSHG 000058.XSHG 000056.XSHG 000004.XSHG 399006.XSHE 000838.XSHG 399102.XSHE
    # ZXBZ CYBZ
    # get_stock_industry()
    # get_stock_weight()
    # get_technical_and_rateBy300()
    # get_finance_rate()
    finance_rate_acceleration()
        
    
    
            
        