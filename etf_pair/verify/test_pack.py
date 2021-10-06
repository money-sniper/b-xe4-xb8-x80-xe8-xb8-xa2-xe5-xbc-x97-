#!/usr/bin/env python
# coding: utf-8
# author:DT
# Date:2021/9/6

"""
对拟合结果检验，R平方，预测胜率，邹至庄检验
"""

import pandas as pd,numpy as np
import os,sys,datetime
import statsmodels.api as sm
from statsmodels.tsa.stattools import coint
from statsmodels.tsa.stattools import adfuller
from ..signal_own import signal_trans
from ..str2dtime import parse2time
parse_temp = parse2time()

def interval_coint_OLS(df,Y,X):
    """
    分段回归核心函数

    Parameters:
    ------
    df:join_data 两列时间序列数据
    Y:Y标的列名
    X:X标的列名

    Returns:
    回归具体结果 pd.Series
    """
    coint_model = coint(df[Y].values,df[X].values)
    if coint_model[1] <= 0.1: #协整检验置信度
        x = df[X]
        y = df[Y]
        model = sm.OLS(y,x)
        results = model.fit()
        return pd.Series([results.params[0],results.pvalues[0],results.rsquared,results.rsquared_adj,adfuller(results.resid.values)[1]],                                 index = ["params","pvalue","rsquared","rsquared_adj","resid_test"])
    else:
        return pd.Series([None,None,None,None,None],index = ["params","pvalue","rsquared","rsquared_adj","resid_test"])

class verify_class:
    def __init__(self,cointegrate_member):
        self.coint_member = cointegrate_member
        self.signal = signal_trans(self.coint_member)

    @staticmethod
    def get_rsquared(OLS_results,X,Y,constant=False,typing = 1):
        """
        计算样本外拟合程度优劣

        Parameters:
        ------
        OLS_results:样本内回归类statsmodels.regression.liner_model.OLS
        X:数据自变量列名
        Y:数据因变量列名
        constant:回归是否带截距项
        typing:计算方式 0,1

        Returns:
        ------
        检验结果R_adjusted
        
        """
        
        if constant:#有常数
            if X.shape[1] != 1:
                raise Exception("wrong match")
            else:
                n,p = X.values.shape
                predict_value = (X.values * OLS_results.params.values.reshape([-1,X.values.shape[1]])).sum(1)
                mean_y = Y.values.mean()
                SSR = ((predict_value - mean_y)**2).sum()
                SST = ((Y.values - mean_y)**2).sum()
                R = SSR/SST
                R_adjusted = 1 - (1-R**2)*(n-1)/(n-p-1)
                return R_adjusted
        else:#无常数
            if X.shape[1] != 1: 
                raise Exception("no constant wrong match")
            else:
                n,p = X.values.shape
                predict_value = (X.values * OLS_results.params).sum(1)
                mean_y = Y.values.mean()
                if typing == 0:
                    SSR = ((predict_value - Y.values)**2).sum()
                    SST = ((Y.values.flatten() - mean_y)**2).sum()
                elif typing == 1:
                    SSR = ((predict_value)**2).sum()
                    SST = ((Y.values.flatten())**2).sum()
                R = SSR/SST
                R_adjusted = 1 - (1-R**2)*(n-1)/(n-p-1)
                return R_adjusted

    def test_day_freq_rsquared(self,typing,resample_freq = "15min",\
        rolling_tick = "2min",test_tick = "1min"):

        """
        测试选用多少样本内时间长度对多久样本外测试时间预测结果最好

        Parameters:
        ------
        typing:回归拟合结果指标计算方式
        resample_freq:每次计算需要取样样本区间长度
        rolling_tick:滚动测试频率
        test_tick:

        Returns:
        ------
        res:每一个test_tick区间检验结果
        count_None:出现多少次上一个rolling区间预测结果检验结果不符合标准
        """
        df = self.coint_member.join_data
        X_name = self.coint_member._X
        Y_name = self.coint_member._Y
        basis = parse_temp.str2delta(self.coint_member._freq)
        resample_time = parse_temp.str2delta(resample_freq)
        rolling_time = parse_temp.str2delta(rolling_tick)
        test_time = parse_temp.str2delta(test_tick)
        rolling_div_second = int(rolling_time / basis)
        resample_div_second = int(resample_time / basis)
        test_tick_second = int(test_time / basis) #1minutes
        #测试1-10min的检测效果
        
        data_pre = [j for j in df.rolling(resample_div_second)][resample_div_second-1:]
        res = {}
        count_None = 0
        for k in range(resample_div_second-1,len(data_pre),rolling_div_second):
            OLS_results = interval_coint_OLS(data_pre[k],X = X_name,Y = Y_name)

            if pd.isnull(OLS_results["params"]):
                count_None += 1
            else:
                for i in range(1,11): #检查10min后数据
                    test_tick_num_start = test_tick_second * (i - 1)
                    test_tick_num_end = test_tick_second * i
                    base_ind = df.index.get_loc(data_pre[k].index[-1])
                    ind_before = base_ind + test_tick_num_start
                    ind_after = base_ind + test_tick_num_end
                    test_tick_df = df.iloc[ind_before:ind_after+1,:]
                    if test_tick_df.shape[0] < test_tick_second:
                        continue
                    else:
                        Insample_R = verify_class.get_rsquared(OLS_results,data_pre[k][["LastPrice_1000"]],\
                                                data_pre[k][["LastPrice_500"]],constant=False,typing = typing)
                        python_Insample_R = OLS_results.rsquared_adj
                        Rsquared_temp = verify_class.get_rsquared(OLS_results,\
                                                    test_tick_df[["LastPrice_1000"]],\
                                                    test_tick_df[["LastPrice_500"]],constant = False,typing = typing)
                        if i in res:
                            res[i].append(Rsquared_temp)
                        else:
                            res.update({i:[Rsquared_temp]})
        return res,count_None

    def caculate_winrate(self):
        """
        Y实际的收益率是否会和拟合的走势相同
        原始行情:
        self.coint_member._data500_resample
        self.coint_member._data1000_resample

        信号:
        self.signal

        """
        pass

    def test_second_transaction(self):
        """
        对于判断np.log.diff即时收益率的买卖点
        能否完成一次买卖
        对单标的

        self.signal
        """
        pass

    def ZZZ_test(self):
        pass
            