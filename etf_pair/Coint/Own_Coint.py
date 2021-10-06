#!/usr/bin/env python
# coding: utf-8

import os
import struct
import numpy as np
import datetime
import pandas as pd
from pdb import set_trace
import datetime
from statsmodels.tsa.stattools import adfuller
from etf_pair import coint
import statsmodels.api as sm
import matplotlib.pyplot as plt
import copy
import math
from ..str2dtime import parse2time
from ..Pre_Data.convert_file import convertdata,convertdata_staticInfo
from ..Pre_Data import clean_data,Pre_Context_by,Resample_data

class cointegrate:
    def __init__(
        self,
        date_range,
        typing = "LastPrice",
        X_name = "LastPrice_500",
        Y_name = "LastPrice_1000"):
        self._date = None
        self.date_range = date_range
        self._X = X_name
        self._Y = Y_name
        self._res = None
        self._type = typing
        self._price_type = None
        self.parsetime = parse2time()
        self.join_data = None
        self.join_data_OLS_res = None #满足interval划分检验条件的回归结果
        self.join_data_unqualified_OLS_res = None #不满足interval划分检验条件的回归结果

    @property
    def typ(self):
        return self._type
    
    @typ.setter
    def typ(self,value):
        self._type = value
        
    @property
    def date(self):
        return self._date
    
    @date.setter
    def date(self,val):
        self._date = val
        
    def get_day_data(
        self,
        date,
        freq):
        """
        目标data500 UpdateTime,LastPrice

        Returns:
        ------
        self._data500_resample  index->UpdateTime 
        self._data1000_resample  index->UpdateTime
        """
        self._date = date
        path = "/Users/dt/Desktop/HA/ETF_rolling/data/510500/"+date+"/SSE.510500.dat"
        data500 = Pre_Context_by(path,"3s",typing=0)
        
        path = "/Users/dt/Desktop/HA/ETF_rolling/data/512100/"+date+"/SSE.512100.dat"
        data1000 = Pre_Context_by(path,"3s",typing=0)

        if freq!='3s':
            t1 = Resample_data(data500,freq)
            t2 = Resample_data(data1000,freq)
            t1 = t1.loc[t1.index.intersection(t2.index)]
            t2 = t2.loc[t1.index.intersection(t2.index)]
        else:
            t1 = data500.set_index("UpdateTime")
            t2 = data1000.set_index("UpdateTime")
            t1 = t1.loc[t1.index.intersection(t2.index)]
            t2 = t2.loc[t1.index.intersection(t2.index)]
        
        #对价格进行正则化
        common_UpdateTime = pd.Index(data500.UpdateTime).intersection(data1000.UpdateTime)
        self._data500 = data500[data500["UpdateTime"].isin(common_UpdateTime)] #index -> range(shape)
        self._data1000 = data1000[data1000["UpdateTime"].isin(common_UpdateTime)]
        self._data500_resample = t1 
        self._data1000_resample = t2 #index->UpdateTime
        self._freq = freq
        return t1,t2

    @staticmethod
    def data2percent(df,col="LastPrice"):
        "正则化"
        base = df[col].iloc[0]
        return df[col]/base
        
    def One_day_test(self):
        """
        计算每日平稳性与回归结果
        """
        res = {}
        left_use = cointegrate.data2percent(self._data1000_resample) - 1 #500->X
        right_use = cointegrate.data2percent(self._data500_resample)  - 1 #1000->Y
        
        #原值平稳性
        steady_left,steady_right = adfuller(left_use),adfuller(right_use)
        res.update({"steady_500":steady_left,"steady_1000":steady_right})
        #价差平稳性
        steady_left1,steady_right1 = adfuller(left_use.diff().dropna()),adfuller(right_use.diff().dropna())
        res.update({"steady_500_diff":steady_left1,"steady_1000_diff":steady_right1})

        #原值协整检验
        coint_test = coint(left_use,right_use)
        #价差平稳性
        coint_test1 = coint(pd.Series(left_use).diff().dropna().values,pd.Series(right_use).diff().dropna().values)
        res.update({"coint_ori":coint_test,"coint_diff":coint_test1})
        
        #价差回归_constant
        Y = (left_use*1000).diff().dropna()
        X = sm.add_constant((right_use*1000).diff().dropna())
        Y_X = pd.merge(Y,X,how='inner',left_index=True,right_index=True).dropna()
        model = sm.OLS(Y_X.iloc[:,0],Y_X.iloc[:,[1,2]])
        results_jiacha = model.fit()
        res.update({"diff_OLS_constant":results_jiacha})
        
        #原值回归_constant
        model_ori= sm.OLS(left_use,sm.add_constant(right_use))
        results_ori = model_ori.fit()
        res.update({"ori_OLS_constant":results_ori})
        
        #价差回归_yd
        Y = (left_use*1000).diff().dropna()
        X = (right_use*1000).diff().dropna()
        Y_X = pd.merge(Y,X,how='inner',left_index=True,right_index=True).dropna()
        model_jiacha_yd = sm.OLS(Y_X.iloc[:,0],Y_X.iloc[:,[1]])
        results_jiacha_yd = model_jiacha_yd.fit()
        res.update({"diff_OLS_yd":results_jiacha_yd})
        
        #原值回归_yd
        model_ori_yd = sm.OLS(left_use,right_use)
        results_ori_yd = model_ori_yd.fit()
        res.update({"ori_OLS_yd":results_ori_yd})
        self._res = res
        return res
    
    def result_transfer(self,data,col:list):
        """
        每日回归结果
        """
        if data is None:
            raise Exception("Error data")
        else:
            another_res = {}
            for u,v in data.items():
                if u in ["diff_OLS_constant","ori_OLS_constant","diff_OLS_yd","ori_OLS_yd"]:
                    for name in col:
                        loc = locals()
                        exec("temp = v."+name)
                        temp = loc["temp"]
                        another_res.update({u+" "+name:temp})
                elif u in ["steady_500","steady_1000","steady_500_diff","steady_1000_diff"]:
                    another_res.update({u+"_pvalue":v[1]})
                elif u in ["coint","coint_diff"]:
                    another_res.update({u+"_pvalue":v[1]})      
        return another_res
        
    def interval_get_ratio(self,freq_own,typing = "net"):
        """
        分段测试coint并进行OLS回归

        Parameters:
        -------
        freq_own:选定频率,一般是多少min
        typing:选择何种方式对原始价格序列进行处理
                一开始对价格序列进行了正则化
                net->是直接使用正则化的当日净值
                cum_ret->代表累积净收益率
                ret->代表每时收益率
        
        -------
        Returns
        join_data_res:每个时间段回归结果,pd.DataFrame
            columns->  "params","pvalue","rsquared","rsquared_adj","resid_test"
        index
        0(signal)
        1

        join_data:归一化价格序列，每一列是两个标的中的一个,pd.DataFrame
            columns-> "LastPrice_500","LastPrice_1000","signal"
        index
        UpdateTime
        """
        assert typing in ["ret","cum_ret","net"],"参数typing只识别 ret, cum_ret, net"
        self._price_type = typing
        left_use = cointegrate.data2percent(self._data500_resample[["LastPrice"]]) #500->X
        right_use = cointegrate.data2percent(self._data1000_resample[["LastPrice"]]) #1000->Y
        join_data = pd.merge(left_use,right_use,left_index=True,right_index=True,suffixes = ("_500","_1000"))
        if typing == "cum_ret":
            join_data[self._Y] = join_data[self._Y] - 1
            join_data[self._X] = join_data[self._X] - 1
        elif typing == "ret":
            join_data[self._Y] = np.log(join_data[self._Y]).diff().dropna()*10
            join_data[self._X] = np.log(join_data[self._X]).diff().dropna()*10
        align_index = pd.date_range(join_data.index[0],join_data.index[-1],freq = freq_own)
        align_index = clean_data(align_index)
        align_data = pd.DataFrame(np.arange(align_index.shape[0]),index = align_index)
        join_data = join_data.merge(align_data,left_index=True,right_index=True,how='left').ffill()
        join_data = join_data.rename(columns = {0:"signal"})
        def interval_coint_OLS(df):
            """
            分段回归
            """
            df = df.dropna() #对于有None值的，跳过
            coint_model = coint(df[self._X].values,df[self._Y].values)
            if coint_model[1] <= 0.1: #协整检验置信度
                x = df[self._X]
                y = df[self._Y]
                model = sm.OLS(y,x)
                results = model.fit()
                return pd.Series([results.params[0],\
                    results.pvalues[0],\
                        results.rsquared,\
                            results.rsquared_adj,\
                                adfuller(results.resid.values)[1]],\
                    index = ["params","pvalue","rsquared","rsquared_adj","resid_test"])
            else:
                return pd.Series([None,None,None,None,None],index = ["params","pvalue","rsquared","rsquared_adj","resid_test"])
        
        def interval_coint_OLS1(df):
            """
            不满足条件的分段回归
            """
            df = df.dropna()
            coint_model = coint(df[self._X].values,df[self._Y].values)
            if coint_model[1] > 0.1:
                x = df[self._X]
                y = df[self._Y]
                model = sm.OLS(y,x)
                results = model.fit()
                return pd.Series([results.params[0],\
                    results.pvalues[0],\
                        results.rsquared,\
                            results.rsquared_adj,\
                                adfuller(results.resid.values)[1]],\
                    index = ["params","pvalue","rsquared","rsquared_adj","resid_test"])
            else:
                return pd.Series([None,None,None,None,None],index = ["params","pvalue","rsquared","rsquared_adj","resid_test"])

        join_data_res = join_data.groupby("signal").apply(lambda x:interval_coint_OLS(x))
        join_data_unqualified_res = join_data.groupby("signal").apply(lambda x:interval_coint_OLS1(x))
        self.join_data = join_data
        self.join_data_OLS_res = join_data_res
        self.join_data_unqualified_OLS_res = join_data_unqualified_res
        return join_data_res,join_data
    
class cointegrate_plot(cointegrate):
    def __init__(
        self,
        date_range,
        X_name = "LastPrice_500",
        Y_name = "LastPrice_1000",
        price_typing = "LastPrice",
        freq_default = "15min"):
        """
        X_name->原始数据列名
        Y_name->原始数据列名
        """
        super(cointegrate_plot,self).__init__(
            date_range,
            price_typing,
            X_name = X_name,
            Y_name = Y_name)
        self._freq = freq_default
        self.join_data_true_OLS = None #interval区间求解实际值
        self.join_data_interval_OLS = None #interval根据上一个区间获得值
        self.join_data_interval_true_OLS_unqualified = None #interval实际值不满足检验区间结果
        self.join_data_interval_OLS_unqualified = None #interval根据上一个区间获得不满足检验区间结果
        self.join_data_rolling_OLS = None #满足检验条件
        self.join_data_rolling_OLS_unqualified = None #不满足检验条件
        self.gap_data_OLS_true = None
        self.gap_data_OLS_model = None
        self.gap_data_rolling = None #滚动包含所有

    @property
    def X(self):
        return self._X

    @X.setter
    def X(self,value):
        self._X = value

    @property
    def Y(self):
        return self._Y

    @Y.setter
    def Y(self,value):
        self._Y = value

    def _freq_plot(self,typing = "cum_ret"):
        """
        指数收益率与收益率差
        """
        #收益率图像
        _,(ax1,ax2) = plt.subplots(2,1,figsize = (12,8))
        left_use = cointegrate.data2percent(self._data500_resample) #X
        right_use = cointegrate.data2percent(self._data1000_resample) #Y
        if typing == "cum_ret":
            left_use = left_use - 1
            right_use = right_use - 1
        elif typing == "ret":
            left_use = np.log(left_use).diff().dropna()
            right_use = np.log(right_use).diff().dropna()
        ax1.plot(left_use.values,c='b',label='zz500_ret'+str(self._freq))
        ax1.plot(right_use.values,c='r',label='zz1000_ret'+str(self._freq))
        max_value = max(max(left_use),max(right_use))
        min_value = min(min(left_use),min(right_use))
        
        Day = pd.to_datetime(self._data500_resample.index[0]).strftime("%Y-%m-%d")
        locat = self._data500_resample.index.get_loc(Day+" "+"13:00:00")
        ax1.vlines(locat,min_value,max_value)
        ax1.set_title("cummulative return")

        ax2.plot((left_use - right_use).values,label = "zz500 - zz1000 ret_"+str(self._freq))
        max_value = max((left_use - right_use).values)
        min_value = min((left_use - right_use).values)
        ax2.vlines(locat,min_value,max_value)
        ax1.legend()
        ax2.legend()

    def plot_cumret_diff(self):
        """
        累计收益率与累积收益率价差图
        """
        left_use = cointegrate.data2percent(self._data500_resample) #500
        right_use = cointegrate.data2percent(self._data1000_resample) #1000
        Y = (left_use).diff().dropna()
        X = (right_use).diff().dropna()
        _,(ax1,ax3) = plt.subplots(2,1,figsize=(24,16))
        ax1.plot(Y.values,c = 'r',label = "zz500")
        ax1.plot(X.values,c = 'b',label = "zz1000")
        ax1.set_title("cumsum_ret diff")
        ax1.legend()

        ax3.plot(left_use.values,c = 'r',label = 'zz500')
        ax3.plot(right_use.values,c = 'b',label = 'zz1000')
        ax3.set_title("cumsum_ret")
        ax3.legend()

    def tick_rolling_plot(
        self,
        freq_own,
        tick_rolling="1min",
        plot_on=True):
        """
        根据滚动
        freq_own, resample时间，用于计算参数需要之前多少时间数据
        tick_rolling_num, 滚动多久计算去一次样

        Parameters:
        ------
        join_data:归一化价格数据两列，LastPrice_500,LastPrice_1000,signal,index->DatetimeIndex
        join_data_OLS:真实数据拟合结果, LastPrice_500,LastPrice_1000,signal
        freq_own:选定resample频率，涉及参数样本选取,默认是min结尾
        tick_rolling_num:滚动选取时间 datetime.timedelta
        typing:对于正则化后的数据如何处理

        Returns:
        ------
        如果对原始数据处理成net或cum_ret，plot两条价格序列
        如果原始数据处理成ret,则plot self._Y对应的ret序列
        """
        join_data = self.join_data
        join_data_rolling = copy.copy(join_data)
        join_data_rolling_unqualified = copy.copy(join_data)
        res = [j for j in join_data.rolling(freq_own)]
        left_use = join_data[self._Y]
        right_use = join_data[self._X]
                        
        interval = self.parsetime.str2delta(self._freq)
        tick_rolling_num = int(np.ceil(self.parsetime.str2delta(tick_rolling)/interval))
        freq_own_len = int(np.ceil(self.parsetime.str2delta(freq_own)/interval))

        def interval_coint_OLS(df):
            """
            分段回归
            """
            coint_model = coint(df[self._Y].values,df[self._X].values)
            if coint_model[1] <= 0.1: #协整检验置信度
                x = df[self._X]
                y = df[self._Y]
                model = sm.OLS(y,x)
                results = model.fit()
                return pd.Series([results.params[0],results.pvalues[0],results.rsquared,results.rsquared_adj,adfuller(results.resid.values)[1]],                                 index = ["params","pvalue","rsquared","rsquared_adj","resid_test"])
            else:
                return pd.Series([None,None,None,None,None],index = ["params","pvalue","rsquared","rsquared_adj","resid_test"])

        def interval_coint_OLS1(df):
            """
            分段回归
            """
            coint_model = coint(df[self._Y].values,df[self._X].values)
            if coint_model[1] > 0.1: #协整检验置信度
                x = df[self._X]
                y = df[self._Y]
                model = sm.OLS(y,x)
                results = model.fit()
                return pd.Series([results.params[0],results.pvalues[0],results.rsquared,results.rsquared_adj,adfuller(results.resid.values)[1]],                                 index = ["params","pvalue","rsquared","rsquared_adj","resid_test"])
            else:
                return pd.Series([None,None,None,None,None],index = ["params","pvalue","rsquared","rsquared_adj","resid_test"])

        for i in range(tick_rolling_num,len(res),tick_rolling_num):
            freq_data_now = res[i]
            freq_data_before = res[i-tick_rolling_num]
            ind_index_last = freq_data_now.index[-2]
            ind_index_first = freq_data_before.index[-1]
            time_start = join_data.index[0] + self.parsetime.str2delta(freq_own) - self.parsetime.str2delta(self._freq)
            if ind_index_first >= time_start: #涉及freq_own
                OLS_res = interval_coint_OLS(freq_data_before) #valid
                OLS_res1 = interval_coint_OLS1(freq_data_before) #invalid
                para = OLS_res["params"]
                para1 = OLS_res1["params"]
                try:
                    if (pd.isnull(OLS_res["params"])) & (pd.notnull(OLS_res1["params"])):
                        join_data_rolling.loc[ind_index_first:ind_index_last,\
                            self._Y] = np.nan
                        join_data_rolling_unqualified.loc[ind_index_first:ind_index_last,\
                            self._Y] = \
                                join_data_rolling_unqualified.loc[ind_index_first:ind_index_last,self._X]*para1
                    elif (pd.isnull(OLS_res1["params"])) & (pd.notnull(OLS_res["params"])):
                        join_data_rolling.loc[ind_index_first:ind_index_last,self._Y] = \
                                join_data_rolling.loc[ind_index_first:ind_index_last,self._X]*para
                        join_data_rolling_unqualified.loc[ind_index_first:ind_index_last,\
                            self._Y] = np.nan
                except:
                    join_data_rolling.loc[ind_index_first:ind_index_last,\
                            self._Y] = np.nan
                    join_data_rolling_unqualified.loc[ind_index_first:ind_index_last,\
                            self._Y] = np.nan
            else:
                join_data_rolling.loc[ind_index_first:ind_index_last,self._Y] = np.nan    
                join_data_rolling_unqualified.loc[ind_index_first:ind_index_last,self._Y] = np.nan          

        from matplotlib.font_manager import FontProperties
        chinese_font = FontProperties(fname = os.path.join("/Users/dt/Desktop/simsun","simsun.ttf"))

        rolling_str = tick_rolling
        freq_resample = freq_own

        valid_true = left_use.values - self.join_data_interval_OLS[self._Y].values
        invalid_true = left_use.values - self.join_data_interval_OLS_unqualified[self._Y].values

        valid_rolling = left_use.values - join_data_rolling[self._Y].values
        invalid_rolling = left_use.values - join_data_rolling_unqualified[self._Y].values
        if plot_on:
            _,(ax1,ax2,ax3) = plt.subplots(3,1,figsize = (12,8),dpi=800)
            ax1.plot(valid_rolling,c = 'r',label = "valid")
            ax1.plot(invalid_rolling,c='b',label ='invalid')
            ax1.set_title("滚动频率:"+rolling_str+"  取样频率:"+freq_resample+"\n"+"真实值-滚动拟合值",fontproperties = chinese_font)
            ax1.hlines(0,xmin=0,xmax=left_use.shape[0])
            ax1.legend(prop = chinese_font)
            
            ax2.plot(valid_true,c = 'r',label = "valid")
            ax2.plot(invalid_true,c = 'b',label = "invalid")
            ax2.set_title("真实值-真实拟合值 interval",fontproperties = chinese_font)
            ax2.hlines(0,xmin=0,xmax=left_use.shape[0])
            ax2.legend(prop = chinese_font)

        def union_valid_invalid(valid,invalid):
            valid[np.isnan(valid)] = invalid[np.isnan(valid)]
            return valid
        self.gap_data_rolling = union_valid_invalid(valid_rolling,invalid_rolling) #Y - para*X

        if plot_on:
            if self._price_type in ["cum_ret","net"]:
                ax3.plot(left_use.values,c = 'r',label = self._Y+" "+self._price_type)
                ax3.plot(right_use.values,c = 'b',label = self._X+" "+self._price_type)
                ax3.set_title(self._price_type+"走势",fontproperties = chinese_font)
                ax3.legend(prop = chinese_font)
                ax3.set_ylim(bottom = min(np.r_[left_use.dropna().values.flatten(),right_use.dropna().values.flatten()]),\
                    top = max(np.r_[left_use.dropna().values.flatten(),right_use.dropna().values.flatten()]))
            else:
                ax3.plot(left_use.values,c = 'r',label = self._Y)
                ax3.set_title("真实"+self._price_type+"走势",fontproperties = chinese_font)
                ax3.legend(prop = chinese_font)
            ax3.set_ylim(bottom = min(left_use.dropna().values),top = max(left_use.dropna().values))

        self.join_data_rolling_OLS = join_data_rolling
        self.join_data_rolling_OLS_unqualified = join_data_rolling_unqualified
        return join_data_rolling

    def specific_plot_tick_rolling(
        self,
        left_index,
        right_index):
        if (left_index < 0)|(right_index > self.join_data.shape[0]):
            raise Exception("left_index, right_index out of limit")
        if left_index > right_index:
            raise Exception("left_index must smaller than right_index")
        
        from matplotlib.font_manager import FontProperties
        chinese_font = FontProperties(fname = os.path.join("/Users/dt/Desktop/simsun","simsun.ttf"))

        plt.subplots_adjust(hspace = 0.5)
        join_data_rolling_temp = self.join_data_rolling_OLS.iloc[left_index:right_index]
        join_data_OLS_temp = self.join_data_true_OLS.iloc[left_index:right_index]
        join_data_temp_Y = self.join_data[self._Y].iloc[left_index:right_index]

        if self._price_type in ["ret"]:
            _,(ax1,ax2,ax3,ax4) = plt.subplots(4,1,figsize = (12,8),dpi=800)
            ax1.plot(join_data_temp_Y.values - join_data_rolling_temp[self._Y].values,c = 'r',label = self._Y)
            ax1.set_title(self._Y+"ret "+"真实值-滚动拟合值",fontproperties = chinese_font)
            ax1.hlines(0,xmin=0,xmax=join_data_temp_Y.shape[0])
            ax1.legend(prop = chinese_font)

            ax2.plot(join_data_temp_Y.values - join_data_OLS_temp[self._Y].values,c = 'r',label = self._Y)
            ax2.set_title(self._Y+"ret "+"真实值-真实拟合值",fontproperties = chinese_font)
            ax2.hlines(0,xmin=0,xmax=join_data_temp_Y.shape[0])
            ax2.legend(prop = chinese_font)

            ax3.plot(join_data_temp_Y.values,c = 'r',label = self._Y)
            ax3.set_title("真实 "+self._Y+"ret 走势",fontproperties = chinese_font)
            ax3.hlines(0,xmin=0,xmax=join_data_temp_Y.shape[0])
            ax3.legend(prop = chinese_font)

            ax4.plot((join_data_temp_Y.values - join_data_rolling_temp[self._Y].values)*3,c = 'r',label = self._Y + "真实值-滚动拟合值")
            ax4.plot(join_data_temp_Y.values,c = 'b',label = self._Y + "真实走势")
            ax4.set_title("图1和图3组合",fontproperties = chinese_font) 
            ax4.hlines(0,xmin=0,xmax=join_data_temp_Y.shape[0])
            ax4.legend(prop = chinese_font)

        elif self._price_type in ["cum_ret","net"]:
            _,(ax1,ax2,ax3,ax4,ax5) = plt.subplots(5,1,figsize = (12,8),dpi=800)
            plt.subplots_adjust(hspace = 0.75)
            join_data_temp_X = self.join_data[self._X].iloc[left_index:right_index]
            ax1.plot(join_data_temp_Y.values - join_data_rolling_temp[self._Y].values,c = 'r',label = self._Y)
            ax1.set_title(self._Y+" "+self._X+"拟合比例价差",fontproperties = chinese_font)
            ax1.hlines(0,xmin=0,xmax=join_data_temp_Y.shape[0])
            ax1.legend(prop = chinese_font)

            ax2.plot(join_data_temp_Y.values - join_data_OLS_temp[self._Y].values,c = 'r',label = self._Y)
            ax2.set_title(self._Y+" "+self._X+"真实比例价差",fontproperties = chinese_font)
            ax2.hlines(0,xmin=0,xmax=join_data_temp_Y.shape[0])
            ax2.legend(prop = chinese_font)

            ax3.plot(join_data_temp_Y.values,c = 'r',label = self._Y)
            ax3.plot(join_data_temp_X.values,c = 'b',label = self._X)
            ax3.set_title("真实 "+self._Y+" "+self._X+" "+self._price_type,fontproperties = chinese_font)
            if self._price_type == "cum_ret":
                ax3.hlines(0,xmin=0,xmax=join_data_temp_Y.shape[0])
            elif self._price_type == "net":
                ax3.hlines(1,xmin=0,xmax=join_data_temp_Y.shape[0])
            ax3.legend(prop = chinese_font)

            ax4.plot((join_data_temp_Y.values - join_data_rolling_temp[self._Y].values)*3,c = 'r',label = "拟合比例价差")
            ax4.plot(join_data_temp_Y.values - join_data_OLS_temp[self._Y].values,c = 'b',label = "真实比例价差")
            ax4.set_title("图1与图2组合",fontproperties = chinese_font)
            ax4.hlines(0,xmin=0,xmax=join_data_temp_Y.shape[0])
            ax4.legend(prop = chinese_font)

            ax5.plot((join_data_temp_Y.values - join_data_rolling_temp[self._Y].values)*3,c = 'r',label = "拟合比例价差")
            ax5.plot(join_data_temp_Y.values - join_data_temp_X.values,c = 'b',label = self._Y+" "+self._X+" 1比1价差")
            ax5.set_title("比例价差",fontproperties = chinese_font)
            ax5.hlines(0,xmin=0,xmax=join_data_temp_Y.shape[0])
            ax5.legend(prop = chinese_font)

    def interval_inner_rolling(
        self,
        join_data,
        join_data_OLS,
        tick_rolling=datetime.timedelta(minutes=2)):
        """
        确定每个interval，再在interval内部取值拟合

        Parameters:
        -------
        join_data:两个标的归一化价格数据
        LastPrice_500,LastPrice_1000,signal,index->UpdateTime

        join_data_OLS:真实拟合出来结果序列
        LastPrice_500,LastPrice_1000,signal,index->UpdateTime

        Returns:
        -------


        """
        pass


    def fixed_interval_plus_plot(
        self,
        plot_on = True):
        """
        按照join_data_res划分频率画出实际拟合曲线，
        与使用上一个有效测试区间的拟合曲线进行对比
        每个时间区间固定频率拟合
        Parameters:
        ------
        join_data_res: DataFrame,self.interval_get_raio获得拟合结果
        join_data: DataFrame,self.interval_get_ratio获得merge结果

        Returns:
        ------
        join_data_OLS: DataFrame, 根据实际拟合结果
        join_data_OLS1: DataFrame, 根据之前一个频段获得参数拟合结果
        """
        join_data_res = self.join_data_OLS_res
        join_data_OLS = copy.copy(self.join_data)
        join_data_OLS1 = copy.copy(self.join_data)
        join_data_unqualified = copy.copy(self.join_data)
        join_data_unqualified1 = copy.copy(self.join_data)

        for i in join_data_res.index:
            """
            para是Y->zz500;
            X->zz1000;
            真实情况
            """
            para = join_data_res.loc[i,"params"]
            para1 = self.join_data_unqualified_OLS_res.loc[i,"params"]
            if pd.isnull(join_data_res.loc[i,"params"]):
                join_data_OLS.loc[join_data_OLS["signal"] == i,[self._Y,self._X]] = np.nan
                join_data_unqualified.loc[join_data_unqualified["signal"] == i,self._Y] = join_data_unqualified.loc[join_data_unqualified["signal"] == i,self._X]*para1
            else:
                join_data_OLS.loc[join_data_OLS["signal"] == i,self._Y] = join_data_OLS.loc[join_data_OLS["signal"] == i,self._X]*para
                join_data_unqualified.loc[join_data_unqualified["signal"] == i,[self._Y,self._X]] = np.nan

        for i in join_data_res.index[1:]:
            para1 = join_data_res.loc[i-1,"params"]
            para11 = self.join_data_unqualified_OLS_res.loc[i-1,"params"]
            if (pd.isnull(join_data_res.loc[i-1,"params"])):
                join_data_OLS1.loc[join_data_OLS1["signal"] == i,[self._Y,self._X]] = np.nan
                join_data_unqualified1.loc[join_data_unqualified1["signal"] == i,self._Y] = join_data_unqualified1.loc[join_data_unqualified["signal"] == i,self._X]*para11
            elif (join_data_res.loc[i-1,"resid_test"] < 0.1):
                join_data_OLS1.loc[join_data_OLS1["signal"] == i,self._Y]=\
                    join_data_OLS1.loc[join_data_OLS1["signal"] == i,self._X]*para1
                join_data_unqualified1.loc[join_data_unqualified1["signal"] == i,[self._Y,self._X]] = np.nan
            else:
                join_data_OLS1.loc[join_data_OLS1["signal"] == i,[self._Y,self._X]] = np.nan
                if pd.notnull(para1):
                    temp_para = para1
                elif pd.notnull(para11):
                    temp_para = para11
                else:
                    temp_para = np.nan
                join_data_unqualified1.loc[join_data_unqualified1["signal"] == i,self._Y] = join_data_unqualified1.loc[join_data_unqualified["signal"] == i,self._X]*temp_para
        
        left_use = self.join_data[self._Y]
        right_use = self.join_data[self._X]
        valid_OLS_true = left_use.values - join_data_OLS[self._Y].values
        invalid_OLS_true = left_use.values - join_data_unqualified[self._Y].values
        valid_OLS1 = left_use.values - join_data_OLS1[self._Y].values
        invalid_OLS1 = left_use.values - join_data_unqualified1[self._Y].values
        if plot_on:
            _,(ax1,ax2,ax3,ax4) = plt.subplots(4,1,figsize = (24,16))
            ax1.plot(left_use.values,c = 'r',label = 'Y')
            ax1.plot(right_use.values,c = 'b',label = 'X')
            ax1.set_title("ori price")
            ax1.legend()

            ax2.plot(join_data_OLS[self._Y].values,c = 'r',label = "Y_OLS")
            ax2.plot(left_use.values,c = 'b',label = "Y")
            ax2.set_title("OLS_500")
            ax2.legend()
            
            ax3.plot(valid_OLS_true,c = 'r',label = "valid")
            ax3.plot(invalid_OLS_true,c='b',label="unvalid")
            ax3.legend()
            ax3.set_title("500-1000OLS500")
            ax3.hlines(0,xmin=0,xmax=left_use.shape[0])
            
            ax4.plot(valid_OLS1,c = 'r',label = "valid")
            ax4.plot(invalid_OLS1,c='b',label="unvalid")
            ax4.legend()
            ax4.set_title("500-1000OLS500_model")
            ax4.hlines(0,xmin=0,xmax=left_use.shape[0])

        self.join_data_true_OLS = join_data_OLS
        self.join_data_interval_OLS = join_data_OLS1
        self.join_data_interval_true_OLS_unqualified = join_data_unqualified
        self.join_data_interval_OLS_unqualified = join_data_unqualified1
        
        def union_valid_invalid(valid,invalid):
            valid[np.isnan(valid)] = invalid[np.isnan(valid)]
            return valid

        self.gap_data_OLS_true = union_valid_invalid(valid_OLS_true,invalid_OLS_true)
        self.gap_data_OLS_model = union_valid_invalid(valid_OLS1,invalid_OLS1)
        return join_data_OLS,join_data_OLS1