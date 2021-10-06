#/Users/dt/Desktop/HA/backtest_dt
#-*- coding:utf-8 -*-
#@Author:DT
#@Date:2021/6/11
#@Version:0.2

"""
minute级别数据
行情数据处理脚本
生成的行情数据包括time(盘口时间标记),code(合约名称),price(设定的盘口成交价格),last(盘口最新价),volume(分割时间段成交量)
Day_Data_Management->Minute_Data_Management
"""

import os,sys,pickle,traceback,copy
import copy,pandas as pd,numpy as np
from pdb import set_trace 

class Minute_Data_Management:
    """
    clean_data2day 修改minute行情数据数据，聚合当日行情数据
    clean_data2minute 矫正minute行情数据
    """
    def __init__(self,data,day_start = "09:35:00",day_end = "14:57:00",whether_split:bool=True,\
        split_two_space_interval = 30,typing = 'open+close'):
        self.data = data
        self.day_start = day_start
        self.day_end = day_end
        self.whether_split = whether_split #用来分开聚合成日频数据
        self.split_interval = split_two_space_interval
        self.price_type = typing

    def clean_data2day(self,code):
        """
        获得price->可以是close或者open或者(open+close)/2
        last->和price重叠都是指定价，应该换close最新价
        volume->分割出来时间段成交量总和

        time,code,price,last,volume : 时间戳、合约标的、自定义买卖价格、当前规定interval收盘价，当前划分split_interval成交量总和
        """
        self.data['time'] = pd.to_datetime(self.data['time'])
        #align
        data = self.data[self.data['code'] == code]
        data = data.set_index('time').resample('1min').ffill().reset_index() #因为resample label和closed都是默认left所以，一开始35分代表35:01-35:59
        #time_span
        data = data[data['time'].apply(lambda x: self.day_start <= x.strftime('%H:%M:%S') <= self.day_end)] 
        #split_two_space_interval
        columns = {}
        all_type = self.price_type.split('+')
        for name in all_type:
            if name == 'open':
                columns.update({name:0})
            elif name == 'close':
                columns.update({name:-1})
        if self.whether_split:
            "切割成前30分钟和后面的时间单独判断行情"
            temp1 = data.iloc[:self.split_interval]
            temp2 = data.iloc[self.split_interval:]
            temp1_df = pd.DataFrame([[0,0,0]],index = [0],columns = ['price','last','volume']);
            temp2_df = pd.DataFrame([[0,0,0]],index = [0],columns = ['price','last','volume']);
            for i in range(len(all_type)):
                col_num = data.columns.get_loc(all_type[i])
                temp1_df['price'] += temp1.iloc[columns[all_type[i]],col_num]/len(all_type)
            temp1_df["last"] = temp1.iloc[-1,data.columns.get_loc("close")]
            temp1_df['volume'] = temp1['volume'].sum()
            for i in range(len(all_type)):
                col_num = data.columns.get_loc(all_type[i])
                temp2_df['price'] += temp2.iloc[columns[all_type[i]],col_num]/len(all_type)
            temp2_df["last"] = temp2.iloc[-1,data.columns.get_loc("close")]
            temp2_df['volume'] = temp2['volume'].sum()
            res = pd.concat([temp1_df,temp2_df],axis = 0,ignore_index=True)
            res["time"] = np.arange(res.shape[0]);res["code"] = code
            res["open"] = temp1.iloc[0,data.columns.get_loc("close")]
            return res.reindex(columns = ["time","code","price","last","open","volume"])
        else:
            "不进行前后时间分割"
            price_own = 0
            for i in range(len(all_type)):
                col_num = data.columns.get_loc(all_type[i])
                price_own += data.iloc[columns[all_type[i]],col_num]/len(all_type)
            volume_own = data['volume'].sum()
            last = data.iloc[-1,data.columns.get_loc("close")]
            final = pd.DataFrame([[price_own,last,volume_own]],index= [0],columns = ['price','last','volume'])
            final["time"] = np.arange(final.shape[0]);final["code"] = code
            final["open"] = data.iloc[0,data.columns.get_loc("open")]
            return final.reindex(columns = ["time","code","price","last","open","volume"])

    def tackle_2minute(self,code):
        """
        修改原始数据中的columns(每一minute级别的data)
        r'/Users/dt/Desktop/minute_data/IF.CFE'对应的文件数据
        """
        self.data['time'] = pd.to_datetime(self.data['time'])
        #align
        data = self.data[self.data['code'] == code]
        data = data.set_index('time').resample('1min').ffill().reset_index()
        #time,code,open,high,low,close,volume,amt,month,margin_rate,maturity_date
        data = data[data['time'].apply(lambda x: self.day_start <= x.strftime('%H:%M:%S') <= self.day_end)]
        #split_two_space_interval
        all_type = self.price_type.split('+')
        for i in range(len(all_type)):
            col_num = data.columns.get_loc(all_type[i])
            data["price"] += data.iloc[:,col_num]/len(all_type)
            # temp1_df['last'] += temp1.iloc[columns[all_type[i]],col_num]/len(all_type)
        data.rename(columns = {"close":"last"},inplace=True)
        return data[["time","code","price","last","open","volume"]]

    @staticmethod
    def erase_zero_volume(data_df,vol_col_list:list):
        """
        去除可能的交易点为0的机会,指定代表成交量的列
        """
        # data_df_copy = copy.copy(data_df)
        for col_num in vol_col_list:
            data_df = data_df[data_df.iloc[:,col_num]!=0]
        return data_df
        


