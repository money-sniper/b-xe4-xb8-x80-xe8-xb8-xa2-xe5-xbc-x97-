#/Users/dt/Desktop/HA/backtest_dt
#-*- coding:utf-8 -*-
#@Author:DT
#@Date:2021/6/11
#@Version:0.2
"""
专门用于管理tick级数据
根据tick级别数据聚合成分钟级数据，日频数据
生成数据结果包含
Day_Data_Management->Minute_Data_Management->Tick_Data_Management
"""

import os,sys,pickle,traceback
import copy,pandas as pd,numpy as np
from minute_data_management import Minute_Data_Management
from pdb import set_trace 

class Tick_Data_Management(Minute_Data_Management):
    """
    专门用于管理tick级数据，变换成分钟级数据，日频数据
    """
    def __init__(self,dirname,combine_path,specific_path):
        "r'/Users/dt/Desktop/HA/tick_data/跨期'"
        self.dirname = dirname
        self.combine_path = combine_path
        self.specific_path = specific_path

    # def get_whole_data(self):
    #     "得到所有的tick_combine_res1数据"
    #     file_path = os.path.join(self.dirname,'tick_combine_res1')
    #     try:
    #         res = data_manage.pickle_get(file_path)
    #     except:
    #         print(traceback.format_exc())
    #         raise Exception("整体数据读取错误")
    #     return res

    # def get_day_combine_contract(self,day,combine_pair_key):
    #     "得到某一天某一对合约的数据"
    #     file_path = os.path.join(self.dirname,self.specific_path,combine_pair_key)
    #     try:
    #         res = data_manage.pickle_get(file_path)
    #     except:
    #         print(traceback.format_exc())
    #         raise Exception("\n%s, %s数据读取错误\n"%(str(day),str(combine_pair_key)))
    #     return res

    # def get_combine_res(self,date,contract1,contract2):
    #     "同一对标的，一段时间的走势"
    #     file_path = os.path.join(self.dirname,'tick_specific_type_data')
    #     try:
    #         res = data_manage.pickle_get(file_path)
    #     except:
    #         print(traceback.format_exc())
    #         raise Exception("\n%s,标的1 %s与标的2 %s数据读取错误\n"%(str(date),str(contract1),str(contract2)))
    #     return res

    @staticmethod
    def change_freq_whole_day(data,typing):
        """
        typing用于提取每项开高低收指标中的其中一项
        """
        #前一分钟的数据
        data = data.reset_index()
        temp_1min = data[data['time'].apply(lambda x:x.strftime('%H:%M:%S') <= '09:36:00')]
        temp_1min = temp_1min.set_index('time').resample('D').ohlc().reset_index()
        #剩余时间
        temp_res = data[data['time'].apply(lambda x:x.strftime('%H:%M:%S') > '09:36:00')]
        temp_res = temp_res.set_index('time').resample('D').ohlc().reset_index()
        res = pd.concat([temp_1min,temp_res],axis=0,ignore_index=True)
        res.columns.names = ['first','second']
        res = res.rename(columns = {'':typing},level='second')
        return res.xs(typing,level='second',axis=1)

    @staticmethod
    def align(data,min_freq):
        "对齐数据，time保持连续"
        return data.set_index('time').resample(min_freq).ffill().reset_index()

    @staticmethod
    def change_freq(data,min_freq:str,typing):
        """
        数据变频,要保证data的index是datetime类型,取某一种精确的价格类型
        """
        temp = data.resample(min_freq).ohlc().reset_index()
        temp.columns.names = ['first','second']
        if len(typing.split('+')) == 1:
            temp = temp.rename(columns={'':typing},level='second')
            return temp.xs(typing,level = 'second',axis= 1)
        else:
            all_name = typing.split('+')
            for i in range(len(all_name)):
                if i == 0:
                    temp_temp = temp.rename(columns = {'':all_name[i]},level = 'second')
                    res = temp_temp.xs(all_name[i],level = 'second',axis = 1).set_index('time')/len(all_name)
                else:
                    temp_temp = temp.rename(columns = {'':all_name[i]},level = 'second')
                    res += temp_temp.xs(all_name[i],level = 'second', axis = 1).set_index('time')/len(all_name)
            return res
    
    @staticmethod
    def change_time_astype(data):
        "变换时间戳类型"
        data['time'] = pd.to_datetime(data['time'])
        return data

    @staticmethod
    def adjust_time_span(data,start_time,end_time):
        """
        data有time列
        start_time,end_time都是pd.Timestamp
        """
        date_time_HMS_bool = data['time'].apply(lambda x: start_time<=x.strftime('%H:%M:%S')<end_time)
        data_select = data[date_time_HMS_bool]
        return data_select

    @staticmethod
    def adjust_str_time_span(data,time_icol,start_time:str,end_time:str):
        """
        对于data中的日期是str的数据
        """
        start_time = pd.to_datetime(start_time);end_time = pd.to_datetime(end_time)
        data_time_bool = data.iloc[:,time_icol].apply(lambda x: start_time<=pd.to_datetime(x.split(' ')[-1])<=end_time)
        data_select = data[data_time_bool]
        return data_select

    @staticmethod
    def pickle_get(path:str):
        "保存与提取数据"
        f = open(path,'rb')
        res = pickle.load(f)
        f.close()
        return res