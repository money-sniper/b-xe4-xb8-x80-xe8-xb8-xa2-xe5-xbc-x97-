#!/usr/bin/env python
# coding: utf-8

"""
自行添加逻辑，将price行情数据转换为0,1,2...信号
以及行为方式字典
"""
import pandas as pd,numpy as np
import os,sys,copy
from pdb import set_trace

def caculate_ewma_std(ratio,df):
    """
    计算真实事件net或者cum_ret滚动ewma波动率

    Parameters:
    -----
    ratio:ewma配比系数
    df:join_data

    Returns:
    -----
    pd.Series
    """
    pass

class signal_trans:
    def __init__(self,coint_member,basis_price_change = 0.001):
        self.coint_member = coint_member
        self.basis_bias = basis_price_change

    def tackle_market_data(self,rolling_time,shift_time,signal_time,std_in = 1.25,std_out = 2.5):
        """
        对原始价格序列join_data进行处理
        Parameters:
        ------
        rolling_time:
        str    每次滚动取样间隔时间
        shift_time:
        str    每次滞后时间长度
        signal_time:
        str    每次选取一次信号长度，选中第一次后等待下一次结果

        Return:
        每个时间上的行为编码

        cum_ret与ret使用类似布林带策略
        """
        if self.coint_member._price_type in ["cum_ret","net"]:
            #{-1:空,1:多,2:无效,3:Nan,10:多止损,-10:空止损}
            #使用连续滚动mean，std，向后推迟半分钟或者一分钟
            #mean，std，原始价差数据合并
            raw_data = self.coint_member.join_data
            use_data = pd.Series(self.coint_member.gap_data_rolling,index = raw_data.index).rename("gap") #Y - X
            interval = self.coint_member.parsetime.str2delta(self.coint_member._freq) 
            rolling_time = int(np.ceil(self.coint_member.parsetime.str2delta(rolling_time)/interval)) 
            shift_time = int(np.ceil(self.coint_member.parsetime.str2delta(shift_time)/interval)) 
            use_data_rolling_mean = use_data.rolling(rolling_time).mean().shift(shift_time).rename("mean") 
            use_data_rolling_std = use_data.rolling(rolling_time).std().shift(shift_time).rename("std") 
            whole_info = pd.concat([use_data_rolling_mean,use_data_rolling_std,use_data],axis=1) 

            def action(df):
                if df["gap"] >= df["mean"] + std_in * df["std"]:
                    return 'S'
                elif df["gap"] <= df["mean"] - std_in * df["std"]:
                    return 'B'
                else:
                    return "wait"

            def action_out(df):
                if df["gap"] > df["mean"] + std_out * df["std"]:
                    return "S_out"
                elif df["gap"] < df["mean"] - std_out * df["std"]:
                    return "B_out"
                else:
                    return "wait"

            whole_info_action = whole_info.apply(lambda x:action(x),axis=1).rename("ori")
            whole_info_action_shift = copy.copy(whole_info_action).shift(1).rename("shift") #shift 1不合理（最小时间单位）
            whole_info_action_change = pd.merge(whole_info_action,whole_info_action_shift,left_index=True,right_index=True)

            whole_info_action_out = whole_info.apply(lambda x:action_out(x),axis=1).rename("out")
            whole_info_action_out = whole_info_action_out[whole_info_action_out!="wait"]
                        
            #转换出来的都是当前时间点的信号
            def action_change(df):
                stat_tuple = df["ori"],df["shift"]
                stat_F = {("wait","S"):"sell",\
                    ("wait","B"):"buy",\
                        ("wait","wait"):"None",\
                            ("B","wait"):"None",\
                                ("B","S"):"sell",\
                                    ("B","B"):"buy",\
                                        ("S","S"):"sell",\
                                            ("S","B"):"buy",\
                                                ("S","wait"):"None"}
                return stat_F[stat_tuple]

            signal2_action = whole_info_action_change.dropna().apply(action_change,axis=1).rename("action")
            signal2_action_shift = copy.copy(signal2_action).shift(1).rename("shift") #shift 1不合理(最小时间单位)
            signal2_action_change = pd.merge(signal2_action,signal2_action_shift,left_index=True,right_index=True)
            
            # 当前时刻连续时间信号，以下是用某个连续指标，强化信号
            # def action_coef(old_action,action):
            #     stat = (old_action,action)
            #     if stat in (("sell","buy"),("buy","sell")):
            #         return -1
            #     elif stat in (("buy","buy"),("sell","sell")):
            #         return 2
            #     elif stat in (("sell","wait"),("buy","wait"),("wait","buy"),("wait","sell")):
            #         return 1
            #     elif stat in (("wait","wait")):
            #         return 0
    
            # #找到连续5个self._freq时间段内有连续触发，中间没有反向信号:
            # def continous_action(df):
            #     """
            #     Parameters:
            #     ------
            #     df:所有时间信号处理,本处使用signal2_action_change,
            #          col -> action, shift
            #     index
            #     UpdateTime

            #     Returns:
            #     ------
            #     每个时间戳对应策略
            #     """
            #     resample_freq = self.coint_member.str2delta(self.coint_member._freq)*5
            #     whole_signal_res = {}
            #     for u,v in df.resample(resample_freq):
            #         num = 0;old_value = None 
            #         for index,value in v.items():
            #             if num > 0:
            #                 if (value == old_value) & (value):
            #                     pass
            #             else:
            #                 old_value = value
            def test(df):
                if df.shape[0]==0:
                    return np.nan
                else:
                    return df.iloc[[0]]
            res1 = signal2_action_change.dropna().resample(signal_time).apply(lambda x:test(x)).dropna()
            return res1,whole_info_action_out

        elif self.coint_member._price_type in ["ret"]:
            """
            间隔收益率服从一个布朗运动，超过一定范围都会均值回归
            """
            #{-1:空,1:多,2:无效,3:Nan}  
            use_data = self.coint_member.join_data_rolling_OLS
            raw_data = self.coint_member.join_data
            basis_bias = np.abs(self.basis_bias/raw_data[self.coint_member._Y].iloc[0])
            def trans(df):
                asset = self.coint_member._Y
                if pd.isnull(df[asset]):
                    return '3'
                elif np.abs(df[asset]) <= basis_bias:
                    return '2'
                elif df[asset] > basis_bias:
                    return '1'
                elif df[asset] < -basis_bias:
                    return '-1'
            res = use_data.apply(trans,axis=1)
            return res,res.shift(1)

