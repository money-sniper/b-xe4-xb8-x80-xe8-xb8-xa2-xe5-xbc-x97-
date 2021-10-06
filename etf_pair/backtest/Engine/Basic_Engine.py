#-*-coding: UTF-8 -*-
#Author:DT
#Date:2021-09-13
"""
创建基本引擎
"""

import os,sys,traceback,copy
from dateutil.parser import parse
from abc import ABC,abstractmethod
import numpy as np,pandas as pd
import pickle
from ..Data_Manage.tick_data_management import Tick_Data_Management
from ..Data_Manage.minute_data_management import Minute_Data_Management
import datetime,time
from pdb import set_trace
import matplotlib.pyplot as plt
from prettytable import PrettyTable
import csv

def func_intro(fn):
    def say(*args,**kwargs):
        print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M%:%S'),'-'*10,fn.__name__)
        return fn(*args,**kwargs)
    return say

class Basic_Engine(ABC):
    def __init__(self,daterange,path_name,initial_money,freq_caculate,slipper):
        self.file_dirpath =  path_name#原始文件路径
        self.daterange:list = daterange
        self.initial_money = initial_money
        self.C_margin_rate = 1
        self.strategy = None
        self.deal_res={}#交易结果
        self.freq_caculate = freq_caculate

        self.caculate_index = 0
        self.today = None
        self.data = {} #当使用到日频data行情时，需要有last列
        self.index = 0
        self.pos = {}#持仓方向明细
        self.txt = ''
        self.specific_margin = {} #记录每个标的占用的保证金
        self.whole_account = {'start_equity':initial_money,'available':initial_money,\
                        'margin_rate':0,'margin':0,'final_equitys':0,'transaction_fee':0,'float_ping_revenue':0,\
                            "hold_on_margin":0}
        self.day_return = {}
        self.asset_group = []

        self.slipper = slipper
        self.open_fee_rate = 0.000023
        self.close_yesterday_fee_rate = 0.000023
        self.close_today_fee_rate = 0.000345
        self.jiaoge_fee = 0.0001
        self.contract_multiplier = 200
        self.contract_multiplier = 300
        self.change_fee_perhands = 10
        self.declare_fee_perhands = 1
        self.margin_use_limite_rate = 0.8

        self.trade_profit_info = {}
        self.signal_count = 0
        self.hangqing = {}
        self.day_change = []

    def write_log(self,string:str,day=None):
        print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),'-------',string,'\n')
        if day:
            self.txt+=(day+'\t'+str(self.index)+'\t'+string+'\n')
        elif day is None:
            self.txt+=(str(self.index) + '\t' + string + '\n')

    @func_intro
    @abstractmethod
    def initialize(self):
        """
        初始资金账户
        """
        pass

    @func_intro
    @abstractmethod
    def check_build_pos(self):
        """
        初始化建立仓位字典
        """
        pass

    @func_intro
    @abstractmethod
    def caculate_margin(self):
        """
        期货账户：更新账户保证金
        普通账户：更新本金
        """
        pass

    @func_intro
    @abstractmethod
    def update_pos_price(self):
        """
        更新账户资产最新价格
        """
        pass

    @func_intro
    @abstractmethod
    def update_pos(self):
        """
        更新账户持仓信息
        """
        pass

    @func_intro
    @abstractmethod
    def caculate_pingcang_profit(self):
        """
        计算平仓盈亏
        """
        pass

    @func_intro
    @abstractmethod
    def caculate_revenue_margin(self):
        """
        计算浮动盈亏
        并且计算保证金
        """
        pass

    @func_intro
    @abstractmethod
    def day_end_reset_whole_account(self):
        """
        当日结算结果
        """
        pass

    @func_intro
    @abstractmethod
    def send_limit_orders(self):
        """
        发送限价单
        """
        pass

    @func_intro
    @abstractmethod
    def send_market_orders(self):
        """
        发送市价单
        """
        pass

    @func_intro
    @abstractmethod
    def send_cancel_orders(self):
        """
        发送撤单
        """
        pass

    @func_intro
    @abstractmethod
    def _OnRtnTrade(self):
        """
        处理订单回报
        """
        pass

    @func_intro
    @abstractmethod
    def run_backtesting(self):
        """
        整体回测框架
        """
        pass

    @func_intro
    @abstractmethod
    def save_data(self):
        """
        保存所有数据结果
        """
        pass
    
    @func_intro
    @abstractmethod
    def update_specific_account(self,code,typing):
        data_use = self.data[code].iloc[self.index]
        if typing == "start":
            price_present = data_use.loc["open"]
        elif typing == "last":
            price_present = data_use.loc["last"]
        max_pos = self.get_max_margin_pos_volume(code)
        self.specific_margin[code] = max_pos*self.contract_multiplier*price_present*self.C_margin_rate

    def get_max_margin_pos_volume(self,code):
        open_B = self.pos[code]['open_B_yesterday'] + self.pos[code]['open_B_today']
        open_S = self.pos[code]['open_S_yesterday'] + self.pos[code]['open_S_today']
        return max(open_B,open_S)

    def get_pos_volume(self,code):
        open_B = self.pos[code]['open_B_yesterday'] + self.pos[code]['open_B_today']
        open_S = self.pos[code]['open_S_yesterday'] + self.pos[code]['open_S_today']
        return {'open_B':open_B,'open_S':open_S}
