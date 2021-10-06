#/Users/dt/Desktop/HA/huatai_strategy
#-*-coding: UTF-8 -*-
#Author: DT
#Date: 2021-04-30
#日内级别回测事例类

import os,sys,traceback,copy
from dateutil.parser import parse
import numpy as np,pandas as pd
from tick_data_management import Tick_Data_Management
import datetime,time
from pdb import set_trace
import matplotlib.pyplot as plt
from .Future_Engine import func_intro,pos_management,Future_Engine
from ....Strategy.strategy_class import Strategy_Class

class Day_strategy_class(Strategy_Class):
    @func_intro
    def core_logic(self,signal):
        code = self.engine.asset_group[0]
        if signal == 1:#买入
            pos = self.engine.get_pos_volume(code)
            if (pos['long_B'] == 0) & (pos['long_S'] == 0):
                self.send_market_order_func(code = code,volume = 1,longshort = 'long',direction = 'B')
            elif (pos['long_B'] == 1) & (pos['long_S'] == 0):
                pass
            elif (pos['long_B'] == 0) & (pos['long_S']  == 1):
                if self.engine.pos[code]['long_S_yesterday']:
                    self.send_market_order_func(code = code,volume = 1,longshort = 'short_yesterday',direction = 'S')
                else:
                    self.send_market_order_func(code = code,volume = 1,longshort = 'short_today',direction = 'S')
                self.send_market_order_func(code = code,volume = 1,longshort = 'long',direction = 'B')
        elif signal == -1:#卖出
            pos = self.engine.get_pos_volume(code)
            if (pos['long_S'] == 0) & (pos['long_B'] == 0):
                self.send_market_order_func(code = code,volume = 1,longshort = 'long',direction = 'S')
            elif (pos['long_S'] == 1) & (pos['long_B'] == 0):
                pass
            elif (pos['long_S'] == 0) & (pos['long_B']  == 1):
                if self.engine.pos[code]['long_B_yesterday']:
                    self.send_market_order_func(code = code,volume = 1,longshort = 'short_yesterday',direction = 'B')
                else:
                    self.send_market_order_func(code = code,volume = 1,longshort = 'short_today',direction = 'B')
                self.send_market_order_func(code = code,volume = 1,longshort = 'long',direction = 'S')

    @func_intro
    def output_signal(self,code):
        """
        freq->second
        0代表卖，1代表买
        """
        signal_random = np.random.randint(2,size = 2)#开盘与收盘的操作
        self.signal = signal_random

class Day_Engine(Future_Engine):
    def define_strategy(self):
        self.strategy = Day_strategy_class(self)

    def huanqi(self):
        for code in self.pos:
            pos = self.get_pos_volume(code)
            if pos['long_B']:
                if self.pos[code]['long_B_yesterday']:
                    self.send_market_orders(code,pos['long_B_yesterday'],longshort = 'short_yesterday',direction = 'B')
                if self.pos[code]['long_B']:
                    self.send_market_orders(code,pos['long_B'],longshort = 'short_today',direction='B')
            if pos['long_S']:
                if self.pos[code]['long_S_yesterday']:
                    self.send_market_orders(code,pos['long_S_yesterday'],longshort='short_yesterday',direction='S')
                if self.pos[code]['long_S']:
                    self.send_market_orders(code,pos['long_S'],longshort='short_today',direction='S')

    def end_subject(self):
        for code_name in self.asset_group:
            self.pos.pop(code_name)
            self.data.pop(code_name)

    @func_intro
    def send_limit_orders(self,code,volume,price,direction,longshort,**args):
        return self._OnRtnTrade(code,price,volume,direction,longshort)

    @func_intro
    def send_market_orders(self,code,volume,direction,longshort,**args):
        direct = Day_Engine.judge_sell_buy(direction,longshort)
        ask_column = self.data[code].columns.get_loc('ask1')
        bid_column = self.data[code].columns.get_loc('bid1')
        if direct == 1:
            price = self.data[code].iloc[self.index,ask_column]
        elif direct == -1:
            price = self.data[code].iloc[self.index,bid_column]
        return self._OnRtnTrade(code,price,volume,direction,longshort,typing = 'market')

    @func_intro
    def send_cancel_orders(self,code,volume,direction,longshort):
        pass

    @func_intro
    def _OnRtnTrade(self,code,price,volume,direction,longshort,typing=None):
        direct = Day_Engine.judge_sell_buy(direction,longshort)
        ask_column = self.data[code].columns.get_loc('ask1')
        bid_column = self.data[code].columns.get_loc('bid1')
        if (direct == 1) & (price >= self.data[code].iloc[self.index,ask_column]):
            vol = min(volume,self.data[code].iloc[self.index]['asize1'])
            pos_args = {'code':code,'price':price + self.slipper,'volume':vol,'direction':direction,'openclose':longshort}
            self.update_pos(**pos_args)
            self.deal_res.setdefault(code,[]);pos_args.update({'day':self.day,'index':self.index})
            self.deal_res[code].append(pos_args)
            full_deal = True if vol == volume else False
            return full_deal,pos_args
        elif (direct == -1) & (price <= self.data[code].iloc[self.index,bid_column]):
            vol = min(volume,self.data[code].iloc[self.index]['bsize1'])
            pos_args = {'code':code,'price':price - self.slipper,'volume':vol,'direction':direction,'openclose':longshort}
            self.update_pos(**pos_args)
            self.deal_res.setdefault(code,[]);pos_args.update({'day':self.day,'index':self.index})
            self.deal_res[code].append(pos_args)
            full_deal = True if vol == volume else False
            return full_deal,pos_args

    def load_resample_data(self,name):
        """
        freq->对应resample中的频率,价格取某一种字段，转换成ask,bid,asize,bsize
        """
        for code in self.data:
            self.data[code] = Tick_Data_Management.change_freq_whole_day(self.data[code].set_index('time')[['open','high','low','close','volume']],name)

    @staticmethod
    def get_main_contract(data):
        stock = data['code'].unique()
        return sorted(stock)[0]

    def run_backtesting(self,day,freq:int,typing='normal'):
        '每天循环回测'
        filepath = os.path.join(self.file_dirpath,day+'.csv')
        _file = pd.read_csv(filepath)
        main_code = Day_Engine.get_main_contract(_file)
        self.load_data_pickle(filepath,day,[main_code])
        self.load_resample_data('close') #可以更改
        self.strategy.output_signal(main_code)
        today_data_shape = self.data[main_code].shape[0]
        self.initialize(main_code) #初始化
        if typing == 'huanqi':
            if self.index == 0:
                signal_use = self.strategy.signal[self.index]
                self.strategy.core_logic(signal_use)
                self.caculate_revenue_margin(day)
                self.index += freq
            elif self.index == 1:#在下一个频率进行换品种
                self.huanqi()
                self.caculate_revenue_margin(day)
                self.end_subject()
        elif typing == 'normal':
            while self.index < today_data_shape:
                signal_use = self.strategy.signal[self.index]
                self.strategy.core_logic(signal_use)
                if self.index == today_data_shape - 1:
                    self.caculate_revenue_margin(day,end_signal = True)
                else:
                    self.caculate_revenue_margin(day,end_signal = False)
                self.index += freq
        self.day_end_reset_whole_account(day)

    def run(self,freq:int = 1):
        for day in self.daterange: #str_list
            self.day = day
            datetime_day = pd.to_datetime(day)
            year,month = datetime_day.year,datetime_day.month
            day_3 = datetime.datetime(year,month,1)+pd.tseries.offsets.WeekOfMonth(week=2,weekday=4)
            day_2 = day_3 - pd.tseries.offsets.BDay(1)
            if datetime_day == day_3:
                pass
            elif datetime_day == day_2:
                self.run_backtesting(**{'day':day,'typing':'huanji','freq':freq})
            else:
                self.run_backtesting(**{'day':day,'typing':'normal','freq':freq})
