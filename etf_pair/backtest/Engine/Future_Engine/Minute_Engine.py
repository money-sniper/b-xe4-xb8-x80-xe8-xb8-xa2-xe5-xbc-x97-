#-*-coding: UTF-8 -*-
#Author: DT
#Date: 2021-04-29
#分钟级别回测事例类

"""
待修改部分
backtesting函数试用metaclass动态定义函数
"""
import os,sys,traceback,copy
from dateutil.parser import parse
import numpy as np,pandas as pd
from ...Data_Manage.tick_data_management import Tick_Data_Management
from ...Data_Manage.minute_data_management import Minute_Data_Management
import datetime,time
from pdb import set_trace
import matplotlib.pyplot as plt
from .Future_Engine import func_intro,pos_management,Future_Engine
from strategy_class import Strategy_Class

class Minute_strategy_class(Strategy_Class):
    @func_intro
    def core_logic(self,signal):
        code = self.engine.asset_group[0]
        if signal == -1:#买入
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
        elif signal == 1:#卖出
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
    def output_signal(self,code,freq):
        'freq->second'
        ewma_10 = self.get_mean_different_freq(code,'10min')
        ewma_20 = self.get_mean_different_freq(code,'20min')
        ewma_signal = pd.merge(ewma_10,ewma_20,left_on='time',right_on='time')
        ewma_signal['diff'] = ewma_signal['10min'] - ewma_signal['20min']
        ewma_signal['diff_shift'] = ewma_signal['diff'].shift(1)
        def judge(raw):
            if (raw.loc['diff'] > 0) & (raw.loc['diff_shift'] < 0):
                return -1
            elif (raw.loc['diff'] < 0) & (raw.loc['diff_shift'] > 0):
                return 1
            else:
                return 0
        ewma_signal['signal'] = ewma_signal.apply(judge,axis=1)
        if freq != 1:
            index_use = np.arange(0,self.engine.data[code].shape[0],freq)
            index_signal = ewma_signal[ewma_signal['signal']!=0].index
            index_use = index_signal.intersection(index_use)
        else:
            index_use = ewma_signal[ewma_signal['signal']!=0].index
        self.signal = ewma_signal.loc[index_use]

    @func_intro
    def get_mean_different_freq(self,code,freq_name):
        return self.engine.data[code][['time','last']].set_index('time').rolling(freq_name).mean().rename(columns = {'last':freq_name}).reset_index()

    @func_intro
    def close(self): #不留隔夜仓位
        for code in self.engine.pos:
            pos_port = self.engine.pos[code]
            if pos_port['long_B_today']:
                self.send_market_order_func(code = code,volume = pos_port['long_B_today'],longshort = 'short_today',direction = 'B')
            if pos_port['long_B_yesterday']:
                self.send_market_order_func(code = code,volume = pos_port['long_B_yesterday'],longshort = 'short_yesterday',direction = 'B')
            if pos_port['long_S_today']:
                self.send_market_order_func(code = code,volume = pos_port['long_S_today'],longshort = 'short_today',direction = 'S')
            if pos_port['long_S_yesterday']:
                self.send_market_order_func(code = code,volume = pos_port['long_S_yesterday'],longshort = 'short_yesterday',direction = 'S')

class Minute_Engine(Future_Engine):
    def define_strategy(self):
        self.strategy = Minute_strategy_class(self)

    def huanqi(self):
        for code in self.pos:
            pos = self.get_pos_volume(code)
            if pos['long_B']:
                if self.pos[code]['long_B_yesterday']:
                    self.send_markets_orders(code,pos['long_B_yesterday'],longshort = 'short_yesterday',direction = 'B')
                if self.pos[code]['long_B']:
                    self.send_markets_orders(code,pos['long_B'],longshort = 'short_today',direction='B')
            if pos['long_S']:
                if self.pos[code]['long_S_yesterday']:
                    self.send_markets_orders(code,pos['long_S_yesterday'],longshort='short_yesterday',direction='S')
                if self.pos[code]['long_S']:
                    self.send_markets_orders(code,pos['long_S'],longshort='short_today',direction='S')

    def load_resample_data(self,freq,name):
        """
        freq->对应resample中的频率
        """
        for code in self.data:
            self.data[code] = data_manage.change_freq(self.data[code].set_index('time')[['last','ask1','bid1','asize1','bsize1']],freq,name)

    def end_subject(self,code):
        self.pos.pop(code)
        self.data.pop(code)

    @func_intro
    def send_limit_orders(self,code,volume,price,direction,longshort,**args):
        # return self._OnRtnTrade(code,price,volume,direction,longshort)
        pass

    @func_intro
    def send_market_orders(self,code,volume,direction,longshort,**args):
        direct = Minute_Engine.judge_sell_buy(direction,longshort)
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
    def _OnRtnTrade(self,code,price,volume,direction,longshort,typing):
        if typing == 'market':
            direct = Minute_Engine.judge_sell_buy(direction,longshort)
            ask_column = self.data[code].columns.get_loc('ask1')
            bid_column = self.data[code].columns.get_loc('bid1')
            if (direct == 1) & (price >= self.data[code].iloc[self.index,ask_column]):
                vol = min(volume,self.data[code].iloc[self.index]['asize1'])
                pos_args = {'code':code,'price':price + self.slipper,'volume':vol,'direction':direction,'longshort':longshort}
                self.update_pos(**pos_args)
                self.deal_res.setdefault(code,[]);pos_args.update({'day':self.day,'index':self.index})
                self.deal_res[code].append(pos_args)
                full_deal = True if vol == volume else False
                return full_deal,pos_args
            elif (direct == -1) & (price <= self.data[code].iloc[self.index,bid_column]):
                vol = min(volume,self.data[code].iloc[self.index]['bsize1'])
                pos_args = {'code':code,'price':price - self.slipper,'volume':vol,'direction':direction,'longshort':longshort}
                self.update_pos(**pos_args)
                self.deal_res.setdefault(code,[]);pos_args.update({'day':self.day,'index':self.index})
                self.deal_res[code].append(pos_args)
                full_deal = True if vol == volume else False
                return full_deal,pos_args
        else:
            pass

    @staticmethod
    def get_main_contract(data):
        stock = data['code'].unique()
        return sorted(stock)[0]

    def run_backtesting(self,day,typing,freq,**args):
        '每天循环回测'
        self.write_log(str(day)+'  begin backtesting')
        filepath = os.path.join(self.file_dirpath,day+'.csv')
        _file = pd.read_csv(filepath)
        main_code = Minute_Engine.get_main_contract(_file)
        self.load_data_pickle(filepath,day,[main_code])
        self.load_resample_data('1min','close') #可以更改
        self.strategy.output_signal(main_code,1)
        today_data_shape = self.data[main_code].shape[0]
        self.initialize(main_code) #初始化
        if typing == 'huanqi':
            while self.index < today_data_shape*3/4:
                if self.index in self.strategy.signal.index:
                    signal_use = self.strategy.signal.loc[self.index,'signal']
                    self.strategy.core_logic(signal_use)
                self.caculate_revenue_margin(day)
                self.index += freq
            else:#在下一个频率进行换品种
                self.huanqi()
                self.caculate_revenue_margin(day)
                self.end_subject(main_code)
        elif typing == 'normal':
            while self.index < today_data_shape:
                if self.index in self.strategy.signal.index:
                    signal_use = self.strategy.signal.loc[self.index,'signal']
                    self.strategy.core_logic(signal_use)
                if self.index == today_data_shape - 1:
                    self.strategy.close()
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
