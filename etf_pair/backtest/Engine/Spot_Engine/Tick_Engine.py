#-*-coding: UTF-8 -*-
#Author: DT
#Date: 2021-04-27
#Tick级别回测事例类

import os,sys,traceback,copy
from dateutil.parser import parse
import numpy as np,pandas as pd
from ...Data_Manage.tick_data_management import Tick_Data_Management
from ...Data_Manage.minute_data_management import Minute_Data_Management
import datetime,time
import dill as pickle
from pdb import set_trace
import matplotlib.pyplot as plt
from ..Basic_Engine import Basic_Engine,func_intro
from etf_pair.Strategy.strategy_instance import signal_instance,strategy_etf_pair_type11
from ....Pre_Data.convert_file import convertdata
from ..Future_Engine import Future_Engine


class Tick_Engine(Future_Engine):
    def __init__(
        self,
        daterange,
        initial_money,
        freq_caculate,
        slipper,
        today,
        path_name = "None"):
        super().__init__(daterange = daterange,path_name = path_name,initial_money = initial_money,\
            freq_caculate = freq_caculate,slipper = slipper)
        self.day = today
        self.open_fee_rate = 0.000004 - 0.0003
        self.close_yesterday_fee_rate = 0.00004 - 0.0003
        self.close_today_fee_rate = 0.000345
        self.tick_res = {}

    def define_strategy(self,strategy,coint_inst):
        self.strategy = strategy
        self.strategy.signal = signal_instance(coint_inst)
        self.contract_multiplier = 1 #每次成交量都是代表一股
        self.strategy.kong_X = 0;self.strategy.kong_Y = 0
        self.strategy_coint_member = self.strategy.signal.coint_member

    def load_data_pickle(self,day):
        """
        读取dat文件数据
        """
        self.write_log('load_data-----'+str(day))
        self.tick_res.setdefault(day,[])
        try:
            self.strategy_coint_member.get_day_data(day,"3s")
            self.data = {"500":self.strategy_coint_member._data500,\
                "1000":self.strategy_coint_member._data1000}
        except:
            raise Exception("ETF_rolling数据提取出错")
        self.write_log("数据历史行情加载完成")

    def check_build_pos(self,code:list):
        assert type(code) is list,"code 需要是一个列表"
        for code_specific in code:
            if code_specific not in self.pos:
                self.pos.update({code_specific:{'open_B_today':0.0,'open_B_yesterday':0.0,'open_S_today':0.0,'open_S_yesterday':0.0,'margin':0.0,\
                    'price_B':0.0,'price_S':0.0}})

    def initialize(self,code,day):
        """
        策略初期建立仓位，包括每日重置循环索引，每日盈亏，手续费
        """
        #对新入的仓位建仓
        self.today = day
        self.check_build_pos(code)
        self.asset_group = {"Y":self.strategy.signal.coint_member._Y.split('_')[-1],\
            "X":self.strategy.signal.coint_member._X.split('_')[-1]}
        #初始化策略freq指标
        self.index = 0
        self.caculate_index = 0
        #初始化账户 
        self.whole_account['transaction_fee'] = 0
        self.whole_account['float_ping_revenue'] = 0
        self.caculate_margin(typing = "start") #包含计算保证金，与包含计算自有资金（今日盈亏等）

    @staticmethod
    def judge_sell_buy(direction,openclose):
        """
        买开，卖平 都是买:total_num->1
        卖开，买平 都是卖:total_num->-1
        """
        direction_num = 1 if direction=='B' else -1
        openclose_num = 1 if openclose.split('_')[0]=='open' else -1
        total_num = direction_num*openclose_num
        return total_num

    @func_intro
    def send_limit_orders(self,code,volume,price,direction,openclose,**args):
        return self._OnRtnTrade(code,price,volume,direction,openclose,typing="limit")

    @func_intro
    def send_market_orders(self,code,volume,direction,openclose,**args):
        return self._OnRtnTrade(code,-1,volume,direction,openclose,typing="market")

    @func_intro
    def send_cancel_orders(self,code,volume,direction,openclose):
        pass

    def get_price_col(self,direct,code):
        if direct == 1:
            price_col = [self.data[code].columns.get_loc("BidPrice1"),\
                    self.data[code].columns.get_loc("BidPrice2"),\
                        self.data[code].columns.get_loc("BidPrice3"),\
                            self.data[code].columns.get_loc("BidPrice4"),\
                                self.data[code].columns.get_loc("BidPrice5")]
            volume_col = [self.data[code].columns.get_loc("BidVolume1"),\
                self.data[code].columns.get_loc("BidVolume2"),\
                    self.data[code].columns.get_loc("BidVolume3"),\
                        self.data[code].columns.get_loc("BidVolume4"),\
                            self.data[code].columns.get_loc("BidVolume5")]
        elif direct == -1:
            price_col = [self.data[code].columns.get_loc("AskPrice1"),\
                    self.data[code].columns.get_loc("AskPrice2"),\
                        self.data[code].columns.get_loc("AskPrice3"),\
                            self.data[code].columns.get_loc("AskPrice4"),\
                                self.data[code].columns.get_loc("AskPrice5")]
            volume_col = [self.data[code].columns.get_loc("AskVolume1"),\
                self.data[code].columns.get_loc("AskVolume2"),\
                    self.data[code].columns.get_loc("AskVolume3"),\
                        self.data[code].columns.get_loc("AskVolume4"),\
                            self.data[code].columns.get_loc("AskVolume5")]
        return price_col,volume_col

    @func_intro
    def _OnRtnTrade(self,code,price,volume,direction,openclose,typing):
        """
        limit每次根据self.index判断能否成交
        market一次完全成交
        """
        if typing == "limit": #依然不完善
            direct = Tick_Engine.judge_sell_buy(direction,openclose)
            ask_column = self.data[code].columns.get_loc('AskPrice1')
            bid_column = self.data[code].columns.get_loc('BidPrice1') 
            if (direct == 1):
                if (price >= self.data[code].iloc[self.index,ask_column]):
                    vol = min(volume,self.data[code].iloc[self.index]['AskVolume1'])
                    pos_args = {'code':code,'price':price + self.slipper,'volume':vol,'direction':direction,'openclose':openclose}
                    self.update_pos(**pos_args)
                    self.deal_res.setdefault(code,[]);pos_args.update({'day':self.day,'index':self.index})
                    self.deal_res[code].append(pos_args)
                    full_deal = True if vol == volume else False
                    pos_args.update({"volume":volume - vol,"price":price})
                    return full_deal,pos_args
                else:
                    pos_args = {"code":code,"price":price,"volume":vol,"direction":direction,"openclose":openclose}
                    return "No deal",pos_args
            elif (direct == -1):
                if (price <= self.data[code].iloc[self.index,bid_column]):
                    vol = min(volume,self.data[code].iloc[self.index]['BidVolume1'])
                    pos_args = {'code':code,'price':price - self.slipper,'volume':vol,'direction':direction,'openclose':openclose}
                    self.update_pos(**pos_args)
                    self.deal_res.setdefault(code,[]);pos_args.update({'day':self.day,'index':self.index})
                    self.deal_res[code].append(pos_args)
                    full_deal = True if vol == volume else False
                    pos_args.update({"volume":volume-vol,"price":price})
                    return full_deal,pos_args
                else:
                    pos_args = {"code":code,"price":price,"volume":vol,"direction":direction,"openclose":openclose}
                    return "No deal",pos_args
        elif typing == "market":
            #最多吃盘口的五档行情
            direct = Tick_Engine.judge_sell_buy(direction,openclose)
            if direct == 1:
                price_col,vol_col = self.get_price_col(1,code)
                start_index = 0
                while (volume > 0) & (start_index < 5): #5是只吃掉5档行情
                    vol = min(volume,self.data[code].iloc[self.index,vol_col[start_index]])
                    price = self.data[code].iloc[self.index,price_col[start_index]]
                    pos_args = {"code":code,"price":price+self.slipper,"volume":vol,"direction":direction,"openclose":openclose}
                    self.update_pos(**pos_args)
                    self.deal_res.setdefault(self.today,[]);self.deal_res[self.today].append(pos_args)
                    volume -= vol
                    start_index+=1
            elif direct == -1:
                price_col,vol_col = self.get_price_col(-1,code)
                start_index = 0
                while (volume > 0) & (start_index < 5): #5是只吃掉5档行情
                    vol = min(volume,self.data[code].iloc[self.index,vol_col[start_index]])
                    price = self.data[code].iloc[self.index,price_col[start_index]]
                    pos_args = {"code":code,"price":price+self.slipper,"volume":vol,"direction":direction,"openclose":openclose}
                    self.update_pos(**pos_args)
                    self.deal_res.setdefault(self.today,[]);self.deal_res[self.today].append(pos_args)
                    volume -= vol
                    start_index += 1

    def update_pos(self,code,price,volume,direction,openclose):
        """
        根据成交信息改变持仓情况
        并且改变账户信息
        更新手续费
        """
        if code not in self.pos:
            initial_dict = {'open_B_today':0.0,'open_B_yesterday':0.0,'open_S_today':0.0,'open_S_yesterday':0.0,'margin':0.0,\
                'price_B':0.0,'price_S':0.0}
            self.pos.update({code:initial_dict})
        if openclose == 'close_today':
            transaction_fee = price*volume*self.contract_multiplier*self.close_today_fee_rate
            self.whole_account['transaction_fee'] += transaction_fee
            pos_input = {'code':code,'price':price,'volume':volume,'direction':direction,'openclose':openclose}
            self.order_change_pos(**pos_input)
        elif openclose == 'close_yesterday':
            transaction_fee = price*volume*self.contract_multiplier*self.close_yesterday_fee_rate
            self.whole_account['transaction_fee'] += transaction_fee
            pos_input = {'code':code,'price':price,'volume':volume,'direction':direction,'openclose':openclose}
            self.order_change_pos(**pos_input)
        elif openclose == 'open':
            transaction_fee = price*volume*self.contract_multiplier*self.open_fee_rate
            self.whole_account['transaction_fee'] += transaction_fee
            pos_input = {'code':code,'price':price,'volume':volume,'direction':direction,'openclose':openclose}
            self.order_change_pos(**pos_input)

    def order_change_pos(self,code,price,volume,direction,openclose):
        "其中对于平仓部分，volume的控制在之前update_pos自行控制有无超过行情许可"
        if code not in self.pos:
            raise Exception('pos argument error')
        else:
            if (direction == 'B') & (openclose.split('_')[0] == 'open'):
                code_pos = self.pos[code]
                price_B = code_pos['price_B'];vol_B = code_pos['open_B_today'] + code_pos['open_B_yesterday']
                price_B = (price_B*vol_B + price*volume)/(vol_B + volume)
                code_pos['open_B_today']+=volume;code_pos['price_B'] = price_B
            elif (direction == 'B') & (openclose.split('_')[0] == 'close'):
                code_pos = self.pos[code]
                price_B_ori = code_pos['price_B'];vol_B = code_pos['open_B_today'] + code_pos['open_B_yesterday']
                assert vol_B>=volume, "下单量错误,当前仓位不满足下单量"
                price_B = (price_B_ori*vol_B - price*volume)/(vol_B - volume) if (vol_B - volume) > 0 else 0;
                if price_B == 0:
                    self.caculate_pingcang_profit(code,price,volume,direction,openclose)
                code_pos['price_B'] = price_B
                if openclose == 'close_today':
                    code_pos['open_B_today']-=volume
                    if code_pos['open_B_today']<0:
                        raise Exception('pos_B_today_error')
                elif openclose == 'close_yesterday':
                    code_pos['open_B_yesterday']-=volume
                    if code_pos['open_B_yesterday']<0:
                        raise Exception('pos_B_yesterday_error')
            elif (direction == 'S') & (openclose.split('_')[0] == 'open'):
                code_pos = self.pos[code]
                price_S = code_pos['price_S'];vol_S = code_pos['open_S_today'] + code_pos['open_S_yesterday']
                price_S = (price_S*vol_S + price*volume)/(vol_S + volume);code_pos['price_S'] = price_S
                code_pos['open_S_today']+=volume
            elif (direction == 'S') & (openclose.split('_')[0] == 'close'):
                code_pos = self.pos[code]
                price_S_ori = code_pos['price_S'];vol_S = code_pos['open_S_today'] + code_pos['open_S_yesterday']
                assert vol_S>=volume,"下单量错误，当前仓位不满足下单量"
                price_S = (price_S_ori*vol_S - price*volume)/(vol_S- volume) if (vol_S - volume) > 0 else 0;
                if price_S == 0:
                    self.caculate_pingcang_profit(code,price,volume,direction,openclose)
                code_pos['price_S'] = price_S
                if openclose == 'close_today':
                    code_pos['open_S_today']-=volume
                    if code_pos['open_S_today']<0:
                        raise Exception('pos_S_today_error')
                elif openclose == 'close_yesterday':
                    code_pos['open_S_yesterday']-=volume
                    if code_pos['open_S_yesterday'] < 0:
                        raise Exception('pos_S_yesterday_error')
            self.caculate_revenue_margin(self.today,end_signal=False,caculate_index_add=False) 

    def caculate_pingcang_profit(self,code,price,volume,direction,openclose):
        """
        部分平仓，其中的平仓盈亏是计算在其中的
        全部平仓，其中的平仓盈亏是无法计算在其中的
        """
        code_pos = self.pos[code]
        if (direction =='B') & (openclose.split('_')[0] == 'close'):
            ori_price = code_pos["price_B"]
            self.whole_account['float_ping_revenue'] += (price - ori_price)*volume*self.contract_multiplier
        elif (direction == 'S') & (openclose.split('_')[0] == 'close'):
            ori_price = code_pos["price_S"]
            self.whole_account['float_ping_revenue'] -= (price - ori_price)*volume*self.contract_multiplier

    def caculate_revenue_margin(self,day,end_signal = False,caculate_index_add = True):
        """
        管理浮动盈亏
        typing是order时，对pos中每个标的的price取平均，即包括开仓和平仓盈亏
        typing是constant时，则对持仓不动计算浮动盈亏
        input:
        code,price,volume,direction,openclose
        end_signal为True时表示当前时间需要进行结算
        self.caculate_index会依次递增

        弊端：不能适合逐笔行情与Tick行情
        """
        name = self.asset_group['Y']
        data_shape = self.data[name].shape[0]
        coef = self.contract_multiplier
        if (self.index < data_shape) & (self.index - self.caculate_index == self.freq_caculate):
            # self.write_log('begin:\n'+str(self.pos),day)
            #更新浮动盈亏
            for code in self.pos:
                data_now = self.data[code].iloc[self.index]
                float_revenue = (data_now['LastPrice']-self.pos[code]['price_B'])*coef*(self.pos[code]['open_B_today']+self.pos[code]['open_B_yesterday'])-\
                    (data_now['LastPrice']-self.pos[code]['price_S'])*coef*(self.pos[code]['open_S_today']+self.pos[code]['open_S_yesterday'])
                self.whole_account['float_ping_revenue'] += float_revenue
            self.update_pos_price() #用于之后计算浮动盈亏
            self.caculate_margin("last")
            self.caculate_index = self.index+1 if caculate_index_add else self.caculate_index
            self.write_log(str(self.whole_account),day)
            self.tick_res[self.today].append(self.whole_account)
        elif end_signal:#当天结束及时结算,不用结算价，当前直接用收盘价进行结算
            # self.write_log('begin:\n'+str(self.pos),day)
            #更新浮动盈亏
            for code in self.pos:
                data_now = self.data[code].iloc[self.index]
                float_revenue = (data_now['last']-self.pos[code]['price_B'])*coef*\
                    (self.pos[code]['open_B_today']+self.pos[code]['open_B_yesterday'])-\
                    (data_now['last']-self.pos[code]['price_S'])*coef*\
                        (self.pos[code]['open_S_today']+self.pos[code]['open_S_yesterday'])
                self.whole_account['float_ping_revenue'] += float_revenue
            self.update_pos_price()
            self.caculate_margin("last")
            self.tick_res[self.today].append(self.whole_account)
            self.caculate_index = self.index+1 if caculate_index_add else self.caculate_index

    def update_pos_price(self):
        for code in self.pos:
            data_use = self.data[code].iloc[self.index]
            if self.pos[code]["price_B"] != 0:
                self.pos[code]["price_B"] = data_use["LastPrice"]
            if self.pos[code]["price_S"] != 0:
                self.pos[code]["price_S"] = data_use["LastPrice"]

    def caculate_margin(self,typing):
        "更新账户总的available,margin"
        margin_tot = 0
        for code in self.pos:
            if typing == "start":
                self.update_specific_account(code,typing = "start")
            elif typing == "last":
                self.update_specific_account(code,typing = "last")
            margin_tot += self.specific_margin[code]
        self.whole_account['margin'] = margin_tot
        #计算开盘浮动盈亏
        self.whole_account['available'] = self.whole_account['start_equity'] - self.whole_account['transaction_fee'] - self.whole_account['margin']\
             + self.whole_account['float_ping_revenue']

    def update_specific_account(self,code,typing):
        data_use = self.data[code].iloc[self.index]
        if typing == "start":
            price_present = data_use.loc["OpenPrice"]
        elif typing == "last":
            price_present = data_use.loc["LastPrice"]
        max_pos = self.get_max_margin_pos_volume(code)
        self.specific_margin[code] = max_pos*self.contract_multiplier*price_present*self.C_margin_rate

    def get_max_margin_pos_volume(self,code):
        open_B = self.pos[code]['open_B_yesterday'] + self.pos[code]['open_B_today']
        open_S = self.pos[code]['open_S_yesterday'] + self.pos[code]['open_S_today']
        return max(open_B,open_S)

    def day_end_reset_whole_account(self,day:str):
        "日结算初始收益"
        today_final = self.whole_account['available'] + self.whole_account['margin']
        self.day_return.update({day: {'abs_ret':today_final - self.whole_account['start_equity'],\
            'compare_ret':(today_final - self.whole_account['start_equity'])/self.whole_account['start_equity'],\
                'final_equity':today_final,\
                    'start_equity':self.whole_account['start_equity'],\
                        'transaction_fee':self.whole_account['transaction_fee'],\
                            'float_ping_revenue':self.whole_account['float_ping_revenue']}})
        self.whole_account['final_equity'] = today_final
        self.write_log(str(self.whole_account),day)
        self.whole_account['start_equity'] = today_final
        #资产仓位变动
        for code in self.pos:
            code_pos = self.pos[code]
            code_pos['open_B_yesterday'] += code_pos['open_B_today']
            code_pos['open_S_yesterday'] += code_pos['open_S_today']
            code_pos['open_S_today'] = 0;code_pos['open_B_today'] = 0

    def end_subject(self):
        pass

    def huanqi(self):
        pass

    def run_backtesting(self):
        pass

    