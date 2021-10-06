#-*-coding: UTF-8 -*-
#Author: DT
#Date: 2021-06-30
#模拟交易所主机撮合功能——基类
#更新hold_on_margin (caculate_revenue_margin计算实时账户，update_pos计算有成交报单信息)

import os,sys,traceback,copy
from dateutil.parser import parse
import pickle
from abc import ABC,abstractmethod
from ..Basic_Engine import Basic_Engine
import numpy as np,pandas as pd
from ...Data_Manage.tick_data_management import Tick_Data_Management
from ...Data_Manage.minute_data_management import Minute_Data_Management
import datetime,time
from pdb import set_trace
import matplotlib.pyplot as plt
from prettytable import PrettyTable
import json,csv

# def func_intro(fn):
#     def say(*args,**kwargs):
#         if saying is None:
#             print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M%:%S'),'-'*10,fn.__name__)
#             return fn(*args,**kwargs)
#         else:
#             print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M%:%S'),'-'*10,fn.__name__,'\t\t',saying)
#             return fn(*args,**kwargs)
#     return say

def func_intro(fn):
    def say(*args,**kwargs):
        print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M%:%S'),'-'*10,fn.__name__)
        return fn(*args,**kwargs)
    return say

class pos_management:
    def __init__(self,code):
        self.code = code
        self.deal = []

    def __add__(self,deal_dict:dict):
        self.deal.append(deal_dict)

class Future_Engine(Basic_Engine):
    """
    对单一品种对进行套利，比如1901，1902；
    展期到1902，1903或者1902，1906，不会考虑1902，1903中间换到1902，1906再回来的情况
    初始资金1000w，保证金都设定在45%，留下10%进行补仓或者暴露敞口
    交易金手续费 8%，期货公司12%
    开平仓手续费->万分之零点二三
    平今仓手续费->万分之三点四五
    锁仓手续费（只计算单边手续费）-> 锁仓的两个持仓只收取保证金高的持仓
    ***************************
    变更手续费->10元/手（移仓手续费）,
    ******在最后一个交易日策略产生不同*********
    申报手续费->每笔1元
    交割手续费->交割金额万分之一
    要不要考虑锁仓,不频繁平仓，采用锁仓进行
    可用资金=账户资金50%-占用资金-浮动盈亏绝对值
    对沪深300股指期货、上证50股指期货和中证500股指期货的跨品种双向持仓，按照交易保证金单边较大者收取交易保证金。
    账户维持保证金最高占比80%
    margin_rate是一个品种所有开仓（锁仓使用单边最大）

    期初权益:可用资金 + 持仓占用保证金 + 手续费 - 持仓盈亏（原始正负带入） - 平仓盈亏（原始正负带入)
    当前权益：期初权益 + 持仓盈亏 + 平仓盈亏 - 手续费
    可用资金：当前权益 - 持仓占用保证金
    保证金计算：
    需要输入initial_money,margin_rate,strategy
    
    在strategy类中，每次send完order后，如果成交，send_order_manage_account管理成交对账户资金影响
    之后在另一个线程实时更新manage_own_money管理盈亏并且管理manage_margin管理保证金

    其中open,close表示开平，B,S表示看多，看空
    """
    def __init__(self,daterange,path_name,initial_money,freq_caculate:int,slipper = 0.2):
        self.file_dirpath =  path_name#原始文件路径
        self.daterange:list = daterange
        self.initial_money = initial_money
        # self.C_margin_rate = 0.14
        self.C_margin_rate = 0.12
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
        # self.open_fee_rate = 0
        # self.close_yesterday_fee_rate = 0
        # self.close_today_fee_rate = 0
        self.jiaoge_fee = 0.0001
        # self.contract_multiplier = 200
        self.contract_multiplier = 300
        self.change_fee_perhands = 10
        self.declare_fee_perhands = 1
        self.margin_use_limite_rate = 0.8

        self.trade_profit_info = {}
        self.signal_count = 0
        self.hangqing = {}
        self.day_change = []

    @func_intro     
    def load_data_pickle(self,filepath,day,code,typing='standard'):
        """
        时间数据分开处理,每种合约，每种标的
        默认数据区间是早上9点35至下午14点57
        """
        self.write_log('load_data-----'+str(day))
        try:
            data = pd.read_csv(filepath)
            data = Tick_Data_Management.change_time_astype(data)#改变时间格式
            if typing == 'standard':
                start_time='09:35:00';end_time='14:57:00'
                data = Tick_Data_Management.adjust_time_span(data,start_time,end_time)
            else:
                start_time='09:30:00';end_time='14:59:59'
                data = Tick_Data_Management.adjust_time_span(data,start_time,end_time)
            for code_specific in code:
                self.asset_group.append(code_specific)
                temp_data = Tick_Data_Management.align(data[data['code'] == code_specific],'500ms')
                self.data.update({code_specific:temp_data})
        except:
            self.write_log(traceback.format_exc())
            raise Exception("dirname,combine_path,specific_path文件路径错误")
        self.write_log("开始加载历史行情")

    def write_log(self,string:str,day=None):
        print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),'-------',string,'\n')
        if day:
            self.txt+=(day+'\t'+str(self.index)+'\t'+string+'\n')
        elif day is None:
            self.txt+=(str(self.index) + '\t' + string + '\n')

    def update_parameter(
        self,string_dict={}):
        """
        初始化基本参数 
        """
        if bool(string_dict):#通过exec输入需要添加的其他操作
            for key_name in string_dict:
                if key_name!='None':
                    exec('%s = %s'%(key_name,string_dict[key_name]))
                else:
                    raise Exception('wrong parameters error')

    def initialize(self,code):
        #对新入的仓位建仓
        self.check_build_pos(code)
        #初始化策略freq指标
        self.index = 0
        self.caculate_index = 0
        #初始化账户 
        self.whole_account['transaction_fee'] = 0
        self.whole_account['float_ping_revenue'] = 0
        self.caculate_margin(typing = "start") #包含计算保证金，与包含计算自有资金（今日盈亏等）
    
    def check_build_pos(self,code):
        for code_specific in code:
            if code_specific not in self.pos:
                self.pos.update({code_specific:{'open_B_today':0.0,'open_B_yesterday':0.0,'open_S_today':0.0,'open_S_yesterday':0.0,'margin':0.0,\
                    'price_B':0.0,'price_S':0.0}})

    @func_intro
    def caculate_margin(self,typing):
        "更新账户总的available,margin,margin_rate"
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
        self.whole_account['margin_rate'] = self.whole_account['margin']/(self.whole_account['available'] + self.whole_account['margin'])
        self.caculate_hold_margin()
        # if self.whole_account["margin_rate"] > self.margin_use_limite_rate: # 保证金比例
        #     raise Exception("high margin rate Error")

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

    def update_pos_price(self):
        for code in self.pos:
            data_use = self.data[code].iloc[self.index]
            if self.pos[code]['price_B'] != 0:
                self.pos[code]['price_B'] = data_use['last']
            if self.pos[code]['price_S'] != 0:
                self.pos[code]['price_S'] = data_use['last']

    def update_pos(self,code,price,volume,direction,openclose):
        """
        根据成交信息改变持仓情况
        并且改变账户信息
        其中的volume要自己控制是否超过行情所能承载的最大容量
        """
        if code not in self.pos:
            initial_dict = {'open_B_today':0.0,'open_B_yesterday':0.0,'open_S_today':0.0,'open_S_yesterday':0.0,'margin':0.0,\
                'price_B':0.0,'price_S':0.0}
            self.pos.update({code:initial_dict})
        if openclose == 'close_today':
            transaction_fee = price*volume*self.contract_multiplier*self.close_today_fee_rate + volume*1
            self.whole_account['transaction_fee'] += transaction_fee
            pos_input = {'code':code,'price':price,'volume':volume,'direction':direction,'openclose':openclose}
            self.order_change_pos(**pos_input)
        elif openclose == 'close_yesterday':
            transaction_fee = price*volume*self.contract_multiplier*self.close_yesterday_fee_rate + volume*1
            self.whole_account['transaction_fee'] += transaction_fee
            pos_input = {'code':code,'price':price,'volume':volume,'direction':direction,'openclose':openclose}
            self.order_change_pos(**pos_input)
        elif openclose == 'open':
            transaction_fee = price*volume*self.contract_multiplier*self.open_fee_rate + volume*1
            self.whole_account['transaction_fee'] += transaction_fee
            pos_input = {'code':code,'price':price,'volume':volume,'direction':direction,'openclose':openclose}
            self.order_change_pos(**pos_input)

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
                        raise Exception('pos_B_yesterday_erro')
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
            # #日线级别的因为数据原因不能添加，分钟，tick级别可以添加

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
        data_shape = self.data[self.asset_group[0]].shape[0]
        coef = self.contract_multiplier
        if (self.index < data_shape) & (self.index - self.caculate_index == self.freq_caculate):
            self.write_log('begin:\n'+str(self.pos),day)
            #更新浮动盈亏
            for code in self.pos:
                data_now = self.data[code].iloc[self.index]
                float_revenue = (data_now['last']-self.pos[code]['price_B'])*coef*(self.pos[code]['open_B_today']+self.pos[code]['open_B_yesterday'])-\
                    (data_now['last']-self.pos[code]['price_S'])*coef*(self.pos[code]['open_S_today']+self.pos[code]['open_S_yesterday'])
                self.whole_account['float_ping_revenue'] += float_revenue
            self.update_pos_price() #用于之后计算浮动盈亏
            self.caculate_margin("last")
            self.caculate_index = self.index+1 if caculate_index_add else self.caculate_index
            try:
                assert self.whole_account['available'] >= 0,'保证金error'
            except:
                set_trace()
            self.write_log(str(self.whole_account),day)
        elif end_signal:#当天结束及时结算,不用结算价，当前直接用收盘价进行结算
            self.write_log('begin:\n'+str(self.pos),day)
            #更新浮动盈亏
            for code in self.pos:
                data_now = self.data[code].iloc[self.index]
                float_revenue = (data_now['last']-self.pos[code]['price_B'])*coef*\
                    (self.pos[code]['open_B_today']+self.pos[code]['open_B_yesterday'])-\
                    (data_now['last']-self.pos[code]['price_S'])*coef*\
                        (self.pos[code]['open_S_today']+self.pos[code]['open_S_yesterday'])
                # set_trace()
                self.whole_account['float_ping_revenue'] += float_revenue
            self.update_pos_price()
            self.caculate_margin("last")
            self.caculate_index = self.index+1 if caculate_index_add else self.caculate_index
            try:
                assert self.whole_account['available'] >= 0,'保证金error'
            except:
                set_trace()

    def caculate_hold_margin(self):
        "计算维持担保保证金"
        avail = self.whole_account['available']
        margin_own = self.whole_account['margin'];borrow = margin_own/self.C_margin_rate*(1-self.C_margin_rate)
        self.whole_account["hold_on_margin"] = (borrow+margin_own+avail)/(borrow+margin_own) if margin_own!=0 else 1

    def day_end_reset_whole_account(self,day:str):
        "日结算初始收益"
        today_final = self.whole_account['available'] + self.whole_account['margin']
        self.day_return.update({day: {'abs_ret':today_final - self.whole_account['start_equity'],\
            'compare_ret':(today_final - self.whole_account['start_equity'])/self.whole_account['start_equity'],\
                'final_equity':today_final,\
                    'start_equity':self.whole_account['start_equity'],\
                        'transaction_fee':self.whole_account['transaction_fee'],\
                            'float_ping_revenue':self.whole_account['float_ping_revenue'],\
                                'margin_rate':self.whole_account['margin']/(self.whole_account['margin']+self.whole_account['available']),\
                                    'hold_on_margin':self.whole_account["hold_on_margin"]}})

        self.whole_account["margin_rate"] = self.whole_account['margin']/(self.whole_account['margin']+self.whole_account['available'])
        self.whole_account['final_equity'] = today_final
        self.write_log(str(self.whole_account),day)
        self.whole_account['start_equity'] = today_final
        #资产仓位变动
        for code in self.pos:
            code_pos = self.pos[code]
            code_pos['open_B_yesterday'] += code_pos['open_B_today']
            code_pos['open_S_yesterday'] += code_pos['open_S_today']
            code_pos['open_S_today'] = 0;code_pos['open_B_today'] = 0

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

    @abstractmethod
    def huanqi(self):
        pass
    
    @abstractmethod
    def end_subject(self,symbol):
        pass

    def get_data_by_time(self,code,index,freq=2):
        "通过时间找到当前时间，下一个500ms，以及1s之后的所有的订单信息，symbol标的合约单一"
        index_time = self.index
        res = [index_time]
        for i in range(freq + 1):
            try:
                info_next_temp = self.data[code].iloc[index_time+1].to_dict('index')[index_time+i]
            except IndexError:
                info_next_temp = {}
            res.append(info_next_temp)
        return res

    def get_symbol_info_from_dict(self,code:str):
        return list(self.data[code].iloc[[self.index]].to_dict('index').values())[0]
    
    @abstractmethod
    def send_limit_orders(self,code,volume,price,direction,openclose,**args):
        pass

    @abstractmethod
    def send_market_orders(self,code,volume,direction,openclose,**args):
        """
        args用于表示交割和移仓
        """
        pass

    @abstractmethod
    def send_cancel_orders(self,code,volume,direction,openclose):
        pass

    @abstractmethod
    def _OnRtnTrade(self,code,volume,direction,openclose,typing):
        pass

    @abstractmethod
    def run_backtesting(self,**args):
        pass

    def save_data(self,save_path,name):
        """
        保存整个类结果，使用pickle保存
        self.deal_res 成交结果，每次同一时间的成交放在一起
        self.txt 所有成交变动时账户总额的状态变化
        self.day_return 
        self.strategy.signal 成交信号处理
        self.day_change 信号改变，换仓时间节点
        self.hangqing 回放行情
        """
        path = os.path.join(save_path,name)
        if os.path.exists(path) & (not os.path.isdir(path)):
            try:
                os.remove(path)
            except:
                os.rmdir(path)
        elif os.path.exists(path) & (os.path.isdir(path)):
            pass
        else:
            os.mkdir(path)
        #deal_res
        #详尽版成交记录
        path1 = os.path.join(path,'deal_res_py.csv')
        with open(path1,'w',newline='',encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            for u,v in self.deal_res.items():#u是字符串日期,v是下单成交信息
                temp = [];temp.extend([u]);temp.extend(v)
                writer.writerow(temp)
        #二进制版本
        path12 = os.path.join(path,'deal_res')
        f = open(path12,'wb')
        pickle.dump(self.deal_res,f)
        f.close()
        #明细版成交记录
        # def write_deal_res(raw_material:dict,path):
        #     "self.deal_res->{today:[{code,index,position_B,position_S,net_position,price,volume,direction,openclose}]}"
        #     path_final = os.path.join(path,'deal_res_official.csv')
        #     with open(path_final,'w',newline='',encoding='utf-8-sig') as f:
        #         writer1 = csv.writer(f)
        #         sheet_title = ['日期','合约名称','成交价','方向','合约净持仓']
        #         writer1.writerow(sheet_title)
        #         for day,content in self.deal_res.items():
        #             for use_cont in content:
        #                 temp_content = [];temp_content.append(day);
        #                 direct = Future_Engine.judge_sell_buy(use_cont['direction'],use_cont['openclose'])
        #                 temp_content.extend([use_cont['code'],use_cont['price'],direct,use_cont['net_position']])
        #                 writer1.writerow(temp_content)
        # write_deal_res(self.deal_res,path) 
        def multiindex_write_deal_res(raw_material:dict,path):
            path_final = os.path.join(path,'交易记录.csv')
            res = pd.DataFrame([],columns = ['TradingDay','InstrumentID','Transaction_price','Direction','Net_position'])
            for day,content in self.deal_res.items():
                for use_cont in content:
                    direct = Future_Engine.judge_sell_buy(use_cont['direction'],use_cont['openclose'])
                    direct_name = 'B' if direct == 1 else 'S'
                    temp_df = pd.DataFrame([[day,use_cont['code'],use_cont['price'],direct_name,use_cont['net_position']]],\
                        columns=['TradingDay','InstrumentID','Transaction_price','Direction','Net_position'])
                    res = pd.concat([res,temp_df],axis=0,ignore_index=True)
            res = res.set_index(['TradingDay','InstrumentID'])
            res.to_csv(path_final,encoding='utf-8-sig')
        multiindex_write_deal_res(self.deal_res,path)

        #state
        path2 = os.path.join(path,'状态'+'.txt')
        f = open(path2,'w+')
        f.write(self.txt)
        f.close()

        #day_return 
        path3 = os.path.join(path,'每日收益'+'.csv')
        temp = pd.DataFrame(self.day_return).T
        temp.to_csv(path3,encoding='utf-8-sig')
        
        #signal
        path4 = os.path.join(path,'信号.csv')
        self.strategy.signal["信号"] = self.strategy.signal[0].shift(1)
        temp = pd.DataFrame(self.strategy.signal)
        temp.to_csv(path4,encoding='utf-8-sig')

        #day_change
        path5 = os.path.join(path,'信号变化日.csv')
        temp = pd.Series(self.day_change)
        temp.to_csv(path5,encoding='utf-8-sig')

        #hangqing
        path6  = os.path.join(path,'行情数据.csv')
        with open(path6,'w',newline='') as f:
            writer = csv.writer(f)
            for u1,v1 in self.hangqing.items():
                temp = [];temp.extend([u1]);temp.extend(v1.columns)
                writer.writerow(temp)
                temp = [];temp.extend([u1]);temp.extend(v1.values.flatten())
                writer.writerow(temp)

        #trade_profit_info
        path7 = os.path.join(path,'每笔交易总结.csv')
        with  open(path7,'w',newline='') as f:
            writer = csv.writer(f)
            sheet_title = ['signal_count','profit','trade_start_day','trade_end_day']
            writer.writerow(sheet_title)
            for u1,v1 in self.trade_profit_info.items():
                temp = [];temp.append(u1);temp.extend(v1.values())
                writer.writerow(temp)

    def caculate_result(self,path,name):
        "输出结果"
        final_backtest_csv = os.path.join(path,'指标.csv')
        def get_specifc_dayret_tradeprofit(start_day:str,end_day=None):
            if end_day is None:
                day_return = {name:self.day_return[name] for name in self.day_return if name>=start_day}
                trade_profit_info = {sig:self.trade_profit_info[sig] for sig in self.trade_profit_info if self.trade_profit_info[sig]['trade_start_day']>=start_day}
            else:
                #此处截断end_day可能会出现，最后一笔成交开始日期在有效期内，结束日在有效期外，从而不包含最后一笔
                day_return = {name:self.day_return[name] for name in self.day_return if ((name>=start_day)&(name<=end_day))}
                trade_profit_info = {sig:self.trade_profit_info[sig] for sig in self.trade_profit_info \
                    if ((self.trade_profit_info[sig]['trade_start_day']>=start_day)&(self.trade_profit_info[sig]['trade_end_day']<=end_day))}
            return day_return,trade_profit_info

        def caculate_once(day_return,trade_profit_info,print_bool=False):
            """
            day_return->self.day_return换日期截选
            print_bool 是否输出策略结果PrettyTable
            typing 1表示全部时间段，2表示最近2年，1表示最近1年
            """
            x = PrettyTable()
            x2 = PrettyTable()
            x.padding_width = 2
            x2.padding_width = 2
            x3 = PrettyTable()
            x3.padding_width = 2
            x4 = PrettyTable()
            x4.padding_width = 2
            day_return_df = pd.DataFrame(day_return).T
            day_return_keys = list(day_return_df.keys())
            total_return_ratio = ((day_return_df.iloc[-1,day_return_df.columns.get_loc('final_equity')] - day_return_df.iloc[0,day_return_df.columns.get_loc('start_equity')])/\
                day_return_df.iloc[0,day_return_df.columns.get_loc('start_equity')])
            avg_day_return = total_return_ratio/day_return_df.shape[0]
            return_annual = avg_day_return*252
            initial_money =  day_return_df.iloc[0,day_return_df.columns.get_loc('start_equity')]
            # abs_day_vol = (day_return_df['final_equity']/initial_money).std()
            abs_day_vol = day_return_df['compare_ret'].std()
            annual_vol = abs_day_vol*np.sqrt(252)
            sharp = (return_annual - 0.03)/annual_vol
            def caculate_retreat(df:pd.DataFrame):
                raw_data = np.r_[initial_money,df['final_equity'].values.flatten()]
                high_value = raw_data[0]
                res = 0
                for i in range(1,len(raw_data)):
                    if raw_data[i] > high_value:
                        high_value = raw_data[i]
                    else:
                        res = max((high_value-raw_data[i])/high_value,res)
                return res

            def caculate_day_win_rate(df:pd.DataFrame):
                raw_data = df['abs_ret'].values
                win_rate = raw_data[raw_data>0].shape[0] / raw_data.shape[0]
                return win_rate

            def caculate_trade_win_rate(trades_dict:dict):
                trade_profit_series = pd.DataFrame(trades_dict).T['profit']
                win_rate = trade_profit_series[trade_profit_series >0].shape[0]/trade_profit_series.shape[0]
                max_win = trade_profit_series.sort_values(ascending=False).iloc[0]
                max_loss = trade_profit_series.sort_values(ascending=True).iloc[0]
                return win_rate,max_loss,max_win
            
            def caculate_ret_retreat_ratio(day_return):
                day_return.index = day_return.index.map(pd.to_datetime)
                def caculate_retreat(df:pd.DataFrame):
                    initial_money = df.iloc[0,df.columns.get_loc('start_equity')]
                    raw_data = np.r_[initial_money,df['final_equity'].values.flatten()]
                    high_value = raw_data[0]
                    res = 0
                    for i in range(1,len(raw_data)):
                        if raw_data[i] > high_value:
                            high_value = raw_data[i]
                        else:
                            res = max((high_value-raw_data[i])/high_value,res)
                    return res

                def year_ret_retreat(year_day_return:pd.DataFrame):
                    year_day_return_series = year_day_return['final_equity'].values
                    ret = (year_day_return_series[-1] - year_day_return_series[0])/year_day_return_series[0]
                    max_drawdown = caculate_retreat(year_day_return)
                    return ret/max_drawdown

                day_return_year = day_return.groupby(lambda x:x.year).apply(year_ret_retreat)
                return day_return_year.mean()

            def get_avg_ratio(day_return_df):
                raw_material = day_return_df['compare_ret']
                compare_retbig0 = raw_material[raw_material>0]
                compare_retsmall0 = raw_material[raw_material<0]
                return compare_retbig0.mean(),compare_retsmall0.mean()

            def find_continous(data):
                "typing->1:表示连续盈利,0:表示连续亏损"
                assert len(data)>1,'数据长度应当大于1'
                continous_win=0;continous_loss=0;
                index_win=-1;index_loss=-1;
                temp_continous_win=0;temp_continous_loss=0;
                max_win_day=0;max_loss_day=0
                temp_win_day=0;temp_loss_day=0
                for i in range(len(data)):
                    if data[i]>0:
                        if i-index_win==1:
                            index_win=i;temp_continous_win+=data[i];continous_win=max(continous_win,temp_continous_win)
                            temp_win_day+=1;max_win_day = max(max_win_day,temp_win_day)
                        else:
                            index_win=i;temp_continous_win=data[i];continous_win=max(continous_win,temp_continous_win)
                            temp_win_day=1;max_win_day = max(max_win_day,temp_win_day)
                    elif data[i]<=0:
                        if i-index_loss==1:
                            index_loss=i;temp_continous_loss+=data[i];continous_loss=min(continous_loss,temp_continous_loss)
                            temp_loss_day+=1;max_loss_day=max(max_loss_day,temp_loss_day)
                        else:
                            index_loss=i;temp_continous_loss=data[i];continous_loss=min(continous_loss,temp_continous_loss)
                            temp_loss_day=1;max_loss_day=max(max_loss_day,temp_loss_day)
                return continous_win,continous_loss,max_win_day,max_loss_day

            backwithdraw = caculate_retreat(day_return_df)
            day_win_rate = caculate_day_win_rate(day_return_df)
            ret_retreat_ratio = caculate_ret_retreat_ratio(day_return_df)
            max_retreat_abs = day_return_df.sort_values('abs_ret',ascending=True).iloc[0,day_return_df.columns.get_loc('abs_ret')]
            max_win_abs = day_return_df.sort_values('abs_ret',ascending=False).iloc[0,day_return_df.columns.get_loc('abs_ret')]
            win_trade_rate,max_trade_loss,max_trade_win = caculate_trade_win_rate(trade_profit_info)
            win_avg_ratio,loss_avg_ratio = get_avg_ratio(day_return_df)
            trade_profit_series = pd.DataFrame(trade_profit_info).T['profit']
            hold_on_margin = day_return_df['hold_on_margin'].mean()
            continous_transaction_win1,continous_transaction_loss1,max_win_day1,max_loss_day1 =\
                find_continous(trade_profit_series.values.flatten())
            continous_transaction_win2,continous_transaction_loss2,max_win_day2,max_loss_day2 =\
                find_continous(day_return_df['abs_ret'].values.flatten())
            margin_ratio_avg = day_return_df['margin_rate'].mean()
            if print_bool:
                x.add_column("  ",['策略 '+name])
                x.add_column("策略日收益",[str(round(float(avg_day_return)*100,4))+'%'])
                x.add_column("策略年化收益",[str(round(float(return_annual)*100,4))+'%'])
                x.add_column("策略累积收益率",[str(round(float(total_return_ratio)*100,4))+'%'])
                x.add_column("策略日波动率",[str(round(float(abs_day_vol)*100,4))+'%'])
                x.add_column("策略年化波动率",[str(round(float(annual_vol)*100,4))+'%'])
                x.add_column("策略日净值胜率",[str(round(float(day_win_rate)*100,4))+'%'])
                x.add_column("策略最大回撤",[str(round(float(backwithdraw)*100,4))+'%'])
                print(x,'\n')
                x2.add_column("  ",['策略'+name])
                x2.add_column("策略单日最大亏损",[str(round(float(max_retreat_abs),2))])
                x2.add_column("策略单日最大盈利",[str(round(float(max_win_abs),2))])
                x2.add_column("策略单笔胜率",[str(round(float(win_trade_rate)*100,4))+'%'])
                x2.add_column("策略单笔最大亏损",[str(round(float(max_trade_loss),2))])
                x2.add_column("策略单笔最大盈利",[str(round(float(max_trade_win),2))])
                x2.add_column("策略总风报比",[str(round(float(ret_retreat_ratio)*100,4))+'%'])
                x2.add_column("策略夏普(3%年化基准)",[str(round(float(sharp),4))])
                print(x2,'\n')
                x3.add_column("  ",['策略'+name])
                x3.add_column("策略总交易次数",[str(len(trade_profit_info))])
                x3.add_column("平均盈利率",[str(round(float(win_avg_ratio*100),2))+'%'])
                x3.add_column("平均亏损率",[str(round(float(loss_avg_ratio*100),2))+'%'])
                x3.add_column("最大连续盈利天数",[str(int(max_win_day2))])
                x3.add_column("最大连续亏损天数",[str(int(max_loss_day2))])
                x3.add_column("最大连续盈利笔数",[str(int(max_win_day1))])
                x3.add_column("最大连续亏损笔数",[str(int(max_loss_day1))])
                print(x3,'\n')
                x4.add_column("  ",['策略'+name])
                x4.add_column("最大连续日盈利金额",[str(round(float(continous_transaction_win2),2))])
                x4.add_column("最大连续日亏损金额",[str(round(float(continous_transaction_loss2),2))])
                x4.add_column("最大连续单笔盈利金额",[str(round(float(continous_transaction_win1),2))])
                x4.add_column("最大连续单笔亏损金额",[str(round(float(continous_transaction_loss1),2))])
                x4.add_column("平均保证金占用比率",[str(round(float(margin_ratio_avg)*100,4))+'%'])
                x4.add_column("平均维持担保保证金",[str(round(float(hold_on_margin)*100,4))+'%'])
                print(x4,'\n')
            #最近一两年收益情况
            #写入csv文件，csv模块续写
            date_use = list(day_return.keys())
            date_start = date_use[0];
            date_start_year = str(pd.to_datetime(date_start).year);date_start_month = str(pd.to_datetime(date_start).month)
            date_end = date_use[-1];
            date_end_year = str(pd.to_datetime(date_end).year);date_end_month = str(pd.to_datetime(date_end).month)
            csv_name = ['时间跨度',date_start_year+'.'+date_start_month+' - '+date_end_year+'.'+date_end_month]

            with open(final_backtest_csv,'a+',newline='',encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                writer.writerow(csv_name)
                writer.writerow(["策略日收益",str(round(float(avg_day_return)*100,4))+'%'])
                writer.writerow(["策略年化收益",str(round(float(return_annual)*100,4))+'%'])
                writer.writerow(["策略累积收益率",str(round(float(total_return_ratio)*100,4))+'%'])
                writer.writerow(["策略日波动率",str(round(float(abs_day_vol)*100,4))+'%'])
                writer.writerow(["策略年化波动率",str(round(float(annual_vol)*100,4))+'%'])
                writer.writerow(["策略日净值胜率",str(round(float(day_win_rate)*100,4))+'%'])
                writer.writerow(["策略最大回撤",str(round(float(backwithdraw)*100,4))+'%'])
                writer.writerow(["策略单日最大亏损",str(round(float(max_retreat_abs),2))])
                writer.writerow(["策略单日最大盈利",str(round(float(max_win_abs),2))])
                writer.writerow(["策略单笔胜率",str(round(float(win_trade_rate)*100,4))+'%'])
                writer.writerow(["策略单笔最大亏损",str(round(float(max_trade_loss),2))])
                writer.writerow(["策略单笔最大盈利",str(round(float(max_trade_win),2))])
                writer.writerow(["策略总风报比",str(round(float(ret_retreat_ratio)*100,4))+'%'])
                writer.writerow(["策略夏普(3%年化基准)",str(round(float(sharp),4))])
                writer.writerow(["策略总交易次数",str(len(trade_profit_info))])
                writer.writerow(["平均亏损率",str(round(float(win_avg_ratio*100),4))+'%'])
                writer.writerow(["平均盈利率",str(round(float(loss_avg_ratio*100),4))+'%'])
                writer.writerow(["最大连续盈利天数",str(int(max_win_day2))])
                writer.writerow(["最大连续亏损天数",str(int(max_loss_day2))])
                writer.writerow(["最大连续盈利笔数",str(int(max_win_day1))])
                writer.writerow(["最大连续亏损笔数",str(int(max_loss_day1))])
                writer.writerow(["最大连续日盈利金额",str(round(float(continous_transaction_win2),2))])
                writer.writerow(["最大连续日亏损金额",str(round(float(continous_transaction_loss2),2))])
                writer.writerow(["最大连续单笔盈利金额",str(round(float(continous_transaction_win1),2))])
                writer.writerow(["最大连续单笔亏损金额",str(round(float(continous_transaction_loss1),2))])
                writer.writerow(["平均保证金占用比率",str(round(float(margin_ratio_avg)*100,4))+'%'])
                writer.writerow(["平均维持担保保证金占用率",str(round(float(hold_on_margin)*100,4))+'%'])
                writer.writerow(['  ','  '])
        #全时期
        caculate_once(self.day_return,self.trade_profit_info,print_bool=True)
        #最近2年  期-现
        year_end = pd.to_datetime(self.daterange[-1]).year
        day_return,trade_profit_info = get_specifc_dayret_tradeprofit(str(year_end-2)+'-01-01')
        caculate_once(day_return,trade_profit_info,print_bool=False)
        #最近1年  
        day_return,trade_profit_info = get_specifc_dayret_tradeprofit(str(year_end-1)+'-01-01')
        caculate_once(day_return,trade_profit_info,print_bool=False)


    
    