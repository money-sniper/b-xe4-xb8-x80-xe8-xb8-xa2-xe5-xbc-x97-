#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os,sys
sys.path.append(os.path.join("/Users/dt/Desktop/HA/ETF_rolling"))
import etf_pair.backtest.Engine.Spot_Engine.Tick_Engine as TE
import etf_pair.signal_own.signal_transfer as signal
from etf_pair.Coint.Own_Coint import cointegrate,cointegrate_plot
from etf_pair.str2dtime import parse2time
import struct
import numpy as np
import datetime
import pandas as pd,copy
from pdb import set_trace
import datetime
import statsmodels.api as sm
import importlib
from pandas.plotting import table
ptime = parse2time()


# In[2]:


class data_signal:
    def __init__(self,coint_temp):
        self.signal = signal.signal_trans(coint_temp)
        
    def tackle_market_data(self,rolling_time,shift_time,signal_time,std_in,std_out):
        signal_res,signal_out = self.signal.tackle_market_data(rolling_time = rolling_time,shift_time = shift_time,                                    signal_time = signal_time,std_in = std_in,std_out = std_out)
        temp = signal_res.dropna()
        t1 = temp[(temp["action"] == temp["shift"]) & ((temp["action"]!="None")&(temp["shift"]!="None"))]
        def get_first(df):
            res = pd.DataFrame([])
            raw = df.iloc[0,0]
            for i in range(1,df.shape[0]):
                if df.iloc[i,0] != raw:
                    res = pd.concat([res,df.iloc[[i]]],axis=0)
                    raw = df.iloc[i,0]
                else:
                    continue
            return res
        return get_first(t1)
    
    def get_ratio(self,rolling_time,shift_time,signal_time,**args):
        coint_member = self.signal.coint_member
        join_data = coint_member.join_data
        interval = coint_member.parsetime.str2delta(coint_member._freq) 
        rolling_time = int(np.ceil(coint_member.parsetime.str2delta(rolling_time)/interval)) 
        shift_time = int(np.ceil(coint_member.parsetime.str2delta(shift_time)/interval)) 
        rolling_data = [j for j in coint_member.join_data.rolling(rolling_time)] #以shift_time为间隔滚动
        coef_df = copy.copy(join_data[coint_member._Y])
        coef_df.iloc[:] = np.nan
        
        for i in range(rolling_time-1,join_data.shape[0],shift_time):
            data = join_data.iloc[i-rolling_time+1:i]
            Y = data[coint_member._Y]
            X = data[coint_member._X]
            res = sm.OLS(Y,X)
            results = res.fit()
            params = results.params[0]
            coef_df.iloc[i+1:i+shift_time+1] = params
        return coef_df #获得对应ratio
    
    def load_data_pickle(self,day,rolling_time,shift_time):
        coint_member = self.signal.coint_member
        _ = coint_member.get_day_data(day,"3s")
        _,_ = coint_member.interval_get_ratio(rolling_time,typing="net")
        _,_ = coint_member.fixed_interval_plus_plot(plot_on = False)
        _ = coint_member.tick_rolling_plot(freq_own = rolling_time,tick_rolling = shift_time,plot_on=False)
        self.data = {"500":coint_member._data500,"1000":coint_member._data1000}
        return self.data


# In[3]:


class get_day_profit:
    def __init__(self,data_signal,times=3):
        """
        times:分多少次进行交易
        """
        self.times = times
        self.asset_group = {"Y":"1000","X":"500"}
        self.pos = {}
        self.kong_X = 0;self.kong_Y = 0
        self.data_signal = data_signal
        
    def one_transaction_profit(self,signal_df,vol_X,vol_Y):
        signal_open_time = signal_df.index[0]
        signal_close_time = signal_df.index[1]
        signal = signal_df.iloc[0,0]
        #市价单
        code_Y = self.asset_group["Y"];code_X = self.asset_group["X"]
        data_Y = self.data_signal.data[code_Y].set_index("UpdateTime")
        data_X = self.data_signal.data[code_X].set_index("UpdateTime")
        if signal == "buy": #多Y空X，再空Y多X
            open_price_Y,open_vol_Y = self.market_order_price(time = signal_open_time,code = code_Y,volume = vol_Y,typing = "buy")
            open_price_X,open_vol_X = self.market_order_price(time = signal_open_time,code = code_X,volume = vol_X,typing = "sell")
            close_price_Y,close_vol_Y = self.market_order_price(time = signal_close_time,code = code_Y,volume = vol_Y,typing = "sell")
            close_price_X,close_vol_X = self.market_order_price(time = signal_close_time,code = code_X,volume = vol_X,typing = "buy")
            profit_temp = (close_price_Y*close_vol_Y - open_price_Y*open_vol_Y)            + (open_price_X*open_vol_X - close_price_X*close_vol_X)
            transaction_fee = ((close_price_Y*close_vol_Y + open_price_Y*open_vol_Y) +                                (open_price_X*open_vol_X + close_price_X*close_vol_X))*(0.00004-0.0003)
        elif signal == "sell": #多X空Y，再空X多Y
            open_price_Y,open_vol_Y = self.market_order_price(time = signal_open_time,code = code_Y,volume = vol_Y,typing = "sell")
            open_price_X,open_vol_X = self.market_order_price(time = signal_open_time,code = code_X,volume = vol_X,typing = "buy")
            close_price_Y,close_vol_Y = self.market_order_price(time = signal_close_time,code = code_Y,volume = vol_Y,typing = "buy")
            close_price_X,close_vol_X = self.market_order_price(time = signal_close_time,code = code_X,volume = vol_X,typing = "sell")
            profit_temp = (close_price_X*close_vol_X - open_price_X*open_vol_X) +            (open_price_Y*open_vol_Y - close_price_Y*close_vol_Y)
            transaction_fee = (close_price_Y*close_vol_Y + open_price_Y*open_vol_Y +                                open_price_X*open_vol_X + close_price_X*close_vol_X)*(0.00004-0.0003)
        return profit_temp - transaction_fee,    {"open_vol_Y":open_vol_Y,"open_price_Y":open_price_Y,     "open_vol_X":open_vol_X,"open_price_X":open_price_X,     "close_vol_Y":close_vol_Y,"close_price_Y":close_price_Y,     "close_vol_X":close_vol_X,"close_price_X":close_price_X}
    
    def get_price_col(self,direct,code):
        if direct == "sell":
            price_col = ["BidPrice1","BidPrice2","BidPrice3","BidPrice4","BidPrice5"]
            volume_col = ["BidVolume1","BidVolume2","BidVolume3","BidVolume4","BidVolume5"]
        elif direct == "buy":
            price_col = ["AskPrice1","AskPrice2","AskPrice3","AskPrice4","AskPrice5"]
            volume_col = ["AskVolume1","AskVolume2","AskVolume3","AskVolume4","AskVolume5"]
        return price_col,volume_col
    
    def get_vol(self,times,time,para,pos_X,pos_Y,signal):
        code_Y,code_X = self.asset_group["Y"],self.asset_group["X"]
        hangqing_X = self.data_signal.data[code_X].set_index("UpdateTime").loc[time]
        LastPrice_X = hangqing_X["LastPrice"]
        hangqing_Y = self.data_signal.data[code_Y].set_index("UpdateTime").loc[time]
        LastPrice_Y = hangqing_Y["LastPrice"]
        #成交量
        if (signal == "buy"):#多Y空X，再空Y多X，多价差
            vol_X = int(np.floor(pos_X/times/100))*100
            vol_Y = int(np.floor(vol_X*LastPrice_X/LastPrice_Y/para/100))*100
            return vol_X,vol_Y
        elif (signal == "sell"):#多X空Y，再空X多Y，空价差
            vol_Y = int(np.floor(pos_Y/times/100))*100
            vol_X = int(np.floor(vol_Y*LastPrice_Y/LastPrice_X*para/100))*100
            return vol_X,vol_Y
    
    def market_order_price(self,time,code,volume,typing):
        if typing == "buy":
            price_col,vol_col = self.get_price_col("buy",code)
            start_index = 0;price_avg = 0
            ori_volume = copy.deepcopy(volume)
            while (volume > 0): #5是只吃掉5档行情
                if start_index < 5:
                    hangqing_df = self.data_signal.data[code].set_index("UpdateTime").loc[time]
                    vol = min(volume,hangqing_df[vol_col[start_index]])
                    price = hangqing_df[price_col[start_index]]
                    volume -= vol
                    price_avg += price*vol
                    start_index += 1
                else:
                    start_index = 0
                    time += ptime.str2delta(self.data_signal.signal.coint_member._freq)
            ori_volume = ori_volume if volume == 0 else ori_volume - volume
            price_avg = price_avg/ori_volume
        elif typing == "sell":
            price_col,vol_col = self.get_price_col("sell",code)
            start_index = 0;price_avg = 0
            ori_volume = copy.deepcopy(volume)
            while (volume > 0):
                if start_index < 5:
                    hangqing_df = self.data_signal.data[code].set_index("UpdateTime").loc[time]
                    vol = min(volume,hangqing_df[vol_col[start_index]])
                    price = hangqing_df[price_col[start_index]]
                    volume -= vol
                    price_avg += price*vol
                    start_index += 1
                else:
                    start_index = 0
                    time += ptime.str2delta(self.data_signal.signal.coint_member._freq)
            ori_volume = ori_volume if volume == 0 else ori_volume - volume
            price_avg = price_avg/ori_volume
        return price_avg,ori_volume


# In[4]:


class Management:
    def __init__(self,date_range,times = 3):
        self.daterange = date_range
        self.day_return = {}
        self.coint_temp = cointegrate_plot(date_range) #47
        self.data_signal = data_signal(self.coint_temp) 
        self.get_day_profit = get_day_profit(self.data_signal,times=times)
        
    def start_value(self,day,rolling_time,shift_time,initial_money = 2000*int(1e4),**args):
        #建仓
        _ = self.data_signal.load_data_pickle(day,rolling_time,shift_time)
        self.initial_money = initial_money
        code_Y,code_X = self.get_day_profit.asset_group["Y"],self.get_day_profit.asset_group["X"]
        hangqing_X = self.data_signal.data[code_X].iloc[-600]
        LastPrice_X = hangqing_X["LastPrice"]
        hangqing_Y = self.data_signal.data[code_Y].iloc[-600]
        LastPrice_Y = hangqing_Y["LastPrice"]
        self.pos_X = np.ceil(initial_money/4/LastPrice_X/100).astype(int)*100
        self.pos_Y = np.ceil(initial_money/4/LastPrice_Y/100).astype(int)*100
        print(datetime.datetime.now(),"  建仓完成")
        
    def get_day_return(self,day,rolling_time,shift_time,signal_time,std_in,std_out,times=3):
        #获取数据
        _ = self.data_signal.load_data_pickle(day,rolling_time,shift_time)
        #确定前几次交易时间
        args = {"rolling_time":rolling_time,"shift_time":shift_time,"signal_time":signal_time,               "std_in":std_in,"std_out":std_out}
        transaction_deal = self.data_signal.tackle_market_data(**args)
        transaction_deal = transaction_deal.iloc[:2*times,:] #
        if transaction_deal.shape[0] < 2*times:
            times = int(np.floor(transaction_deal.shape[0]/2))
        ratio_df = self.data_signal.get_ratio(rolling_time,shift_time,signal_time)
        whole_profit = 0
        trans_info_all = {}
        for i in range(times):
            signal_df = transaction_deal.iloc[2*i:2*(i+1)]
            temp_profit,trans_info = self.get_one_time_profit(times,signal_df,ratio_df,self.pos_X,self.pos_Y)
            whole_profit += temp_profit
            trans_info_all.update({(signal_df.index[0],signal_df.index[1]):trans_info})
        return whole_profit,trans_info_all
        
    def get_one_time_profit(self,times,signal_df,ratio_df,pos_X,pos_Y):
        time = signal_df.index[0]
        signal = signal_df.iloc[0,0]
        #确定拟合比例
        ratio = ratio_df.loc[time]
        #确定交易量
        vol_X,vol_Y = self.get_day_profit.get_vol(times = times,time = time,para = ratio,pos_X = pos_X,pos_Y = pos_Y,signal = signal)
        temp_profit,trans_info_temp = self.get_day_profit.one_transaction_profit(signal_df,vol_X,vol_Y)
        return temp_profit,trans_info_temp
    
    def run(
        self,
        rolling_time,
        shift_time,
        signal_time,
        std_in,
        std_out,
        initial_money,
        times):
        #建仓
        import datetime
        daterange = self.daterange
        self.start_value(daterange[0],rolling_time,shift_time,initial_money)
        ret = {}
        trans = {}
        for day in daterange[1:]:
            try:
                day_return,trans_day = self.get_day_return(day,rolling_time,shift_time,signal_time,std_in,std_out,times)
                ret.update({day:day_return})
                trans.update({day:trans_day})
                print(datetime.datetime.now()," " + str(day) +"  回溯完成")
            except:
                continue
        return ret,trans
