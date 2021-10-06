#-*-coding: UTF-8 -*-
#Author: DT
#Date: 2021-09-13

from .strategy_class import Strategy_Class
from ..signal_own.signal_transfer import signal_trans
import numpy as np,pandas as pd
import sys,os,copy,pickle
from pdb import set_trace
import statsmodels.api as sm
from ..backtest import func_intro
from etf_pair.Coint.Own_Coint import cointegrate_plot

class signal_instance(signal_trans):
    """
    获得每天交易信号
    self.coint_member是中获取数据根据天数进行拟合
    再设置分批次订单买卖量形成字典
    """
    def signal_refine(self,rolling_time,shift_time,signal_time,std_in,std_out):
        signal_res,signal_out = self.tackle_market_data(
            rolling_time=rolling_time,
            shift_time=shift_time,
            signal_time=signal_time,
            std_in=std_in,
            std_out=std_out)
        temp = signal_res.dropna()
        t1 = temp[(temp["action"] == temp["shift"]) & ((temp["action"]!="None") & (temp["shift"]!="None"))]
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
        t1_res = get_first(t1)
        return t1_res #不考虑止损

    def get_ratio(self,rolling_time,shift_time,signal_time):
        """
        每次回归出来的参数,使用shift_time间隔滚动
        而上下限则使用rolling_time根据self.coint_member._freq进行时刻计算
        以shift_time更新间隔回归出两个序列之间的相关关系，取样频率使用rolling_time
        Parameters:
        ------
        rolling_time:
        shift_time:
        signal_time:

        Returns:
        ------
        """
        join_data = self.coint_member.join_data
        interval = self.coint_member.parsetime.str2delta(self.coint_member._freq) 
        rolling_time = int(np.ceil(self.coint_member.parsetime.str2delta(rolling_time)/interval)) 
        shift_time = int(np.ceil(self.coint_member.parsetime.str2delta(shift_time)/interval)) 
        rolling_data = [j for j in self.coint_member.join_data.rolling(rolling_time)] #以shift_time为间隔滚动
        coef_df = copy.copy(join_data[self.coint_member._Y])
        coef_df.iloc[:] = np.nan
        
        for i in range(rolling_time-1,join_data.shape[0],shift_time):
            data = join_data.iloc[i-rolling_time+1:i]
            Y = data[self.coint_member._Y]
            X = data[self.coint_member._X]
            res = sm.OLS(Y,X)
            results = res.fit()
            params = results.params[0]
            coef_df.iloc[i+1:i+shift_time+1] = params
        return coef_df #获得对应ratio

class strategy_etf_pair_type11(Strategy_Class):
    """
    type11,type01,type10,type00
    第一个1代表-> 不带止损条件,0->带止损条件
    第二个1代表-> 分批买入,0->一次性买入
    """
    def __init__(self,engine,freq=3):
        """
        Parameters:
        ------
        freq:买入频率（次数）
        engine:撮合引擎
        """
        super().__init__(engine=engine)
        self._freq = freq
        self.strategy_signal = True
        self.position_close_log = {} 
        self.remain_X_position = 0;self.remain_Y_position = 0
        self.position_closelog = {}

    @func_intro
    def core_logic(self,signal,para,pos_X,pos_Y,times = 3):
        """
        信号是Y-X的价差 self.coint_member._Y - para * self.coint_member._X
        buy->多Y，空X
        sell->多X，空Y

        Parameters:
        ------
        signal: 收到信号
        para: Y = para*X 中的配比
        vol_X,vol_Y下单量根据需要卖出的仓位调整,freq = 3，则说明每次卖出long_B_yesterday的三分之一
        其中vol_X与vol_Y都是下单手数，非股数，股数 = 手数*100
        pos_X -> 当天开盘初始self.pos[code_X]["long_B_yesterday"]
        pos_Y -> 当天开盘初始self.pos[code_Y]["long_B_yesterday"]

        Returns:
        ------
        pass
        """
        code_Y,code_X = self.engine.asset_group["Y"],self.engine.asset_group["X"]
        hangqing_X = self.engine.get_symbol_info_from_dict(code_X);LastPrice_X = hangqing_X["LastPrice"]
        hangqing_Y = self.engine.get_symbol_info_from_dict(code_Y);LastPrice_Y = hangqing_Y["LastPrice"]
        #成交量
        if (signal == "buy") & (self.kong_X < times):#多Y，空X
            vol_X = int(np.floor(pos_X/times/100))*100
            vol_Y = int(np.floor(vol_X*LastPrice_X/LastPrice_Y/para/100))*100
            self.send_market_order_func(code = code_X, volume = vol_X, openclose = "close_yesterday", direction = 'B')
            self.send_market_order_func(code = code_Y, volume = vol_Y, openclose = "open", direction = 'B')
            self.kong_X += 1
        elif (signal == "sell") & (self.kong_Y < times):#多X，空Y
            vol_Y = int(np.floor(pos_Y/times/100))*100
            vol_X = int(np.floor(vol_Y*LastPrice_Y/LastPrice_X*para/100))*100
            self.send_market_order_func(code = code_Y, volume = vol_Y, openclose = "close_yesterday", direction = 'B')
            self.send_market_order_func(code = code_X, volume = vol_X, openclose = "open", direction = 'B')
            self.kong_Y += 1
        else:
            pass

    @func_intro
    def output_signal(self,**args):
        """
        买卖的是一个价差，信号全不从signal_instance.signal_refine中获得

        Parameters:
        ------
        rolling_time:取样频率长度
        shift_time:移动窗口时间
        signal_time:信号选取频率
        std_in:进入标准差
        std_out:止损标准差
        """
        t1_res = self.signal.signal_refine(**args)[["action"]]
        all_time = self.signal.coint_temp.join_data[["LastPrice_500"]]
        res = all_time.merge(t1_res,how='left',left_index=True,right_index=True)[["action"]]
        return res

    def mark_asset(self):
        """
        用于去除当日多余头寸，记录期初和期末标的头寸
        """
        code_Y = self.engine.asset_group["Y"];code_X = self.engine.asset_group["X"]
        self.start_Y = self.pos[code_Y]["open_B_today"] + self.pos[code_Y]["open_B_yesterday"]
        self.start_X = self.pos[code_X]["open_B_today"] + self.pos[code_X]["open_B_yesterday"]

    @func_intro
    def close(self): #增加的仓位
        code_Y = self.engine.asset_group["Y"];code_X = self.engine.asset_group["X"]
        self.end_Y = self.pos[code_Y]["open_B_today"] + self.pos[code_X]["open_B_yesterday"]
        self.end_X = self.pos[code_X]["open_B_today"] + self.pos[code_X]["open_B_yesterday"]
        if self.end_Y < self.start_Y:
            vol_gap = np.abs(self.start_Y - self.end_Y)
            args = {"code":code_Y,"volume":vol_gap,"openclose":"open","direction":'B'}
            order_return_sig = self.send_market_order_func(**args);args.update({"order_message":order_return_sig,"ori_volume":vol_gap})
            self.position_close_endday(**args)
        elif self.end_Y > self.start_Y:
            vol_gap = np.abs(self.end_Y - self.start_Y)
            vol_yesterday = self.engine.pos[code_Y]["open_B_yesterday"]
            vol = min(vol_gap,vol_yesterday)
            args = {"code":code_Y,"volume":vol,"openclose":"close_yesterday","direction":'B'}
            order_return_sig = self.send_market_order_func(**args);args.update({"order_message":order_return_sig,"ori_volume":vol_gap})
            self.position_close_endday(**args)
            self.remain_Y_position = vol_gap - vol_yesterday if vol_gap > vol_yesterday else 0
        if self.end_X < self.start_X:
            vol_gap = np.abs(self.start_X - self.end_X)
            args = {"code":code_X,"volume":vol_gap,"openclose":"open","direction":'B'}
            order_return_sig = self.send_market_order_func(**args);args.update({"order_message":order_return_sig,"ori_volume":vol_gap})
            self.position_close_endday(**args)
        elif self.end_X > self.start_X:
            vol_gap = np.abs(self.end_X - self.start_X)
            vol_yesterday = self.engine.pos[code_X]["open_B_yesterday"]
            vol = min(vol_gap,vol_yesterday)
            args = {"code":code_X,"volume":vol,"openclose":"close_yesterday","direction":'B'}
            order_return_sig = self.send_market_order_func(**args);args.update({"order_message":order_return_sig,"ori_volume":vol_gap})
            self.position_close_endday(**args)
            self.remain_X_position = vol_gap - vol_yesterday if vol_gap > vol_yesterday else 0

    def position_close_endday(self,order_message,code,openclose,direction,volume,ori_volume):
        self.position_closelog.setdefault(self.engine.today,{})
        self.position_closelog[self.engine.today].setdefault(code,{})
        if volume == ori_volume:
            info = ["success",openclose,direction,volume,ori_volume]
        else:
            info = ["failed",openclose,direction,volume,ori_volume]
        self.position_closelog[self.engine.today][code] = info

    def clear_yesterday_error_position(self):
        if self.remain_Y_position != 0:
            args = {"code":self.engine.asset_group["Y"],"volume":self.remain_Y_position,"openclose":"close_yesterday","direction":"B"}
            _ = self.send_market_order_func(**args);
        if self.remain_X_position != 0:
            args = {"code":self.engine.asset_group["X"],"volume":self.remain_X_position,"openclose":"close_yesterday","direction":'B'}
            _ = self.send_market_order_func(**args);

    def run_backtesting_oneday(self,day,coint_temp,**args):
        """
        Parameters:
        ------
        day:当天日期 %Y%m%d 取决于文件保存下来的
        coint_temp:处理信号，读取数据的cointegrity类实例
        freq:滚动频率
        args:主要包括signal.signal_refine的参数,
            rolling_time,shift_time,signal_time,std_in,std_out
        """
        freq = self._freq
        code = ["500","1000"]
        self.engine.define_strategy(self,coint_temp)
        self.engine.load_data_pickle(day)
        self.engine.initialize(code,day)
        self.engine.write_log("回测天:   "+ str(day))
        Y_name = self.engine.asset_group['Y'];X_name = self.engine.asset_group['X']
        self.mark_asset()
        self.clear_yesterday_error_position() 
        pos_X = self.engine.pos[self.signal.coint_member._X.split('_')[-1]]["open_B_yesterday"]
        pos_Y = self.engine.pos[self.signal.coint_member._Y.split('_')[-1]]["open_B_yesterday"]
        today_data_shape = self.engine.data[Y_name].shape[0]
        self.signal = self.engine.strategy.signal
        signal_all = self.signal.signal_refine(\
            **{"rolling_time":args["rolling_time"],\
            "shift_time":args["shift_time"],\
                "signal_time":args["signal_time"],\
                    "std_in":args["std_in"],\
                        "std_out":args["std_out"]})
        para_today = self.signal.get_ratio(\
            **{"rolling_time":args["rolling_time"],\
                "shift_time":args["shift_time"],\
                    "signal_time":args["signal_time"]})
        while self.engine.index < today_data_shape - 1:
            time = self.engine.data[Y_name]["UpdateTime"].iloc[self.engine.index]
            signal_use = signal_all.loc[time,"action"] if time in signal_all.index else "no signal"
            para = para_today.loc[time]
            self.core_logic(signal_use,para = para,pos_X = pos_X,pos_Y = pos_Y)
            self.engine.caculate_revenue_margin(day,end_signal = False)
            self.engine.index += freq
        self.close()
        self.engine.day_end_reset_whole_account(day)

    def strategy_start_tackle(self,day,coint_temp):
        """
        开盘买入半仓500和半仓1000底仓,200W现金，400W500底仓，400W1000底仓
        """
        freq = self._freq
        code = ["500","1000"]
        self.engine.define_strategy(self,coint_temp)
        self.engine.load_data_pickle(day)
        self.engine.initialize(code,day)
        self.engine.write_log("回测天:建仓  "+str(day))
        code_Y = self.engine.asset_group['Y'];code_X = self.engine.asset_group['X']
        today_data_shape = self.engine.data[code_Y].shape[0]
        self.signal = self.engine.strategy.signal
        money = self.engine.initial_money;money_500 = money/4;money_1000 = money/4
        while self.engine.index != today_data_shape - 600: #收盘前10分钟买入   
            self.engine.index += freq
        else:
            BidPrice1X_col = self.engine.data[code_X].columns.get_loc("BidPrice1")
            price_X = self.engine.data[code_X].iloc[self.engine.index,BidPrice1X_col]
            BidPrice1Y_col = self.engine.data[code_Y].columns.get_loc("BidPrice1")
            price_Y =  self.engine.data[code_Y].iloc[self.engine.index,BidPrice1Y_col]
            volume_X = np.ceil(int(money_500/price_X/100))*100
            volume_Y = np.ceil(int(money_1000/price_Y/100))*100
            self.send_market_order_func(code = code_Y,volume = volume_Y,direction = 'B', openclose = "open")
            self.send_market_order_func(code = code_X,volume = volume_X,direction = 'B', openclose = "open")
            print("建仓完成")
        self.engine.day_end_reset_whole_account(day)

    def main(self,**args):
        """
        回测所有天数

        Parameters:
        -----
        主要包括rolling_time,shift_time,signal_time,std_in,std_out
        """
        date_range = self.engine.daterange
        coint_temp = cointegrate_plot(date_range)
        f = open(os.path.join("/Users/dt/Desktop/HA/ETF_rolling/etf_pair/Strategy","tick_res"),"wb")
        for i in range(len(date_range)):
            if i == 0:
                "第一天根据收盘价格买入"
                date = coint_temp.date_range[i]
                _,_ = coint_temp.get_day_data(date,"3s")
                _,_ = coint_temp.interval_get_ratio("15min")
                _,_ = coint_temp.fixed_interval_plus_plot(plot_on = False)
                _ = coint_temp.tick_rolling_plot(freq_own=args["rolling_time"],tick_rolling=args["shift_time"],plot_on = False)
                self.strategy_start_tackle(date_range[0],coint_temp)
            else:
                try:
                    date = coint_temp.date_range[i]
                    _,_ = coint_temp.get_day_data(date,"3s")
                    _,_ = coint_temp.interval_get_ratio("15min")
                    _,_ = coint_temp.fixed_interval_plus_plot(plot_on = False)
                    _ = coint_temp.tick_rolling_plot(freq_own=args["rolling_time"],tick_rolling=args["shift_time"],plot_on = False)
                    self.run_backtesting_oneday(date,coint_temp,rolling_time = args["rolling_time"],\
                        shift_time = args["shift_time"],signal_time = args["signal_time"],std_in = args["std_in"],\
                            std_out = args["std_out"])
                    pickle.dump(self.engine.tick_res,f)
                except TypeError: #回归出来内部求解参数不对
                    continue;
        res_dir_path = "/Users/dt/Desktop/HA/ETF_rolling/etf_pair"
        self.engine.save_data(res_dir_path,"tick_results_etf_rolling")



        