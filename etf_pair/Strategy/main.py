import sys,os
sys.path.append("/Users/dt/Desktop/HA/ETF_rolling")
import etf_pair.Strategy.strategy_instance as SI
# as strategy_etf_pair_type11 
# import etf_pair.Strategy.dstrategy_instance.signal_instance as signal_instance
import etf_pair.backtest.Engine.Spot_Engine as TE
import importlib
# importlib.reload(TE)
importlib.reload(SI)

date_range = sorted(list(os.walk("/Users/dt/Desktop/HA/ETF_rolling/data/510500"))[0][1])

if __name__ == "__main__":
    # print(TE)
    engine = TE.Tick_Engine(daterange = date_range[-150:],freq_caculate = 3,slipper = 0,\
                     today = date_range[-150],initial_money = 2000*int(1e4))
    strategy_instance = SI.strategy_etf_pair_type11(engine,freq=1)
    args = {"rolling_time":"10min","shift_time":"4min","signal_time":"45s",\
        "std_in":1,"std_out":2.5}

    strategy_instance.main(**args)