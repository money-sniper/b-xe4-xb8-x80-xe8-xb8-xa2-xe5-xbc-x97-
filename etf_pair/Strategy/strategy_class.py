from abc import ABC,abstractmethod

class Strategy_Class(ABC):
    def __init__(self,engine):
        "profit计算单个策略，可能多个信号的收益"
        self.engine = engine
        self.pos = engine.pos
        self.send_limit_order_func = engine.send_limit_orders
        self.cancel_order_func = engine.send_cancel_orders
        self.send_market_order_func = engine.send_market_orders
        self.signal_freq = 2*60 #交易信号频率

    @abstractmethod
    def core_logic(self):
        "主体交易逻辑,对于信号定义做什么"
        pass

    @abstractmethod
    def output_signal(self):
        '生成出一个dataframe,对全天数据直接处理,signal:pd.DataFrame'
        pass

