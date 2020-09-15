from datetime import timedelta, date, datetime
from math import isnan

from pandas import merge
from sqlalchemy import Column, Integer, Sequence, String, ForeignKey, Date, Numeric, create_engine, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from tushare import pro_api

from constants import database_url, tu_token
from stock import Stock


class StockDay1(declarative_base()):
    __tablename__ = 'fm11'
    d1id = Column(Integer(), Sequence('fm11_d1id'), primary_key=True)
    d1stid = Column(String(16), ForeignKey(Stock.stid))
    d1date = Column(Date())
    d1open = Column(Numeric())
    d1close = Column(Numeric())
    d1high = Column(Numeric())
    d1low = Column(Numeric())
    d1volume = Column(Numeric())
    d1amount = Column(Numeric())
    d1totalshare = Column(Numeric())
    d1flowshare = Column(Numeric())
    d1factor = Column(Numeric())

    d1buy1 = Column(Numeric())
    d1sell1 = Column(Numeric())
    d1buy2 = Column(Numeric())
    d1sell2 = Column(Numeric())
    d1buy3 = Column(Numeric())
    d1sell3 = Column(Numeric())
    d1buy4 = Column(Numeric())
    d1sell4 = Column(Numeric())

    def __init__(self, d1stid, d1date, d1open, d1close, d1high, d1low, d1volume, d1amount,
                 d1totalshare, d1flowshare, d1factor,
                 d1buy1, d1sell1, d1buy2, d1sell2, d1buy3, d1sell3, d1buy4, d1sell4):
        self.d1stid = d1stid
        self.d1date = d1date
        self.d1open = d1open
        self.d1close = d1close
        self.d1high = d1high
        self.d1low = d1low
        self.d1volume = d1volume
        self.d1amount = d1amount
        self.d1totalshare = d1totalshare
        self.d1flowshare = d1flowshare
        self.d1factor = d1factor
        self.d1buy1 = d1buy1
        self.d1sell1 = d1sell1
        self.d1buy2 = d1buy2
        self.d1sell2 = d1sell2
        self.d1buy3 = d1buy3
        self.d1sell3 = d1sell3
        self.d1buy4 = d1buy4
        self.d1sell4 = d1sell4


if __name__ == '__main__':
    engine = create_engine(database_url)
    DBSession = sessionmaker(bind=engine)
    session = DBSession()

    stockList = session.query(Stock.stid, Stock.stlisted, Stock.stdelisted).all()
    pro = pro_api(tu_token)
    for stock in stockList:
        lastDate = session.query(func.max(StockDay1.d1date)).filter(StockDay1.d1stid == stock.stid).first()[0]
        fromDate = lastDate + timedelta(days=1) if lastDate is not None else stock.stlisted
        toDate = stock.stdelisted - timedelta(days=1) if stock.stdelisted is not None else date.today()
        startDate = fromDate
        while startDate <= toDate:
            endDate = startDate + timedelta(days=4999)
            if endDate > toDate:
                endDate = toDate
            dfMarket = pro.daily(ts_code=stock.stid.replace('-', '.'),
                                 fields='ts_code,trade_date,open,close,high,low,vol,amount',
                                 start_date=startDate.strftime('%Y%m%d'), end_date=endDate.strftime('%Y%m%d'))
            dfFactor = pro.adj_factor(ts_code=stock.stid.replace('-', '.'),
                                      fields='ts_code,trade_date,adj_factor',
                                      start_date=startDate.strftime('%Y%m%d'), end_date=endDate.strftime('%Y%m%d'))
            dfBasic = pro.daily_basic(ts_code=stock.stid.replace('-', '.'),
                                      fields='ts_code,trade_date,total_share,float_share',
                                      start_date=startDate.strftime('%Y%m%d'), end_date=endDate.strftime('%Y%m%d'))
            dfFlow = pro.moneyflow(ts_code=stock.stid.replace('-', '.'),
                                   fields='ts_code,trade_date,buy_sm_vol,sell_sm_vol,buy_md_vol,sell_md_vol,buy_lg_vol,sell_lg_vol,buy_elg_vol,sell_elg_vol',
                                   start_date=startDate.strftime('%Y%m%d'), end_date=endDate.strftime('%Y%m%d'))
            # sleep(0.12)
            df = merge(dfMarket, dfFactor, how='inner', on=['ts_code', 'trade_date'])
            df = merge(df, dfBasic, how='inner', on=['ts_code', 'trade_date'])
            df = merge(df, dfFlow, how='left', on=['ts_code', 'trade_date'])
            for row in df.itertuples():
                trade_date = datetime.strptime(row.trade_date, '%Y%m%d').date()
                stockDay1 = StockDay1(stock.stid, trade_date, row.open, row.close, row.high, row.low,
                                      row.vol * 100, row.amount * 1000,
                                      row.total_share * 10000, row.float_share * 10000, row.adj_factor,
                                      row.buy_sm_vol * 100 if row.buy_sm_vol is not None
                                                              and not isnan(row.buy_sm_vol) else None,
                                      row.sell_sm_vol * 100 if row.sell_sm_vol is not None
                                                               and not isnan(row.sell_sm_vol) else None,
                                      row.buy_md_vol * 100 if row.buy_md_vol is not None
                                                              and not isnan(row.buy_md_vol) else None,
                                      row.sell_md_vol * 100 if row.sell_md_vol is not None
                                                               and not isnan(row.sell_md_vol) else None,
                                      row.buy_lg_vol * 100 if row.buy_lg_vol is not None
                                                              and not isnan(row.buy_lg_vol) else None,
                                      row.sell_lg_vol * 100 if row.sell_lg_vol is not None
                                                               and not isnan(row.sell_lg_vol) else None,
                                      row.buy_elg_vol * 100 if row.buy_elg_vol is not None
                                                               and not isnan(row.buy_elg_vol) else None,
                                      row.sell_elg_vol * 100 if row.sell_elg_vol is not None
                                                                and not isnan(row.sell_elg_vol) else None)
                session.add(stockDay1)
            startDate += timedelta(days=5000)
        session.commit()

    session.close()
