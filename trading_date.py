from datetime import timedelta

from pandas import concat
from sqlalchemy import Column, Integer, Sequence, String, Date, create_engine, func, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from tushare import pro_api

from constants import database_url, tu_token
from exchange import Exchange


class TradingDate(declarative_base()):
    __tablename__ = 'fs02'
    trid = Column(Integer(), Sequence('fs02_trid'), primary_key=True)
    trexid = Column(String(4), ForeignKey(Exchange.exid))
    trdate = Column(Date())

    def __init__(self, trid, trexid, trdate):
        self.trid = trid
        self.trexid = trexid
        self.trdate = trdate


if __name__ == '__main__':
    engine = create_engine(database_url)
    DBSession = sessionmaker(bind=engine)
    session = DBSession()

    latestSSE = session.query(func.max(TradingDate.trdate)).filter(TradingDate.trexid == 'SSE').first()[0]
    latestSZSE = session.query(func.max(TradingDate.trdate)).filter(TradingDate.trexid == 'SZSE').first()[0]

    pro = pro_api(tu_token)
    dfSSE = pro.trade_cal(exchange='SSE',
                          start_date=(latestSSE + timedelta(days=1)).strftime('%Y%m%d')
                          if latestSSE is not None else '19900101',
                          end_date='', is_open='1')
    dfSZSE = pro.trade_cal(exchange='SZSE',
                           start_date=(latestSZSE + timedelta(days=1)).strftime('%Y%m%d')
                           if latestSZSE is not None else '19900101',
                           end_date='', is_open='1')
    df = concat([dfSSE, dfSZSE])
    for row in df.itertuples():
        trading_date = TradingDate(None, row.exchange, row.cal_date)
        session.add(trading_date)

    session.commit()
    session.close()
