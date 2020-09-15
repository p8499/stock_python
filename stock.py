from datetime import datetime

from pandas import concat
from sqlalchemy import create_engine, String, Column, Date, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from tushare import pro_api

from constants import database_url, tu_token
from exchange import Exchange


class Stock(declarative_base()):
    __tablename__ = 'fs11'
    stid = Column(String(16), primary_key=True)
    stexid = Column(String(4), ForeignKey(Exchange.exid))
    stcode = Column(String(6))
    stname = Column(String(16))
    stlisted = Column(Date())
    stdelisted = Column(Date())

    def __init__(self, stid, stexid, stcode, stname, stlisted, stdelisted):
        self.stid = stid
        self.stexid = stexid
        self.stcode = stcode
        self.stname = stname
        self.stlisted = stlisted
        self.stdelisted = stdelisted


if __name__ == '__main__':
    engine = create_engine(database_url)
    DBSession = sessionmaker(bind=engine)
    session = DBSession()

    pro = pro_api(tu_token)
    dfL = pro.stock_basic(exchange='', list_status='L', fields='ts_code,exchange,symbol,name,list_date,delist_date')
    dfP = pro.stock_basic(exchange='', list_status='P', fields='ts_code,exchange,symbol,name,list_date,delist_date')
    dfD = pro.stock_basic(exchange='', list_status='D', fields='ts_code,exchange,symbol,name,list_date,delist_date')
    df = concat([dfL, dfP, dfD])
    for row in df.itertuples():
        stock = Stock(row.ts_code.replace('.', '-'), row.exchange, row.symbol, row.name,
                      datetime.strptime(row.list_date, '%Y%m%d').date() if row.list_date is not None else None,
                      datetime.strptime(row.delist_date, '%Y%m%d').date() if row.delist_date is not None else None)
        session.merge(stock)

    session.commit()
    session.close()
