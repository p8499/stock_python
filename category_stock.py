from datetime import date

from sqlalchemy import Column, Integer, Sequence, String, ForeignKey, Numeric, create_engine, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from tushare import pro_api

from category import Category
from constants import database_url, tu_token
from stock import Stock
from trading_date import TradingDate


class CategoryStock(declarative_base()):
    __tablename__ = 'fs22'
    csid = Column(Integer(), Sequence('fs22_csid'), primary_key=True)
    cscaid = Column(String(16), ForeignKey(Category.caid))
    csstid = Column(String(16), ForeignKey(Stock.stid))
    csweight = Column(Numeric())

    def __init__(self, cscaid, csstid, csweight):
        self.cscaid = cscaid
        self.csstid = csstid
        self.csweight = csweight


if __name__ == '__main__':
    engine = create_engine(database_url)
    DBSession = sessionmaker(bind=engine)
    session = DBSession()
    session.execute('delete from fs22 where true')

    pro = pro_api(tu_token)
    tradingDate = session.query(func.max(TradingDate.trdate)).filter(TradingDate.trdate < date.today()).first()[0]
    df = pro.index_weight(trade_date=tradingDate.strftime('%Y%m%d'))
    for row in df.itertuples():
        session.add(CategoryStock(row.index_code.replace('.', '-'), row.con_code.replace('.', '-'), row.weight))

    session.commit()
    session.close()
