from pandas import concat
from sqlalchemy import Column, String, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from tushare import pro_api

from constants import database_url, tu_token


class Category(declarative_base()):
    __tablename__ = 'fs21'
    caid = Column(String(32), primary_key=True)
    caname = Column(String(64))
    catype = Column(String(1))

    def __init__(self, caid, caname, catype):
        self.caid = caid
        self.caname = caname
        self.catype = catype


if __name__ == '__main__':
    engine = create_engine(database_url)
    DBSession = sessionmaker(bind=engine)
    session = DBSession()

    pro = pro_api(tu_token)
    df = concat([pro.index_basic(market='MSCI'),
                 pro.index_basic(market='CSI'),
                 pro.index_basic(market='SSE'),
                 pro.index_basic(market='SZSE'),
                 pro.index_basic(market='CICC'),
                 pro.index_basic(market='SW'),
                 pro.index_basic(market='OTH')])

    for row in df.itertuples():
        session.merge(Category(row.ts_code.replace('.', '-'), row.name, 'X'))

    session.commit()
    session.close()
