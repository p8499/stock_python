from sqlalchemy import String, Column, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from constants import database_url


class Exchange(declarative_base()):
    __tablename__ = 'fs01'
    exid = Column(String(4), primary_key=True)
    exname = Column(String(8))

    def __init__(self, exid, exname):
        self.exid = exid
        self.exname = exname


if __name__ == '__main__':
    engine = create_engine(database_url)
    DBSession = sessionmaker(bind=engine)
    session = DBSession()

    sh = Exchange('SSE', '上交所')
    sz = Exchange('SZSE', '深交所')
    session.merge(sh)
    session.merge(sz)

    session.commit()
    session.close()
