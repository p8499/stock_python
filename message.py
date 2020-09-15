from datetime import datetime, date, timedelta
from math import floor
from time import sleep

from sqlalchemy import create_engine, Column, Integer, Sequence, String, ForeignKey, Date, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from tushare import pro_api

from constants import database_url, tu_token
from stock import Stock


class Message(declarative_base()):
    __tablename__ = 'fs13'
    msid = Column(Integer(), Sequence('fs13_msid'), primary_key=True)
    msstid = Column(String(16), ForeignKey(Stock.stid))
    msyear = Column(Integer())
    msperiod = Column(Integer())
    msseq = Column(Integer())
    mspublish = Column(Date())
    mssubject = Column(String(64))
    mscontent = Column(String(1024))

    def __init__(self, msstid, msyear, msperiod, msseq, mspublish, mssubject, mscontent):
        self.msstid = msstid
        self.msyear = msyear
        self.msperiod = msperiod
        self.msseq = msseq
        self.mspublish = mspublish
        self.mssubject = mssubject
        self.mscontent = mscontent


def get_date(year: int, period: int) -> date:
    if period == 1:
        return date(year, 3, 31)
    elif period == 2:
        return date(year, 6, 30)
    elif period == 3:
        return date(year, 9, 30)
    else:
        return date(year, 12, 31)


def get_period(d: date) -> int:
    return floor(d.month / 4) + 1


if __name__ == '__main__':
    engine = create_engine(database_url)
    DBSession = sessionmaker(bind=engine)
    session = DBSession()

    stockList = session.query(Stock.stid, Stock.stlisted, Stock.stdelisted).all()
    pro = pro_api(tu_token)
    for stock in stockList:
        timeStart = datetime.now()
        lastDate = session.query(func.max(Message.mspublish)).filter(Message.msstid == stock.stid).first()[0]
        fromDate = lastDate + timedelta(days=1) if lastDate is not None else stock.stlisted
        toDate = stock.stdelisted - timedelta(days=1) if stock.stdelisted is not None else date.today()
        df = pro.forecast(ts_code=stock.stid.replace('-', '.'),
                          start_date=fromDate.strftime('%Y%m%d'), end_date=toDate.strftime('%Y%m%d'),
                          fields='ann_date,end_date,type,p_change_min,p_change_max,change_reason')
        df.sort_values(by=['end_date', 'ann_date'], ascending=(True, True), inplace=True)
        df.drop_duplicates(['end_date'], keep='last', inplace=True)
        df.reset_index(drop=True, inplace=True)
        for row in df.itertuples():
            endDate = datetime.strptime(row.end_date, '%Y%m%d').date()
            year = endDate.year
            period = get_period(endDate)
            annDate = datetime.strptime(row.ann_date, '%Y%m%d').date()
            subject = '%s (%.4f%% - %.4f%%)' % (row.type,
                                                row.p_change_min if row.p_change_min is not None else float('nan'),
                                                row.p_change_max if row.p_change_max is not None else float('nan'))
            content = row.change_reason
            session.add(Message(stock.stid, year, period, 0, annDate, subject, content))
        session.commit()
        timeEnd = datetime.now()
        delta = timeEnd - timeStart
        deltaSeconds = (delta.seconds * 1000000 + delta.microseconds) / 1000000
        if deltaSeconds < 1.5:
            sleep(1.5 - deltaSeconds)

    session.close()
