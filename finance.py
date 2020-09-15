from datetime import date, datetime
from math import floor, isnan
from time import sleep

from pandas import merge
from sqlalchemy import Column, Integer, Sequence, String, ForeignKey, Date, Numeric, create_engine, and_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from tushare import pro_api

from constants import tu_token, database_url
from stock import Stock


class Finance(declarative_base()):
    __tablename__ = 'fs12'
    fiid = Column(Integer(), Sequence('fs12_fiid'), primary_key=True)
    fistid = Column(String(16), ForeignKey(Stock.stid))
    fiyear = Column(Integer())
    fiperiod = Column(Integer())
    fistatus = Column(String(1))
    fipublish = Column(Date())
    fina = Column(Numeric())
    fioi = Column(Numeric())
    finpe = Column(Numeric())
    fiocn = Column(Numeric())

    def __init__(self, fistid, fiyear, fiperiod, fistatus, fipublish, fina, fioi, finpe, fiocn):
        self.fistid = fistid
        self.fiyear = fiyear
        self.fiperiod = fiperiod
        self.fistatus = fistatus
        self.fipublish = fipublish
        self.fina = fina
        self.fioi = fioi
        self.finpe = finpe
        self.fiocn = fiocn


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

    stockList = session.query(Stock.stid).all()
    pro = pro_api(tu_token)
    for stock in stockList:
        timeStart = datetime.now()
        dfBS = pro.balancesheet(ts_code=stock.stid.replace('-', '.'),
                                fields='ann_date,end_date,total_hldr_eqy_exc_min_int,update_flag')
        dfBS.sort_values(by=['end_date', 'update_flag'], ascending=(True, True), inplace=True)
        dfBS.drop_duplicates(['end_date'], keep='last', inplace=True)
        dfBS.reset_index(drop=True, inplace=True)
        dfPL = pro.income(ts_code=stock.stid.replace('-', '.'),
                          fields='ann_date,end_date,revenue,n_income_attr_p,update_flag')
        dfPL.sort_values(by=['end_date', 'update_flag'], ascending=(True, True), inplace=True)
        dfPL.drop_duplicates(['end_date'], keep='last', inplace=True)
        dfPL.reset_index(drop=True, inplace=True)
        dfCF = pro.cashflow(ts_code=stock.stid.replace('-', '.'),
                            fields='ann_date,end_date,n_cashflow_act,update_flag')
        dfCF.sort_values(by=['end_date', 'update_flag'], ascending=(True, True), inplace=True)
        dfCF.drop_duplicates(['end_date'], keep='last', inplace=True)
        dfCF.reset_index(drop=True, inplace=True)
        df = merge(dfBS, dfPL, how='inner', on=['end_date'])
        df = merge(df, dfCF, how='inner', on=['end_date'])
        for row in df.itertuples():
            endDate = datetime.strptime(row.end_date, '%Y%m%d').date()
            year = endDate.year
            period = get_period(endDate)
            annDates = list(map(lambda x: datetime.strptime(x, '%Y%m%d').date(), filter(
                lambda x: x is not None, [row.ann_date, row.ann_date_x, row.ann_date_y])))
            annDate = min(annDates) if len(annDates) > 0 else endDate
            financeExisting = session.query(Finance).filter(and_(
                Finance.fistid == stock.stid, Finance.fiyear == year, Finance.fiperiod == period)).first()
            if financeExisting is None:
                financeNew = Finance(stock.stid, year, period, 'A', annDate,
                                     row.total_hldr_eqy_exc_min_int if not isnan(
                                         row.total_hldr_eqy_exc_min_int) else None,
                                     row.revenue if not isnan(row.revenue) else None,
                                     row.n_income_attr_p if not isnan(row.n_income_attr_p) else None,
                                     row.n_cashflow_act if not isnan(row.n_cashflow_act) else None)
                session.add(financeNew)
            else:
                financeExisting.fistatus = 'A'
                financeExisting.fipublish = annDate
                financeExisting.fina = row.total_hldr_eqy_exc_min_int if not isnan(
                    row.total_hldr_eqy_exc_min_int) else None
                financeExisting.fioi = row.revenue if not isnan(row.revenue) else None
                financeExisting.finpe = row.n_income_attr_p if not isnan(row.n_income_attr_p) else None
                financeExisting.fiocn = row.n_cashflow_act if not isnan(row.n_cashflow_act) else None
                session.add(financeExisting)
        lastActual = session.query(Finance.fiyear, Finance.fiperiod).filter(and_(
            Finance.fistid == stock.stid, Finance.fistatus == 'A')).order_by(
            Finance.fiyear.desc(), Finance.fiperiod.desc()).first()
        lastActualDate = get_date(lastActual.fiyear, lastActual.fiperiod) if lastActual is not None else None
        dfEx = pro.express(ts_code=stock.stid.replace('-', '.'),
                           fields='ann_date,end_date,revenue,total_hldr_eqy_exc_min_int,is_audit')
        dfEx.sort_values(by=['end_date', 'is_audit'], ascending=(True, True), inplace=True)
        dfEx.drop_duplicates(['end_date'], keep='last', inplace=True)
        dfEx.reset_index(drop=True, inplace=True)
        dfEx = dfEx.loc[dfEx['end_date'] > lastActualDate.strftime('%Y%m%d')] if lastActualDate is not None else dfEx
        for row in dfEx.itertuples():
            endDate = datetime.strptime(row.end_date, '%Y%m%d').date()
            year = endDate.year
            period = get_period(endDate)
            annDate = datetime.strptime(row.ann_date, '%Y%m%d').date() if row.ann_date is not None else endDate
            financeExisting = session.query(Finance).filter(and_(
                Finance.fistid == stock.stid, Finance.fiyear == year, Finance.fiperiod == period)).first()
            if financeExisting is None:
                financeNew = Finance(stock.stid, year, period, 'F', annDate,
                                     row.total_hldr_eqy_exc_min_int if not isnan(
                                         row.total_hldr_eqy_exc_min_int) else None,
                                     row.revenue if not isnan(row.revenue) else None,
                                     None, None)
                session.add(financeNew)
            else:
                financeExisting.fistatus = 'F'
                financeExisting.fipublish = annDate
                financeExisting.fina = row.total_hldr_eqy_exc_min_int if not isnan(
                    row.total_hldr_eqy_exc_min_int) else None
                financeExisting.fioi = row.revenue if not isnan(row.revenue) else None
                financeExisting.finpe = None
                financeExisting.fiocn = None
                session.add(financeExisting)
        session.commit()
        timeEnd = datetime.now()
        delta = timeEnd - timeStart
        deltaSeconds = (delta.seconds * 1000000 + delta.microseconds) / 1000000
        if deltaSeconds < 2:
            sleep(2 - deltaSeconds)

    session.close()
