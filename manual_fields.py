from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from constants import database_url
from stock import Stock

if __name__ == '__main__':
    engine = create_engine(database_url)
    DBSession = sessionmaker(bind=engine)
    session = DBSession()

    stockList = session.query(Stock.stid).all()
    for stock in stockList:
        pbm = session.execute(text(
            'SELECT PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY D1PB) PBM FROM FM11 WHERE D1PB IS NOT NULL AND D1PB > 0 AND D1STID = :stid'),
            {'stid': stock.stid}).first()['pbm']
        pb = session.execute(text(
            'SELECT t0.D1PB PB FROM FM11 t0 WHERE t0.D1STID = :stid AND t0.D1PB IS NOT NULL AND NOT EXISTS (SELECT 1 FROM FM11 t1 WHERE t1.D1STID = t0.D1STID AND t1.D1PB IS NOT NULL AND t1.D1DATE > t0.D1DATE)'),
            {'stid': stock.stid}).first()
        pb = pb['pb'] if pb is not None else None
        pbrate = pb / pbm if pb is not None and pbm is not None and pbm != 0 else None
        pbrank = session.execute(text(
            'SELECT t0.PBRANK PBRANK FROM (SELECT D1STID, D1DATE, PERCENT_RANK() OVER(ORDER BY D1PB) PBRANK FROM FM11 WHERE d1stid=:stid) t0 WHERE NOT EXISTS (SELECT 1 FROM FM11 t1 WHERE t1.D1STID = t0.D1STID AND t1.D1DATE > t0.D1DATE)'),
            {'stid': stock.stid}).first()
        pbrank = pbrank['pbrank'] if pbrate is not None else None
        psm = session.execute(text(
            'SELECT PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY D1PS) PSM FROM FM11 WHERE D1PS IS NOT NULL AND D1PS > 0 AND D1STID = :stid'),
            {'stid': stock.stid}).first()['psm']
        ps = session.execute(text(
            'SELECT t0.D1PS PS FROM FM11 t0 WHERE t0.D1STID = :stid AND t0.D1PS IS NOT NULL AND NOT EXISTS (SELECT 1 FROM FM11 t1 WHERE t1.D1STID = t0.D1STID AND t0.D1PS IS NOT NULL AND t1.D1DATE > t0.D1DATE)'),
            {'stid': stock.stid}).first()
        ps = ps['ps'] if ps is not None else None
        psrate = ps / psm if ps is not None and psm is not None and psm != 0 else None
        psrank = session.execute(text(
            'SELECT t0.PSRANK PSRANK FROM (SELECT D1STID, D1DATE, PERCENT_RANK() OVER(ORDER BY D1PS) PSRANK FROM FM11 WHERE d1stid=:stid) t0 WHERE NOT EXISTS (SELECT 1 FROM FM11 t1 WHERE t1.D1STID = t0.D1STID AND t1.D1DATE > t0.D1DATE)'),
            {'stid': stock.stid}).first()
        psrank = psrank['psrank'] if psrate is not None else None
        pem = session.execute(text(
            'SELECT PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY D1PE) PEM FROM FM11 WHERE D1PE IS NOT NULL AND D1PE > 0 AND D1STID = :stid'),
            {'stid': stock.stid}).first()['pem']
        pe = session.execute(text(
            'SELECT t0.D1PE PE FROM FM11 t0 WHERE t0.D1STID = :stid AND t0.D1PE IS NOT NULL AND NOT EXISTS (SELECT 1 FROM FM11 t1 WHERE t1.D1STID = t0.D1STID AND t0.D1PE IS NOT NULL AND t1.D1DATE > t0.D1DATE)'),
            {'stid': stock.stid}).first()
        pe = pe['pe'] if pe is not None else None
        perate = pe / pem if pe is not None and pem is not None and pem != 0 else None
        perank = session.execute(text(
            'SELECT t0.PERANK PERANK FROM (SELECT D1STID, D1DATE, PERCENT_RANK() OVER(ORDER BY D1PE) PERANK FROM FM11 WHERE d1stid=:stid) t0 WHERE NOT EXISTS (SELECT 1 FROM FM11 t1 WHERE t1.D1STID = t0.D1STID AND t1.D1DATE > t0.D1DATE)'),
            {'stid': stock.stid}).first()
        perank = perank['perank'] if perate is not None else None
        pcfm = session.execute(text(
            'SELECT PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY D1PCF) PCFM FROM FM11 WHERE D1PCF IS NOT NULL AND D1PCF > 0 AND D1STID = :stid'),
            {'stid': stock.stid}).first()['pcfm']
        pcf = session.execute(text(
            'SELECT t0.D1PCF PCF FROM FM11 t0 WHERE t0.D1STID = :stid AND t0.D1PCF IS NOT NULL AND NOT EXISTS (SELECT 1 FROM FM11 t1 WHERE t1.D1STID = t0.D1STID AND t0.D1PCF IS NOT NULL AND t1.D1DATE > t0.D1DATE)'),
            {'stid': stock.stid}).first()
        pcf = pcf['pcf'] if pcf is not None else None
        pcfrate = pcf / pcfm if pcf is not None and pcfm is not None and pcfm != 0 else None
        pcfrank = session.execute(text(
            'SELECT t0.PCFRANK PCFRANK FROM (SELECT D1STID, D1DATE, PERCENT_RANK() OVER(ORDER BY D1PCF) PCFRANK FROM FM11 WHERE d1stid=:stid) t0 WHERE NOT EXISTS (SELECT 1 FROM FM11 t1 WHERE t1.D1STID = t0.D1STID AND t1.D1DATE > t0.D1DATE)'),
            {'stid': stock.stid}).first()
        pcfrank = pcfrank['pcfrank'] if pcfrate is not None else None
        session.execute(text(
            'UPDATE FS11 SET STPBRATE = :pbrate, STPSRATE = :psrate, STPERATE = :perate, STPCFRATE = :pcfrate, STPBRANK = :pbrank, STPSRANK = :psrank, STPERANK = :perank, STPCFRANK = :pcfrank WHERE STID = :stid'),
            {'pbrate': pbrate, 'psrate': psrate, 'perate': perate, 'pcfrate': pcfrate,
             'pbrank': pbrank, 'psrank': psrank, 'perank': perank, 'pcfrank': pcfrank,
             'stid': stock.stid})
        session.commit()
    session.close()
