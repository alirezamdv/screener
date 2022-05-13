import requests
import pandas as pd
import logging
import sqlite3
import config
import time
from utils import get_all_assets
import os
os.environ["HDF5_USE_FILE_LOCKING"] = "FALSE"

REPORTS = ['BALANCE_SHEET', 'CASH_FLOW', 'INCOME_STATEMENT']
KEY = config.ALPHA
DATA_STORE = config.HDF


def get_financials(ticker: str) -> dict:
    reports = {}
    for r in REPORTS:
        log.info(f'downloading {r} for {ticker}')
        reports[r] = {}
        url = f'https://www.alphavantage.co/query?function={r}&symbol={ticker}&apikey={KEY}'
        resp = requests.get(url)
        if resp.status_code == 200:
            data = resp.json()
            try:
                reports[r]['quarterly'] = pd.DataFrame(data['quarterlyReports'])
                reports[r]['annual'] = pd.DataFrame(data['annualReports'])
            except:
                log.error(f"there is no quarterly or annualReports key in {data.keys()}")
                return {}
            time.sleep(5)
        else:
            log.error(f'http error: {resp.json()}')
    return reports


if __name__ == '__main__':
    logging.basicConfig(
        format='%(asctime)s - %(levelname)s:\t%(message)s',
        datefmt='%d-%b-%y %H:%M:%S',
        level=logging.DEBUG,
        filename='logs/download.log',
        filemode="w"
    )
    log = logging.getLogger(__name__)
    stocks = get_all_assets(1)['ticker']
    for stock in stocks:
        with pd.HDFStore(DATA_STORE) as store:
            if not store.__contains__(f'{stock}/BALANCE_SHEET/quarterly'):
                if store.__contains__(f'{stock}/daily'):
                    reports = get_financials(stock)
                    if len(reports)>0:
                        for key in reports.keys():
                            if f"/{stock}/{key}/annual" in store.keys(include='pandas'):
                                store.append(f"/{stock}/{key}/annual",
                                            reports[key]['annual'])
                            else:
                                store.put(f"/{stock}/{key}/annual",
                                        reports[key]['annual'], format='table')

                            if f"/{stock}/{key}/quarterly" in store.keys(include='pandas'):
                                store.append(f"/{stock}/{key}/quarterly",
                                            reports[key]['quarterly'])
                            else:
                                store.put(f"/{stock}/{key}/quarterly",
                                        reports[key]['quarterly'], format='table')
                        SLEEP = 60
                        log.info(f'saved {reports.keys()} for {stock}')
                    
                else:   log.info(f'skipping {stock} becoause there is not enough price data') 

            else:
                SLEEP = 0.1         
                log.info(f'skiping {stock}')

        time.sleep(SLEEP)
