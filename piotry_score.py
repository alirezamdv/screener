import pandas as pd
import config
import os
os.environ["HDF5_USE_FILE_LOCKING"] = "FALSE"

DATA_STORE = config.HDF


def get_net_income(income_df):
    return float(income_df['netIncome'][0])


def get_roa(balance_df, income_df):
    current = float(balance_df['totalAssets'][0])
    previous = float(balance_df['totalAssets'][1])
    av_assets = (current+previous)/2
    return get_net_income(income_df)/av_assets


def get_ocf(cash_df):
    return float(cash_df['operatingCashflow'][0])


def get_ltdebt(balance_df):
    try:
        current = float(balance_df['longTermDebt'][0])
        previous = float(balance_df['longTermDebt'][1])
    except ValueError as e :
        print(f'no value on balance for {balance_df["fiscalDateEnding"][0]} or {balance_df["fiscalDateEnding"][1]} \n {e}')
        return 0
    return previous - current


def get_fundamentals(ticker: str):
    REPORTS = ['BALANCE_SHEET', 'CASH_FLOW', 'INCOME_STATEMENT']
    with pd.HDFStore(DATA_STORE) as store:
        balance_sheet = pd.read_hdf(store, f'{ticker}/BALANCE_SHEET/quarterly')
        cashflow = pd.read_hdf(store, f'{ticker}/CASH_FLOW/quarterly')
        income_statement = pd.read_hdf(store, f'{ticker}/INCOME_STATEMENT/quarterly')

    return income_statement, balance_sheet, cashflow


def get_new_shares(balance_df):
    current = float(balance_df['commonStock'][0])
    previous = float(balance_df['commonStock'][1])
    return current - previous


def get_gross_margin(income_df):
    current = float(income_df['grossProfit'][0]) / \
        float(income_df['totalRevenue'][0])
    previous = float(income_df['grossProfit'][1]) / \
        float(income_df['totalRevenue'][1])
    return current - previous


def get_asset_turnover_ratio(income_df, balance_df):
    current = float(balance_df['totalAssets'][0])
    prev_1 = float(balance_df['totalAssets'][1])
    prev_2 = float(balance_df['totalAssets'][2])
    av_assets1 = (current+prev_1)/2
    av_assets2 = (prev_1 + prev_2)/2
    atr1 = float(income_df['totalRevenue'][0])/av_assets1
    atr2 = float(income_df['totalRevenue'][1])/av_assets2
    return atr1-atr2


def get_current_ratio(balance_df):
    current_TCA = float(balance_df['totalCurrentAssets'][0])
    previous_TCA = float(balance_df['totalCurrentAssets'][1])
    current_TCL = float(balance_df['totalCurrentLiabilities'][0])
    previous_TCL = float(balance_df['totalCurrentLiabilities'][1])
    ratio1 = current_TCA / current_TCL
    ratio2 = previous_TCA / previous_TCL
    return ratio1-ratio2


def get_piotroski_score(income_df, balance_df, cash_df):
    score = 0
    if get_net_income(income_df) > 0:
        score += 1

    if get_roa(balance_df, income_df) > 0:
        score += 1

    if get_ocf(cash_df) > 0:
        score += 1

    if get_ocf(cash_df) > get_net_income(income_df):
        score += 1

    if get_ltdebt(balance_df) > 0:
        score += 1

    if get_current_ratio(balance_df) > 0:
        score += 1

    if get_new_shares(balance_df) > 0:
        score += 1

    if get_gross_margin(income_df) > 0:
        score += 1

    if get_asset_turnover_ratio(income_df, balance_df) > 0:
        score += 1

    return score


def fundamental_score(ticker):
    score=0
    try:
        income , balance , cash = get_fundamentals(ticker)
        score = get_piotroski_score(income,balance,cash)
        # for i in range(0,len(balance)-2):
        #     print("geting scor for ",balance['fiscalDateEnding'][i], "to ", balance['fiscalDateEnding'])
        #     b = balance.iloc[i:].reset_index(drop=True)
        #     c = cash.iloc[i:].reset_index(drop=True)
        #     inc = income.iloc[i:].reset_index(drop=True)
        #     score = get_piotroski_score(income_df=inc,balance_df=b,cash_df=c)
        #     scores.append(score)
    except Exception as e:
        print(e)
        return score
    
    return score

if __name__ == '__main__':
    
    ticker = 'DLTR'
    print(fundamental_score(ticker))

    
    