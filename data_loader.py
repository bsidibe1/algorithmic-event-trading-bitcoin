import yfinance as yf
import pandas as pd

# Fonction pour charger les données 
def load_bitcoin_daily_prices(ticker = 'BTC-USD', period = 'max'):
    """
    load bitcoin daily prices for Yahoo finance website:
    https://finance.yahoo.com/quote/BTC-USD?p=BTC-USD
    """
    data = yf.Ticker(ticker).history(period = period, interval = '1d')['Close']
    return pd.DataFrame(data)


def load_bitcoin_daily_returns(ticker = 'BTC-USD', period = 'max'):
    """
    load bitcoin daily prices for Yahoo finance website and compute daily returns 
    """
    data = yf.Ticker(ticker).history(period = period, interval = '1d')['Close']
    ret = pd.DataFrame(data.pipe(lambda x : 100*(x - x.shift(1))/x.shift(1)))
    return ret.rename(columns = {'Close':'Return'})
