import yfinance as yf
import pandas as pd

ticker = ('CBRL')
t = yf.Ticker(ticker)
fast_info = t.fast_info
#fast_info = t.fast_info.previous_close

info = t.get_info()
#print(fast_info)


print(fast_info)