from pandas import DataFrame, read_csv
from urllib2 import urlopen, Request
from datetime import datetime, timedelta, date

finam_symbols = urlopen('http://www.finam.ru/cache/icharts/icharts.js').readlines()
periods = { 'tick' : 1, 'min' : 2, '5min' : 3, '10min' : 4, '15min' : 5,
            '30min' : 6, 'hour' : 7, 'daily' : 8, 'week' : 9, 'month' : 10 }

__all__ = ['periods', 'get_quotes_finam']
			
def __get_finam_code__(symbol):
    s_id = finam_symbols[0]
    s_code = finam_symbols[2]
    names = s_code[s_code.find('[\'')+1:s_code.find('\']')].split('\',\'')
    ids = s_id[s_id.find('[')+1:s_id.find(']')].split(',')
    if (symbol in names):
        symb_id = names.index(symbol)
        return int(ids[symb_id])
    else:
        print "%s not found\r\n" % symbol
    return 0

def __get_url__(symbol, period, start_date, end_date):
    symb = __get_finam_code__(symbol)
    datf = 5
    if period == periods['tick']:
        datf = 11
    url = 'http://195.128.78.52/table.csv?market=1&f=table&e=.csv&dtf=1&tmf=3'+\
          '&MSOR=0&mstime=on&mstimever=1&sep=3&sep2=1&at=1'+\
          '&datf='+str(datf)+\
          '&p=' +str(period)+\
          '&em='+str(symb)+\
          '&df='+str(start_date.day)+\
          '&mf='+str(start_date.month-1)+\
          '&yf='+str(start_date.year)+\
          '&dt='+str(end_date.day)+\
          '&mt='+str(end_date.month-1)+\
          '&yt='+str(end_date.year)+\
          '&cn='+symbol
    return url

def __period__(s):
    return periods[s]

def __get_daily_quotes_finam__(symbol, start_date='20070101', end_date=date.today().strftime('%Y%m%d'),
                           period='daily'):
    """
    Return downloaded daily or more prices about symbol from start date to end date
    """
    start_date = datetime.strptime(start_date, "%Y%m%d").date()
    end_date = datetime.strptime(end_date, "%Y%m%d").date()
    url = __get_url__(symbol, __period__(period), start_date, end_date)
    pdata = read_csv(url, index_col=0, parse_dates={'index':[0,1]}, sep=';').sort_index()
    pdata.columns = [ symbol + '.' + i for i in ['Open', 'High', 'Low', 'Close', 'Vol'] ]
    return pdata

def get_quotes_finam(symbol, start_date='20070101', end_date=date.today().strftime("%Y%m%d"),
                     period='daily'):
    """
    Return downloaded prices for symbol from start date to end date with default period daily
    Date format = YYYYMMDD
    Period can be in ['tick','min','5min','10min','15min','30min','hour','daily','week','month']
    """
    if __period__(period) == periods['tick']:
        return __get_tick_quotes_finam__(symbol, start_date, end_date)
    elif __period__(period) >= periods['daily']:
        return __get_daily_quotes_finam__(symbol, start_date, end_date, period)
    else:
        start_date = datetime.strptime(start_date, "%Y%m%d").date()
        end_date = datetime.strptime(end_date, "%Y%m%d").date()
        url = __get_url__(symbol, __period__(period), start_date, end_date)
        pdata = read_csv(url, index_col=0, parse_dates={'index':[0,1]}, sep=';').sort_index()
        pdata.columns = [ symbol + '.' + i for i in ['Open', 'High', 'Low', 'Close', 'Vol'] ]
        return pdata

def __get_tick_quotes_finam__(symbol, start_date, end_date):
    start_date = datetime.strptime(start_date, "%Y%m%d").date()
    end_date = datetime.strptime(end_date, "%Y%m%d").date()
    delta = end_date - start_date
    data = DataFrame()
    for i in range(delta.days+1):
        day = timedelta(i)
        url = __get_url__(symbol, periods['tick'], start_date + day, start_date + day)
        req = Request(url)
        req.add_header('Referer', 'http://www.finam.ru/analysis/profile0000300007/default.asp')
        r = urlopen(req)

        tmp_data = read_csv(r, index_col=0, parse_dates={'index':[0,1]}, sep=';').sort_index()
        if data.empty:
            data = tmp_data
        else:
            data = data.append(tmp_data)
    data.columns = [ symbol + '.' + i for i in ['Last', 'Vol', 'Id'] ]
    return data

def __get_tick_quotes_finam_all__(symbol, start_date, end_date):
    start_date = datetime.strptime(start_date, "%Y%m%d").date()
    end_date = datetime.strptime(end_date, "%Y%m%d").date()
    url = __get_url__(symbol, periods['tick'], start_date, end_date)
    req = Request(url)
    req.add_header('Referer', 'http://www.finam.ru/analysis/profile0000300007/default.asp')
    r = urlopen(req)
    pdata = read_csv(r, index_col=0, parse_dates={'index':[0,1]}, sep=';').sort_index()
    pdata.columns = [ symbol + '.' + i for i in ['Last', 'Vol', 'Id'] ]
    return pdata

if __name__ == "__main__":
    code = 'SiM3'
    per = 'hour'
    print 'download %s data for %s' %(per, code)
    quote = get_quotes_finam(code, start_date='20130513', period=per)
    print quote.head()