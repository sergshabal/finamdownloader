from pandas import DataFrame, read_csv
from urllib import urlencode
from urllib2 import urlopen, Request
from datetime import datetime, timedelta, date

finam_symbols = urlopen('http://www.finam.ru/cache/icharts/icharts.js').readlines()
periods = {'tick': 1, 'min': 2, '5min': 3, '10min': 4, '15min': 5,
           '30min': 6, 'hour': 7, 'daily': 8, 'week': 9, 'month': 10}

__all__ = ['periods', 'get_quotes_finam']


def __get_finam_code__(symbol):
    s_id = finam_symbols[0]
    s_code = finam_symbols[2]
    names = s_code[s_code.find('[\'') + 1:s_code.find('\']')].split('\',\'')
    ids = s_id[s_id.find('[') + 1:s_id.find(']')].split(',')
    if symbol in names:
        max_id = 0
        for i, name in enumerate(names):
            if name == symbol and i > max_id:
                max_id = i
        return int(ids[max_id])
    else:
        raise Exception("%s not found\r\n" % symbol)


def __get_url__(symbol, period, start_date, end_date):
    finam_HOST = "195.128.78.52"
    #'http://195.128.78.52/table.csv?market=1&em=3&code=SBER&df=9&mf=11&yf=2013&dt=9&mt=11&yt=2013&p=1&f=table&e=.csv&cn=SBER&dtf=1&tmf=1&MSOR=0&mstime=on&mstimever=1&sep=3&sep2=1&datf=9&at=1'
    #'http://195.128.78.52/table.csv?d=d&market=1&f=table&e=.csv&dtf=1&tmf=3&MSOR=0&mstime=on&mstimever=1&sep=3&sep2=1&at=1&em=20509&p=1&mf=10&cn=FEES&mt=10&df=22&dt=22&yt=2013&yf=2013&datf=11'
    #finam_URL = "/table.csv?d=d&market=1&f=table&e=.csv&dtf=1&tmf=1&MSOR=0&sep=1&sep2=1&at=1&"
    finam_URL = "/table.csv?d=d&market=1&f=table&e=.csv&dtf=1&tmf=3&MSOR=0&mstime=on&mstimever=1&sep=3&sep2=1&at=1&"
    #'/table.csv?d=d&market=1&f=table&e=.csv&dtf=1&tmf=3&MSOR=0&mstime=on&mstimever=1&sep=3&sep2=1&at=1'
    symb = __get_finam_code__(symbol)
    params = urlencode({"p": period, "em": symb,
                        "df": start_date.day, "mf": start_date.month - 1,
                        "yf": start_date.year,
                        "dt": end_date.day, "mt": end_date.month - 1,
                        "yt": end_date.year, "cn": symbol})

    stock_URL = finam_URL + params
    if period == periods['tick']:
        return "http://" + finam_HOST + stock_URL + '&code='+ symbol + '&datf=11'
    else:
        return "http://" + finam_HOST + stock_URL + '&datf=5'


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
    pdata = read_csv(url, index_col=0, parse_dates={'index': [0, 1]}, sep=';').sort_index()
    pdata.columns = [symbol + '.' + i for i in ['Open', 'High', 'Low', 'Close', 'Vol']]
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
        pdata = read_csv(url, index_col=0, parse_dates={'index': [0, 1]}, sep=';').sort_index()
        pdata.columns = [symbol + '.' + i for i in ['Open', 'High', 'Low', 'Close', 'Vol']]
        return pdata


def __get_tick_quotes_finam__(symbol, start_date, end_date):
    start_date = datetime.strptime(start_date, "%Y%m%d").date()
    end_date = datetime.strptime(end_date, "%Y%m%d").date()
    delta = end_date - start_date
    data = DataFrame()
    for i in range(delta.days + 1):
        day = timedelta(i)
        url = __get_url__(symbol, periods['tick'], start_date + day, start_date + day)
        req = Request(url)
        req.add_header('Referer', 'http://www.finam.ru/analysis/profile0000300007/default.asp')
        r = urlopen(req)
        try:
            tmp_data = read_csv(r, index_col=0, parse_dates={'index': [0, 1]}, sep=';').sort_index()
            if data.empty:
                data = tmp_data
            else:
                data = data.append(tmp_data)
        except Exception:
            print 'error on data downloading {} {}'.format(symbol, start_date + day)

    data.columns = [symbol + '.' + i for i in ['Last', 'Vol', 'Id']]
    return data


def __get_tick_quotes_finam_all__(symbol, start_date, end_date):
    start_date = datetime.strptime(start_date, "%Y%m%d").date()
    end_date = datetime.strptime(end_date, "%Y%m%d").date()
    url = __get_url__(symbol, periods['tick'], start_date, end_date)
    req = Request(url)
    req.add_header('Referer', 'http://www.finam.ru/analysis/profile0000300007/default.asp')
    r = urlopen(req)
    pdata = read_csv(r, index_col=0, parse_dates={'index': [0, 1]}, sep=';').sort_index()
    pdata.columns = [symbol + '.' + i for i in ['Last', 'Vol', 'Id']]
    return pdata


if __name__ == "__main__":
    code = 'FEES'
    per = 'tick'
    print 'download %s data for %s' % (per, code)
    quote = get_quotes_finam(code, start_date='20131122', end_date='20131125', period=per)
    print quote
    print quote.head()