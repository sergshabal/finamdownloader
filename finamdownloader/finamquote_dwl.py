from pandas import DataFrame, read_csv
from urllib import urlencode, urlopen
from urllib2 import Request
from urllib2 import urlopen as urlopen2
from datetime import datetime, timedelta, date

finam_symbols = urlopen('http://www.finam.ru/cache/icharts/icharts.js').readlines()
periods = {'tick': 1, '1min': 2, '5min': 3, '10min': 4, '15min': 5,
           '30min': 6, 'hour': 7, 'daily': 8, 'week': 9, 'month': 10}

__all__ = ['periods', 'get_quotes_finam']


def __print__(s, verbose=False):
    if verbose:
        print(s)
    return


def __get_finam_code__(symbol, verbose=False):
    s_id = finam_symbols[0]
    s_code = finam_symbols[2]
    s_market = finam_symbols[3]

    ids = s_id[s_id.find('[') + 1:s_id.find(']')].split(',')
    codes = s_code[s_code.find('[\'') + 1:s_code.find('\']')].split('\',\'')
    markets = s_market[s_market.find('[') + 1:s_market.find(']')].split(',')
    data = DataFrame(data=ids, columns=['ids'])
    data['code'] = codes
    data['market'] = markets
    idx = (data.market != '-1') & (data.market != '3')
    data = data[idx].sort_values(by=['market'])
    res = data[data.code == symbol].iloc[0].ids
    if res is None or res == '':
        raise Exception("%s not found\r\n" % symbol)
    __print__("{0}, {1}".format(res, symbol), verbose)
    return res


def __get_url__(symbol, period, start_date, end_date, verbose=False):
    # finam_HOST = "195.128.78.52"
    finam_HOST = "export.finam.ru"
    #'http://195.128.78.52/table.csv?market=1&em=3&code=SBER&df=9&mf=11&yf=2013&dt=9&mt=11&yt=2013&p=1&f=table&e=.csv&cn=SBER&dtf=1&tmf=1&MSOR=0&mstime=on&mstimever=1&sep=3&sep2=1&datf=9&at=1'
    #'http://195.128.78.52/table.csv?d=d&market=1&f=table&e=.csv&dtf=1&tmf=3&MSOR=0&mstime=on&mstimever=1&sep=3&sep2=1&at=1&em=20509&p=1&mf=10&cn=FEES&mt=10&df=22&dt=22&yt=2013&yf=2013&datf=11'
    #finam_URL = "/table.csv?d=d&market=1&f=table&e=.csv&dtf=1&tmf=1&MSOR=0&sep=1&sep2=1&at=1&"
    finam_URL = "/table.csv?d=d&market=1&f=table&e=.csv&dtf=1&tmf=3&MSOR=0&mstime=on&mstimever=1&sep=3&sep2=1&at=1&"
    #'/table.csv?d=d&market=1&f=table&e=.csv&dtf=1&tmf=3&MSOR=0&mstime=on&mstimever=1&sep=3&sep2=1&at=1'
    symb = __get_finam_code__(symbol, verbose)
    params = urlencode({"p": period, "em": symb,
                        "df": start_date.day, "mf": start_date.month - 1,
                        "yf": start_date.year,
                        "dt": end_date.day, "mt": end_date.month - 1,
                        "yt": end_date.year, "cn": symbol,
                        "code": symbol,})

    stock_URL = finam_URL + params
    s = "http://" + finam_HOST + stock_URL
    s = s + ('&datf=11' if period == periods['tick'] else '&datf=5')
    __print__(s, verbose)
    return s


def __period__(s):
    return periods[s]


def __get_daily_quotes_finam__(symbol, start_date='20070101',
                               end_date=date.today().strftime('%Y%m%d'),
                               period='daily', verbose=False):
    """
    Return downloaded daily or more prices about symbol from start date to end date
    """
    start_date = datetime.strptime(start_date, "%Y%m%d").date()
    end_date = datetime.strptime(end_date, "%Y%m%d").date()
    url = __get_url__(symbol, __period__(period), start_date, end_date, verbose)
    try:
        pdata = read_csv(url, index_col=0, parse_dates={'index': [0, 1]}, sep=';').sort_index()
    except:
        e = "ERROR on {}".format(url)
        raise Exception(e)
    pdata.columns = [symbol + '.' + i for i in ['Open', 'High', 'Low', 'Close', 'Vol']]
    return pdata


def get_quotes_finam(symbol, start_date='20070101',
                     end_date=None,
                     period='daily', verbose=False):
    """
    Return downloaded prices for symbol from start date to end date with default period daily
    Date format = YYYYMMDD
    Period can be in ['tick','1min','5min','10min','15min','30min','hour','daily','week','month']
    """
    if end_date is None:
        end_date = date.today().strftime("%Y%m%d")

    if __period__(period) == periods['tick']:
        return __get_tick_quotes_finam__(symbol, start_date, end_date, verbose)
    elif __period__(period) >= periods['daily']:
        return __get_daily_quotes_finam__(symbol, start_date, end_date, period, verbose)
    else:
        start_date = datetime.strptime(start_date, "%Y%m%d").date()
        end_date = datetime.strptime(end_date, "%Y%m%d").date()
        url = __get_url__(symbol, __period__(period), start_date, end_date, verbose)
        try:
            pdata = read_csv(url, index_col=0, parse_dates={'index': [0, 1]}, sep=';').sort_index()
        except:
            e = "ERROR on {}".format(url)
            raise Exception(e)
        
        pdata.columns = [symbol + '.' + i for i in ['Open', 'High', 'Low', 'Close', 'Vol']]
        return pdata


def __get_tick_quotes_finam__(symbol, start_date, end_date, verbose=False):
    start_date = datetime.strptime(start_date, "%Y%m%d").date()
    end_date = datetime.strptime(end_date, "%Y%m%d").date()
    delta = end_date - start_date
    data = DataFrame()
    for i in range(delta.days + 1):
        day = timedelta(i)
        url = __get_url__(symbol, periods['tick'], start_date + day, start_date + day, verbose)
        req = Request(url)
        req.add_header('Referer', 'http://www.finam.ru/analysis/profile0000300007/default.asp')
        r = urlopen2(req)
        try:
            tmp_data = read_csv(r, index_col=0, parse_dates={'index': [0, 1]}, sep=';').sort_index()
            if data.empty:
                data = tmp_data
            else:
                data = data.append(tmp_data)
        except Exception:
            print('error on data downloading {} {}'.format(symbol, start_date + day))

    data.columns = [symbol + '.' + i for i in ['Last', 'Vol', 'Id']]
    return data


def __get_tick_quotes_finam_all__(symbol, start_date, end_date, verbose=False):
    start_date = datetime.strptime(start_date, "%Y%m%d").date()
    end_date = datetime.strptime(end_date, "%Y%m%d").date()
    url = __get_url__(symbol, periods['tick'], start_date, end_date, verbose)
    req = Request(url)
    req.add_header('Referer', 'http://www.finam.ru/analysis/profile0000300007/default.asp')
    r = urlopen(req)
    pdata = read_csv(r, index_col=0, parse_dates={'index': [0, 1]}, sep=';').sort_index()
    pdata.columns = [symbol + '.' + i for i in ['Last', 'Vol', 'Id']]
    return pdata


if __name__ == "__main__":
    code = 'SBER'
    per = 'hour'
    print 'download %s data for %s' % (per, code)
    s = """http://195.128.78.52/SBER_140101_150101.csv?market=1
&em=3&code=SBER&df=1&mf=0&yf=2014&from=01.01.2014&dt=1&mt=0
&yt=2015&to=01.01.2015&p=7&f=table&e=.csv&cn=SBER&dtf=1&tmf=1
&MSOR=1&mstime=on&mstimever=1&sep=3&sep2=1&datf=5&at=1"""
    s = s.replace("\n", "")
    url = __get_url__(code, __period__(per), start_date=datetime(2014,1,1), end_date=datetime(2015,1,1), verbose=True)
    #print(url)
    ss = s.split('&')
    aa = ss.sort()
    print("")

    for i in ss:
        if i.find('http') != -1:
            x = i.split('?')
            print(x[-1])
        else:
            print(i)

    ss1 = url.split('&')
    aa = ss1.sort()
    print("")

    for i,j in zip(ss, ss1):
        ii,jj = i,j
        if ii.find('http') != -1:
            x = ii.split('?')
            ii = x[-1]
        if jj.find('http') != -1:
            x = jj.split('?')
            jj = x[-1]

        if True:#ii != jj:
            print(ii, "\t", jj)


    quote = get_quotes_finam(code, start_date='20150101', period=per, verbose=True)
    print quote.head()
    quote = get_quotes_finam(code, start_date='20150106', end_date='20150106', period='tick', verbose=True)
    print quote.head()
