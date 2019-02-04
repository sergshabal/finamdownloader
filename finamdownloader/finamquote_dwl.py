# -*- coding: utf-8 -*-

import sys

pyver = sys.version_info[0]

from pandas import DataFrame, read_csv

if pyver > 2:
    from urllib.parse import urlencode
    from urllib.request import urlopen
    from io import StringIO
else: 
    from urllib import urlencode
    from urllib2 import Request
    from urllib2 import urlopen
    from StringIO import StringIO

from datetime import datetime, timedelta, date
from time import sleep


__finam_symbols = urlopen('http://www.finam.ru/cache/icharts/icharts.js').readlines()
if pyver > 2:
    for i in [0,2,3]:
        __finam_symbols[i] = __finam_symbols[i].decode()

def _unicode(sourse, decoding):
    if pyver > 2:
        return str(sourse)
    return unicode(sourse, decoding)


periods = {
    'tick': 1, '1min': 2, '5min': 3, '10min': 4, '15min': 5,
    '30min': 6, 'hour': 7, 'daily': 8, 'week': 9, 'month': 10
}

__col_names = {
    'tick': ['Last', 'Vol', 'Id'],
    'bar': ['Open', 'High', 'Low', 'Close', 'Vol'],
}

__all__ = ['periods', 'get_quotes_finam', 'get_quotes_as_buf']


def __print__(s, verbose=False):
    if verbose:
        print(s)
    return


def __get_finam_code__(symbol, verbose=False):
    s_id = __finam_symbols[0]
    s_code = __finam_symbols[2]
    s_market = __finam_symbols[3]

    # if pyver > 2:
    #     ids = s_id[s_id.find(b'[') + 1:s_id.find(b']')].split(b',')
    #     codes = s_code[s_code.find(b'[\'') + 1:s_code.find(b'\']')].split(b'\',\'')
    #     markets = s_market[s_market.find(b'[') + 1:s_market.find(b']')].split(b',')
    # else:
    ids = s_id[s_id.find('[') + 1:s_id.find(']')].split(',')
    codes = s_code[s_code.find('[\'') + 1:s_code.find('\']')].split('\',\'')
    markets = s_market[s_market.find('[') + 1:s_market.find(']')].split(',')
    res = []
    for (i, c, m) in zip(ids, codes, markets):
        if c == symbol:
            res.append((i, c, m))

    res = sorted(res, key=lambda icm: int(icm[2]))
    if not res:
        raise Exception("%s not found." % symbol)
    __print__("{0}, {1}".format(res, symbol), verbose)
    _id, _, _ = res[0]
    return _id


def __get_url__(symbol, period, start_date, end_date, verbose=False):
    finam_HOST = "export.finam.ru"
    finam_URL = "/table.csv?d=d&market=1&f=table&e=.csv&dtf=1&tmf=3&MSOR=0&mstime=on&mstimever=1&sep=3&sep2=1&at=1&"
    symb = __get_finam_code__(symbol, verbose)
    params = urlencode({"p": period, "em": symb,
                        "df": start_date.day, "mf": start_date.month - 1,
                        "yf": start_date.year,
                        "dt": end_date.day, "mt": end_date.month - 1,
                        "yt": end_date.year, "cn": symbol,
                        "code": symbol, })

    stock_URL = finam_URL + params
    result_url = "http://" + finam_HOST + stock_URL
    result_url += ('&datf=11' if period == periods['tick'] else '&datf=5')
    __print__(result_url, verbose)
    return result_url


def __period__(s):
    return periods[s]


def __get_buf_data(url):
    resp = urlopen(url)
    rawstr = resp.read()
    if pyver > 2:
        rawstr = rawstr.decode()
    erros_cnt = 0
    while not __has_data(rawstr):
        sleep(5)
        erros_cnt += 1
        if erros_cnt > 6:
            raise Exception(_unicode(rawstr, 'cp1251'))
    return StringIO(rawstr)


def __has_data(rawstr):
    error_data = u'Система уже обрабатывает Ваш запрос.'
    rawstr = _unicode(rawstr, 'cp1251')

    if rawstr.startswith(error_data):
        return False
    elif False:  # here must be other server exception
        return False
    else:
        return True


def __pandasDF_from_buf(buf):
    head = buf.readline()
    col_names = list(map(str.capitalize, head.strip().replace('<',
        '').replace('>', '').split(';')))
    df = read_csv(buf, index_col=0, parse_dates={'index': [0, 1]}, sep=';',
            names=col_names, header=None)
    buf.close()
    return df


def __update_tick_id(buf):
    def update_id(s):
        if len(s) == 0:
            return s
        separator = ';'
        arr = s.split(separator)
        try:
            arr[-1] = str(4294967296 + int(arr[-1]))
        except:
            return s
        return separator.join(arr)

    return "\r\n".join(map(update_id, buf.read().split('\r\n')))


def __get_tick_quotes_finam__(symbol, start_date, end_date, verbose=False):
    delta = end_date - start_date
    data = StringIO()
    is_data_writed = False

    for i in range(delta.days + 1):
        day = timedelta(i)
        url = __get_url__(symbol, periods['tick'], start_date + day, start_date + day, verbose)
        tmp_data = __get_buf_data(url)
        # _len = len(data) if pyver > 2 else data.len
        if is_data_writed:  # skip head
            _ = tmp_data.readline()

        data.write(__update_tick_id(tmp_data))
        is_data_writed = True
        tmp_data.close()

    return StringIO(data.getvalue())


def get_quotes_as_buf(symbol, start_date='20070101', end_date=None,
                      period='daily', verbose=False):
    """
    Return prices for symbol from start date to end date with default period daily.
    Return type is StringIO.
    Date format = YYYYMMDD
    Period can be in ['tick','1min','5min','10min','15min','30min','hour','daily','week','month']
    """
    if end_date is None:
        end_date = date.today().strftime("%Y%m%d")

    start_date = datetime.strptime(start_date, "%Y%m%d").date()
    end_date = datetime.strptime(end_date, "%Y%m%d").date()

    if __period__(period) == periods['tick']:
        return __get_tick_quotes_finam__(symbol, start_date, end_date, verbose)
    else:
        url = __get_url__(symbol, __period__(period), start_date, end_date, verbose)
        buf = __get_buf_data(url)
        return buf


def get_quotes_finam(symbol, start_date='20070101', end_date=None,
                     period='daily', verbose=False):
    """
    Return prices for symbol from start date to end date with default period daily.
    Return type is pandas DataFrame.
    Date format = YYYYMMDD
    period can be in ['tick','1min','5min','10min','15min','30min','hour','daily','week','month']
    """
    buf = get_quotes_as_buf(symbol, start_date, end_date, period, verbose)
    return __pandasDF_from_buf(buf)


if __name__ == "__main__":
    code = 'AKRN'
    per = 'hour'
    print('download %s data for %s' % (per, code))
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

    for i, j in zip(ss, ss1):
        ii, jj = i, j
        if ii.find('http') != -1:
            x = ii.split('?')
            ii = x[-1]
        if jj.find('http') != -1:
            x = jj.split('?')
            jj = x[-1]

        if True:  # ii != jj:
            print(ii, '\t', jj)

    quote = get_quotes_finam(code, start_date='20150101', period=per, verbose=True)
    print(quote.head())
    quote = get_quotes_finam(code, start_date='20150205', end_date='20150206', period='tick', verbose=True)
    print(quote.head())
    print(quote.tail())
