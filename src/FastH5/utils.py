import h5py
from datetime import datetime, timedelta

from FastH5.configs import *

#------------------------------------------------------------------------------
# give a symbol and a date, get the next valid trade date of this symbol
#------------------------------------------------------------------------------
def get_next_trade_date(f, symbol, date):
    date_indexs = f[symbol]['index'][:]
    last_valid_date = f[symbol]['index'][-1].decode('utf8')

    next_date = datetime.strptime(date, "%Y-%m-%d") + timedelta(days=1)
    next_datestr = next_date.strftime("%Y-%m-%d")

    while next_datestr <= last_valid_date:
        if next_datestr.encode('utf8') in date_indexs:
            return next_datestr

        next_date += timedelta(days=1)
        next_datestr = next_date.strftime("%Y-%m-%d")

    raise KeyError("can not find next valid trade date in dataset for %s" % date)

#------------------------------------------------------------------------------
# give a symbol and a date, get the previous valid trade date of this symbol
#------------------------------------------------------------------------------
def get_prev_trade_date(f, symbol, date):
    date_indexs = f[symbol]['index'][:]
    first_valid_date = f[symbol]['index'][0].decode('utf8')

    prev_date = datetime.strptime(date, "%Y-%m-%d") + timedelta(days=-1)
    prev_datestr = prev_date.strftime("%Y-%m-%d")

    while prev_datestr >= first_valid_date:
        if prev_datestr.encode('utf8') in date_indexs:
            return prev_datestr

        prev_date += timedelta(days=-1)
        prev_datestr = prev_date.strftime("%Y-%m-%d")

    raise KeyError("can not find next valid trade date in dataset for %s" % date)

#------------------------------------------------------------------------------
# get all symbols in a h5 file
#------------------------------------------------------------------------------
def get_all_symbols_in_file(filepath):
    with h5py.File(filepath) as f:
        return list(f.keys())

#------------------------------------------------------------------------------
# datetime string delta
#------------------------------------------------------------------------------
def datetime_string_delta(datetimestr, n):
    delta_date = datetime.strptime(datetimestr, "%Y-%m-%d %H:%M:%S") + timedelta(days=n)
    delta_datestr = delta_date.strftime("%Y-%m-%d %H:%M:%S")
    return delta_datestr

#------------------------------------------------------------------------------
# do parameter checking before the real work
#------------------------------------------------------------------------------
def do_para_checking(datasource, start_date, end_date):
    if datasource not in market_data_infos:
        raise ValueError("%s is not a valid data source![%s]" % (datasource, market_data_infos))

    if start_date > end_date:
        raise ValueError("start_date(%s) > end_date(%s), check your parameters!" % (start_date, end_date))

    return True