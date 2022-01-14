import h5py
from datetime import datetime, timedelta
from filelock import Timeout, FileLock
import numpy as np
import pandas as pd
import multiprocessing as mp
from multiprocessing import Pool, Value, Process
import os

from FastH5.configs import *
from FastH5.utils import *

#3个和3个股票串行读取，3个以上并发读取
seq_para_threshold = 3 

#------------------------------------------------------------------------------
# get one symbol data in a date range
#------------------------------------------------------------------------------
def __get_one_symbol_data(datasource, start_date, end_date, symbol, columns):
    data_file_path = market_data_infos[datasource][0]
    recnum_per_day = market_data_infos[datasource][1]

    with h5py.File(data_file_path, 'r') as f:
        #如果文件中并没有这只股票的数据
        if symbol not in list(f.keys()):
            return pd.DataFrame()

        #这支股票的日期索引
        indexs = f[symbol]['index'][:]

        #这只股票的行情数据是空的，直接返回空的dataframe
        if len(indexs) == 0:
            return pd.DataFrame()

        #股票数据生效日期(从此日期之后，文件中存在该股票的数据)
        valid_begin_date = indexs[0].decode("utf8")
        #股票数据结束日期(文件中该股票数据的最后日期)
        valid_end_date = indexs[-1].decode("utf8")

        #获取数据的开始日期 在 股票结束日期之后，这种情况下返回一个空的DataFrame
        if start_date > valid_end_date:
            return pd.DataFrame()

        #获取数据的结束日期 在 股票开始日期之前，这种情况下返回一个空的DataFrame
        if end_date < valid_begin_date:
            return pd.DataFrame()

        utf_start = start_date.encode('utf8')
        utf_end = end_date.encode('utf8')

        if utf_start not in indexs:
            try:
                utf_start = get_next_trade_date(f, symbol, start_date).encode('utf8')
            except KeyError:
                utf_start = indexs[0]

        if utf_end not in indexs:
            try:
                utf_end = get_prev_trade_date(f, symbol, end_date).encode('utf8')
            except KeyError:
                utf_end = indexs[-1]

        #起止日期索引定位        
        start_index = np.where(indexs == utf_start)[0][0]
        end_index = np.where(indexs == utf_end)[0][0]
    
        #列索引定位
        all_columns = market_data_infos[datasource][2]
        column_indexs = [all_columns.index(column) for column in columns]

        #数据定位
        #start1 = time.time()
        data = f[symbol]['data'][start_index * recnum_per_day: (end_index + 1) * recnum_per_day, column_indexs]
        #end1 = time.time()
        #print("data location: (%f)" % (end1 - start1))

        #时间数据定位
        #start2 = time.time()
        datetimes = f[symbol]['index_all'][start_index * recnum_per_day: (end_index + 1) * recnum_per_day]  
        #end2 = time.time()
        #print("datetime location: (%f)" % (end2 - start2))

        #start3 = time.time()
        datetime_strs = [x.decode('utf8') for x in datetimes]
        #end3 = time.time()
        #print("datetime transform: (%f)" % (end3 - start3))
        
        #construct the dateframe
        #start4 = time.time()
        dfdata = pd.DataFrame(data=data, index=datetime_strs, columns=columns)
        #end4 = time.time()
        #print("datetime construction: (%f)" % (end4 - start4))
        return dfdata

#------------------------------------------------------------------------------
# get multiple symbols data in a date range use sequencial method
#------------------------------------------------------------------------------
def __sequence_get_symbols_data(datasource, start_date, end_date, symbols, columns):
    res_dict = {}

    #if symbols list is empty, then get data for all symbols
    if len(symbols) == 0:
        symbols = get_all_symbols_in_file(market_data_infos[datasource][0])

    for symbol in symbols:
        frame = __get_one_symbol_data(datasource, start_date, end_date, symbol, columns)
        res_dict[symbol] = frame
    return res_dict


#------------------------------------------------------------------------------
# get multiple symbols data(for one column) in a date range use sequencial method
#------------------------------------------------------------------------------
def __sequence_get_symbols_data_for_one_column(datasource, start_date, end_date, symbols, column):
    #if symbols list is empty, then get data for all symbols
    if len(symbols) == 0:
        symbols = get_all_symbols_in_file(market_data_infos[datasource][0])

    small_frame_list = []
    for symbol in symbols:
        tmp_frame = __get_one_symbol_data(datasource, start_date, end_date, symbol, [column,])
        tmp_frame.rename(columns={column: symbol}, inplace=True)
        small_frame_list.append(tmp_frame)
    
    res_frame=pd.concat(small_frame_list, axis=1, join='outer')
    return res_frame

#------------------------------------------------------------------------------
# get multiple symbols data in a date range use multi processing
#------------------------------------------------------------------------------
def __parallel_get_symbols_data(datasource, start_date, end_date, symbols, columns, level):
    res_dict = {}
    res_data_list = []

    poolsize = int(os.cpu_count() / level)

    #if symbols list is empty, then get data for all symbols
    if len(symbols) == 0:
        symbols = get_all_symbols_in_file(market_data_infos[datasource][0])

    with Pool(processes=poolsize) as p:
        for symbol in symbols: 
            tmp_data = p.apply_async(__get_one_symbol_data, args=(datasource, start_date, end_date, symbol, columns))
            res_data_list.append((symbol, tmp_data))

        for res_data in res_data_list:
            res_dict[res_data[0]] = res_data[1].get()

    return res_dict

#------------------------------------------------------------------------------
# get multiple symbols data(one column) in a date range use multi processing
#------------------------------------------------------------------------------
def __parallel_get_symbols_data_for_one_column(datasource, start_date, end_date, symbols, column, level):
    res_dict = {}
    res_data_list = []

    poolsize = int(os.cpu_count() / level)

    #if symbols list is empty, then get data for all symbols
    if len(symbols) == 0:
        symbols = get_all_symbols_in_file(market_data_infos[datasource][0])

    with Pool(processes=poolsize) as p:
        for symbol in symbols: 
            tmp_data = p.apply_async(__get_one_symbol_data, args=(datasource, start_date, end_date, symbol, [column,]))
            res_data_list.append((symbol, tmp_data))

        small_frame_list = []
        for res_data in res_data_list:
            tmp_frame = res_data[1].get()
            tmp_frame.rename(columns={column: res_data[0]}, inplace=True)
            small_frame_list.append(tmp_frame)

        res_frame=pd.concat(small_frame_list, axis=1, join='outer')
        return res_frame

#------------------------------------------------------------------------------
# get multiple symbols data in a date range use multi processing(for one attribute)
# parameters:
#   datasource: e.g "jk" or "yk"
#   start_date: the begin of the date
#   end_date: the end of the date
#   symbols: a symbol list, e.g. ["000001", "000002"]
#   column: a column name, e.g. "open"
# return value:
#   a padas data frame(datetime string as indexes, symbols as columns)
#------------------------------------------------------------------------------
def get_data_with_one_column(datasource, start_date="2001-01-01", end_date="2099-01-01", symbols=[], column="close", level=4):
    isSuccess = do_para_checking(datasource, start_date, end_date)
    if isSuccess:
        all_columns = market_data_infos[datasource][2]
        if column not in all_columns:
            raise ValueError("%s is not a valid column in %s!" % (column, all_columns))

        if len(symbols) == 0:
            symbols = get_all_symbols_in_file(market_data_infos[datasource][0])

        #得到symbol到dataframe的dict
        if len(symbols) > seq_para_threshold:
            return __parallel_get_symbols_data_for_one_column(datasource, start_date, end_date, symbols, column, level)
        else:
            return __sequence_get_symbols_data_for_one_column(datasource, start_date, end_date, symbols, column)

#------------------------------------------------------------------------------
# get multiple symbols data in a date range use multi processing(for all attribute)
# parameters:
#   datasource: e.g "jk" or "yk"
#   start_date: the begin of the date
#   end_date: the end of the date
#   symbols: s symbol list, e.g. ["000001", "000002"]
# return value:
#   a dict which key is symbol, value is dataframe
#------------------------------------------------------------------------------
def get_data_with_all_columns(datasource, start_date="2001-01-01", end_date="2099-01-01", symbols=[], level=4):
    isSuccess = do_para_checking(datasource, start_date, end_date)
    if isSuccess:
        all_columns = market_data_infos[datasource][2].copy()
        if 'time' in all_columns:
            all_columns.remove('time')
        if 'date' in all_columns:
            all_columns.remove('date')
        
        if len(symbols) == 0:
            symbols = get_all_symbols_in_file(market_data_infos[datasource][0])

        if len(symbols) > seq_para_threshold:
            return __parallel_get_symbols_data(datasource, start_date, end_date, symbols, all_columns, level)
        else:
            return __sequence_get_symbols_data(datasource, start_date, end_date, symbols, all_columns)