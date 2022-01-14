import h5py
from datetime import datetime, timedelta
from filelock import Timeout, FileLock
from h5py._hl import dataset
import numpy as np
import pandas as pd

from FastH5.configs import *
from FastH5.utils import *

#------------------------------------------------------------------------------
# check if the frame_dict can be write to h5 file
#------------------------------------------------------------------------------
def __check_dataframe_dict(datasource, frame_dict):
    global market_data_infos
    for symbol, frame in frame_dict.items():
        #行数检查
        rows_one_day = market_data_infos[datasource][1]
        if frame.shape[0] % rows_one_day != 0:
            raise ValueError("frame rows number[%d] not (n * %d) for symbol[%s]" % (frame.shape[0], rows_one_day, symbol))

        #列数检查和列名检查
        columns_in_config = market_data_infos[datasource][2].copy()
        if set(frame.columns) != set(columns_in_config):
            raise ValueError("frame columns[%s] not equal to config columns[%s]" % (frame.columns, columns_in_config))

        frame = frame[columns_in_config]
    return True

#------------------------------------------------------------------------------
# dump data to h5 file
# parameters:
#   datasource: e.g "jk" or "yk"
#   frame_dict: key is symbol, value is a dataframe[indexes: datetime string, columns: attributes]
# return value:
#   a dict which key is symbol, value is dataframe
#------------------------------------------------------------------------------
def dump_data_to_file(datasource, frame_dict):
    data_valid = __check_dataframe_dict(datasource, frame_dict)
    if data_valid:
        #行情数据文件和锁文件（两个要不一样）
        data_file_path = market_data_infos[datasource][0]
        lock_file_path = market_data_infos[datasource][0] + '.lock'
        with h5py.File(data_file_path, 'a') as f, FileLock(lock_file_path) as l:
            for symbol, frame in frame_dict.items():
                #写入新的股票行情数据，需要创建新的group和对应的datasets
                if symbol not in f.keys():
                    group = f.create_group(symbol)
                    columns = market_data_infos[datasource][2].copy()
                    columns_len = len(columns)

                    data = group.create_dataset('data', (0, columns_len), maxshape=(2000000, columns_len), chunks=True)
                    data.attrs['columns'] = market_data_infos[datasource][2]

                    group.create_dataset('index', data=[], maxshape=(10000), chunks=True, dtype='S10')
                    group.create_dataset('index_all', data=[], maxshape=(2000000), chunks=True, dtype='S19')

                if f[symbol]['index'].shape[0] == 0:
                    #表示文件中还没有该股票的数据
                    last_valid_date = '1970-01-01'
                else:
                    last_valid_date = f[symbol]['index'][-1].decode('utf8')

                #chuck the frame in 240 or 241
                chunck_size = market_data_infos[datasource][1]
                for key, chunk in frame.groupby(np.arange(len(frame)) // chunck_size):
                    chunk_date = chunk.index[0].split(' ')[0]

                    if chunk_date > last_valid_date:
                        #write this chunk of data into the h5 file
                        #'data' dataset
                        old_data_rows = f[symbol]['data'].shape[0]
                        f[symbol]['data'].resize((old_data_rows + chunck_size), axis=0)
                        f[symbol]['data'][old_data_rows:] = chunk.to_numpy()

                        #'index' dataset
                        old_index_rows = f[symbol]['index'].shape[0]
                        f[symbol]['index'].resize((old_index_rows + 1), axis = 0)
                        f[symbol]['index'][old_index_rows:] = chunk_date.encode('utf8')
                        
                        #'index_all' dataset
                        old_index_all_rows = f[symbol]['index_all'].shape[0]
                        f[symbol]['index_all'].resize((old_index_all_rows + chunck_size), axis=0)
                        f[symbol]['index_all'][old_index_all_rows:] = [x.encode("utf8") for x in chunk.index.values]

#------------------------------------------------------------------------------
# rollback data to a date for a symbol
# parameters:
#   filepath: filepath
#   lastdate: the last date you want to rollback, all the data after this date will be 
#   deleted
#------------------------------------------------------------------------------
def roll_back_for_symbols(datasource, destdate, symbols=[]):
    datapath = market_data_infos[datasource][0]
    lockpath = datapath + ".lock"
    chunksize = market_data_infos[datasource][1]

    with h5py.File(datapath, 'a') as f, FileLock(lockpath) as l:
        if len(symbols) == 0:
            symbols = list(f.keys())

        for symbol in symbols:
            if symbol in f.keys():
                if f[symbol]['index'].shape[0] == 0:
                    #表示文件中还没有该股票的数据, 不对该股票做回滚操作
                    continue
                else:
                    first_valid_date = f[symbol]['index'][0].decode('utf8')
                    last_valid_date = f[symbol]['index'][-1].decode('utf8')

            #if roll back date late than the last valid date, just continue
            if destdate >= last_valid_date:
                continue

            if destdate >= first_valid_date:
                if destdate.encode('utf8') not in f[symbol]['index'][:]:
                    destdate = get_prev_trade_date(f, symbol, destdate)

                indexs = list(f[symbol]['index'])
                start_index = indexs.index(destdate.encode('utf8'))
            else:
                #not a valid trade date, find the previous valid trade date
                start_index = -1

            #roll back the 'data' dataset
            data_dataset = f[symbol]['data']
            data_dataset.resize((start_index + 1) * chunksize, axis=0)

            #roll back the 'index' dataset
            index_dataset = f[symbol]['index']
            index_dataset.resize((start_index + 1), axis=0)
        
            #roll back the 'index_all' dataset
            indexall_dataset = f[symbol]['index_all']
            indexall_dataset.resize((start_index + 1) * chunksize, axis=0)

            f.flush()