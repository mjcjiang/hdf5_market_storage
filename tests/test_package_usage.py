from FastH5 import h5read
from FastH5 import h5write

import time
import h5py
from multiprocessing import Process, Queue
import matplotlib.pyplot as plt

import socket
from FastH5 import configs

def test_reading():
    #print("-----------test parallel data fetch start----------------------")
    #start_time = time.time()
    #write
    symbol = "000001"
    symbol_new = "hjiang1998"

    #原来数据
    data = h5read.get_data_with_all_columns("jq", "2021-10-13", "2021-10-20", [symbol])
    frame = data[symbol]

    #改造日期索引
    new_index = [h5write.datetime_string_delta(x, 1) for x in frame.index]
    frame.index = new_index

    #add 'time' and 'date' column
    frame['date'] = [0.0] * configs.market_data_infos["jq"][1] * 1
    frame['time'] = [0.0] * configs.market_data_infos["jq"][1] * 1

    #fail data
    #frame.drop(frame.tail(1).index, inplace=True)

    new_dict = {}
    new_dict[symbol_new] = frame

    h5write.dump_data_to_file("jq", new_dict)
    print("dump finish!")

    data_new = h5read.get_data_with_all_columns("jq", "2021-10-13", "2021-10-20", [symbol_new])
    print(data_new) 

#测试多用户同时进行读取的包装函数
def read_wrapper(username, datasource, start_date, end_date, symbols, metric_queue):
    test_start = time.time()
    data = h5read.get_data_with_all_columns(datasource, start_date, end_date, symbols)
    mem_usage = 0.0
    for k, v in data.items():
        mem_usage = mem_usage + (v.memory_usage(deep=True).sum() / (1024 * 1024))
    metric_queue.put((username, mem_usage, time.time() - test_start))
    return data

#测试多用户同时读取文件
def test_multi_users_reading():
    symbol_list = []
    with h5py.File(configs.market_data_infos["yk"][0], 'r') as f:
        symbol_list = list(f.keys())

    plt.figure(figsize=(12, 8))
    chunk_size_list = []
    mem_avg_list = []

    user_num = 10
    for i in range(10):
        chunk_size = 2 ** i

        plist = []
        q = Queue()
        for i in range(user_num):
            p = Process(target = read_wrapper, args=("user" + str(i+1), "yk", "2001-01-01", "2022-01-01", symbol_list[i * chunk_size: (i+1) * chunk_size], q), )
            plist.append(p)

        for p in plist:
            p.start()

        time_sum = 0.0
        mem_sum = 0.0
        for p in plist:
            tmp = q.get()
            mem_sum += tmp[1]
            time_sum += tmp[2]
        time_avg = time_sum / len(plist)
        mem_avg = mem_sum / len(plist)

        print("user_num: [%d], chunk_size: [%d], memory_average: [%f MB], time_average[%f second]," % (user_num, chunk_size, mem_avg, time_avg))
        chunk_size_list.append(chunk_size)
        mem_avg_list.append(mem_avg)

        for p in plist:
            p.join()

    plt.plot(chunk_size_list, mem_avg_list, '--g,', lw=2, label='mem_avg')
    plt.xlim(0, 256)
    plt.xlabel('number of symbols', fontsize=15)
    plt.ylim(0, 15000)
    plt.ylabel('avg mem usage', fontsize=15)
    plt.title('average memory usage', fontsize=25)
    plt.show()

#测试dataframe在机器之间的收发
def test_memory_send():
    data = h5read.get_data_with_all_columns("jq", "2021-10-13", "2021-10-13", ["000001",])
    s = socket.socket()
    port = 12345
    ip = '127.0.0.1'

    s.connect((ip, port))
    s.send(data.to_json())
    s.close()

def test_default_paras():
    data1 = h5read.get_data_with_one_column('jq')
    print(data1)

def test_roll_back():
    source = "yk"
    symbols = ["000001"]
    
    '''
    data = h5read.get_data_with_all_columns(source, "2021-10-15", "2021-10-15", symbols)
    print("original data:")
    print(data)
    
    framedict = {} 
    for symbol, frame in data.items():
        new_index = [h5write.datetime_string_delta(x, 1) for x in frame.index]
        frame.index = new_index
        frame['date'] = [0.0] * configs.market_data_infos[source][1] * 1
        frame['time'] = [0.0] * configs.market_data_infos[source][1] * 1
        framedict[symbol] = frame
    
    print("framedict: ")
    print(framedict)

    h5write.dump_data_to_file(source, framedict)
    print("dump to file finish!")
    '''

    '''
    data_new = h5read.get_data_with_all_columns(source, "2021-10-15", "2021-11-20", symbols)
    print(data_new)
    '''

    source = "yk"
    symbols = ["000002"]

    h5write.roll_back_for_symbols(source, "2000-12-31", symbols)
    print("roll back finished!")

    data_after_roll_back = h5read.get_data_with_all_columns(source, "2000-12-30", "2021-11-11", symbols)
    print(data_after_roll_back)

if __name__ == "__main__":
    #data1 = h5read.get_data_with_one_column("yk", "2001-01-01", "2021-10-10", ["000001", ], "open")
    #print(data1)
    #test_reading()
    #test_multi_users_reading()
    #test_default_paras()
    test_roll_back()