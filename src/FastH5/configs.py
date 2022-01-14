#行情数据信息
# key: 行情名
# value: [行情h5文件路径，单日行情条数(用于索引数据)]
market_data_infos = {
    "yk": [
            "/mnt/NAS/sda/AllData/cn_ashare/alpha/yk/yk_minute.h5", 
            241, 
            ['open', 'close', 'high', 'low', 'value', 'volume', 'time', 'date']
        ],
    "jq": [
            "/mnt/NAS/sda/AllData/cn_ashare/alpha/jq/jq_minute.h5", 
            240, 
            ['avg', 'close', 'high', 'low', 'open', 'pre_close', 'money', 'volume', 'time', 'date']
        ],
    "jq_post": [
            "/mnt/NAS/sda/AllData/cn_ashare/alpha/jq/jq_post_minute.h5", 
            240,
            ['avg', 'close', 'high', 'low', 'open', 'money', 'volume', 'volume_ratio','time', 'date']
        ]
}