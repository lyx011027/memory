from datetime import timedelta,datetime
import json
# 存放原始数据的文件夹
DATA_SOURCE_PATH = "/home/hw-admin/yixuan/ProjectTest/data"

# split 执行的 batch 数量，出现OOM时调大
BATCH_NUM = 10

# 存放生成数据集的路径
DATA_SET_PATH = "data_set"
# 存放训练得到的数据集
MODEL_PATH = "model"
# 存放R-P曲线图的文件夹
PIC_PATH = "pic"
SPLIT_DATA_PATH = "split"
# 提前预测时间，单位为minute
AHEAD_TIME_List = [timedelta(seconds=15),timedelta(minutes=1),timedelta(minutes=15),timedelta(minutes=30),timedelta(minutes=60)]
# 按batch生成数据集时，batch中dimm的数量，如果使用sample_batch.py生成数据集时发生OOM，则降低该值
BATCH_SIZE = 10000
MAXIMUM_RATIO = 100
vendors = {
    'Samsung':0,
    'Hynix':1,
    'Micron':2,
    'Ramaxel':3
    
}
def getVendor(vendor):
    return vendors[vendor]
    
def getPosition(row):
    return 16*row['CpuId'] + 8*row['DimmId'] + row['ChannelId']

# 将str形式的log转为[]json
def logs_transaction(logs):
    transaction_log =[]
    logs = logs.replace('"','').replace('\\','"')
    logs = logs[1:len(logs) - 1].split("},")
    lastLog = logs[len(logs)-1]
    lastLog = lastLog[:len(lastLog)-1]
    logs[len(logs)-1] = lastLog
    for log in logs:
        log += '}'
        log = json.loads(log)
        log['TimeStamp'] = datetime.fromtimestamp(log['TimeStamp'])
        log["Type"] = int(log["Type"])
        transaction_log.append(log)
        
    return transaction_log
# 获得cpu类别
def getCpuType(cpuInfo):
    cpuInfo = json.loads(cpuInfo.replace('"','').replace('\\','"'))
    if cpuInfo['ProcessorArchitecture'] == 'ARM':
        return 2
    if cpuInfo['Name'] == 'CPU1':
        return 0
    else:
        return 1

STATIC_ITEM = ['ns', 'IP', 'BiosADDDCEn','Name', 'memory_type', 'SN', 'Manufacturer', 'PN', 'OriginalPN', 'BitWidth', 'CpuId', 'ChannelId', 'DimmId', 'CpuInfo', 'region']

STATIC_TRAIN_ITEM = [ 'BiosADDDCEn',
                     'Manufacturer', 
                     'BitWidth', 
                     'CpuId', 
                     'ChannelId', 
                     'DimmId', 
                     'CpuInfo',
                     'Position']
DYNAMIC_ITEM = ['BankId', 'BankgroupId', 'ChipId', 'ColumnId', 'Count', 'RankId','RowId', 'TimeStamp', 'Type']