# 按 dimm_sn 号切分 error event 数据集，并保存出现 error 的 dimm 静态信息

import pandas as pd
from config import *
import os, math, sys
from multiprocessing import Process, Queue
import csv
# import moxing as mox
sampleItem = ['ns','IP','BiosADDDCEn','timestamp','Name','memory_type','SN','Manufacturer','PN','OriginalPN','BitWidth','CpuId','ChannelId','DimmId','DynamicData','CpuInfo','Log','region','source_tag','source_type','agentSN','dates','hour','type']
targetItem = ['ns','IP','BiosADDDCEn','timestamp','Name','memory_type','SN','Manufacturer','PN','OriginalPN','BitWidth','CpuId','ChannelId','DimmId','DynamicData','CpuInfo','region','source_tag','source_type','agentSN','dates','hour','type','BankId','BankgroupId','ChipId','ColumnId','Count','RankId','RowId','TimeStamp','Type']

dynamicItem = ['BankId','BankgroupId','ChipId','ColumnId','Count','RankId','RowId','TimeStamp','Type']
def getFileList():
    fileList = []
    
    # fileSet = mox.file.walk(DATA_SOURCE_PATH)
    
    fileSet = os.walk(DATA_SOURCE_PATH)
    
    for root ,_, files in fileSet:
    
        for f in files:
            if not f.endswith('csv'):
                continue
            fileList.append(os.path.join(root,f))
    return fileList

def get_writer(file, items):
    f1 = open(file, mode="w")
    writer = csv.DictWriter(f1, items)
    itemMap = {}
    for item in items:
        itemMap [item] = item
    writer.writerow(itemMap)
    return writer


def parseFile(file):
    
    outputFile = os.path.join(PARSE_DATA_PATH, os.path.basename(file))
    
    writer = get_writer(outputFile, targetItem)
    
    # with mox.file.File(file, 'rb') as fn:
        
    with open(file, 'r') as fn:
        r = csv.DictReader(fn, fieldnames=sampleItem, delimiter=';')
        next(r)
        for row in r:
            logs = logs_transaction(row['Log'])
            del row['Log']
            for log in logs:
                for item in dynamicItem:
                    row[item] = log[item]

                writer.writerow(row)
def parseFileList(q, fileList):
    for file in fileList:
        parseFile(file)
        q.put(file)
        
def main():
    if not os.path.exists(PARSE_DATA_PATH):
        os.makedirs(PARSE_DATA_PATH)
    fileList = getFileList()
    
    q = Queue()        
    processList = []
    cpuCount = os.cpu_count() * 2
    subListSize = math.ceil(len(fileList) / cpuCount)
    for i in range(cpuCount):
        subFile = fileList[i*subListSize:(i + 1)*subListSize]
        processList.append(Process(target=parseFileList, args=([q, subFile])))
        
    for p in processList:
        p.start()
    
    length = len(fileList)
    for i in range(length):
        q.get()
        print("运行进度: {}/{}".format(i, length))
        
    for p in processList:
        p.join()

main()