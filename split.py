# 按 dimm_sn 号切分 error event 数据集，并保存出现 error 的 dimm 静态信息

import pandas as pd
from config import *
import os, math, sys
from multiprocessing import Process, Queue
import csv


usingMox = False

if usingMox:
    import moxing as mox
    openFileFn = mox.file.File
    walkFileFn =  mox.file.walk
else:
    openFileFn = open
    walkFileFn =  os.walk

# 遍历路径，得到所有dimm的sn号列表
def getDIMMList():
    if not os.path.exists(SPLIT_DATA_PATH):
        os.makedirs(SPLIT_DATA_PATH)
    snMap = {}
    
    fileSet = walkFileFn(DATA_SOURCE_PATH)

    
    for root ,_, files in fileSet:
        for f in files:
            if not f.endswith('csv'):
                continue
            with openFileFn(os.path.join(root,f), 'rb') as fn:

                df = pd.read_csv(fn, sep=';') 
                for index, row in df.iterrows():
                    sn = row['SN']
                    if sn not in snMap:
                        snMap[sn] = True
                        subPath = os.path.join(SPLIT_DATA_PATH, sn)
                        if not os.path.exists(subPath):
                            os.mkdir(subPath)
                        # 解析静态信息
                        
                        staticDf = pd.DataFrame(columns=STATIC_ITEM)
                        staticRow = row[STATIC_ITEM]

                        
                        staticRow['CpuInfo'] = getCpuType(staticRow['CpuInfo'])

                        staticDf.loc[0] = staticRow
                        staticDf.to_csv(os.path.join(subPath, sn+"_static.csv"), index=False)
                        
    return list(snMap.keys())

# 多线程读取属于dimmList中的dimm数据，并聚合成单个dataframe
def getLogInDIMMListMultipro(dimmList):
    fileList = []
    fileSet = walkFileFn(DATA_SOURCE_PATH)
    
    for root ,_, files in fileSet:
        for f in files:
            if not f.endswith('csv'):
                continue
            fileList.append(os.path.join(root,f))
            
    q = Queue()        
    processList = []
    cpuCount = os.cpu_count() * 2
    subListSize = math.ceil(len(fileList) / cpuCount)
    for i in range(cpuCount):
        subFile = fileList[i*subListSize:(i + 1)*subListSize]
        processList.append(Process(target=getLogInFileList, args=([q, subFile,dimmList])))
        
    for p in processList:
        p.start()
    mergedDf = pd.DataFrame()
    length = len(processList)
    subDfList = []
    while True:
        if length == len(subDfList):
            break
        subDfList.append(q.get())

    mergedDf = pd.concat(subDfList)
        
        
    for p in processList:
        p.join()
    return mergedDf
        
# 读取fileList中属于dimmList的数据，并聚合成单个dataframe
def getLogInFileList(q, fileList, dimmList):
    mergedDf =  pd.DataFrame()

    for f in fileList:
        if not f.endswith('csv'):
                continue
        with openFileFn(f, 'rb') as fn:
            
            df = pd.read_csv(fn, sep=';')[['SN', 'Log']]
            df = df[df['SN'].isin(dimmList)] 
        
        if mergedDf.empty:
            mergedDf = df
            continue
        mergedDf = pd.concat([mergedDf, df])
    q.put(mergedDf)

# 单线程读取属于dimmList中的dimm数据，并聚合成单个dataframe
def getLogInDIMMList(dimmList):
    mergedDf =  pd.DataFrame()
    
    fileSet = walkFileFn(DATA_SOURCE_PATH)

    
    for root ,_, files in fileSet:
    
        for f in files:
            if not f.endswith('csv'):
                continue
            with openFileFn(os.path.join(root,f), 'rb') as fn:
                
                df = pd.read_csv(fn, sep=';')[['SN', 'Log']]
                df = df[df['SN'].isin(dimmList)] 
            
            if mergedDf.empty:
                mergedDf = df
                continue
            mergedDf = pd.concat([mergedDf, df])
    return mergedDf



# 解析logs   
def genDynamicInfo(df):
    dynamicDf = pd.DataFrame(columns=DYNAMIC_ITEM)
    for index, row in df.iterrows():
        logs = logs_transaction(row['Log'])
        for log in logs:
            dynamicDf = dynamicDf.append(log,ignore_index=True)
    dynamicDf = dynamicDf.sort_values(by=['TimeStamp']).reset_index(drop=True)
    dynamicDf = dynamicDf[DYNAMIC_ITEM]
    return dynamicDf

# 解析dimm的error log，并保存
def parseDIMM(dfList):
    for df in dfList:
        sn = df[0]
        subdf = df[1].reset_index(drop=True)
        subPath = os.path.join(SPLIT_DATA_PATH, sn)
        if not os.path.exists(subPath):
            os.mkdir(subPath)

        # 解析动态信息
        dynamicDf = genDynamicInfo(subdf)
        dynamicDf.to_csv(os.path.join(subPath, sn+"_error.csv"), index=False)
        # break

# 按dimm sn号切分数据，并解析
def splitByDIMM(df):
    q = Queue()
    dfList = list(df.groupby(by='SN'))
    processList = []
    cpuCount = os.cpu_count() * 2
    subListSize = math.ceil(len(dfList) / cpuCount)
    for i in range(cpuCount):
        subDimm = dfList[i*subListSize:(i + 1)*subListSize]
        processList.append(Process(target=parseDIMM, args=([subDimm])))
    

    for p in processList:
        p.start()

    for p in processList:
        p.join()
        

# 主函数
def main():
    dimmList = getDIMMList()
    print("dimm number = {}".format(len(dimmList)))
    subListSize = math.ceil(len(dimmList) / BATCH_NUM)
    for i in range(BATCH_NUM):
        print("process batch {}".format(i))
        subDimmList = dimmList[i*subListSize:(i + 1)*subListSize]
        subDf = getLogInDIMMListMultipro(subDimmList)
        splitByDIMM(subDf)
        print("batch {} finished".format(i))
print("split dimm")
# 命令行输入
DATA_SOURCE_PATH, BATCH_NUM = sys.argv[1], int(sys.argv[2])

print("source path = {}, batch number = {}".format(DATA_SOURCE_PATH, BATCH_NUM))
main()