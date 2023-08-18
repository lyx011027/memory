# 按 dimm_sn 号切分 error event 数据集，并保存出现 error 的 dimm 静态信息

import pandas as pd
from config import DATA_SOURCE_PATH,SPLIT_DATA_PATH,SPLIT_DATA_PATH,STATIC_ITEM,DYNAMIC_ITEM,getVendor,getCpuType,logs_transaction
import os, math
from multiprocessing import Process
import moxing as mox

    
def mergeDataSet():
    mergedDf =  pd.DataFrame()
    fileSet = mox.file.walk(DATA_SOURCE_PATH)
    
    # fileSet = os.walk(DATA_SOURCE_PATH)
    
    for root ,_, files in fileSet:
    
        for f in files:
            with mox.file.File(os.path.join(root,f), 'rb') as fn:
            
            # with open(os.path.join(root,f), 'rb') as fn:
                
                df = pd.read_csv(fn, sep=';')  
            
            if mergedDf.empty:
                mergedDf = df
                continue
            mergedDf = pd.concat([mergedDf, df])
    print("read finish")
    return mergedDf


def genStaticInfo(df):
    staticDf = pd.DataFrame(columns=STATIC_ITEM)
    row = df.loc[0,STATIC_ITEM]
    row['Manufacturer'] = getVendor(row['Manufacturer'])
    row['CpuInfo'] = getCpuType(row['CpuInfo'])
    staticDf = staticDf.append(row,ignore_index=True)
    return staticDf
    
def genDynamicInfo(df):
    dynamicDf = pd.DataFrame(columns=DYNAMIC_ITEM)
    for index, row in df.iterrows():
        logs = logs_transaction(row['Log'])
        for log in logs:
            dynamicDf = dynamicDf.append(log,ignore_index=True)
    dynamicDf = dynamicDf.sort_values(by=['TimeStamp']).reset_index(drop=True)
    dynamicDf = dynamicDf[DYNAMIC_ITEM]
    return dynamicDf

def parseDIMM(dfList):
    for df in dfList:
        sn = df[0]
        subdf = df[1].reset_index(drop=True)
        subPath = os.path.join(SPLIT_DATA_PATH, sn)
        if not os.path.exists(subPath):
            os.mkdir(subPath)
        # 解析静态信息
        staticDf = genStaticInfo(subdf)
        staticDf.to_csv(os.path.join(subPath, sn+"_static.csv"), index=False)
        
        # 解析动态信息
        dynamicDf = genDynamicInfo(subdf)
        dynamicDf.to_csv(os.path.join(subPath, sn+"_error.csv"), index=False)
        # break
def splitByDIMM(df):
    dfList = list(df.groupby(by='SN'))
    if not os.path.exists(SPLIT_DATA_PATH):
        os.makedirs(SPLIT_DATA_PATH)
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
        # break
print("split dimm")
df = mergeDataSet()
splitByDIMM(df)