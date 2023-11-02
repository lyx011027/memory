# 按 dimm_sn 号切分 error event 数据集，并保存出现 error 的 dimm 静态信息

import pandas as pd
from config import *
import os, math, sys
from multiprocessing import Process, Queue
import csv
beforeMergePath = "beforeMerge"


usingMox = False

if usingMox:
    import moxing as mox
    openFileFn = mox.file.File
    walkFileFn =  mox.file.walk
else:
    openFileFn = open
    walkFileFn =  os.walk






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

def getSourceFileList(DATA_SOURCE_PATH):
    fileList = []
    fileSet = walkFileFn(DATA_SOURCE_PATH)
    
    for root ,_, files in fileSet:
        for f in files:
            if not f.endswith('csv'):
                continue
            fileList.append(os.path.join(root,f))
            
    return fileList

def mergeFileList(q, dimmList):
    for sn in dimmList:
        
        sourcePath = os.path.join(SPLIT_DATA_PATH, sn, 'beforeMerge')
        
        sourceFileList = os.listdir(sourcePath)
        count = len(sourceFileList)
        df = pd.read_csv(os.path.join(sourcePath,sourceFileList[0]))
        # df.to_csv(targetFile, index=False)
        for i in range(1, count):
            subDf = pd.read_csv(os.path.join(sourcePath,sourceFileList[i]))
            df = pd.concat([df, subDf])
            
        # 生成静态信息文件
        staticDf = pd.DataFrame(columns=STATIC_ITEM)
        row = df.iloc[0]
        # print(row)
        staticRow = row[STATIC_ITEM]
        staticRow['CpuInfo'] = getCpuType(staticRow['CpuInfo'])

        staticDf.loc[0] = staticRow
        staticDf.to_csv(os.path.join(SPLIT_DATA_PATH,sn, sn+"_static.csv"), index=False)
        
        # 生成动态信息

        dynamicDf = genDynamicInfo(df)
        dynamicDf.to_csv(os.path.join(SPLIT_DATA_PATH,sn, sn+"_error.csv"), index=False)
        q.put(True)
                
def merge():
    q = Queue()
    dimmList = os.listdir(SPLIT_DATA_PATH)
    processList = []
    cpuCount = os.cpu_count() * 2
    # cpuCount = 1
    subListSize = math.ceil(len(dimmList) / cpuCount)
    for batchIndex in range(cpuCount):
        subList = dimmList[batchIndex*subListSize:(batchIndex + 1)*subListSize]
        processList.append(Process(target=mergeFileList, args=([q, subList])))
    
    pMerge = Process(target=mergeFun, args=([q, len(dimmList)]))
    pMerge.start()
    for p in processList:
        p.start()

    for p in processList:
        p.join()
    q.put(False)
    pMerge.join()

def splitFileList(q, batchIndex,sourceFileList):
    fileCount = len(sourceFileList)
    for fileIndex in range(fileCount):
        splitFileName = "{}_{}.csv".format(batchIndex, fileIndex)
        fileName = sourceFileList[fileIndex]
        
        with openFileFn(fileName, 'rb') as fn:

            df = pd.read_csv(fn, low_memory=False, sep=';') 

            subDfList = list(df.groupby('SN'))
            for sn , subDf in subDfList:
                targetPath = os.path.join(SPLIT_DATA_PATH, sn, 'beforeMerge')

                
                targetFile = os.path.join(targetPath, splitFileName)
                subDf.to_csv(targetFile, index = False)
        q.put(True)

     
def split(DATA_SOURCE_PATH):
    q = Queue()
    sourceFileList = getSourceFileList(DATA_SOURCE_PATH)
    sourceFileListLength = len(sourceFileList)
    processList = []
    cpuCount = os.cpu_count() * 2
    cpuCount = 1
    subListSize = math.ceil(sourceFileListLength/ cpuCount)
    for batchIndex in range(cpuCount):
        subFileList = sourceFileList[batchIndex*subListSize:(batchIndex + 1)*subListSize]

        processList.append(Process(target=splitFileList, args=([q, batchIndex, subFileList])))
    
    pMerge = Process(target=mergeFun, args=([q, sourceFileListLength]))
    pMerge.start()
    for p in processList:
        p.start()

    for p in processList:
        p.join()
    q.put(False)
    pMerge.join()

    
    
    
def mergeFun(q, count):
    currentIndex = 0
    currentCount = 0
    while True:

        flag = q.get()
        if not flag:
            return
        else:
            currentCount += 1
            if int(currentCount/count * 100) == currentIndex:
                continue
            currentIndex = int(currentCount/count * 100)
            print('process {}%'.format(currentIndex))


# 遍历路径，得到所有dimm的sn号列表
def makeDir(DATA_SOURCE_PATH):
    snMap = set()
    sourceFileList = getSourceFileList(DATA_SOURCE_PATH)
    for file in sourceFileList:
        with openFileFn(file, 'rb') as fn:
            df = pd.read_csv(fn, sep=';') 
            for _,row in df.iterrows():
                sn = row['SN']
                if sn not in snMap:
                    snMap.add(sn) 
                    targetPath = os.path.join(SPLIT_DATA_PATH, sn, 'beforeMerge')
                    if not os.path.exists(targetPath):
                        os.makedirs(targetPath)




# 主函数
def main():
    
    # 命令行输入
    DATA_SOURCE_PATH = sys.argv[1]
    makeDir(DATA_SOURCE_PATH)
    print("split dimm start :{}\n".format(datetime.now()))
    split(DATA_SOURCE_PATH)
    print("split dimm finish :{}\n".format(datetime.now()))
    
    print("merge log start :{}\n".format(datetime.now()))
    merge()
    print("merge log finish :{}\n".format(datetime.now()))
     
main()