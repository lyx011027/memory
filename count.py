import pandas as pd
import json
from datetime import datetime, timedelta
import csv
from config import DATA_SET_PATH,DATA_SOURCE_FILE_PATH,AHEAD_TIME_List,logs_transaction
import os
# import moxing as mox


aheadTime = timedelta(milliseconds=0)

currentTime = datetime.now()
if not os.path.exists(DATA_SET_PATH):
    os.makedirs(DATA_SET_PATH)




        
        


# 生成数据集，data为dataFrame格式
# 可以将sparkSQL查询到的结果传入该函数



def getSnMap():
    snMap = {}
    for root ,_, files in os.walk(DATA_SOURCE_FILE_PATH):
    # for root ,_, files in mox.file.walk(DATA_SOURCE_FILE_PATH):
        for f in files:
            df = pd.read_csv(os.path.join(root,f),sep=';')
            # with mox.file.File(os.path.join(root,f), 'rb') as fn:
            #     df = pd.read_csv(fn, sep=';')  
            for  _, row in df.iterrows():
                sn = row['SN']
                if sn not in snMap:
                    snMap[sn] = [currentTime,currentTime,False]
                logs = row['Log'].replace('"','').replace('\\','"')
                logs = logs_transaction(logs)
                for log in logs:
                    errorType = log["Type"]
                    errorTime = log['TimeStamp']
                    if errorType == 0:
                        if errorTime < snMap[sn][0]:
                            snMap[sn][0] = errorTime
                    else:
                        snMap[sn][2] = True
                        if errorTime < snMap[sn][1]:
                            snMap[sn][1] = errorTime
                    
    return snMap
def main():
    snMap = getSnMap()
    uceDimmCount = 0
    aheadListLength = len(AHEAD_TIME_List)
    predictableList = [0]*aheadListLength
    for dimm in snMap.values():
        if dimm[2]:
            uceDimmCount += 1
            for i in range(aheadListLength):
                if dimm[1] - dimm[0] > AHEAD_TIME_List[i]:
                    predictableList[i] += 1
    print("发生UCE的DIMM数量{}".format(uceDimmCount))
    for i in range (aheadListLength):
        print("提前预测时间为{}时,可进行预测的UCE DIMM数量:{}".format(AHEAD_TIME_List[i], predictableList[i]))
          
main()
