
import os
import random
import pandas as pd
import numpy as np
import pickle
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
from sklearn.metrics import precision_recall_curve,PrecisionRecallDisplay,average_precision_score
import matplotlib.pyplot as plt
from sklearn.utils import shuffle
from config import DATA_SET_PATH, MODEL_PATH, AHEAD_TIME_List,PIC_PATH,STATIC_TRAIN_ITEM
from multiprocessing import Process
staticItem = STATIC_TRAIN_ITEM

dynamicItem = [
    "CE_number", 
    "CE_bank_number", 
    "row_number", 
    "column_number", 
    "position_number", 
    "same_row", 
    "same_column", 
    "same_position",
    "interval",
    "avg_CE",
    "CE_window",
    "max_row", 
    "max_column", 
    "max_position"
    ]

trainItem = dynamicItem + staticItem

if not os.path.exists(MODEL_PATH):
    os.makedirs(MODEL_PATH)
if not os.path.exists(PIC_PATH):
    os.makedirs(PIC_PATH)


def plot_feature_importances(feature_importances,title,feature_names, picFile):
#    将重要性值标准化
    feature_importances = 100.0*(feature_importances/max(feature_importances))
    # index_sorted = np.flipud(np.argsort(feature_importances)) #上短下长
    #index_sorted装的是从小到大，排列的下标
    index_sorted = np.argsort(feature_importances)# 上长下短
#    让X坐标轴上的标签居中显示
    bar_width = 1
    # 相当于y坐标
    pos = np.arange(len(feature_importances))+bar_width/2
    plt.figure(figsize=(16,4))
    # plt.barh(y,x)
    plt.barh(pos,feature_importances[index_sorted],align='center')
    # 在柱状图上面显示具体数值,ha参数控制参数水平对齐方式,va控制垂直对齐方式
    for y, x in enumerate(feature_importances[index_sorted]):
        plt.text(x+2, y, '%.4s' %x, ha='center', va='bottom')
    plt.yticks(pos,feature_names[index_sorted])
    plt.title(title)
    plt.savefig(picFile,dpi=1000)
    
def trainAndTest(time, trainItem):
    # time = AHEAD_TIME_List[0]
    dataSetFile = os.path.join(DATA_SET_PATH,"{}_window.csv".format(time))
    df = pd.read_csv(os.path.join(dataSetFile))
    # print("提前预测时间 = {}".format(time))
    # 提取正样本，并切分为train,test
    trueDf = df[df['label'] == True]
    true_sn = trueDf['SN'].drop_duplicates().tolist()
    true_sn_train = random.sample(true_sn, int(len(true_sn)*0.6))

    true_df_train = trueDf[trueDf['SN'].isin(true_sn_train)]
    true_Y_train = true_df_train['label']
    true_X_train = true_df_train.fillna(-1)

    true_df_test = trueDf[~trueDf['SN'].isin(true_sn_train)]
    true_Y_test = true_df_test['label']
    true_X_test = true_df_test.fillna(-1) 

    # 提取负样本，并切分为train,test
    falseDf = df[df['label'] == False]
    false_sn = falseDf['SN'].drop_duplicates().tolist()
    false_sn_train = random.sample(false_sn, int(len(false_sn)*0.6))

    false_df_train = falseDf[falseDf['SN'].isin(false_sn_train)]
    false_Y_train = false_df_train['label']
    false_X_train = false_df_train.fillna(-1) 

    false_df_test = falseDf[~falseDf['SN'].isin(false_sn_train)]
    false_Y_test = false_df_test['label']
    false_X_test = false_df_test.fillna(-1) 



    # 合并正负样本，生成测试集与数据集
    X_train = pd.concat([true_X_train,false_X_train])
    X_test = pd.concat([true_X_test,false_X_test])
    Y_train = np.concatenate((true_Y_train.tolist(), false_Y_train.tolist()))
    Y_test = np.concatenate((true_Y_test.tolist(), false_Y_test.tolist()))
    # 获得测试集dimm sn号列表
    dimm_sn_list =  X_test['SN']
    # 提取特征
    X_train = X_train[trainItem]
    X_test = X_test[trainItem]
    # 训练
    rfc = RandomForestClassifier()
    rfc.fit(X_train, Y_train)
    # 保存特征重要性图
    picFile = os.path.join(PIC_PATH, "{}-importance.png".format(time))
    # for i in range (len(trainItem)):
    #     print(trainItem[i], rfc.feature_importances_[i])
    trainItem = np.array(trainItem)
    plot_feature_importances(rfc.feature_importances_, "feature importances", trainItem,picFile)

    # 输出测试集结果
    threshold = 0.5
    predicted_proba = rfc.predict_proba(X_test)
    Y_pred = (predicted_proba [:,1] >= threshold).astype('int')
    # Y_pred = rfc.predict(X_test) 

    # 绘制 precision-recall 曲线
    prec, recall, _ = precision_recall_curve(Y_test, predicted_proba [:,1], pos_label=1)
    pr_display = PrecisionRecallDisplay(estimator_name = 'rf',precision=prec, recall=recall, average_precision=average_precision_score(Y_test, predicted_proba [:,1], pos_label=1))
    pr_display.average_precision
    pr_display.plot()
    plt.savefig(os.path.join(PIC_PATH,'{}-p-r.png'.format(time)))
    plt.cla()
    with open(os.path.join(MODEL_PATH,'{}.pkl'.format(time)), 'wb') as fw:
        pickle.dump(rfc, fw)

pList = []
print("train and test")
for time in AHEAD_TIME_List :
    pList.append(Process(target=trainAndTest, args=(time,trainItem)))
[p.start() for p in pList]
[p.join() for p in pList]
