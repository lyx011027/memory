import pandas as pd 
df = pd.read_csv("/home/hw-admin/yixuan/ProjectTest/data/data1/part0.csv", sep=';')
df = df[['Log']]
df.to_csv("x.csv")