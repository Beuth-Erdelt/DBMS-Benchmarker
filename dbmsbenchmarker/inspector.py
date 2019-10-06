import pickle
import pandas as pd

from dbmsbenchmarker import benchmarker


def getIntersection(df1, df2):
    return pd.merge(df1, df2, how='inner')
def getUnion(df1, df2):
    return pd.concat([df1, df2])
def getDifference12(df1, df2):
    return pd.concat([df1, df2, df2]).drop_duplicates(keep=False)
def completeSort(df):
    return df.sort_values(by=[df_union_all.columns[i] for i in range(0,len(df.columns))], ascending=True)

