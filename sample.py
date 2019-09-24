#we will exceute this file as task in azure batch nodes
import pandas as pd
from sys import argv

def print_df_column(df,index):
    print(df[index])

if __name__=="__main__":
    index = argv[0]
    data = {'index1': ['Tom', 'nick', 'krish', 'jack'], 'index2': [20, 21, 19, 18]}
    df = pd.DataFrame(data)
    print_df_column(df,index)