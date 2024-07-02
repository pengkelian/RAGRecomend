import json
#读取csv文件
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from DataTools import generate_insert_sql
#调用MySQLDatabase.py插入data数据
from MySQLDatabase import *




if __name__ == '__main__':
    df = pd.read_csv('data/meta_AMAZON_FASHION_1000.csv')
    print(df)
    sql_statements = generate_insert_sql(df, 'products')
    db = MySQLDatabase('43.138.111.201', 'root', 'PKL.19881001', 'llm_product')

    for sql in sql_statements:
        print(sql)
        db.insert_data(sql)