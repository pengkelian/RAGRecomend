import json

import pandas as pd
import gzip


def generate_insert_sql(dataframe, table_name):
  """
  Generates SQL INSERT statements for a given pandas DataFrame.

  Args:
  dataframe (pd.DataFrame): The pandas DataFrame containing the data.
  table_name (str): The name of the database table where data will be inserted.

  Returns:
  list: A list of SQL INSERT statements.
  """
  sql_statements = []
  columns = ', '.join([f"`{column}`" for column in dataframe.columns])

  for index, row in dataframe.iterrows():
    values = []
    for item in row:
      if isinstance(item, str):
        escaped_item = item.replace("'", "''")  # Double single quotes for SQL
        value = f"'{escaped_item}'"
      elif pd.isna(item):
        value = 'NULL'  # Handling NULL values explicitly
      else:
        value = str(item)
      values.append(value)
    values_string = ', '.join(values)
    sql = f"INSERT INTO {table_name} ({columns}) VALUES ({values_string});"
    sql_statements.append(sql)

  return sql_statements
def parse(path):
  g = gzip.open(path, 'rb')
  for l in g:
    yield json.loads(l)

def getDF(path):
  i = 0
  df = {}
  for d in parse(path):
    df[i] = d
    i += 1
  return pd.DataFrame.from_dict(df, orient='index')

if __name__ == '__main__':
  df = getDF('data/meta_AMAZON_FASHION.json.gz')
  print(df.head(1))
  print(df.columns)
  # 完成代码，随机选择df里面的1000数据，并保存
  import random
  random.seed(1)
  #从df中选出来各列都不为空的1000条数据
  print(len(df))
  #循环查看df每个列的空值情况
  for i in df.columns:
    print(i,df[i].isnull().sum())

  df = df[df['title'].notnull()]
  df = df[df['brand'].notnull()]
  df = df[df['price'].notnull()]
  df = df[df['imageURL'].notnull()]
  #df的数量
  print(len(df))
  df = df.sample(1000)
  #df.to_csv('data/meta_AMAZON_FASHION_1000.csv')