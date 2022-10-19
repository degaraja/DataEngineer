import json
from traceback import print_tb
import psycopg2 as pg
import logging
from zipimport import zipfile
import pandas as pd
from sqlalchemy import create_engine

schema_json = 'C:\\Users\\dgaluh\\Project\\Python\\Json\\table.json'
zip_small_file='C:\\Users\\dgaluh\\Project\\Python\\Json\\dataset=small.zip'
small_file_name= 'datacoba.csv'
database='DataEngineer'
user='dgaluh'
password='Loader2024'
host='10.246.128.137'
port='4022'
table_name='datafromjson'

with open(schema_json, 'r') as schema:
    content = json.loads(schema.read())

list_schema = []
for c in content:
    col_name = c['column_name']
    coltype = c['column_type']
    constraint = c['is_null_able']
    ddl_list = [col_name, coltype, constraint]
    list_schema.append(ddl_list)

print(list_schema)

list_schema2 = []
for d in list_schema:
    s = ' '.join(d)
    list_schema2.append(s)

print(list_schema2)

create_schema_sql = """create table if not exist datafromjson {};"""
create_schema_sql_final = create_schema_sql.format(tuple(list_schema2)).replace("'","")

print(create_schema_sql_final)

#init connection
conn = pg.connect(database='',user='',password='',host='',port='')
conn.autocommit=True
cursor = conn.cursor()

cursor.execute(create_schema_sql_final)
logging.info("success create db")

#load zipped file
# 
zf=zipfile(zip_small_file)
df = pd.read_csv(zf.open(small_file_name), header=None)

print(df.head())

col_name_df=[c['column_name'] for c in content]
df.columns = col_name_df

df_filtered =  df[(df['created_at'] >= '2018-20-02') & (df['created_at'] >= '2018-20-02')]

engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

df_filtered.to_sql(table_name, engine, if_exist='replace', index=False)

print(f'Total inserted rows:{len(df_filtered)}')
print(f'created at :{df_filtered.created_at.min()}')
print(f'created at :{df_filtered.created_at.max()}')
