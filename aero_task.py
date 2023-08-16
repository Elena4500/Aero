#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Aug 16

@author: Elena Shumilina
"""

import json as js 
import pandas as pd 
# для анализа большого объема данных потенциально интересно использовать polars
# Polars позваляет распараллелить процесс обработки данных, задействовав все ядра 
import requests 
import psycopg2
import io
from sqlalchemy import create_engine


def get_data(url):
    # отправляем запрос в конечную точку 
    # и забираем данные в json формате
    try:
        data = requests.get(url).json() 
    except:
        print(f"Не удалось забрать данные по конечному адресу {url}")
    return data

def create_df(data):
    df = pd.DataFrame(data)
    # добавляем поле - время вставки, 
    # для обегчения дальнейшего анализа данных
    # в таблице приемнике
    df['datetime_insert'] = pd.to_datetime('now')
    return df

def bd_conn_gp(type='postgresql', host='localhost', 
               db='database', user='user'):
    # возможно добавить аутентификацию через kerberos? например          
    password="pwd"
    try:
        conn_str = f"{type}://{user}:{password}@{fullhost}:{port}/{database}"
        db_engine = create_engine(conn_str)
    except:
        print('GP engine creation failed'')
    return db_engine


def create_table_gp(db_engine, schema, table_name, 
                    if_exists='append', dtype=None):
    # создаем таблицу в GP, по умолчанию с настройками:
    # appendonly=true,
    # compresstype=zstd
    # дистрибьюция по первому атрибуту - id
    try:
        pd_sql_engine = pd.io.sql.pandasSQL_builder(db_engine, schema=schema)
        table = pd.io.sql.SQLTable(table_name, pd_sql_engine, frame=df,
                               index=False, if_exists=if_exists, 
                               schema=schema, dtype=dtype) 
        table.create()
    except:
        print('Table creation failed)
    return


def add_data(df, db_engine, schema, table_name):
    string_data_io = io.StringIO()  # создается файлоподобный объект
    df.to_csv(string_data_io, sep='|', index=False)  # запись в "файл"
    tring_data_io.seek(0)  # перемещение курсора на начало
    # вставка данных в таблицу GP
    try:
        with db_engine.connect() as connection:
            with connection.connection.cursor() as cursor:
                copy_cmd = f"COPY {schema}.{table_name} FROM STDIN HEADER DELIMITER '|' CSV" 
                cursor.copy_expert(copy_cmd, string_data_io) 
                #connection.connection.commit()
    except:
        print('Problem with data writing')
    return
    
def table_exists_check(table_name):  
    # проверка существования таблицы в БД
    query = 'select * from information_schema.tables where table_name={table_name}'
    with db_engine.connect() as connection:
        with connection.connection.cursor() as cursor:
            cursor.execute(query)  
    return bool(cur.rowcount)

def main():
    url = 'https://random-data-api.com/api/cannabis/random_cannabis?size=10'
    table_name = 'mytable'
    schema = 'myschema'
    
    data = get_data(url)  # собираем данные
    df = create_df(data)  # формируем Dataframe
    db_engine = bd_conn_gp()  # подключение к БД
    # при регулярном использовании таблицы следующую проверку 
    # существования таблицы можно убратть
    if not table_exists_check(table_name):
        create_table_gp(db_engine, schema, table_name)
    add_data(df, db_engine, schema, table_name)  # запись данных
    


if __name__ == "__main__":
    main()




 '''def crete_table(cursor, schema= 'myschema', table_name = 'mytable'):
     ...
    ''create_table_query = f''
                create table {schema}.{table_name}(
                    id int,
                    uid text,
                    strain text,
                    cannabinoid_abbreviation text,
                    cannabinoid text,
                    terpene text,
                    medical_use text,
                    health_benefit text,
                    category text,
                    type text,
                    buzzword text,
                    brand	 text,
                    datetime_insert
                    ) distributed by (id);''
    cursor.execute(create_table_query)
''
    return'''  
