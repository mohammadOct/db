import jaydebeapi
import psycopg2
import csv

import  sqlalchemy 
import pandas as pd

def fk(gen, tu, schem, tabelle, datp):
    conn = psycopg2.connect(database='d_poc_markendatenplattform', user='r_poc_markendatenplattform',         
    password='c87e0311e19d9d066e15e6e3afe106e1', host='postgres04.dev.cloud.ruv.de', port='6432')
    cursor = conn.cursor()
    if gen == 1:
        print(conn.get_dsn_parameters(),"\n")
        sql = """CREATE SCHEMA IF NOT EXISTS {schem}; 
        CREATE TABLE IF NOT EXISTS {schem}.{tabelle}()""".format(schem=schem, tabelle=tabelle)
        cursor.execute(sql)
        conn.commit()
    # else:
    #     print('Annahme: Tabelle oder Schema existieren')
        if tu == 'ins':
            df = pd.read_csv(datp, error_bad_lines=False,sep=';', encoding="cp1252")
            engine = sqlalchemy.create_engine("postgresql://r_poc_markendatenplattform:c87e0311e19d9d066e15e6e3afe106e1@postgres04.dev.cloud.ruv.de:6432/d_poc_markendatenplattform")
            con = engine.connect()
            table_name = tabelle      
            df.to_sql(table_name, con, schema = schem)
        elif tu=='app':
            df = pd.read_csv(datp, error_bad_lines=False,sep=';', encoding="cp1252")
            engine = sqlalchemy.create_engine("postgresql://r_poc_markendatenplattform:c87e0311e19d9d066e15e6e3afe106e1@postgres04.dev.cloud.ruv.de:6432/d_poc_markendatenplattform")
            con = engine.connect()
            table_name = tabelle      
            df.to_sql(table_name, con, if_exists='append', schema = schem)
    elif gen==0:
        print(conn.get_dsn_parameters(),"\n")
        sql = """CREATE SCHEMA IF NOT EXISTS {schem}; 
        CREATE TABLE IF NOT EXISTS {schem}.{tabelle}()""".format(schem=schem, tabelle=tabelle)
        cursor.execute(sql)
        conn.commit()

        
        



fk(0, 'app', 'karintest2', 'karinTab', r'C:\Users\XV60789\Work\DB_Cloud\tempdata2.csv') 