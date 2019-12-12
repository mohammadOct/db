# -*- coding: utf-8 -*-
"""
databasewrite v1.0

"""

import psycopg2

import  sqlalchemy 
import pandas as pd



"""
Überblick:
    
func_CreateSchema: Anlage eines Schemas
func_WriteFromDF:  Schreiben eines pd.DataFrames in ein existierendes Schema
func_WriteFromCSV: Schreiben einer CSV Datei in ein existierendes Schema
    
"""


def func_CreateSchema(schema) -> bool:
    """
    Funktion zur Anlage eines Schemas und (optional einer Tabelle)
    
    INPUTS
    schema: String mit Name des Schemas
    
    OUTPUTS
    bool, die True ist, falls Anlage erfolgreich; False sonst    
    """
    
    
    #Verbindung zu Datenbank
    conn = psycopg2.connect(database='d_poc_markendatenplattform', 
                            user='r_poc_markendatenplattform',
                            password='c87e0311e19d9d066e15e6e3afe106e1',
                            host='postgres04.dev.cloud.ruv.de',
                            port='6432') 
    cursor = conn.cursor()
    print(conn.get_dsn_parameters(),"\n")
     
    
    #SQL Befehl zum Anlegen des Schemas (falls noch nicht existent)
    try:    
        sql_command_1 = 'CREATE SCHEMA IF NOT EXISTS {0:s}'.format(schema) 
        cursor.execute(sql_command_1)        
        conn.commit()         
        
    except Exception as e:
        print('FEHLER bei Anlage von Schema / Tabelle')
        print(e)
        return False
    
    
    print('Anlegen von Schema erfolgreich')
    
    return True


#%%
    
def func_WriteFromDF( df: pd.DataFrame, 
                      schema: str,
                      table: str, 
                      opt_ifexists: str = 'append'  ) -> bool:
    """
    Funktion zum Schreiben eines Pandas Dataframes in ein bestehendes(!) Schema.
    
    INPUTS
    df: der zu schreibende Dataframe
    schema: String mit Name des Schemas. Schema muss bereits existieren
    table: String mit Name der Tabelle. Tabelle kann, muss aber nicht existieren
    opt_ifexists: String mit einem der Werte "append", "replace" oder "fail". Default ist "append"
        Verhalten des Schreibbefehls, falls Tabelle bereits existiert
    
    OUTPUTS
    bool, die True ist, falls Speicherung erfolgreich; False sonst    
    """
    
    #Check, ob korrekter Input für opt_ifexists gegeben wurde
    if opt_ifexists not in ['append', 'replace', 'fail']: 
        print('Kein gültiger Wert für \"opt_ifexists\" übergeben.\n' + 
              'Wert wird auf Defaultwert \"fail\" gesetzt.')
        opt_ifexists = 'fail'
        
    #Verbindung zu r_poc_markendatenplattform herstellen
    try:
        engine = sqlalchemy.create_engine("postgresql://r_poc_markendatenplattform:c87e0311e19d9d066e15e6e3afe106e1@postgres04.dev.cloud.ruv.de:6432/d_poc_markendatenplattform")
        con = engine.connect() 
    except Exception as e:
        print('FEHLER bei Verbidnung zu r_poc_markendatenplattform:')
        print(e) 
        return False
    
    #Schreibbefehl
    try:
        df.to_sql(name=table, con=con , schema=schema, if_exists = opt_ifexists )
    except Exception as e:
        print('FEHLER bei Speicherung:')
        print(e) 
        return False
    
    print('Speicherung erfolgreich')
    return True


#%%

def func_WriteFromCSV(csv_path: str, 
                      schema: str,
                      table: str, 
                      opt_ifexists: str = 'append',
                      csv_sep: str = ';',
                      csv_encoding: str = 'cp1252') -> bool:
    """
    Funktion zum Schreiben einer CSV in ein bestehendes(!) Schema.
    Die Funktion liest eine CSV als Pandas-Dataframe ein und ruft dann func_WriteFromDF() auf.
    
    INPUTS
    csv_path: Pfad der CSV Datei; relativ oder absolut
    schema: String mit Name des Schemas. Schema muss bereits existieren
    table: String mit Name der Tabelle. Tabelle kann, muss aber nicht existieren
    opt_ifexists: String mit einem der Werte "append", "replace" oder "fail". Default ist "append"
        Verhalten des Schreibbefehls, falls Tabelle bereits existiert
    csv_sep: (optional) String des CSV Separators am Zeilenende. Default ist ';'
    csv_encoding: (optional) String des CSV Encoding. Default ist 'cp1252'
    
    OUTPUTS
    bool, die True ist, falls Speicherung erfolgreich; False sonst    
    """     
    
    #Einlesen des Dataframes von CSV:
    try:
        df = pd.read_csv(csv_path, sep=csv_sep, encoding=csv_encoding, error_bad_lines=False)
    except Exception as e:
        print('FEHLER bei Einlesen der CSV-Datei:')
        print(e) 
        return False
     
    #Aufrufen von func_WriteFromDF()
    bool_SaveSucces = func_WriteFromDF(df, schema, table, opt_ifexists)
     
     
    return bool_SaveSucces
     
     
   
#%%
    

#%% Skript zur Ausführung 

if __name__ == '__main__':
    
    #Namen von Schema und Tabelle
    schema = 'test_schema'
    table = 'test_tabelle'
    
    #Beispieldataframe
    df = pd.DataFrame(data = {'A':[10,20,30], 'B':['AAA','BBB','CCC']} , 
                              index =  pd.Index(['aa','bb','cc'], name='myindex' ) ) 
    
    #Anlegen des Schemas (nur nötig, falls es noch nicht existiert)
    func_CreateSchema(schema=schema)
    
    #Speichern des Dataframes    
    bool_SaveSuccess = func_WriteFromDF(df=df, schema=schema, table=table, opt_ifexists='replace')
    
    
    
    
    