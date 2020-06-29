# -*- coding: utf-8 -*-
"""
databasecommans v1.3

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

# Datenbankparameter
database = 'd_poc_markendatenplattform'
host = 'flt027673.ruv.de'
port = '6432'


def func_CreateSchema(user, password, schema) -> bool:
    """
    Funktion zur Anlage eines Schemas und (optional einer Tabelle)
    
    INPUTS
    schema: String mit Name des Schemas
    
    OUTPUTS
    bool, die True ist, falls Anlage erfolgreich; False sonst    
    """
    
    # Verbindung zu Datenbank
    conn = psycopg2.connect(database=database,
                            user=user,
                            password=password,
                            host=host,
                            port=port)
    cursor = conn.cursor()
    # print(conn.get_dsn_parameters(),"\n")
    
    # SQL Befehl zum Anlegen des Schemas (falls noch nicht existent)
    try:    
        sql_command_1 = 'CREATE SCHEMA IF NOT EXISTS {0:s}'.format(schema) 
        cursor.execute(sql_command_1)        
        conn.commit()         
        
    except Exception as e:
        print('FEHLER bei Anlage von Schema / Tabelle')
        print(e)
        return False
    
    
    print('Anlegen von Schema erfolgreich.\n')
    
    return True


# ###############################################################
    
def func_WriteFromDF(user: str,
                     password: str,
                     df: pd.DataFrame,
                     schema: str,
                     table: str,
                     opt_ifexists: str = 'append',
                     opt_writeindex: bool = False,
                     ) -> bool:
    """
    Funktion zum Schreiben eines Pandas Dataframes in ein bestehendes(!) Schema.
    
    INPUTS
    df: der zu schreibende Dataframe
    schema: String mit Name des Schemas. Schema muss bereits existieren
    table: String mit Name der Tabelle. Tabelle kann, muss aber nicht existieren
    opt_ifexists: String mit einem der Werte "append", "replace" oder "fail". Default ist "append"
        Verhalten des Schreibbefehls, falls Tabelle bereits existiert
    opt_writeindex: Bool, die angibt, ob der Index des Dataframes als eigene Spalte in der Datenbank
        gespeichert werden soll (True) oder nicht gespeichert wird (False); False könnte gerade
        geeignet sein, wenn man append-Option wählt und fortlaufend eine Tabelle ergänzen möchte.
    
    OUTPUTS
    bool, die True ist, falls Speicherung erfolgreich; False sonst    
    """
    
    # Check, ob korrekter Input für opt_ifexists gegeben wurde
    if opt_ifexists not in ['append', 'replace', 'fail']: 
        print('Kein gültiger Wert für \"opt_ifexists\" übergeben.\n' + 
              'Wert wird auf Defaultwert \"fail\" gesetzt.')
        opt_ifexists = 'fail'
        
    # Verbindung zu r_poc_markendatenplattform herstellen
    try:
        engine_str = 'postgresql://' + user + ':' + password + '@' \
                     + host + ':' + port + '/' + database
    
        engine = sqlalchemy.create_engine(engine_str)
        con = engine.connect() 
    except Exception as e:
        print('FEHLER bei Verbidnung zu r_poc_markendatenplattform:')
        print(e) 
        return False
    
    # Schreibbefehl
    try:
        df.to_sql(name=table, con=con, schema=schema,
                  if_exists=opt_ifexists, index=opt_writeindex)
    except Exception as e:
        print('FEHLER bei Speicherung:')
        print(e) 
        return False
    
    print('Speicherung erfolgreich.\n')
    return True

# ###############################################################


def func_WriteFromCSV(user: str,
                      password: str,
                      csv_path: str,
                      schema: str,
                      table: str, 
                      opt_ifexists: str = 'append',
                      opt_writeindex: bool = False,
                      csv_sep: str = ';',
                      csv_dec: str = ',',
                      csv_encoding: str = 'cp1252',
                      ) -> bool:
    """
    Funktion zum Schreiben einer CSV in ein bestehendes(!) Schema.
    Die Funktion liest eine CSV als Pandas-Dataframe ein und ruft dann func_WriteFromDF() auf.
    
    INPUTS
    csv_path: Pfad der CSV Datei; relativ oder absolut
    schema: String mit Name des Schemas. Schema muss bereits existieren
    table: String mit Name der Tabelle. Tabelle kann, muss aber nicht existieren
    opt_ifexists: String mit einem der Werte "append", "replace" oder "fail". Default ist "append"
        Verhalten des Schreibbefehls, falls Tabelle bereits existiert
    opt_writeindex: Bool, die angibt, ob der Index des Dataframes als eigene Spalte in der Datenbank
        gespeichert werden soll (True) oder nicht gespeichert wird (False); False könnte gerade
        geeignet sein, wenn man append-Option wählt und fortlaufend eine Tabelle ergänzen möchte.
    csv_sep: (optional) String des CSV Separators am Zeilenende. Default ist ';'
    csv_dec: (optional) String des CSV Dezimalzeichens. Default ist ','
    csv_encoding: (optional) String des CSV Encoding. Default ist 'cp1252'
    
    OUTPUTS
    bool, die True ist, falls Speicherung erfolgreich; False sonst    
    """     
    
    #Einlesen des Dataframes von CSV:
    try:
        df = pd.read_csv(csv_path, sep=csv_sep, decimal = csv_dec ,
                         encoding=csv_encoding, error_bad_lines=False)
    except Exception as e:
        print('FEHLER bei Einlesen der CSV-Datei:')
        print(e) 
        return False
     
    #Aufrufen von func_WriteFromDF()
    bool_SaveSucces = func_WriteFromDF(user, password, schema, table, opt_ifexists, opt_writeindex)
     
     
    return bool_SaveSucces
    
# ###############################################################


def func_ReadTableFromDB(user: str,
                         password: str,
                         schema: str,
                         table: str,
                         columns: list = '*',
                         ) -> pd.DataFrame:
    """
    Funktion zum Einlesen einer bestehenden Tabelle als pandas DataFrame

    INPUTS
    columns: list mit Strings der Spaltennamen, die eingelesen werden sollen.
        Möglich ist auch ein String mit einzelnem Spaltennamen
        oder '*' zum einlesen aller Spalten
        oder alle Spaltennamen als String wie in der Form '"Spalte1", "Spalte2", "Spalte3"'
        BEACHTE: SQL Befehle aus Python werden automatisch downcastst; deshalb müssen zusätzlich
        die Spaltennamen mit doppelten Anführungszeichen umgeben werden, um Groß/Kleinschreibung
        beizubehalten.
    schema: String mit Name des Schemas.
    table: String mit Name der Tabelle.

    OUTPUTS
    df, eingelesener pd.DataFrame; leerer DataFrame, falls Fehler auftritt
    """
    
    #Verbindung zu Datenbank
    conn = psycopg2.connect(database=database,
                            user=user,
                            password=password,
                            host=host,
                            port=port)
    cursor = conn.cursor()
    # print(conn.get_dsn_parameters(), "\n")
    
    
    # Vorbereiten des SQL Statements """SELECT columns FROM schema.table"""
    
    # String der Columns im Stil: '"Spalte1", "Spalte2", ..., "SpalteN"'
    # Fall 1: Liste wurde gegeben
    if isinstance(columns, list):
        
        # Spaltennamen in doppelten Anführungsstrichen
        col_string = '"' + str(columns[0]) + '"'
        
        for i in range(1, len(columns)):
            col_string += ', "' + str(columns[i]) + '"'
    # Fall 2: String wurde direkt gegeben
    elif isinstance(columns, str):
        col_string = columns
    else:
        print('FEHLER: Es wurde ein ungültiger Input für abzufragende Spalten übergeben')
        col_string = ''
    
    # SQL Statement
    sql_statement = ('SELECT ' + col_string + '\n'
                     'FROM ' + schema + '.' + table)
    
    # Ausgabe des SQL Statements für visuelle Überprüfung
    print('SQL-Abfrage:\n' + sql_statement + '\n')
    
    # Einlesen der Tabelle als DataFrame:
    try:
        df = pd.read_sql(sql_statement, conn)
        print('Einlesen erfolgreich!\n')
        
    except Exception as e:
        print('FEHLER bei Datenbankabfrage:')
        print(e)
        
        print('Leerer DataFrame wird zurückgegeben.\n')
        df = pd.DataFrame()
    
    return df

# ###############################################################


def func_ReadSQLStatementFromDB(user: str,
                                password: str,
                                sql_statement: str,
                                ) -> pd.DataFrame:
    """
    Funktion zum Einlesen eines pd.DataFrames durch komplettes SQL Statement

    INPUTS
    sql_statement: string; das SQL Statemetn für die Abfrage ("SELECT (...) FROM (...) ..."
        BEACHTE: SQL Befehle aus Python werden automatisch downcastst; deshalb müssen zusätzlich
        die Spaltennamen mit doppelten Anführungszeichen umgeben werden, um Groß/Kleinschreibung
        beizubehalten.

    OUTPUTS
    df, eingelesener pd.DataFrame; leerer DataFrame, falls Fehler auftritt
    """
    
    # Verbindung zu Datenbank
    conn = psycopg2.connect(database=database,
                            user=user,
                            password=password,
                            host=host,
                            port=port)
    cursor = conn.cursor()
    # print(conn.get_dsn_parameters(), "\n")
    
    
    # Ausgabe des SQL Statements für visuelle Überprüfung
    print('SQL-Abfrage:\n' + sql_statement + '\n')
    
    # Einlesen des SQL Statements:
    try:
        df = pd.read_sql(sql_statement, conn)
        print('Einlesen erfolgreich!\n')
    
    except Exception as e:
        print('FEHLER bei Datenbankabfrage:')
        print(e)
        
        print('Leerer DataFrame wird zurückgegeben.\n')
        df = pd.DataFrame()
    
    return df

# ###############################################################
# ###############################################################


# Skript zur Ausführung
if __name__ == '__main__':
    
    # user & passwort
    user = 'r_poc_markendatenplattform'
    password = '***'

    print(f'Es wird mit folgenden Zugangsdaten gearbeitet:\n'
          f'User: {user}\n'
          f'Pwrd: {password}\n'
          )

    # Namen von Schema und Tabelle
    schema = 'test_schema'
    table = 'test_tabelle'

    # Beispieldataframe
    df = pd.DataFrame(data={'A': [10, 20, 30], 'B': ['AAA', 'BBB', 'CCC']},
                      index=pd.Index(['aa', 'bb', 'cc'], name='myindex'))

    # Anlegen des Schemas (nur nötig, falls es noch nicht existiert)
    # func_CreateSchema(schema=schema)

    # Speichern des Dataframes
    # Im Beispiel ausgewählt, dass ...
    # - möglicherweise bestehender Dataframe ersetzt wird,
    # - und der Index des Dataframes als eigene Spalte gesichert wird.
    bool_SaveSuccess = func_WriteFromDF(user=user,
                                        password=password,
                                        df=df,
                                        schema=schema,
                                        table=table,
                                        opt_ifexists='replace',
                                        opt_writeindex=True,
                                        )
    
    
    # Einlesen von Spalten der gespeicherten Tabelle
    df_read1 = func_ReadTableFromDB(user=user,
                                    password=password,
                                    schema=schema,
                                    table=table,
                                    columns=['A', 'B'],
                                    )

    # Alternatives Einlesen der Tabelle über explizites SQL Statement
    sql_statement = 'SELECT *\n' \
                    'FROM ' + schema + '.' + table

    df_read2 = func_ReadSQLStatementFromDB(user=user,
                                           password=password,
                                           sql_statement=sql_statement,
                                           )

