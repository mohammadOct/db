# -*- coding: utf-8 -*-
"""
databasecommans v1.6

"""

import psycopg2

import sqlalchemy
import pandas as pd



"""
Überblick:
    
func_CreateSchema: Anlage eines Schemas
func_WriteFromDF:  Schreiben eines pd.DataFrames in ein existierendes Schema
func_WriteFromCSV: Schreiben einer CSV Datei in ein existierendes Schema
"""


# IC-Test:

database_default = 'fh_data_mart_t01'  # _t01 für "Test"
host_default = 'lxm00334.ruv.de'
port_default = '6432'
print('IC postgres Test')
'''
# IC-Prod:
database_default = 'fh_data_mart_p01'  # _p01 für "Prod"
host_default = 'lxm00332.ruv.de'
port_default = '6432'
print('IC postgres Prod')
'''

def func_CreateSchema(user: str,
                      password: str,
                      schema: str,
                      database: str = database_default,
                      host: str = host_default,
                      port: str = port_default
                      ) -> bool:
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
                     database: str = database_default,
                     host: str = host_default,
                     port: str = port_default
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
                  if_exists=opt_ifexists, index=opt_writeindex,
                  method='multi')
    except Exception as e:
        print('FEHLER bei Speicherung:')
        print(e)
        return False

    print('Speicherung erfolgreich.')
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
                      database: str = database_default,
                      host: str = host_default,
                      port: str = port_default
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

    # Einlesen des Dataframes von CSV:
    try:
        df = pd.read_csv(csv_path, sep=csv_sep, decimal=csv_dec,
                         encoding=csv_encoding, error_bad_lines=False)
    except Exception as e:
        print('FEHLER bei Einlesen der CSV-Datei:')
        print(e)
        return False

    # Aufrufen von func_WriteFromDF()
    bool_SaveSucces = func_WriteFromDF(user, password, schema, table, opt_ifexists, opt_writeindex,
                                       database=database, host=host, port=port)

    return bool_SaveSucces


# ###############################################################


def func_ReadTableFromDB(user: str,
                         password: str,
                         schema: str,
                         table: str,
                         columns: list = '*',
                         where_stmt: str = '',
                         database: str = database_default,
                         host: str = host_default,
                         port: str = port_default
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
    columns: Liste mit Spaltennamen die gezogen werden sollen; alternativ auch einzelner Name oder
        '*' für das Ziehen aller Spalten
    where_stmt: optionaler String, der eine WHERE-clause an das Abfrage SQL Statement hängt. Dabei
        ist nur der String hinter "WHERE " anzugeben, also z.B. 'wertspalte=10' oder
        'namensspalte=\'Egon\''. Bei leerem String '' (Defaultwert) wird nichts angehängt.
        Beachte: Spaltennamen mit Großbuchstaben müssten in double quotes ("") innerhalb des Strings
        gefasst werden; String-Argumente müssen in single quotes ('') bzw. (\'\') gefasst werden.

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

    # Anhängen des optionalen WHERE Statements:
    if len(where_stmt) > 0:
        sql_statement += '\nWHERE ' + where_stmt

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
                                database: str = database_default,
                                host: str = host_default,
                                port: str = port_default
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
def func_ListAllTablesOfSchema(
        user: str,
        password: str,
        schema: str,
        database: str = database_default,
        host: str = host_default,
        port: str = port_default
) -> list:
    """
    Funktion zum Schreiben eines Pandas Dataframes in ein bestehendes(!) Schema.

    INPUTS
    df: der zu schreibende Dataframe
    schema: String mit Name des Schemas. Schema muss bereits existieren

    OUTPUTS
    liste mit Tabellennamen
    """

    # Verbindung zu r_poc_markendatenplattform herstellen
    engine = func_GetEngine(user=user, password=password,
                            database=database, host=host, port=port)

    if engine is False:
        return []

    # Lesebefehl
    try:
        inspector = sqlalchemy.inspect(engine)
        list_of_tables = inspector.get_table_names(schema=schema)

    except Exception as e:
        print('FEHLER bei Inspector Verbidnung:')
        print(e)
        return []

    return list_of_tables


# ###############################################################
def func_GetEngine(
        user: str,
        password: str,
        database: str = database_default,
        host: str = host_default,
        port: str = port_default
) -> list:
    """
    Funktion Verbinden einer engine für weitere Verarbeitung mit SQLalchemy.

    OUTPUTS
    sql engine
    """

    # Verbindung zu r_poc_markendatenplattform herstellen
    try:
        engine_str = 'postgresql://' + user + ':' + password + '@' \
                     + host + ':' + port + '/' + database

        engine = sqlalchemy.create_engine(engine_str)
    except Exception as e:
        print('FEHLER bei Verbidnung zu r_poc_markendatenplattform:')
        print(e)
        return False

    return engine


# ###############################################################
# ###############################################################


# Skript zur Ausführung