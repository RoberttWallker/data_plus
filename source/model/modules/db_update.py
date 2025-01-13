from sqlalchemy import create_engine, inspect, Table, Column, Text
from sqlalchemy.sql import text
from datetime import datetime
from pathlib import Path
import time
import json

from .db_connector import (
    mysql_connection,
    postgresql_connection,
    load_config_file
)

MODEL_PATH = Path(__file__).absolute().parent.parent
CONFIG_PATH = MODEL_PATH / "config"
TEMP_FILE_PATH = MODEL_PATH / "data/temp_file_data"


def save_incremental_column_config(incremental_config, filename):

    filename.parent.mkdir(parents=True, exist_ok=True)

    try:
        with open(filename, "r") as file:
            update_configs = json.load(file)

    except (FileNotFoundError, json.JSONDecodeError):
        update_configs = []

    for item in update_configs:
        print(item.keys())
    update_configs.append(incremental_config)

    with open(filename, "w") as file:
        json.dump(update_configs, file, indent=4)

def alter_column_to_datetime_mysql(connection_, table_, column_):
    # Formato atual das strings, reconhecido no MYSQL
    # tem de ser usado esses padrões para ser reconhecido no MYSQL
    root_format = "%d/%m/%Y %H:%i:%s"
    mysql_format = "%Y-%m-%d %H:%i:%s"
    try:

        update_query = text(f"""
            UPDATE {table_}
            SET {column_} = DATE_FORMAT(STR_TO_DATE({column_}, :root_format), :mysql_format)
            WHERE {column_} IS NOT NULL;
        """)

        alter_query = text(f"""
            ALTER TABLE {table_}
            MODIFY {column_} DATETIME;
        """)

        connection_.execute(update_query, {'root_format': root_format, 'mysql_format': mysql_format})
        connection_.execute(alter_query)
        connection_.commit()

        print(f"Coluna '{column_}' da tabela '{table_}' atualizada com sucesso!")
    except Exception as e:
        print(f"Erro ao atualizar coluna '{column_}' na tabela '{table_}': {e}")

def alter_column_to_datetime_postgresql(connection_, table_, column_):
    # Formato atual das strings, reconhecido no MYSQL
    # tem de ser usado esses padrões para ser reconhecido no MYSQL
    root_format = "%d/%m/%Y %H:%M:%S"
    mysql_format = "%Y-%m-%d %H:%i:%s"
    try:

        update_query = text(f'''
            UPDATE "{table_}"
            SET "{column_}" = TO_TIMESTAMP("{column_}", :root_format)
            WHERE "{column_}" IS NOT NULL;
        ''')

        alter_query = text(f'''
            ALTER TABLE "{table_}"
            ALTER COLUMN "{column_}" TYPE TIMESTAMP USING "{column_}"::TIMESTAMP;
        ''')

        connection_.execute(update_query, {'root_format': root_format, 'mysql_format': mysql_format})
        connection_.execute(alter_query)
        connection_.commit()

        print(f"Coluna '{column_}' da tabela '{table_}' atualizada com sucesso!")
    except Exception as e:
        print(f"Erro ao atualizar coluna '{column_}' na tabela '{table_}': {e}")

def fill_with_null(conn, table_, column_):
    connection = conn.connection
    dialect = conn.dialect
    # Alterar o tipo da coluna para DATETIME
    try:
        # Preencher valores nulos ou vazios com uma data padrão
        if dialect == "mysql":
            preencher_query = text(f"""
                UPDATE {table_}
                SET {column_} = NULL
                WHERE TRIM({column_}) = '';
            """)
        elif dialect == "postgresql":
            preencher_query = text(f'''
                UPDATE "{table_}"
                SET "{column_}" = NULL
                WHERE TRIM("{column_}") = '';
            ''')

        connection.execute(preencher_query)
        print(f"Valores nulos atribuidos aos vazios na coluna {column_} na tabela {table_}")
        connection.commit()

    except Exception as e:
        print(f"Erro ao alterar coluna {column_} na tabela {table_}: {e}")

def alter_date_format(conn, table_, column_):
    connection = conn.connection
    db_name = conn.db_name
    if conn.dialect == "mysql":
        alter_column_to_datetime_mysql(connection, table_, column_)

        incremental_config = {
            db_name: [{
            "table": table_,
            "column": column_
            }]
        }

        save_incremental_column_config(incremental_config, CONFIG_PATH / "incremental_config/incremental_config_mysql.json")

    elif conn.dialect == "postgresql":
        alter_column_to_datetime_postgresql(connection, table_, column_)

        incremental_config = {
            db_name: {
            "table": table_,
            "column": column_
            }
        }

        save_incremental_column_config(incremental_config, CONFIG_PATH / "incremental_config/incremental_config_postgresql.json")

def create_column_incremental_update_pbi():
    table_column_mappings = []
    while True:
        table_ = input(f'{"-"*56}\nCopie e cole a tabela (ou digite "sair" para encerrar): ')
        time.sleep(1)
        if table_.strip().lower() == "sair":
            break
        column_ = input('Agora insira a coluna que será usada para atualização incremental no Power BI: ')
        time.sleep(1)
        if not table_ or not column_:
            print("Tabela ou coluna não podem estar vazios. Tente novamente.")
            time.sleep(1)
            continue
        tbl_col = {table_: column_}
        table_column_mappings.append(tbl_col)
        print(f"\nPerfil adicionado: {tbl_col}\n{'-'*(len(str(tbl_col))+19)}")
    return table_column_mappings

def get_tables_columns_date(conn):
    connection = conn.connection
    inspector = inspect(conn.engine)
    tables = inspector.get_table_names()
    dialect = conn.dialect

    tables_columns = []
    
    for table in tables:
        columns = inspector.get_columns(table)
        list_columns = []
        for column in columns:
            list_columns.append(column['name'])
        tables_columns.append((table, list_columns))

    tables_columns_date = []
    for table, columns in tables_columns:
        columns_date = []
        for column in columns:
            try:
                if dialect == "mysql":
                    query = text(f"SELECT DISTINCT {column} FROM {table} LIMIT 2")
                elif dialect == "postgresql":
                    query = text(f'SELECT DISTINCT "{column}" FROM "{table}" LIMIT 2')

                result = connection.execute(query)
                
                formato_exemplo = "%d/%m/%Y %H:%M:%S"

                rows = [row[0] for row in result]

                for value in rows:
                    if value and isinstance(value, datetime):
                        continue
                        #print(f"FORMATO DATETIME: {table} - {column} - {value}")
                    elif value:
                        try:
                            formated = datetime.strptime(value, formato_exemplo).strftime(formato_exemplo)
                            #print(f"FORMATO STR: {table} - {column} - {formated}")
                            if column not in columns_date:
                                columns_date.append(column)
                        except ValueError:
                            continue
                    else:
                        continue

            except Exception as e:
                print(f"capturado no except exterior : {e}")     
        
        tables_columns_date.append((table, columns_date))
    #print(f"\n{'*'*32}\n{tables_columns_date}")
    return tables_columns_date      

def update_flow(file):
    db_configs = load_config_file(file)
    
    for config in db_configs:
        print("\nIniciando...")
        time.sleep(1)
        print(f"\nEssas são as configuraçãoes do banco de dados:\n{config}\n")
        time.sleep(1)
        if file.name == "db_config_mysql.json":
            conn = mysql_connection(
                config["host"],
                config["port"],
                config["user"],
                config["password"],
                config["dbname"],
            )
        elif file.name == "db_config_postgresql.json":
            conn = postgresql_connection(
                config["host"],
                config["port"],
                config["user"],
                config["password"],
                config["dbname"],
            )

        tables_columns_date = get_tables_columns_date(conn)
        #print(f"TESTE AQUI DIALETC: {conn.dialect}")
        for table, columns in tables_columns_date:
            print(f"Tabela: {table}")
            if columns:
                print(f"    Colunas:")
                for column in columns:
                    print(f"    - {column}")
            elif not columns:
                print(f"    Sem colunas no formato correto.")
        
        tables_columns_to_update = create_column_incremental_update_pbi()

        while True:
            response = input(f'''\n
Confirme se essas são as tabelas e respectivas colunas que quer atualizar:
    \n{tables_columns_to_update}\n
(s/n)>>> ''').strip().lower()
            if response == 's':
                print('Continuando...\n')
                time.sleep(1)
                break
            elif response == 'n':
                print('Repetindo...\n')
                time.sleep(1)
                tables_columns_to_update = create_column_incremental_update_pbi()
            else:
                print('Opção inválida. Tente novamente...')

        for profile in tables_columns_to_update:
            # Usar next(iter(...)) é uma maneira eficiente e direta
            # de acessar o primeiro item de um iterável.
            table, column = next(iter(profile.items()))

            fill_with_null(conn, table, column)

            alter_date_format(conn, table, column)

def manager_update_date():

    count = len(list(CONFIG_PATH.rglob("db_config/*.json")))
    if count > 1:
        file_list = list(CONFIG_PATH.rglob("db_config/*.json"))
        print(f"Existe mais de um SGBD configurado, qual deles você deseja usar?\n")
        for idx, file in enumerate(file_list):
            print(f"{idx} - {file.name}")
        
        while True:
            choice = int(input(f"Qual dos SGBDs você deseja configurar?\n>>> ").strip())
            if choice in range(len(file_list)):
                file = file_list[choice]
                break
            else:
                print("Índice inválido. Tente novamente.")
        update_flow(file)
    else:
        for file in CONFIG_PATH.rglob("db_config/*.json"):
            update_flow(file)