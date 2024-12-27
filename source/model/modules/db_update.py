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

def preencher_nulos(connection, table_, column_):
    # Alterar o tipo da coluna para DATETIME
    try:
        # Preencher valores nulos ou vazios com uma data padrão
        preencher_query = text(f"""
            UPDATE {table_}
            SET {column_} = '1970-01-01 00:00:00'
            WHERE {column_} IS NULL OR TRIM({column_}) = '';
        """)
        connection.execute(preencher_query)
        print(f"Valores nulos ou vazios da coluna {column_} na tabela {table_} preenchidos com data padrão.")

        # Alterar o tipo da coluna para DATETIME
        alter_query = text(f"ALTER TABLE {table_} MODIFY COLUMN {column_} DATETIME")
        connection.execute(alter_query)
        print(f"Coluna {column_} na tabela {table_} alterada para DATETIME.")
    except Exception as e:
        print(f"Erro ao alterar coluna {column_} na tabela {table_}: {e}")

def update_columns_date_mysql(conn):
    inspector = inspect(conn.engine)

    table_columns = {}

    for table_name in inspector.get_table_names():

        columns = inspector.get_columns(table_name)
        columns_list = []
        for column in columns:
            column_name = column['name']
            columns_list.append(column_name)
        table_columns[table_name] = columns_list
    
    
    connection = conn.connection

    valid_columns_date = []

    for table_, columns_ in table_columns.items():
        for column_ in columns_:
            query = text(f"SELECT DISTINCT {column_} FROM {table_} LIMIT 3")
            result = connection.execute(query)
            
            formato_exemplo = "%d/%m/%Y %H:%M:%S"
            formato_mysql = "%Y-%m-%d %H:%M:%S"

            rows = [row[0] for row in result]

            # Filtrando valores vazios ou nulos
            valid_rows = [value for value in rows if value and (not isinstance(value, datetime))]


            try:
                data = [datetime.strptime(value, formato_exemplo).strftime(formato_exemplo) for value in valid_rows]
                if data:
                    valid_columns_date.append((table_, column_))
                else:
                    continue
            except ValueError:
                try:
                    data = [datetime.strptime(value, formato_mysql).strftime(formato_mysql) for value in valid_rows]
                    if data:
                        valid_columns_date.append((table_, column_))
                    else:
                        continue
                except:
                    continue
   
    print(valid_columns_date)

    for table_, column_ in valid_columns_date:
        query = text(f"SELECT {column_} FROM {table_}")
        result = connection.execute(query)

        rows = [row[0] for row in result]

        # Filtrando valores vazios ou nulos
        valid_rows = [value for value in rows if value]# and value != '']

        for row in valid_rows:
            try:
                data_formatada = datetime.strptime(row, formato_exemplo).strftime(formato_mysql)
                update_query = text(f"UPDATE {table_} SET {column_} = :data_formatada WHERE {column_} = :old_value")
                connection.execute(update_query, {'data_formatada': data_formatada, 'old_value': row})
            except ValueError:
                try:
                    data_formatada = datetime.strptime(row, formato_mysql).strftime(formato_mysql)
                    update_query = text(f"UPDATE {table_} SET {column_} = :data_formatada WHERE {column_} = :old_value")
                    connection.execute(update_query, {'data_formatada': data_formatada, 'old_value': row})
                except:
                    continue
        # Alterar o tipo da coluna para DATETIME
        try:
            alter_query = text(f"ALTER TABLE {table_} MODIFY COLUMN {column_} DATETIME")
            connection.execute(alter_query)
            print(f"Coluna {column_} na tabela {table_} alterada para DATETIME.")
        except Exception as e:
            print(f"Erro ao alterar coluna {column_} na tabela {table_}: {e}")
        
        preencher_nulos(connection, table_, column_)

        

                    
def get_tables(conn):
    connection = conn.connection
    inspector = inspect(conn.engine)
    tabelas = inspector.get_table_names()

    tabela_colunas = []
    
    for tabela in tabelas:
        colunas = inspector.get_columns(tabela)
        list_colunas = []
        for coluna in colunas:
            list_colunas.append(coluna['name'])
        tabela_colunas.append((tabela, list_colunas))

    for tabela, colunas in tabela_colunas:
        for coluna in colunas:
            try:
                query = text(f"SELECT DISTINCT {coluna} FROM {tabela} LIMIT 3")
                result = connection.execute(query)
                
                formato_exemplo = "%d/%m/%Y %H:%M:%S"
                formato_mysql = "%Y-%m-%d %H:%M:%S"

                rows = [row[0] for row in result]

            #     # Filtrando valores vazios ou nulos
                valid_rows = [value for value in rows if value and isinstance(value, datetime)]
                print(valid_rows)

            #     valid_colunas_data = []
            #     try:
            #         data = [datetime.strptime(value, formato_exemplo).strftime(formato_exemplo) for value in valid_rows]
            #         if data:
            #             valid_colunas_data.append((tabela, coluna))
            #         else:
            #             continue
            #     except ValueError:
            #         continue
            except Exception as e:
                print(f"capturado no except exterior : {e}")     
        


def manager_update_date():
    for file in CONFIG_PATH.rglob("db_config/*.json"):
        db_configs = load_config_file(file)
        for config in db_configs:

            print("\nIniciando...")
            if file.name == "db_config_mysql.json":
                time.sleep(1)
                print(f"\nEssas são as configuraçãoes do banco de dados:\n{config}\n")
                time.sleep(1)
                conn = mysql_connection(
                    config["host"],
                    config["port"],
                    config["user"],
                    config["password"],
                    config["dbname"],
                )
                get_tables(conn)