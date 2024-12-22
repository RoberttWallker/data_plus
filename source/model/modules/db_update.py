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

            rows = [row[0] for row in result]

            # Filtrando valores vazios ou nulos
            valid_rows = [value for value in rows if value and value.strip()]


            try:
                data = [datetime.strptime(value, formato_exemplo).strftime(formato_exemplo) for value in valid_rows]
                if data:
                    valid_columns_date.append((table_, column_))
                else:
                    continue
            except ValueError:
                continue
   
    print(valid_columns_date)

    for table_, column_ in valid_columns_date:
        query = text(f"SELECT {column_} FROM {table_}")
        result = connection.execute(query)

        rows = [row[0] for row in result]

        # Filtrando valores vazios ou nulos
        valid_rows = [value for value in rows if value]# and value != '']

        #print(f"\n{table_} - {column_}:\n{valid_rows}\n")
        formato_exemplo = "%d/%m/%Y %H:%M:%S"
        formato_mysql = "%Y-%m-%d %H:%M:%S"
        for row in valid_rows:
            try:
                data_formatada = datetime.strptime(row, formato_exemplo).strftime(formato_mysql)
                if row != data_formatada:
                    alter_query = text(f"UPDATE {table_} SET {column_} = :data_formatada WHERE {column_} = :old_value")
                    connection.execute(alter_query, {'data_formatada': data_formatada, 'old_value': row})
            except ValueError:
                continue

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
                update_columns_date_mysql(conn)