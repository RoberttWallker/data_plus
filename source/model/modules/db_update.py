from sqlalchemy import create_engine, inspect, Table, Column, Text
from sqlalchemy.sql import text
from datetime import datetime
from pathlib import Path
import time


from .db_connector import (
    mysql_connection,
    postgresql_connection,
    load_config_file
)

MODEL_PATH = Path(__file__).absolute().parent.parent
CONFIG_PATH = MODEL_PATH / "config"
TEMP_FILE_PATH = MODEL_PATH / "data/temp_file_data"


def convert_column_to_date_or_datetime_mysql(conn, table_name, column_name):
    query = text(f"SELECT DISTINCT {column_name} FROM {table_name} LIMIT 10")
    result = conn.connection.execute(query)

    formato_exemplo = "%d/%m/%Y %H:%M:%S"

    rows = []
    for row in result:
        value = row[0]
        try:
            data_formatada = datetime.strptime(value, formato_exemplo)
        except (ValueError, TypeError):
            continue

        else:
            rows.append(data_formatada)
            print(f"O valor {value} na coluna {column_name} segue o padrao {formato_exemplo}")

    if len(rows) == 10:
        alter_query = text(f"ALTER TABLE {table_name} MODIFY COLUMN {column_name} DATETIME")
        conn.connection.execute(alter_query)


def update_columns_date_mysql(conn):
    inspector = inspect(conn.engine)

    for table_name in inspector.get_table_names():
        print(f"Verificando a tabela: {table_name}")

        columns = inspector.get_columns(table_name)
        for column in columns:
            column_name = column['name']
            column_type = column['type']

            if isinstance(column_type, Text):
                # print(f"Verificando coluna: {column_name}")

                convert_column_to_date_or_datetime_mysql(conn, table_name, column_name)


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