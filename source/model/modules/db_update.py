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
            SET {column_} = '07/07/1970 07:07:07'
            WHERE {column_} IS NULL OR TRIM({column_}) = '';
        """)
        connection.execute(preencher_query)
        print(f"Valores nulos ou vazios da coluna {column_} na tabela {table_} preenchidos com data padrão.")
        connection.commit()

    #     # Alterar o tipo da coluna para DATETIME
    #     alter_query = text(f"ALTER TABLE {table_} MODIFY COLUMN {column_} DATETIME")
    #     connection.execute(alter_query)
    #     print(f"Coluna {column_} na tabela {table_} alterada para DATETIME.")
    except Exception as e:
        print(f"Erro ao alterar coluna {column_} na tabela {table_}: {e}")

def alterar_formato_data(conn, table_, column_):
    connection = conn.connection
    formato_fonte = "%d/%m/%Y %H:%M:%S"

    if conn.dialect == "mysql":
        formato_mysql = "%Y-%m-%d %H:%M:%S"
        try:

            update_query = text(f"""
                UPDATE {table_}
                SET {column_} = 
                    DATE_FORMAT(STR_TO_DATE({column_}, :formato_fonte), :formato_mysql)
                WHERE {column_} IS NOT NULL;
            """)
            connection.execute(update_query, {'formato_fonte': formato_fonte, 'formato_mysql': formato_mysql})
            connection.commit()
            print(f"Coluna '{column_}' da tabela '{table_}' atualizada com sucesso!")
        except Exception as e:
            print(f"Erro ao atualizar coluna '{column_}' na tabela '{table_}': {e}")
    


# def update_columns_date_mysql(conn):
#     inspector = inspect(conn.engine)

#     table_columns = {}

#     for table_name in inspector.get_table_names():

#         columns = inspector.get_columns(table_name)
#         columns_list = []
#         for column in columns:
#             column_name = column['name']
#             columns_list.append(column_name)
#         table_columns[table_name] = columns_list
    
    
#     connection = conn.connection

#     valid_columns_date = []

#     for table_, columns_ in table_columns.items():
#         for column_ in columns_:
#             query = text(f"SELECT DISTINCT {column_} FROM {table_} LIMIT 3")
#             result = connection.execute(query)
            
#             formato_exemplo = "%d/%m/%Y %H:%M:%S"
#             formato_mysql = "%Y-%m-%d %H:%M:%S"

#             rows = [row[0] for row in result]

#             # Filtrando valores vazios ou nulos
#             valid_rows = [value for value in rows if value and (not isinstance(value, datetime))]


#             try:
#                 data = [datetime.strptime(value, formato_exemplo).strftime(formato_exemplo) for value in valid_rows]
#                 if data:
#                     valid_columns_date.append((table_, column_))
#                 else:
#                     continue
#             except ValueError:
#                 try:
#                     data = [datetime.strptime(value, formato_mysql).strftime(formato_mysql) for value in valid_rows]
#                     if data:
#                         valid_columns_date.append((table_, column_))
#                     else:
#                         continue
#                 except:
#                     continue
   
#     print(valid_columns_date)

#     for table_, column_ in valid_columns_date:
#         query = text(f"SELECT {column_} FROM {table_}")
#         result = connection.execute(query)

#         rows = [row[0] for row in result]

#         # Filtrando valores vazios ou nulos
#         valid_rows = [value for value in rows if value]# and value != '']

#         for row in valid_rows:
#             try:
#                 data_formatada = datetime.strptime(row, formato_exemplo).strftime(formato_mysql)
#                 update_query = text(f"UPDATE {table_} SET {column_} = :data_formatada WHERE {column_} = :old_value")
#                 connection.execute(update_query, {'data_formatada': data_formatada, 'old_value': row})
#             except ValueError:
#                 try:
#                     data_formatada = datetime.strptime(row, formato_mysql).strftime(formato_mysql)
#                     update_query = text(f"UPDATE {table_} SET {column_} = :data_formatada WHERE {column_} = :old_value")
#                     connection.execute(update_query, {'data_formatada': data_formatada, 'old_value': row})
#                 except:
#                     continue
#         # Alterar o tipo da coluna para DATETIME
#         try:
#             alter_query = text(f"ALTER TABLE {table_} MODIFY COLUMN {column_} DATETIME")
#             connection.execute(alter_query)
#             print(f"Coluna {column_} na tabela {table_} alterada para DATETIME.")
#         except Exception as e:
#             print(f"Erro ao alterar coluna {column_} na tabela {table_}: {e}")
        
#         preencher_nulos(connection, table_, column_)


def criar_col_atualizacao_incremental_pbi():
    perfis = []
    while True:
        tabela_ = input(f'{"-"*56}\nCopie e cole a tabela (ou digite "sair" para encerrar): ')
        time.sleep(1)
        if tabela_.strip().lower() == "sair":
            break
        coluna_ = input('Agora insira a coluna que será usada para atualização incremental no Power BI: ')
        time.sleep(1)
        if not tabela_ or not coluna_:
            print("Tabela ou coluna não podem estar vazios. Tente novamente.")
            time.sleep(1)
            continue
        tbl_col = {tabela_: coluna_}
        perfis.append(tbl_col)
        print(f"\nPerfil adicionado: {tbl_col}\n{'-'*(len(str(tbl_col))+19)}")
    return perfis

def get_tables_columns_date(conn):
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

    tabela_colunas_de_data = []
    for tabela, colunas in tabela_colunas:
        colunas_de_data = []
        for coluna in colunas:
            try:
                query = text(f"SELECT DISTINCT {coluna} FROM {tabela} LIMIT 2")
                result = connection.execute(query)
                
                formato_exemplo = "%d/%m/%Y %H:%M:%S"
                formato_mysql = "%Y-%m-%d %H:%M:%S"

                rows = [row[0] for row in result]

                for value in rows:
                    if value and isinstance(value, datetime):
                        print(f"FORMATO DATETIME: {tabela} - {coluna} - {value}")
                    elif value:
                        try:
                            formatado = datetime.strptime(value, formato_exemplo).strftime(formato_exemplo)
                            #print(f"FORMATO STR: {tabela} - {coluna} - {formatado}")
                            if coluna not in colunas_de_data:
                                colunas_de_data.append(coluna)
                        except ValueError:
                            continue
                    else:
                        continue

            except Exception as e:
                print(f"capturado no except exterior : {e}")     
        
        tabela_colunas_de_data.append((tabela, colunas_de_data))
    #print(f"\n{'*'*32}\n{tabela_colunas_de_data}")
    return tabela_colunas_de_data      

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
                tabelas_e_colunas_de_datas = get_tables_columns_date(conn)
                #print(f"TESTE AQUI DIALETC: {conn.dialect}")
                for tabela, colunas in tabelas_e_colunas_de_datas:
                    print(f"Tabela: {tabela}")
                    if colunas:
                        print(f"    Colunas:")
                        for coluna in colunas:
                            print(f"    - {coluna}")
                
                
                tabelas_colunas_atualizar = criar_col_atualizacao_incremental_pbi()
                while True:
                    resposta = input(f'''\n
Confirme se essas são as tabelas e respectivas colunas que quer atualizar:
    \n{tabelas_colunas_atualizar}\n
(s/n)>>> ''').strip().lower()
                    if resposta == 's':
                        print('Continuando...\n')
                        time.sleep(1)
                        break
                    elif resposta == 'n':
                        print('Repetindo...\n')
                        time.sleep(1)
                        tabelas_colunas_atualizar = criar_col_atualizacao_incremental_pbi()
                    else:
                        print('Opção inválida. Tente novamente...')

                for perfil in tabelas_colunas_atualizar:
                    # Usar next(iter(...)) é uma maneira eficiente e direta
                    # de acessar o primeiro item de um iterável.
                    tabela, coluna = next(iter(perfil.items()))

                    alterar_formato_data(conn, tabela, coluna)