from pathlib import Path
import ijson
import traceback
import time
import re
from datetime import datetime
import json
from sqlalchemy import Table, Column, Text, delete
from sqlalchemy.sql import text


from .db_connector import (
    mysql_connection,
    postgresql_connection,
    load_config_file,
    load_config_file_update
)
from .db_update import formatar_datas_incrementais
from .aux_func_app import delete_temp_files, no_date_api_list
from .aux_func_inserter import tabelas_e_colunas, tabelas_e_dados

MODEL_PATH = Path(__file__).absolute().parent.parent
CONFIG_PATH = MODEL_PATH / "config"
TEMP_FILE_PATH = MODEL_PATH / "data/temp_file_data"

def comparar_tabelas(conn):
    connection = conn.connection

    tabelas_e_diferencas = []

    def datas_max_min(file, col_data):
        max_date = None
        min_date = None
        with open(file, "r") as f:
            dados_carregados = json.load(f)
            file_flat = [item for sublist in dados_carregados for item in sublist]
            if file_flat:
                datas = [datetime.strptime(item[col_data], "%Y-%m-%d %H:%M:%S") for sublista in dados_carregados for item in sublista if col_data in item]  
                try:
                    max_date = max(datas).strftime("%Y-%m-%d %H:%M:%S")
                    min_date = min(datas).strftime("%Y-%m-%d %H:%M:%S")
                except:
                    pass
        return max_date, min_date
    
    def select_from_db(table_name, col_data, min_date, max_date):
        table = []

        query = text(f"SELECT * FROM {table_name} WHERE {col_data} BETWEEN :min_date AND :max_date")
        result = connection.execute(query, {"min_date": min_date, "max_date": max_date})
        colunas = result.keys()
        valores = result.fetchall()

        for linha in valores:
            linha_dict = dict(zip(colunas, linha))
            if col_data in linha_dict and isinstance(linha_dict[col_data], datetime):
                linha_dict[col_data] = linha_dict[col_data].strftime("%Y-%m-%d %H:%M:%S")

            table.append(linha_dict)
        
        return table
    
    def comparar(table, file_content):
        # Transforma file_content (lista de listas) em uma lista de dicionários
        file_content_flat = [item for sublista in file_content for item in sublista]
        # Encontra as diferenças
        diff = []
        for linha_content in file_content_flat:
            if linha_content not in table:
                diff.append(linha_content)
        
        return diff
    
    for file in TEMP_FILE_PATH.rglob("*.json"):
        table_name = re.sub(r"([a-z])([A-Z])", r"\1_\2", file.name.split("Grid.")[0]).lower()
        file_content = load_config_file_update(file)
        
        if file.name in ["ContasReceberRecebidasGrid.json", "ContasPagarPagasGrid.json"]:
            col_data = "EMISSAO"
        elif file.name == "EntradasEstoqueGrid.json":
            col_data = "DATAENTRADA"
        elif file.name == "ProdutosPorOSGrid.json":
            col_data = "DATA"
        else:
            continue

        max_date, min_date = datas_max_min(file=file, col_data=col_data)
        table = select_from_db(table_name, col_data, min_date, max_date)
        diferencas = comparar(table, file_content)

        tabelas_e_diferencas.append({"arquivo": file, "diferencas": diferencas})

        # Exibe as diferenças
        print(f"Foram encontradas {len(diferencas)} linhas de dados novos no o arquivo {file.name}.")
        # for linha in diferencas:
        #     print(json.dumps(linha[2:], indent=4))

    return tabelas_e_diferencas
            
# Inserção de dados
def insert_tables_metadata(conn):
    perfil_colunas = tabelas_e_colunas(TEMP_FILE_PATH)

    for tabela, colunas in perfil_colunas:
        if not colunas:
            print(f">>>Tabela '{tabela}' ignorada: dados do JSON estão vazios.\n{' '*11}{'-'*len(tabela)}")
            continue

        columns = [Column(column_name, Text) for column_name in colunas.keys()]

        table = Table(tabela, conn.metadata, *columns)

    conn.metadata.create_all(conn.engine)
       
def insert_data(conn):
    dados_completos = tabelas_e_dados(TEMP_FILE_PATH)

    connection = conn.connection
    connection_name = f"{conn.db_name} - {conn.dialect}"

    print(f"{'-'*44}{'-'*len(connection_name)}\nIniciando processo de inserção de dados em: {connection_name}\n{'-'*44}{'-'*len(connection_name)}\n")
    try:
        for tabela, dados in dados_completos:
            if dados.exists():
                if tabela not in conn.metadata.tables:
                    print(f">>>Tabela '{tabela}' não encontrada no metadata.\n{' '*11}{'-'*len(tabela)}")
                    continue

                table = conn.metadata.tables[tabela]
                with open(dados, "r", encoding="utf-8") as f:
                    try:

                        parser = ijson.items(f, "item")
                        lote_tam = 1000
                        lote = []

                        for item in parser:
                            if isinstance(item, list):
                                lote.extend(item)
                            else:
                                lote.append(item)

                        print(f"Inserindo os dados na tabela: {tabela}")
                        time.sleep(1)
                        if len(lote) >= lote_tam:
                            try:
                                connection.execute(table.insert(), lote)
                                lote.clear()
                            except Exception as e:
                                print(
                                    f"Erro ao fazer o .execute() na tabela: {tabela}: {e}"
                                )

                        if lote:
                            connection.execute(table.insert(), lote)

                    except Exception as e:
                        print(f"Erro ao processar dados para '{tabela}': {e}")

                try:
                    connection.commit()
                except Exception as e:
                    print(f"\nErro ao fazer commit das alterações: {e}\n")
            else:
                print("Arquivo não existe.")
    finally:
        print(f"\n{'-'*37}{'-'*len(connection_name)}\nFechando conexão com banco de dados: {connection_name}\n{'-'*37}{'-'*len(connection_name)}\n")
        connection.close()

def insert_manager(conn):
    try:
        try:
            insert_tables_metadata(conn)
        except Exception as e:
            print(
                f"Ocorreu um erro ao inserir as tabelas no metadata. Erro: {e}",
            )
            traceback.print_exc()

        try:
            insert_data(conn)
        except Exception as e:
            print(
                f"Ocorreu um erro ao inserir os dados no banco de dados. Erro: {e}",
            )
            traceback.print_exc()

    except Exception as e:
        print(f"Ocorreu um erro: {e} na função insert_manager()")

def insert_manager_incremental(conn):
    try:

        try:
            formatar_datas_incrementais()
        except Exception as e:
            print(
                f"Ocorreu um erro ao gravar um arquivo temporário: {e}",
            )
            traceback.print_exc()

        try:
            tabelas_e_diferencas = comparar_tabelas(conn)
            for item in tabelas_e_diferencas:
                tabela = item['arquivo']
                diferencas = item['diferencas']
                if len(diferencas) > 0:
                    with open(tabela, "w", encoding="utf-8") as f:
                        f.write(f"[\n")
                        json.dump(diferencas, f, ensure_ascii=False)
                        f.write(f"\n]")
                else:
                    if tabela.exists():
                        tabela.unlink()

        except Exception as e:
            print(
                f"Ocorreu um erro ao gravar um arquivo temporário: {e}",
            )
            traceback.print_exc()

        try:
            insert_tables_metadata(conn)
        except Exception as e:
            print(
                f"Ocorreu um erro ao inserir as tabelas no metadata. Erro: {e}",
            )
            traceback.print_exc()
        try:
            tabelas_sem_data = [re.sub(r"([a-z])([A-Z])", r"\1_\2", tabela).lower() for tabela in no_date_api_list]
            tabelas_no_metada = list(conn.metadata.tables.keys())
            for tabela in tabelas_no_metada:
                if tabela in tabelas_sem_data:
                    #Limpar tabela totalmente
                    stmt = delete(conn.metadata.tables[tabela])
                    conn.connection.execute(stmt)
                    conn.connection.commit()
                    #Depois inserir os dados
                    
            insert_data(conn)
        except Exception as e:
            print(
                f"Ocorreu um erro ao inserir os dados no banco de dados. Erro: {e}",
            )
            traceback.print_exc()

    except Exception as e:
        print(f"Ocorreu um erro: {e} na função insert_manager()")
        traceback.print_exc()

def insert_total_into_db():
    controle = {}
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

                if conn == None:
                    print("Conexão falhou. Não é possível construir as tabelas.")
                    controle[config["dbname"]] = False
                else:
                    insert_manager(conn)
                    controle[conn.db_name] = True

            elif file.name == "db_config_postgresql.json":
                print(f"\nEssas são as configuraçãoes do db:\n{config}\n")
                conn = postgresql_connection(
                    config["host"],
                    config["port"],
                    config["user"],
                    config["password"],
                    config["dbname"],
                )

                if conn == None:
                    print("Conexão falhou. Não é possível construir as tabelas.")
                    controle[config["dbname"]] = False
                else:
                    insert_manager(conn)
                    controle[conn.db_name] = True

            else:
                print(
                    "Arquivo fora do padrão, ou SGBD ainda não configurado na ferramenta!"
                )
                controle["Fora_padrao_ou_sgbd_nao_configurado"] = False

    if all(controle.values()):
        delete_temp_files(TEMP_FILE_PATH)
    else:
        print("Nem todas as inserções foram bem-sucedidas. Arquivos mantidos.")

def insert_increment_into_db():
    controle = {}
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

                if conn == None:
                    print("Conexão falhou. Não é possível retirar última data.")
                    controle[config["dbname"]] = False
                else:
                    try:
                        insert_manager_incremental(conn)
                        controle[config["dbname"]] = True
                    except Exception as e:
                        print(f"Erro ao tentar inserir os dados no MySQL '{config['dbname']}': {e}")
                        traceback.print_exc()
                        controle[config["dbname"]] = False

            elif file.name == "db_config_postgresql.json":
                print(f"\nEssas são as configuraçãoes do db:\n{config}\n")
                conn = postgresql_connection(
                    config["host"],
                    config["port"],
                    config["user"],
                    config["password"],
                    config["dbname"],
                )

                if conn == None:
                    print("Conexão falhou. Não é possível construir as tabelas.")
                    controle[config["dbname"]] = False
                else:
                    try:
                        insert_manager_incremental(conn)
                        controle[config["dbname"]] = True
                    except Exception as e:
                        print(f"Erro ao tentar inserir os dados no PostgreSQL '{config['dbname']}': {e}")
                        traceback.print_exc()
                        controle[config["dbname"]] = False
            else:
                print(
                    "Arquivo fora do padrão, ou SGBD ainda não configurado na ferramenta!"
                )
                controle["Fora_padrao_ou_sgbd_nao_configurado"] = False

    if all(controle.values()):
        delete_temp_files(TEMP_FILE_PATH)
    else:
        print("Nem todas as inserções foram bem-sucedidas. Arquivos mantidos.")





