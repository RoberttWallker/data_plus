import json
import time
from datetime import datetime, timedelta

from sqlalchemy import inspect
from sqlalchemy.sql import text

from .db_connector import (load_config_file, mysql_connection,
                           postgresql_connection)
from .constants import (DB_CONFIG_PATH, INCREMENT_CONFIG_PATH,
                        TEMP_DATA_PATH, file_db_config_mysql,
                        file_db_config_postgresql,
                        file_incremental_config_mysql,
                        file_incremental_config_postgresql)


def datetime_converter(o):
    if isinstance(o, datetime):
        return o.strftime("%Y-%m-%d %H:%M:%S")
    raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")

def formatar_datas_incrementais(identificador):
    #A função tem de receber file carregado json.load(temp_file)
    def modificar(temp_file, column):
        if temp_file.exists():
            file_content = load_config_file(temp_file)
            for increment in file_content:
                for row in increment:
                    data = row[column]
                    try:
                        nova_data = datetime.strptime(data, "%d/%m/%Y %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S")
                        row[column] = nova_data
                    except ValueError:
                            print(f"Data inválida encontrada: {data}")
            
            # Após as modificações, salva o arquivo novamente
            with open(temp_file, "w") as outfile:
                json.dump(file_content, outfile, indent=4, ensure_ascii=False)
    
    for file in INCREMENT_CONFIG_PATH.rglob("*.json"):
        incremental_config = load_config_file(file)
        for db_config in incremental_config:
            for db_name in db_config:
                if identificador in db_name.split("_"):
                    for table_info in db_config[db_name]:
                        if table_info['table'] == "contas_receber_recebidas":
                            temp_file = TEMP_DATA_PATH / f"APIRelatoriosCR/ContasReceberRecebidasGrid_{identificador}.json"
                            modificar(temp_file, 'EMISSAO')
                        elif table_info['table'] == "contas_pagar_pagas":
                            temp_file = TEMP_DATA_PATH / f"APIRelatoriosCR/ContasPagarPagasGrid_{identificador}.json"
                            modificar(temp_file, 'EMISSAO')
                        elif table_info['table'] == "produtos_por_os":
                            temp_file = TEMP_DATA_PATH / f"APIRelatoriosCR/ProdutosPorOSGrid_{identificador}.json"
                            modificar(temp_file, 'DATA')
                        elif table_info['table'] == "entradas_estoque":
                            temp_file = TEMP_DATA_PATH / f"APIRelatoriosCR/EntradasEstoqueGrid_{identificador}.json"
                            modificar(temp_file, 'DATAENTRADA')

def get_incremental_date(identificador):
    for file in DB_CONFIG_PATH.rglob("*.json"):
        db_configs = load_config_file(file)
        for config in db_configs:
            for key, value in config.items():
                if key == 'dbname':
                    print(f"Verificando se '{identificador}' está em '{value}'")
                    if identificador in value.split("_"):
                        if file.name == file_db_config_mysql.name:
                            time.sleep(1)
                            # print(f"\nEssas são as configuraçãoes do banco de dados:\n{config}\n")
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
                                break
                            else:
                                connection = conn.connection
                                for file in INCREMENT_CONFIG_PATH.rglob("*.json"):
                                    if file.name == file_incremental_config_mysql.name:
                                        incremental_configs = load_config_file(file)
                                        
                                        table_last_date = []
                                        for config in incremental_configs:
                                            for db_name, dados in config.items():
                                                if db_name == conn.db_name:
                                                    for table_column in dados:
                                                        
                                                        table = table_column['table']
                                                        column = table_column['column']
                                                        query = text(f"SELECT MAX({column}) FROM {table}")
                                                        result = connection.execute(query).scalar()

                                                        if result:
                                                            current_date = datetime.now()
                                                            if current_date.date() > result.date():
                                                                #max_date = datetime.strftime(result, "%d/%m/%Y")
                                                                max_date = (result + timedelta(days=1)).strftime("%d/%m/%Y")
                                                            else:
                                                                max_date = result.strftime("%d/%m/%Y")
                                                        else:
                                                            max_date = None

                                                        table_last_date.append((table, max_date))

                                        return(table_last_date)


                        elif file.name == file_db_config_postgresql.name:
                            # print(f"\nEssas são as configuraçãoes do db:\n{config}\n")
                            conn = postgresql_connection(
                                config["host"],
                                config["port"],
                                config["user"],
                                config["password"],
                                config["dbname"],
                            )

                            if conn == None:
                                print("Conexão falhou. Não é possível construir as tabelas.")
                                break
                            else:
                                connection = conn.connection
                                for file in INCREMENT_CONFIG_PATH.rglob("*.json"):
                                    if file.name == file_incremental_config_postgresql.name:
                                        incremental_configs = load_config_file(file)

                                        table_last_date = []
                                        for config in incremental_configs:
                                            for db_name, dados in config.items():
                                                if db_name == conn.db_name:
                                                    for table_column in dados:
                                                        
                                                        table = table_column['table']
                                                        column = table_column['column']
                                                        query = text(f"SELECT MAX({column}) FROM {table}")
                                                        result = connection.execute(query).scalar()

                                                        if result:
                                                            max_date = datetime.strftime(result, "%d/%m/%Y")
                                                        else:
                                                            max_date = None

                                                        table_last_date.append((table, max_date))

                                        return(table_last_date)
                        else:
                            print(
                                "Arquivo fora do padrão, ou SGBD ainda não configurado na ferramenta!"
                            )

def save_incremental_column_config(incremental_config, filename):
    filename.parent.mkdir(parents=True, exist_ok=True)
    try:
        with open(filename, "r") as file:
            update_configs = json.load(file)

    except (FileNotFoundError, json.JSONDecodeError):
        update_configs = []

    for db_name, new_entries in incremental_config.items():
        db_found = False

        for config in update_configs:
            if db_name in config:
                existing_entries = config[db_name]

                for entry in new_entries:
                    if entry not in existing_entries:
                        existing_entries.append(entry)

                db_found = True
                break

        if not db_found:
            update_configs.append({db_name: new_entries})

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

        save_incremental_column_config(incremental_config, file_incremental_config_mysql)

    elif conn.dialect == "postgresql":
        alter_column_to_datetime_postgresql(connection, table_, column_)

        incremental_config = {
            db_name: [{
            "table": table_,
            "column": column_
            }]
        }

        save_incremental_column_config(incremental_config, file_incremental_config_postgresql)

def create_column_incremental_update_pbi():
    table_column_mappings = []
    while True:
        table_ = input(f'{"-"*56}\nCopie e cole a tabela (ou digite "sair" para encerrar): ').strip()
        time.sleep(1)
        if table_.strip().lower() == "sair":
            break
        column_ = input('Agora insira a coluna que será usada para atualização incremental no Power BI: ').strip()
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

                    elif value:
                        try:
                            #formated = datetime.strptime(value, formato_exemplo).strftime(formato_exemplo)
                            datetime.strptime(value, formato_exemplo)
                            if column not in columns_date:
                                columns_date.append(column)

                        except ValueError:
                            continue
                    else:
                        continue

            except Exception as e:
                print(f"capturado no except exterior : {e}")     
        
        tables_columns_date.append((table, columns_date))
    return tables_columns_date      

def update_flow(file):
    db_configs = load_config_file(file)
    for config in db_configs:
        print("\nIniciando...")
        time.sleep(1)
        print(f"\nEssas são as configuraçãoes do banco de dados:\n{config}\n")
        time.sleep(1)
        if file.name == file_db_config_mysql.name:
            conn = mysql_connection(
                config["host"],
                config["port"],
                config["user"],
                config["password"],
                config["dbname"],
            )
        elif file.name == file_db_config_postgresql.name:
            conn = postgresql_connection(
                config["host"],
                config["port"],
                config["user"],
                config["password"],
                config["dbname"],
            )

        tables_columns_date = get_tables_columns_date(conn)
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

    count = len(list(DB_CONFIG_PATH.rglob("*.json")))
    if count > 1:
        file_list = list(DB_CONFIG_PATH.rglob("*.json"))
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
        for file in DB_CONFIG_PATH.rglob("*.json"):
            update_flow(file)