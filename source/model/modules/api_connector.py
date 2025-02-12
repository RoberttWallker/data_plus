from datetime import datetime, timedelta, date
import inspect
from pathlib import Path
import requests
import json
import time
import traceback


from model.modules.classes import ConexaoAPI, UnidadeAPI
from model.modules.db_update import get_incremental_date
from model.modules.aux_func_app import get_identifiers, formatar_nome_para_root, no_date_api_list

ROOT_PATH = Path.cwd()
file_requests_config = ROOT_PATH / "source/model/config/requests_config/requests_config.json"

data_final = datetime.today()
dias_incremento = timedelta(days=90)

# Configurações de APIs
def save_requests_config(conexao_api, unidade_api):
    file_path = file_requests_config

    file_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        with open(file_path, "r") as file:
            loaded_requests_config = json.load(file)
            # for config in loaded_requests_config:
                # if (
                #     config["identificador"] == request_config["identificador"]
                #     and config["relative_path"] == request_config["relative_path"]
                # ):
                #     print(
                #         f'A Request: {request_config['relative_path']} com o identificador: {request_config["identificador"]} já existe!'
                #     )
                #     return
            identifcador_existente = False
            for config in loaded_requests_config:
                if conexao_api.identificador in config:
                    identifcador_existente = True
                    for item in config[conexao_api.identificador]:
                        if item['relative_path'] == unidade_api.relative_path:
                            print(f'A configuração com o identificador {conexao_api.identificador} e o path {unidade_api.relative_path} já existe!')
                            return
                        
                    config[conexao_api.identificador].append({
                        "url_base": conexao_api.url_base,
                        "identificador": conexao_api.identificador,
                        "authorization": conexao_api.authorization,
                        "relative_path": unidade_api.relative_path,
                        "body": unidade_api.body
                    })
                    print(f'Configuração do identificador {conexao_api.identificador} foi atualizada.')
                    break
            
            if not identifcador_existente:
                request_config = { 
                        conexao_api.identificador: [
                        {
                        "url_base": conexao_api.url_base,
                        "identificador": conexao_api.identificador,
                        "authorization": conexao_api.authorization,
                        "relative_path": unidade_api.relative_path,
                        "body": unidade_api.body
                        }
                        ]
                    }
                
                loaded_requests_config.append(request_config)
                print(f'Nova configuração para o identificador {conexao_api.identificador} foi adicionada.')

    except (FileNotFoundError, json.JSONDecodeError):
        loaded_requests_config = []

    with open(file_path, "w") as file:
        json.dump(loaded_requests_config, file, indent=4)

def request_config():
    print("Insira os dados solicitados para configurar a consulta da API")
    while True:
        url_base = input(
            "Informe a URL base da API, Ex(https://api.savwinweb.com.br/api/): "
        )
        identificador = input("Identificador: ")
        authorization = input("Authorization: ")
        relative_path = input("Pasta relativa, Ex(Relatorios/EstoqueAnaliticoGrid): ")
        body = {}

        # Agora, solicitamos os parâmetros chave-valor do usuário
        print(
            """
    ###########################################################################################
        Insira os parâmetros da consulta no formato chave=valor. Digite 'fim' para encerrar.
    ###########################################################################################
            """
        )

        while True:
            chave = input("Chave: ").strip()
            if (
                chave.lower() == "fim"
            ):  # Permite ao usuário terminar a inserção de parâmetros
                break
            valor = input(f"Valor para {chave}: ").strip()
            body[chave] = valor

        try:
            conexao_api = ConexaoAPI(url_base, identificador, authorization)
            unidade_api = UnidadeAPI(relative_path, body)
            save_requests_config(conexao_api, unidade_api)

        except Exception as e:
            print("Ocorreu um erro ao salvar a configuração:")
            print(f"Tipo do erro: {type(e).__name__}")
            traceback.print_exc()

        while True:
            escolha = input("Deseja inserir outra configuração de API? s/n\n>>> ").lower()
            if escolha == "s":
                print("Reiniciando configuração de APIs...\n")
                time.sleep(1)
                break
            elif escolha == "n":
                print("Encerrando configuração de APIs...\n")
                time.sleep(1)
                return
            else:
                print("Opção incorreta, tente novamente.\n")
                time.sleep(1)

# Tipos de requests
def full_requests(config, temp_file):
    headers = {
        "Identificador": config["identificador"],
        "Authorization": config["authorization"],
        "Content-Type": "application/json",
    }
    response = requests.post(
        f"{config['url_base']}{config['relative_path']}",
        headers=headers,
        json=config["body"],
        stream=True,
    )
    if response.status_code == 200:
        data = response.json()

        json.dump(data, temp_file, ensure_ascii=False)
    else:
        raise Exception(
            f"Erro na requisição: {response.status_code} - {response.text}"
        )

def chunks_requests(config, data_inicial, data_final, dias_incremento, temp_file):
    
    total_iteracoes = 0

    api_data_fields =  {
        "data_inicio": {
            "APIRelatoriosCR/ContasReceberRecebidasGrid": "DUPEMISSAO1",
            "APIRelatoriosCR/ContasPagarPagasGrid": "DUPEMISSAO1",
            "APIRelatoriosCR/EntradasEstoqueGrid": "DATAINICIO",
            "APIRelatoriosCR/ProdutosPorOSGrid": "DATAINICIAL"
        },
        "data_fim": {
            "APIRelatoriosCR/ContasReceberRecebidasGrid": "DUPEMISSAO2",
            "APIRelatoriosCR/ContasPagarPagasGrid": "DUPEMISSAO2",
            "APIRelatoriosCR/EntradasEstoqueGrid": "DATAFINAL",
            "APIRelatoriosCR/ProdutosPorOSGrid": "DATAFINAL"
        }
    }

    headers = {
        "Identificador": config["identificador"],
        "Authorization": config["authorization"],
        "Content-Type": "application/json",
    }

    if data_final is None and config['relative_path'] in api_data_fields['data_fim']:
        campo_data = api_data_fields['data_fim'][config['relative_path']]
        if campo_data in config['body']:
            data_final = datetime.strptime(config['body'][campo_data], "%d/%m/%Y")
        else:
            print(f"Erro: Campo '{campo_data}' não encontrado no corpo da requisição.")

    while data_inicial < data_final: # type: ignore

        data_final_periodo = min(data_inicial + dias_incremento, data_final)

        key_data_inicial_interna_update = None
        key_data_final_interna_update = None

        if config['relative_path'] in api_data_fields['data_inicio']:
            key_data_inicial_interna_update  = api_data_fields['data_inicio'][config['relative_path']]
            key_data_final_interna_update = api_data_fields['data_fim'][config['relative_path']]
        else:
            raise ValueError(f"Endpoint desconhecido: {config['relative_path']}")

        config['body'][key_data_inicial_interna_update] = data_inicial.strftime("%d/%m/%Y")
        config["body"][key_data_final_interna_update] = data_final_periodo.strftime("%d/%m/%Y")

        response = requests.post(
            f"{config['url_base']}{config['relative_path']}",
            headers=headers,
            json=config["body"],
            stream=True,
        )

        if response.status_code == 200:
            data = response.json()

            if total_iteracoes > 0:
                temp_file.write(",\n")

            json.dump(data, temp_file, ensure_ascii=False)
            total_iteracoes += 1
        else:
            raise Exception(
                f"Erro na requisição: {response.status_code} - {response.text}"
            )

        data_inicial = data_final_periodo
        time.sleep(1)

def incremental_requests(config, data_inicial, data_final, temp_file):

    headers = {
        "Identificador": config["identificador"],
        "Authorization": config["authorization"],
        "Content-Type": "application/json",
    }

    if config['relative_path'] == 'APIRelatoriosCR/ProdutosPorOSGrid':
        body = {
            "DATAINICIAL": data_inicial,
            "DATAFINAL": data_final,
            "LOJAS": "1,2,3,4,5,6,7,8,9,10,11,12,15,9000",
            "MARKUPUNICO": "",
            "INCIOSEQ": "",
            "FINALSEQ": "",
            "TIPOPRODUTO": "|0|1|2|3|4|5|6|7|8|9|10|11|",
            "SOMENTECONTROLAEST": "T",
            "MULTIPLOSMARKUPS": ""
        }
    
    elif config['relative_path'] == "APIRelatoriosCR/EntradasEstoqueGrid":
        body = {
            "INICIOSEQ": "1",
            "FINALSEQ": "99999999",
            "DATAINICIO": data_inicial,
            "DATAFINAL": data_final
        }

    elif config['relative_path'] in ["APIRelatoriosCR/ContasReceberRecebidasGrid", "APIRelatoriosCR/ContasPagarPagasGrid"]:
        body = {
            "FILID": "1",
            "DUPEMISSAO1": data_inicial,
            "DUPEMISSAO2": data_final,
            "PARVENCIMENTO1": None,
            "PARVENCIMENTO2": None,
            "RECRECEBIMENTO1": None,
            "RECRECEBIMENTO2": None,
            "PAGAMENTOVENDA1": None,
            "PAGAMENTOVENDA2": None,
            "TIPOPERIODO": "1",
            "STATUSRECEBIDO": ""
        }

    response = requests.post(
        f"{config['url_base']}{config['relative_path']}",
        headers=headers,
        json=body,
        stream=True,
    )
    if response.status_code == 200:
        data = response.json()

        json.dump(data, temp_file, ensure_ascii=False)
    else:
        raise Exception(
            f"Erro na requisição: {response.status_code} - {response.text}"
        )
    
# Métodos de DATA
def get_initial_date(config, incremental_date=False, identificador=False):
    data_inicial = None
    if incremental_date == False and identificador == False:
        try:
            if "ProdutosPorOS" in config["relative_path"]:
                data_inicial = datetime.strptime(
                    config["body"]["DATAINICIAL"], "%d/%m/%Y"
                )
            elif "EntradasEstoque" in config["relative_path"]:
                data_inicial = datetime.strptime(
                    config["body"]["DATAINICIO"], "%d/%m/%Y"
                )
            elif "ContasPagarPagas" in config["relative_path"]:
                data_inicial = datetime.strptime(
                    config["body"]["DUPEMISSAO1"], "%d/%m/%Y"
                )
            elif "ReceberRecebidas" in config["relative_path"]:
                data_inicial = datetime.strptime(
                    config["body"]["DUPEMISSAO1"], "%d/%m/%Y"
                )

        except KeyError as e:
            print(f"Chave não encontrada: {e}. Pulando para o próximo caso.")
        except ValueError as e:
            print(f"Erro ao converter data: {e}. Verifique o formato.")

        return data_inicial
    
    else:
        try:
            tables_initial_date = get_incremental_date(identificador)
            if tables_initial_date is None:
                pass
            elif not tables_initial_date:
                print(f"Nenhuma tabela com data inicial encontrada na função: > get_initial_date-tables_initial_date = get_incremental_date <")
            else:
                for table, date in tables_initial_date:
                    table = formatar_nome_para_root(table)
                    if table in config["relative_path"]:
                        data_inicial = date
                
        except KeyError as e:
            print(f"Chave não encontrada: {e}. Pulando para o próximo caso.")
        except ValueError as e:
            print(f"Erro ao converter data: {e}. Verifique o formato.")

        return data_inicial
        

# Download de dados das APIs
def request_total_memory_saving():
    if not file_requests_config.exists():
        print("Arquivo de configurações de APIs não existe!")
        return
    
    with open(file_requests_config, "r") as file:
        requests_config = json.load(file)  # Lista de dicionários
        identifiers = get_identifiers(requests_config)  # Lista com os identificadores, ex: ["04056", "04091"]
        identifiers_formated = "\n".join([f"{idx + 1} - {identifier}" for idx, identifier in enumerate(identifiers)])

        identificador_escolhido = None

        while True:
            escolha = input(f'''
Esses são os identificadores de clientes já configurados:
{identifiers_formated}
Q - Sair
>>> ''')
            if escolha in ["Q", "q"]:
                print("Saindo...")
                return

            if escolha.isdigit():  # Verifica se a entrada é um número
                escolha_idx = int(escolha) - 1  # Converte a string para inteiro
                
                if 0 <= escolha_idx < len(identifiers):  # Verifica se o índice é válido
                    identificador_escolhido = identifiers[escolha_idx]
                    print(f"Você selecionou o identificador: {identificador_escolhido}")

                    # Busca pelo identificador dentro da lista de dicionários
                    sub_configs = None
                    for config in requests_config:
                        if identificador_escolhido in config:
                            sub_configs = config[identificador_escolhido]
                            break

                    if sub_configs is not None:
                        for sub_config in sub_configs: 
                            temp_file = ROOT_PATH / f"source/model/data/temp_file_data/{sub_config['relative_path']}_{identificador_escolhido}.json"

                            if not temp_file.exists():
                                temp_file.parent.mkdir(parents=True, exist_ok=True)

                                with temp_file.open(mode="w", encoding="utf-8") as temp_file:
                                    temp_file.write("[\n")

                                    if any(item in sub_config["relative_path"] for item in no_date_api_list):
                                        full_requests(config=sub_config, temp_file=temp_file)
                                        temp_file.write("\n]")
                                    else:
                                        data_inicial = get_initial_date(config=sub_config)

                                        if data_inicial is None:
                                            print(f"Verifique as configurações de: {sub_config} - {json.dumps(sub_config, indent=4)}.")
                                            temp_file.write("\n]")
                                            continue
                                        
                                        chunks_requests(
                                            config=sub_config, 
                                            data_inicial=data_inicial, 
                                            data_final=None, 
                                            dias_incremento=dias_incremento, 
                                            temp_file=temp_file,
                                        )

                                        temp_file.write("\n]")
                            else:
                                print(f"O arquivo para: {sub_config['relative_path']}, já existe na pasta de arquivos temporários.")
                    else:
                        print(f"Erro: identificador {identificador_escolhido} não encontrado no arquivo de configuração!")
                    
                    return identificador_escolhido

                else:
                    print("Índice fora do intervalo! Tente novamente.")
            else:
                print("Entrada inválida! Digite apenas o número correspondente ao identificador.")
    
    return None  # Caso ocorra um erro inesperado

def request_incremental_memory_saving(identificador):
    file_name = file_requests_config

    if not file_name.exists():
        print("Arquivo de configurações de APIs não existe!")
        return

    with open(file_name, "r") as file:
        requests_config = json.load(file)

        print(f"{'-'*75}\nIniciando processo de download incremental para o identificador: {identificador}...\n{'-'*75}\n")
        time.sleep(2)
        for item in requests_config:
            if identificador in item:
                for config in item[identificador]:
                    temp_file = (
                        ROOT_PATH
                        / f"source/model/data/temp_file_data/{config['relative_path']}_{identificador}.json"
                    )
                    

                    if not temp_file.exists():
                        temp_file.parent.mkdir(parents=True, exist_ok=True)

                        with temp_file.open(mode="w", encoding="utf-8") as temp_file:
                            temp_file.write("[\n")

                            if any(item in config["relative_path"] for item in no_date_api_list):
                                
                                print(f"-Download total para: {config["relative_path"]}\n")
                                time.sleep(1)
                                full_requests(config=config, temp_file=temp_file)

                                temp_file.write("\n]")

                            else:
                                print(f"-Fazendo consulta de data incremental para: {config["relative_path"]} - {identificador}")
                                time.sleep(1)
                                data_inicial = get_initial_date(config=config, incremental_date=True, identificador=identificador)

                                if data_inicial is None:
                                    print(f"Não foi possível obter a data incremental para a tabela: {config['relative_path']}")
                                    temp_file.write("\n]")
                                    continue

                                data_fim = data_final.strftime("%d/%m/%Y")
                                
                                print(f"-Iniciando download incremental para: {config["relative_path"]}")
                                incremental_requests(
                                    config=config, 
                                    data_inicial=data_inicial, 
                                    data_final=data_fim,
                                    temp_file=temp_file,
                                    )

                                temp_file.write("\n]")

                                print(f"-Download incremental de: {config["relative_path"]}, finalizado!\n ")

                    else:
                        print(f"O arquivo para: {config['relative_path']}, já existe na pasta de arquivos temporários.")