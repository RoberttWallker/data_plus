from datetime import datetime, timedelta, date
from pathlib import Path
import requests
import json
import time
import traceback
from model.modules.classes import ConexaoAPI, UnidadeAPI

ROOT_PATH = Path.cwd()
file_requests_config = (
    ROOT_PATH / "source/model/config/requests_config/requests_config.json"
)


no_date_api_list = ["EstoqueAnalitico", "ProdutosCadastrados"]

data_final = datetime.today()
dias_incremento = timedelta(days=120)

<<<<<<< HEAD
def get_initial_date(config):
    data_inicial = None

    if "ProdutosPorOS" in config["relative_path"]:
        data_inicial_str = config['body'].get('DATAINICIAL')
        if data_inicial_str:
            data_inicial = datetime.strptime(
            data_inicial_str, "%d/%m/%Y"
        )
    elif "EntradasEstoque" in config["relative_path"]:
        data_inicial_str = config['body'].get('DATAINICIO')
        if data_inicial_str:
            data_inicial = datetime.strptime(
            data_inicial_str, "%d/%m/%Y"
        )
    elif any(item in config["relative_path"] for item in ["ContasPagarPagas", "ReceberRecebidas"]):
        data_inicial_str = config['body'].get("DUPEMISSAO1")
        if data_inicial_str:
            data_inicial = datetime.strptime(
            data_inicial_str, "%d/%m/%Y"
        )
    dados = (data_inicial, config["relative_path"])

    return dados


=======
# Configurações de APIs
>>>>>>> 123aa947d75d40024462cd796a6cc8010a8f30db
def save_requests_config(conexao_api, unidade_api):
    file_name = file_requests_config

    file_name.parent.mkdir(parents=True, exist_ok=True)

    request_config = {
        "url_base": conexao_api.url_base,
        "identificador": conexao_api.identificador,
        "authorization": conexao_api.authorization,
        "relative_path": unidade_api.relative_path,
        "body": unidade_api.body,
    }

    try:
        with open(file_name, "r") as file:
            requests_config = json.load(file)
            for config in requests_config:
                if (
                    config["identificador"] == request_config["identificador"]
                    and config["relative_path"] == request_config["relative_path"]
                ):
                    print(
                        f'A Request: {request_config['relative_path']} com o identificador: {request_config["identificador"]} já existe!'
                    )
                    return

    except (FileNotFoundError, json.JSONDecodeError):
        requests_config = []

    requests_config.append(request_config)

    with open(file_name, "w") as file:
        json.dump(requests_config, file, indent=4)


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
            chave = input("Chave: ")
            if (
                chave.lower() == "fim"
            ):  # Permite ao usuário terminar a inserção de parâmetros
                break
            valor = input(f"Valor para {chave}: ")
            body[chave] = valor

        try:
            conexao_api = ConexaoAPI(url_base, identificador, authorization)
            unidade_api = UnidadeAPI(relative_path, body)
            save_requests_config(conexao_api, unidade_api)
            print("\nConfiguração API cadastrada com sucesso.")
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

    headers = {
        "Identificador": config["identificador"],
        "Authorization": config["authorization"],
        "Content-Type": "application/json",
    }

    while data_inicial < data_final: # type: ignore

        data_final_periodo = min(data_inicial + dias_incremento, data_final)

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


# Métodos de DATA
def get_initial_date(config):
    data_inicial = None
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


# Download de dados das APIs
def request_memory_saving():
    file_name = file_requests_config

    if not file_name.exists():
        print("Arquivo de configurações de APIs não existe!")
        return

    with open(file_name, "r") as file:
        requests_config = json.load(file)

        for config in requests_config:
            temp_file = (
                ROOT_PATH
                / f"source/model/data/temp_file_data/{config['relative_path']}.json"
            )
            
            if not temp_file.exists():

                temp_file.parent.mkdir(parents=True, exist_ok=True)

<<<<<<< HEAD
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

            with temp_file.open(mode="w", encoding="utf-8") as temp_file:
                temp_file.write("[\n")
=======
                with temp_file.open(mode="w", encoding="utf-8") as temp_file:
                    temp_file.write("[\n")
>>>>>>> 123aa947d75d40024462cd796a6cc8010a8f30db

                    if any(item in config["relative_path"] for item in no_date_api_list):
                        
                        full_requests(config=config, temp_file=temp_file)

<<<<<<< HEAD
                if any(item in config["relative_path"] for item in no_date_api_list):
                    
                    if response.status_code == 200:
                        data = response.json()

                        json.dump(data, temp_file, ensure_ascii=False)
                    else:
                        raise Exception(
                            f"Erro na requisição: {response.status_code} - {response.text}"
                        )
                    temp_file.write("\n]")

                else:    
                    data_inicial, api_nome = get_initial_date(config)
                    if not data_inicial:
                        print(f"Não existe Data Inicial, nos parâmetros de {api_nome}. \nIgorando essa requisição...")
                        time.sleep(1)
                        continue
                        

                    while data_inicial < data_final: # type: ignore
                        data_final_periodo = min(data_inicial + dias_incremento, data_final)
                    
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

                    temp_file.write("\n]")
=======
                        temp_file.write("\n]")
                    else:

                        data_inicial = get_initial_date(config=config)

                        if data_inicial is None:
                            print(f"Verifique as configurações de: {json.dumps(config, indent=4)}.")
                            temp_file.write("\n]")
                            continue
                        
                        chunks_requests(
                            config=config, 
                            data_inicial=data_inicial, 
                            data_final=data_final, 
                            dias_incremento=dias_incremento, 
                            temp_file=temp_file,
                            )

                        temp_file.write("\n]")
            else:
                print(f"O arquivo para: {config['relative_path']}, já existe na pasta de arquivos temporários.")
>>>>>>> 123aa947d75d40024462cd796a6cc8010a8f30db

