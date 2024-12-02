from datetime import datetime, timedelta, date
from pathlib import Path
import requests
import json
import time
from model.modules.classes import ConexaoAPI, UnidadeAPI

ROOT_PATH = Path.cwd()
file_requests_config = ROOT_PATH / 'source/model/config/requests_config.json'
file_requests_config.parent.mkdir(parents=True, exist_ok=True)

lista_api_sem_data_inicial = ['EstoqueAnalitico', 'ProdutosCadastrados']

data_final = datetime.today()
dias_incremento = timedelta(days=120)

def save_requests_config(conexao_api, unidade_api):
    file_name = file_requests_config

    request_config = {
        'url_base': conexao_api.url_base,
        'identificador': conexao_api.identificador,
        'authorization': conexao_api.authorization,
        'relative_path': unidade_api.relative_path,
        'body': unidade_api.body
    }

    try:
        with open(file_name, 'r') as file:
            requests_config = json.load(file)
            for config in requests_config:
                if config['identificador'] == request_config['identificador'] and config['relative_path'] == request_config['relative_path']:
                    print(f'A Request: {request_config['relative_path']} com o identificador: {request_config["identificador"]} já existe!')
                    return

    except (FileNotFoundError, json.JSONDecodeError):
        requests_config = []

    requests_config.append(request_config)

    with open(file_name, 'w') as file:
        json.dump(requests_config, file, indent=4)

def initiate_config():
    print('Insira os dados solicitados para configurar a consulta da API')
    url_base = input('Informe a URL base da API, Ex(https://api.savwinweb.com.br/api/): ')
    identificador = input('Identificador: ')
    authorization = input('Authorization: ')
    relative_path = input('Pasta relativa, Ex(Relatorios/EstoqueAnaliticoGrid): ')
    body = {}

    # Agora, solicitamos os parâmetros chave-valor do usuário
    print('''
###########################################################################################
    Insira os parâmetros da consulta no formato chave=valor. Digite 'fim' para encerrar.
###########################################################################################
          ''')
    while True:
        chave = input("Chave: ")
        if chave.lower() == 'fim':  # Permite ao usuário terminar a inserção de parâmetros
            break
        valor = input(f"Valor para {chave}: ")
        body[chave] = valor

    print("\nConfiguração API cadastrada com sucesso.")

    conexao_api = ConexaoAPI(url_base, identificador, authorization)
    unidade_api = UnidadeAPI(relative_path, body)

    save_requests_config(conexao_api, unidade_api)

def request_memory_saving():
    file_name = file_requests_config

    if not file_name.exists():
        print('Arquivo de configurações de APIs não existe!')
        return
    
    with open(file_name, 'r') as file:
        requests_config = json.load(file)

        for config in requests_config:
            temp_file = ROOT_PATH / f"source/model/data/temp_file_data/{config['relative_path']}.json"

            temp_file.parent.mkdir(parents=True, exist_ok=True)

            with temp_file.open(mode='w', encoding='utf-8') as temp_file:
                temp_file.write("[\n") 
            
                total_iteracoes = 0

                if any(item in config['relative_path'] for item in lista_api_sem_data_inicial):
                    headers = {
                    'Identificador': config['identificador'],
                    'Authorization': config['authorization'],
                    'Content-Type': 'application/json'
                    }
                    response = requests.post(f"{config['url_base']}{config['relative_path']}", headers=headers, json=config['body'], stream=True)
                    if response.status_code == 200:
                        data = response.json()

                        json.dump(data, temp_file, ensure_ascii=False)
                    else:
                        raise Exception(f"Erro na requisição: {response.status_code} - {response.text}")

                if "ProdutosPorOS" in config['relative_path']:
                    data_inicial = datetime.strptime(config['body']['DATAINICIAL'], "%d/%m/%Y")
                elif "EntradasEstoque" in config['relative_path']:
                    data_inicial = datetime.strptime(config['body']['DATAINICIO'], "%d/%m/%Y")
                elif "ContasPagarPagas" in config['relative_path']:
                    data_inicial = datetime.strptime(config['body']['DUPEMISSAO1'], "%d/%m/%Y")
                elif "ReceberRecebidas" in config['relative_path']:
                    data_inicial = datetime.strptime(config['body']['DUPEMISSAO1'], "%d/%m/%Y")

                headers = {
                    'Identificador': config['identificador'],
                    'Authorization': config['authorization'],
                    'Content-Type': 'application/json'
                    }
                
                while data_inicial < data_final:
                    data_final_periodo = min(data_inicial + dias_incremento, data_final)
                    response = requests.post(f"{config['url_base']}{config['relative_path']}", headers=headers, json=config['body'], stream=True)

                    if response.status_code == 200:
                        data = response.json()

                        if total_iteracoes > 0:
                            temp_file.write(",\n")

                        json.dump(data, temp_file, ensure_ascii=False)
                        total_iteracoes += 1
                    else:
                        raise Exception(f"Erro na requisição: {response.status_code} - {response.text}")
                    
                    data_inicial = data_final_periodo
                    time.sleep(1)
                
                temp_file.write("\n]")

