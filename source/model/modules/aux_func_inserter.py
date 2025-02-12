from datetime import datetime
import re
import ijson

# Perfil das tabelas
def obter_colunas(arquivo):
    with open(arquivo, "r", encoding="utf-8") as f:
        try:
            # Tenta acessar como uma lista de listas
            parser = ijson.items(f, "item.item")
            primeiro_dicionario = next(parser)
        except StopIteration:
            # Se falhar, volta ao início do arquivo e tenta como lista simples
            f.seek(0)  # Reinicia a leitura do arquivo
            try:
                parser = ijson.items(f, "item")
                primeiro_dicionario = next(parser)
            except StopIteration:
                print(f"Erro: O arquivo {arquivo} não contém dados válidos.")
                return {}
            except Exception as e:
                print(f"Erro inesperado ao processar {arquivo}: {e}")
                return {}

        return primeiro_dicionario

def tabelas_e_colunas(path):
    tabela_e_colunas = []
    for file in path.rglob("*.json"):
        try:
            file_name = file.name.split("Grid")[0]
            table_name = re.sub(r"([a-z])([A-Z])", r"\1_\2", file_name).lower()
            colunas = obter_colunas(file)

            if not colunas:
                print(f"Arquivo {file} não contém dados válidos.")
                continue  # Ignora o arquivo se não houver dados válidos

        except Exception as e:
            print(f"Erro ao processar o arquivo {file}: {e}")
            continue  # Ignora o arquivo e continua o loop
        
        tabela_e_colunas.append((table_name, colunas))

    return tabela_e_colunas

def tabelas_e_dados(path):
    tabelas_e_dados = []
    for file in path.rglob("*.json"):
        file_name = file.name.split("Grid")[0]
        table_name = re.sub(r"([a-z])([A-Z])", r"\1_\2", file_name).lower()
        tabelas_e_dados.append((table_name, file))
    return tabelas_e_dados


