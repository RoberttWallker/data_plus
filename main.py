import time
from source.controller.controller import (
    init_creation_db,
    init_creation_requests,
    total_data_requests,
    total_inserter
)

def main():
    while True:
        resposta = input('''
    Escolha uma das opções abaixo:
    1 - Iniciar criação do banco de dados completo.
    2 - Gravar configuração de banco de dados.
    3 - Gravar configuração de nova API. 
    Q - Sair                                                                               
    >>>'''
).upper()
        if resposta == "1":
            init_creation_db()

            init_creation_requests()

            print("Iniciando download de arquivos...\n")
            time.sleep(1)
            total_data_requests()

            time.sleep(1)
            total_inserter()
        
        elif resposta == "2":
            init_creation_db()

        elif resposta == "3":
            init_creation_requests()

        elif resposta == "Q":
            print("Encerrando...")
            time.sleep(1)
            exit()

        else:
            print("Opção incorreta, tente novamente.")


if __name__ == "__main__":
    main()