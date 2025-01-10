import time
from source.controller.controller import (
    init_creation_db,
    init_creation_requests,
    total_data_requests,
    total_inserter,
    create_column_incremental
)

def main():
    while True:
        resposta = input('''
Escolha uma das opções abaixo:
1 - Iniciar criação do banco de dados completo.
2 - Gravar configuração de banco de dados.
3 - Gravar configuração de nova API.
4 - Criar colunas para atualização incremental no Power BI.
Q - Sair                                                                               
>>> '''
).upper()
        if resposta == "1":
            while True:
                resposta_2 = input('Deseja inserir as configurações de um novo banco de dados? s/n\n>>> ').lower()
                if resposta_2 == "s":
                    time.sleep(1)
                    init_creation_db()
                    break
                elif resposta_2 == "n":
                    print('Nenhum banco de dados adicionado.\n')
                    time.sleep(1)
                    break
                else:
                    print("Opção inválida.\n")
                    time.sleep(1)
                
            while True:
                resposta_3 = input('Deseja inserir as configurações das requisições API? s/n\n>>> ').lower()
                if resposta_3 == "s":
                    time.sleep(1)
                    init_creation_requests()
                    break
                elif resposta_3 == "n":
                    print('Nenhuma requisição de API adicionada.\n')
                    time.sleep(1)
                    break
                else:
                    print("Opção inválida.\n")
                    time.sleep(1)

            print("Iniciando download de arquivos...\n")
            time.sleep(1)
            total_data_requests()

            time.sleep(1)
            total_inserter()
        
        elif resposta == "2":
            init_creation_db()

        elif resposta == "3":
            init_creation_requests()
        
        elif resposta == "4":
            create_column_incremental()

        elif resposta == "Q":
            print("Encerrando...")
            time.sleep(1)
            exit()

        else:
            print("Opção incorreta, tente novamente.")


if __name__ == "__main__":
    main()