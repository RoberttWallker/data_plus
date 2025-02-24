import time

from source.controller.controller import (create_column_incremental,
                                          create_scheduler_windows,
                                          init_creation_db,
                                          init_creation_requests,
                                          init_incremental_update,
                                          manager_init_database,
                                          total_data_requests, total_inserter)


def main():
    while True:
        resposta = input('''
Escolha uma das opções abaixo:
1 - Iniciar criação do banco de dados completo.
2 - Gravar configuração de banco de dados.
3 - Gravar configuração de nova API.
4 - Criar colunas para atualização incremental no Power BI.
5 - Fazer atualização incremental.
6 - Criar tarefa de atualização incremental, no Agendador de
    Tarefas do Windows.
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
                    manager_init_database()                    
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
            #Fazendo download de dados e capturando o identificador do cliente.
            identificador = total_data_requests()

            time.sleep(1)
            total_inserter(identificador)
        
        elif resposta == "2":
            init_creation_db()

        elif resposta == "3":
            init_creation_requests()
        
        elif resposta == "4":
            create_column_incremental()
        
        elif resposta == "5":
            init_incremental_update()
        
        elif resposta == "6":
            create_scheduler_windows()

        elif resposta == "Q":
            print("Encerrando...")
            time.sleep(1)
            exit()

        else:
            print("Opção incorreta, tente novamente.")


if __name__ == "__main__":
    main()