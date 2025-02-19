import subprocess
import time
import json

from .constants import (ROOT_PATH, TASK_PATH, ghost_incremental_vbs_file,
                        task_incremental_bat_file, file_token)

no_date_api_list = ["EstoqueAnalitico", "ProdutosCadastrados"]

def delete_temp_files(temp_file_path):
    for item in temp_file_path.glob("*"):
        try:
            if item.is_file():
                item.unlink()
            elif item.is_dir():
                for sub_item in item.glob("*"):
                    sub_item.unlink() if sub_item.is_file() else sub_item.rmdir()
                item.rmdir()
        except Exception as e:
            print(f"Erro ao remover {item}: {e}")

    print("Arquivos e pastas temporárias, removidos com sucesso.")

def formatar_nome_para_root(nome):
    # Divide o nome pelas sublinhas, capitaliza cada palavra e adiciona "Grid" no final
    partes = nome.split('_')
    partes_capitalizadas = [parte.capitalize() for parte in partes]
    return ''.join(partes_capitalizadas)

def ghost_exec_creation():
    bat_file = task_incremental_bat_file
    vbs_file = ghost_incremental_vbs_file

    task_files = [bat_file, vbs_file]

    for file in task_files:
        file.parent.mkdir(parents=True, exist_ok=True)

    if bat_file.exists() and vbs_file.exists():
        print("Os arquivos BAT e VBS já existem. Nenhuma ação foi realizada.\n")
        return  # Sai da função se os arquivos já existem

    venv_folder = None
    for folder in ROOT_PATH.iterdir():
        if folder.is_dir() and (folder / "pyvenv.cfg").exists():
            venv_folder = folder.name
            break

    if venv_folder is None:
        print(
            "Ambiente virtual não encontrado. Certifique-se de que existe uma pasta com 'pyvenv.cfg'."
        )
        return

    bat_content = f"""@echo off
cd "{ROOT_PATH}"
call {venv_folder}\\Scripts\\activate
python source\\controller\\incremental_schedule.py
"""
    with open(bat_file, "w") as bat_file_name:
        bat_file_name.write(bat_content)
    print(f"Arquivo BAT criado em: {bat_file}")

    # Criação do arquivo .vbs
    vbs_content = f'''Set WshShell = CreateObject("WScript.Shell")
WshShell.Run """{str(bat_file)}""", 0, False
'''
    with open(vbs_file, "w") as vbs_file_name:
        vbs_file_name.write(vbs_content)
    print(f"Arquivo VBS criado em: {vbs_file}\n")

def create_task_scheduler_windows():
    bat_file_temp = TASK_PATH / "bat_file_temp.bat"

    bat_content = f"""
@echo off
setlocal

:: Definição de variáveis
set "TASK_NAME=GhostExecIncrementalSavWin"
set "VBS_PATH={ghost_incremental_vbs_file}"

:: Criar a tarefa agendada
schtasks /create /tn "%TASK_NAME%" /tr "wscript.exe \"%VBS_PATH%\"" /sc HOURLY /F /RL HIGHEST /RU SYSTEM

:: Verificar se a tarefa foi criada com sucesso
if %errorlevel%==0 (
    echo Tarefa criada com sucesso!
) else (
    echo Falha ao criar a tarefa.
)

pause
exit
"""
    with open(bat_file_temp, "w") as bft:
        bft.write(bat_content)
    print(f"Arquivo BAT criado em: {bat_file_temp}")

    if not bat_file_temp.exists():
        print(f"Arquivo {bat_file_temp} não encontrado!")
        return
    
    try:
        # Executar o .bat como administrador usando PowerShell
        subprocess.run([
            "powershell",
            "-Command",
            f"Start-Process cmd.exe -ArgumentList '/c \"{bat_file_temp}\"' -Verb RunAs"
        ], check=True)
        
        print(f"{bat_file_temp} executado com sucesso!")

        time.sleep(1)
        bat_file_temp.unlink()

    except subprocess.CalledProcessError as e:
        print(f"Ocorreu um erro ao executar o script: {e}")

def get_identifiers(file):
    identifiers = []
    
    for item in file:
        if isinstance(item, dict):
            identifiers.extend(item.keys())
    
    return identifiers

def create_token_file():
    token = input("Insira o seu Token Data Plus Engine: ").strip()
    identificador_savwin = input("Insira o identificador SavWin: ").strip()

    if token:
        try:
            with open(file_token, 'w', encoding='utf-8') as ft:
                json.dump({"identificador": identificador_savwin, "token": token}, ft, indent=4)
                print("Token salvo com sucesso!")

        except PermissionError:
            print("Erro: Permissão negada para gravar o arquivo.")
        except FileNotFoundError:
            print("Erro: Caminho do arquivo não encontrado.")
        except OSError as e:
            print(f"Erro do sistema operacional: {e}")
        except Exception as e:
            print(f"Ocorreu um erro inesperado: {e}")