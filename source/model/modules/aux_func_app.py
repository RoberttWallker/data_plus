from pathlib import Path
import subprocess
import time

SRC_PATH = Path(__file__).resolve().parent.parent.parent
ROOT_PATH = Path(__file__).cwd()

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

    task_path = "controller/tasks"
    
    bat_file = SRC_PATH / task_path / "task_exec_incremental.bat"
    vbs_file = SRC_PATH / task_path / "ghost_exec_task.vbs"

    task_files = [bat_file, vbs_file]

    for file in task_files:
        file.parent.mkdir(parents=True, exist_ok=True)

    if Path(bat_file).exists() and Path(vbs_file).exists():
        print("Os arquivos BAT e VBS já existem. Nenhuma ação foi realizada.\n")
        return  # Sai da função se os arquivos já existem

    root = ROOT_PATH
    venv_folder = None

    for folder in root.iterdir():
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

    vbs_file_ghost_exec = SRC_PATH / "controller" / "tasks" / "ghost_exec_task.vbs"
    bat_file_temp = SRC_PATH / "controller" / "tasks" / "bat_file_temp.bat"

    bat_content = f"""
@echo off
setlocal

:: Definição de variáveis
set "TASK_NAME=GhostExecIncrementalSavWin"
set "VBS_PATH={vbs_file_ghost_exec}"

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
    with open(bat_file_temp, "w") as bat_file_name:
        bat_file_name.write(bat_content)
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