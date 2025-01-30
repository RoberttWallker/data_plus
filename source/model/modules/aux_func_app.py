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

