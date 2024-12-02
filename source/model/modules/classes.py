#Módulo de classes
#Classes para configuração de API
class ConexaoAPI():
    def __init__(self, url_base, identificador, authorization):
        self.url_base = url_base
        self.identificador = identificador
        self.authorization = authorization

class UnidadeAPI():
    def __init__(self, relative_path, body):
        self.relative_path = relative_path
        self.body = body

#Classes para configuração de banco de dados