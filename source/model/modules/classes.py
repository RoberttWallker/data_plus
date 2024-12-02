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

class Dialects:
    def __init__(self,engine, connection, metadata, identificador_api, authorization, data_inicial, dialect, db_name):
        self.engine = engine
        self.connection = connection
        self.metadata = metadata
        self.identificador_api = identificador_api
        self.authorization = authorization
        self.data_inicial = data_inicial
        self.dialect = dialect
        self.db_name = db_name

class DbSqlite(Dialects):
    def __init__(self, **keyargs):
        super().__init__(**keyargs)

class DbFireBird(Dialects):
    def __init__(self, **keyargs):
        super().__init__(**keyargs)

class DbMySql(Dialects):
    def __init__(self, **keyargs):
        super().__init__(**keyargs)

class DbPostgreSql(Dialects):
    def __init__(self, **keyargs):
        super().__init__(**keyargs)