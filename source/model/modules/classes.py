# Módulo de classes
# Classes para configuração de API
class ConexaoAPI:
    def __init__(self, url_base, identificador, authorization):
        self.url_base = url_base
        self.identificador = identificador
        self.authorization = authorization


class UnidadeAPI:
    def __init__(self, relative_path, body):
        self.relative_path = relative_path
        self.body = body


# Classes para configuração de banco de dados
class ConfigDB:
    def __init__(
        self,
        host,
        port,
        user,
        password,
        dbname,
        identificador_api,
        authorization,
    ):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.dbname = dbname
        self.identificador_api = identificador_api
        self.authorization = authorization


class Dialects:
    def __init__(
        self,
        engine,
        connection,
        metadata,
        identificador_api,
        authorization,
        dialect,
        db_name,
    ):
        self.engine = engine
        self.connection = connection
        self.metadata = metadata
        self.identificador_api = identificador_api
        self.authorization = authorization
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
