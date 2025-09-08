from connectors.snowflake import Snowflake
from typing import Hashable, MutableMapping, Type, Optional, Union, Dict, Any

DATABASE_BY_SCHEME = {
    "snowflake": Snowflake,
    "databricks": None,
    
}

class Connect(): 

    database_by_scheme: Dict
    _conn : Any
    _dialects: Any

    def __init__(self, database_by_scheme):
        super().__init__()
        self.database_by_scheme = database_by_scheme

    def __call__(
        self, **kwargs
    ):

        db = self.database_by_scheme[kwargs['scheme']]
        scheme = kwargs['scheme']
        _conn = self.connect_to_uri(db, **kwargs)
        return _conn

    def connect_to_uri(self, db, scheme, **kwargs):
        if scheme == "snowflake":
            kw = {}
            kw["account"] = kwargs['account']
            kw["user"] = kwargs['user']
            kw["password"] = kwargs['password']
            _conn = db(**kwargs)
            return _conn

    def _connection_created(self, db):
        "Nop function to be overridden by subclasses."
        return db

connect = Connect(DATABASE_BY_SCHEME)




