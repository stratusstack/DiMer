import snowflake.connector
import pandas as pd
from typing import Any, ClassVar, Union, List, Type, Optional

class Snowflake:
    _conn: Any
    _dialects: Any
    _cursor: Any

    def __init__(self, **kw):
        self._conn = snowflake.connector.connect(**kw)
        self._dialects= {
            "hash": "HASH({COL})",
            "concatination" : "||"
        }
        self._cursor = self._conn.cursor()
        print("Snowflake Connection Established")
