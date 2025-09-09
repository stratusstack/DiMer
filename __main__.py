from diffforge.core.compare import Diffcheck
from diffforge.core.models import ConnectionConfig
from diffforge.core.factory import ConnectorFactory


def create_connection(db_type, **kw ):
    config = ConnectionConfig(**kw)
    connector = ConnectorFactory.create_connector(db_type, config)
    connector.connect()
    return connector


def main(**kw):

    db1_type = kw['database1']['type']
    db2_type = kw['database2']['type']

    conn1 = create_connection(db1_type, **kw['database1']['config'])
    conn2 = create_connection(db2_type, **kw['database2']['config'])
    diff_check = Diffcheck(conn1, conn2, kw['database1'], kw['database2'])

    if(db1_type == db2_type):
        diff_check.data_diff('JOIN_DIFF')
   

if __name__ == "__main__":
    main(**kw)