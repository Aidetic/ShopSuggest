import logging

import config_helper

from .ms_sql_connector import MS_SQLDataConnector
from .sqlite_connector import SQLiteDataConnector

logger = logging.getLogger(__name__)
config = config_helper.read_config()


def sql_dbms_connector():
    dbms = config["db_config"]["dbms"]
    sql_connector = None
    if dbms == "sqlite":
        sql_connector = SQLiteDataConnector()
    elif dbms == "mssql":
        sql_connector = MS_SQLDataConnector()
    return sql_connector


sql_connector = sql_dbms_connector()
