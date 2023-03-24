import logging
import os
import sqlite3
import sys

import pandas as pd

from settings import env

from .base import BaseDataConnector

# import pyodbc


BASE_PATH = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(BASE_PATH)

logger = logging.getLogger(__name__)

from sqlalchemy import create_engine
from sqlalchemy.engine import URL


class SQLiteDataConnector(BaseDataConnector):
    def __init__(self) -> None:
        super().__init__()

        # self.connection = sqlite3.connect("../fa_db.db")

        # sqlite_select_Query = "select sqlite_version();"
        # self.cursor.execute(sqlite_select_Query)
        # record = self.cursor.fetchall()
        # logger.info(f"SQLite Database Version is: {record}")

    def connect(self, connection_string: str):
        self.connection = sqlite3.connect(
            BASE_PATH + connection_string
        )  # "/../data/aid_db.db"
        self.cursor = self.connection.cursor()
        # for i in range(5):
        #     try:
        #         self.connection = pyodbc.connect(connection_string)
        #         logger.info("sql pyodbc connected!")
        #         break
        #     except:
        #         logger.info("sql connection retry...")

        # self.cursor = self.connection.cursor()

        # connection_url = URL.create(
        #     "mssql+pyodbc", query={"odbc_connect": connection_string}
        # )

        # self.engine = create_engine(url=connection_url, fast_executemany=True)

        logger.info("SQL Server Connected!")

    def get_data(self, query: str, connection_string: str):
        self.connect(connection_string=connection_string)
        self.cursor.execute(query)
        records = self.cursor.fetchall()
        records = [list(record) for record in records]
        self.connection.close()

        return records

    def push_data(
        self,
        table_name: str,
        data: pd.DataFrame,
        connection_string: str,
        if_exists: str = "fail",
    ):
        print("Pushing data...", data.shape)
        self.connect(connection_string=connection_string)
        data.to_sql(
            name=table_name,
            con=self.connection,
            if_exists=if_exists,
            index=False,
            chunksize=1000000
            # method="multi",
        )
        self.connection.close()

        print("Data added to SQL Server!")

    def update_data(self, query: str, connection_string: str):
        self.connect(connection_string=connection_string)
        self.cursor.execute(query)
        self.connection.commit()
        self.connection.close()

    def remove_data(self, query: str, connection_string: str):
        self.connect(connection_string=connection_string)
        self.cursor.execute(query)
        self.connection.commit()
        self.connection.close()

    def create_table(self, query: str, connection_string: str):
        self.connect(connection_string=connection_string)
        self.cursor.execute(query)
        self.connection.commit()
        self.connection.close()


# sqlite_connector = SQLiteDataConnector()
