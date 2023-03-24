import logging
import os
import sys

import pandas as pd
import pyodbc

from settings import env

from .base import BaseDataConnector

# BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# sys.path.append(BASE_PATH)

logger = logging.getLogger(__name__)

from sqlalchemy import create_engine
from sqlalchemy.engine import URL


class MS_SQLDataConnector(BaseDataConnector):
    def __init__(self) -> None:
        super().__init__()
        # connection string
        # f"DRIVER={DATABASE_DRIVER};SERVER={DATABASE_SERVER};DATABASE={DATABASE_NAME};UID={DATABASE_USER};PWD={DATABASE_PWD}"

    def connect(self, connection_string: str):
        for i in range(5):
            try:
                self.connection = pyodbc.connect(connection_string)
                logger.info("sql pyodbc connected!")
                break
            except:
                logger.info("sql connection retry...")

        try:
            self.cursor = self.connection.cursor()
        except:
            return False

        connection_url = URL.create(
            "mssql+pyodbc", query={"odbc_connect": connection_string}
        )

        self.engine = create_engine(url=connection_url, fast_executemany=True)
        # logger.info("SQL Server Connected!")
        return True

    def get_data(self, query: str, connection_string: str):
        status = self.connect(connection_string=connection_string)
        if status:
            self.cursor.execute(query)
            records = self.cursor.fetchall()
            records = [list(record) for record in records]
            self.connection.close()

            return records
        raise Exception("Cannot connect to database! Try Again!")

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
            con=self.engine,
            if_exists=if_exists,
            index=False,
            chunksize=1000000
            # method="multi",
        )
        self.connection.close()

        print("Data added to SQL Server!")

    def update_data(self, query: str, connection_string: str):
        status = self.connect(connection_string=connection_string)
        if status:
            self.cursor.execute(query)
            self.connection.commit()
            self.connection.close()
        raise Exception("Cannot connect to database! Try Again!")

    def remove_data(self, query: str, connection_string: str):
        status = self.connect(connection_string=connection_string)
        if status:
            self.cursor.execute(query)
            self.connection.commit()
            self.connection.close()
        raise Exception("Cannot connect to database! Try Again!")

    def create_table(self, query: str, connection_string: str):
        """
        Create Query Example:
            IF OBJECT_ID('{table_name}') IS NULL
                CREATE TABLE {table_name}(
                col_1 INT,
                col_2 VARCHAR(255),
                col_3 DATETIME
                );
        """
        status = self.connect(connection_string=connection_string)
        if status:
            self.cursor.execute(query)
            self.connection.commit()
            self.connection.close()
        raise Exception("Cannot connect to database! Try Again!")
