import logging

import pandas as pd

import config_helper
from category_bestsellers import BaseCategoryBestsellers
from utils import sql_connector

logger = logging.getLogger(__name__)

config = config_helper.read_config()


class CategoryBestsellers(BaseCategoryBestsellers):
    def __init__(self) -> None:
        super().__init__()

    def get_bestseller_data_from_sql(
        self,
        CompanyId,
        StartDate,
        EndDate,
    ):
        bestseller_query = f"""
            SELECT
                {config["bestseller_data"]["primary_category_col"]},
                {config["bestseller_data"]["secondary_category_col"]},
                a.{config["bestseller_data"]["product_id_col"]},
                Quantity,
                Revenue
            FROM
            (
                SELECT
                    {config["bestseller_data"]["product_id_col"]},
                    sum({config["bestseller_data"]["quantity_col"]}) as Quantity,
                    sum({config["bestseller_data"]["revenue_col"]}) as Revenue
                from
                    {config["bestseller_data"]["transaction_table_name"]}
                where
                    {config["bestseller_data"]["company_id_col"]} = {CompanyId}
                and
                    {config["bestseller_data"]["order_date_col"]} >= {StartDate}
                and
                    {config["bestseller_data"]["order_date_col"]} <={EndDate}
                group by
                    {config["bestseller_data"]["product_id_col"]}
                ) a
            INNER JOIN
                {config["bestseller_data"]["product_table_name"]} b
            ON 
                a.{config["bestseller_data"]["product_id_col"]} =  b.{config["bestseller_data"]["product_id_col"]}
        ;"""
        connection_string = config["db_config"]["db_connection_string"]
        bestseller_data = sql_connector.get_data(
            query=bestseller_query, connection_string=connection_string
        )

        bestseller_data = pd.DataFrame(
            bestseller_data,
            columns=[
                config["bestseller_data"]["primary_category_col"],
                config["bestseller_data"]["secondary_category_col"],
                config["bestseller_data"]["product_id_col"],
                "Quantity",
                "Revenue",
            ],
        )
        return bestseller_data

    def save_bestseller_data_to_sql(self, bestseller_data, CompanyId):
        bestseller_data_table_name = "bestseller_data"
        # create if not exist
        if config["db_config"]["dbms"] == "sqlite":
            query_head = f"""
            CREATE TABLE IF NOT EXISTS {bestseller_data_table_name}(
            """
        elif config["db_config"]["dbms"] == "mssql":
            query_head = f"""
            IF OBJECT_ID('{bestseller_data_table_name}') IS NULL
                CREATE TABLE {bestseller_data_table_name}(
            """

        bestseller_data_create_query = (
            query_head
            + f"""
            {config["bestseller_data"]["company_id_col"]} INT,
            {config["bestseller_data"]["primary_category_col"]} VARCHAR(255),
            {config["bestseller_data"]["secondary_category_col"]} VARCHAR(255),
            {config["bestseller_data"]["product_id_col"]} VARCHAR(255),
            Quantity INT,
            Revenue REAL,
            FOREIGN KEY(CompanyId) REFERENCES company_details(CompanyId)
    );"""
        )
        connection_string = config["db_config"]["db_connection_string"]
        sql_connector.create_table(
            query=bestseller_data_create_query, connection_string=connection_string
        )

        # remove old data for same Company
        remove_old_bestseller_data_query = f"""
            delete from 
                {bestseller_data_table_name}
            where
                {config["bestseller_data"]["company_id_col"]}={CompanyId}
        ;"""
        connection_string = config["db_config"]["db_connection_string"]
        sql_connector.remove_data(
            query=remove_old_bestseller_data_query, connection_string=connection_string
        )

        connection_string = config["db_config"]["db_connection_string"]
        sql_connector.push_data(
            table_name=bestseller_data_table_name,
            data=bestseller_data[
                [
                    config["bestseller_data"]["company_id_col"],
                    config["bestseller_data"]["primary_category_col"],
                    config["bestseller_data"]["secondary_category_col"],
                    config["bestseller_data"]["product_id_col"],
                    "Quantity",
                    "Revenue",
                ]
            ],
            connection_string=connection_string,
            if_exists="append",
        )
        print("Bestseller data added to DB!")

    def find_bestseller(
        self,
        CompanyId,
        CategoryLevel,
        CategoryValue,
        BestsellerMetric,
        MaxResults=10,
    ):
        find_bestseller_query = f"""
        SELECT
            {config["bestseller_data"]["primary_category_col"]},
            {config["bestseller_data"]["secondary_category_col"]},
            {config["bestseller_data"]["product_id_col"]},
            Quantity,
            Revenue
        FROM
            bestseller_data
        WHERE
            {config["bestseller_data"]["company_id_col"]} = {CompanyId}
        AND
            {CategoryLevel} = '{CategoryValue}'
        ORDER BY 
            {BestsellerMetric} DESC
        LIMIT {MaxResults}"""

        connection_string = config["db_config"]["db_connection_string"]

        bestseller_data = sql_connector.get_data(
            query=find_bestseller_query, connection_string=connection_string
        )

        if len(bestseller_data) == 0:
            return {
                "code": False,
                "status": "failure",
                "message": "Bestsellers not found for given Input!",
            }
        logger.info("Data Loaded from Database!")

        bestseller_data = pd.DataFrame(
            bestseller_data,
            columns=[
                config["bestseller_data"]["primary_category_col"],
                config["bestseller_data"]["secondary_category_col"],
                config["bestseller_data"]["product_id_col"],
                "Quantity",
                "Revenue",
            ],
        )

        return {
            "code": True,
            "status": "success",
            "data": bestseller_data.to_dict("records"),
        }

    def process(self, CompanyId, StartDate, EndDate):
        bestseller_data = self.get_bestseller_data_from_sql(
            CompanyId, StartDate, EndDate
        )
        if bestseller_data.shape[0] == 0:
            return {
                "code": False,
                "status": "failure",
                "message": "No data found for given Inputs!",
            }
        logger.info("Data Loaded from Database!")

        bestseller_data[config["bestseller_data"]["company_id_col"]] = CompanyId

        self.save_bestseller_data_to_sql(
            bestseller_data=bestseller_data, CompanyId=CompanyId
        )
        logger.info("Bestseller Data Saved to Database!")

        return {
            "code": True,
            "status": "success",
            "message": "Bestseller data Processed!",
        }
