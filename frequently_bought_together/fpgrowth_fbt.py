import logging

import pandas as pd
from mlxtend.frequent_patterns import association_rules, fpgrowth
from mlxtend.preprocessing import TransactionEncoder

import config_helper
from frequently_bought_together import BaseFrequentlyBoughtTogether
from utils import sql_connector

logger = logging.getLogger(__name__)


config = config_helper.read_config()


class FPGrowthFBT(BaseFrequentlyBoughtTogether):
    def __init__(self) -> None:
        super().__init__()

    def load_data_from_sql(self, CompanyId, StartDate, EndDate):
        basket_data_query = f"""
            select distinct
                (CAST({config["fbt_data"]["order_date_col"]} as varchar) || '_' ||
                CAST({config["fbt_data"]["user_id_col"]} as varchar)) as InvoiceId,
                {config["fbt_data"]["product_id_col"]}
            from 
                {config["fbt_data"]["transaction_table_name"]}
            where
                {config["fbt_data"]["company_id_col"]} = {CompanyId}
            and
                {config["fbt_data"]["order_date_col"]} >= {StartDate}
            and
                {config["fbt_data"]["order_date_col"]} <= {EndDate}
        ;"""

        connection_string = config["db_config"]["db_connection_string"]
        basket_data = sql_connector.get_data(
            basket_data_query, connection_string=connection_string
        )
        basket_data = pd.DataFrame(
            basket_data, columns=["InvoiceId", config["fbt_data"]["product_id_col"]]
        )

        return basket_data

    def preprocess_data(self, basket_data):  # , product_data
        # convert product_id to string in basket data
        basket_data[config["fbt_data"]["product_id_col"]] = basket_data[
            config["fbt_data"]["product_id_col"]
        ].apply(lambda x: "_" + str(int(x)))

        # one row for each basket with list of products
        basket_data = (
            basket_data.groupby(["InvoiceId"])[config["fbt_data"]["product_id_col"]]
            .apply(list)
            .reset_index()
        )

        return basket_data

    def fit_fpgrowth(self, basket_data):
        # fit TransactionEncoder
        te = TransactionEncoder()
        te.fit(basket_data[config["fbt_data"]["product_id_col"]])

        orders_1hot = te.transform(basket_data[config["fbt_data"]["product_id_col"]])
        orders_1hot = pd.DataFrame(orders_1hot, columns=te.columns_)
        logger.debug(f"Orders_1_hot:{orders_1hot.shape}")

        fp_sup = fpgrowth(
            orders_1hot,
            min_support=float(config["fbt_data"]["min_support"]),
            max_len=2,
            use_colnames=True,
        )
        logger.debug(f"Min support sets:{fp_sup.shape}")

        assn_rules = association_rules(
            fp_sup,
            metric="lift",
            min_threshold=float(config["fbt_data"]["min_threshold"]),
        )

        return assn_rules

    def save_rules_to_sql(self, assn_rules, CompanyId):
        fbt_assn_rules_table_name = "fbt_assn_rules"
        # create if not exist
        if config["db_config"]["dbms"] == "sqlite":
            query_head = f"""
            CREATE TABLE IF NOT EXISTS {fbt_assn_rules_table_name}(
            """
        elif config["db_config"]["dbms"] == "mssql":
            query_head = f"""
            IF OBJECT_ID('{fbt_assn_rules_table_name}') IS NULL
                CREATE TABLE {fbt_assn_rules_table_name}(
            """
        fbt_assn_rules_create_query = (
            query_head
            + f"""
            CompanyId INT,
            antecedents INT,
            consequents INT,
            confidence REAL,
            lift REAL,
            FOREIGN KEY(CompanyId) REFERENCES company_details(CompanyId)
    );"""
        )
        connection_string = config["db_config"]["db_connection_string"]
        sql_connector.create_table(
            query=fbt_assn_rules_create_query, connection_string=connection_string
        )

        # remove old data for same Company
        remove_old_rules_query = f"""
            delete from 
                {fbt_assn_rules_table_name}
            where
                CompanyId={CompanyId}
        ;"""
        connection_string = config["db_config"]["db_connection_string"]
        sql_connector.remove_data(
            query=remove_old_rules_query, connection_string=connection_string
        )

        connection_string = config["db_config"]["db_connection_string"]
        sql_connector.push_data(
            table_name=fbt_assn_rules_table_name,
            data=assn_rules[
                ["CompanyId", "antecedents", "consequents", "confidence", "lift"]
            ],
            connection_string=connection_string,
            if_exists="append",
        )
        logger.info("Rules added to DB!")

    def load_rules_from_sql(self, CompanyId):
        fbt_assn_rules_table_name = "fbt_assn_rules"
        fbt_assn_rules_query = f"""
            SELECT
                antecedents, consequents, confidence, lift
            FROM
                {fbt_assn_rules_table_name}
            WHERE
                CompanyId={CompanyId}
            ;"""
        connection_string = config["db_config"]["db_connection_string"]
        assn_rules = sql_connector.get_data(
            query=fbt_assn_rules_query, connection_string=connection_string
        )
        assn_rules = pd.DataFrame(
            assn_rules, columns=["antecedents", "consequents", "confidence", "lift"]
        )
        return assn_rules

    def predict(self, CompanyId, ProductId, MaxResults=6):
        # load assn_rules
        assn_rules = self.load_rules_from_sql(CompanyId=CompanyId)
        if assn_rules.shape[0] == 0:
            return {
                "code": "False",
                "status": "Failure",
                "message": "No FBT data for given company!",
            }
        logger.info("Rules Loaded from Database!")

        # find rules of given antecedent
        relevant_rules = assn_rules[assn_rules["antecedents"] == ProductId].sort_values(
            "lift", ascending=False
        )

        relevant_rules = relevant_rules["consequents"]  # .apply(iter).apply(next)

        relevant_rules = (
            relevant_rules[:MaxResults].reset_index(drop=True).reset_index()
        )

        relevant_rules["index"] += 1
        relevant_rules = relevant_rules.rename(
            {"index": "rank", "consequents": "FBTProductId"}, axis=1
        )
        relevant_rules = relevant_rules.to_dict("records")

        return {"code": True, "status": "success", "data": relevant_rules}

    def process(self, CompanyId, StartDate, EndDate):
        basket_data = self.load_data_from_sql(CompanyId, StartDate, EndDate)

        if basket_data.shape[0] == 0:
            return {
                "code": False,
                "status": "failure",
                "message": "No data found for given Inputs!",
            }
        logger.info("Data Loaded from Database!")

        basket_data = self.preprocess_data(basket_data=basket_data)
        logger.info("Data Preprocessing Done!")

        assn_rules = self.fit_fpgrowth(basket_data=basket_data)
        logger.info(f"Assosciation Rules Generated {assn_rules.shape}!")

        assn_rules["consequents"] = (
            assn_rules["consequents"]
            .apply(iter)
            .apply(next)
            .apply(lambda x: int(x[1:]))
        )

        assn_rules["antecedents"] = (
            assn_rules["antecedents"]
            .apply(iter)
            .apply(next)
            .apply(lambda x: int(x[1:]))
        )

        assn_rules = assn_rules[["antecedents", "consequents", "confidence", "lift"]]
        assn_rules["CompanyId"] = CompanyId

        self.save_rules_to_sql(assn_rules=assn_rules, CompanyId=CompanyId)
        logger.info("Association Rules Saved to Database!")

        return {
            "code": True,
            "status": "success",
            "message": "FBT processed!",
        }
