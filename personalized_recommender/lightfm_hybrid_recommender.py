import json
import logging
import os
from datetime import datetime

import joblib
import numpy as np
import pandas as pd
from lightfm import LightFM
from lightfm.data import Dataset

# Import LightFM's evaluation metrics
from lightfm.evaluation import auc_score

# Import repo's evaluation metrics
from recommenders.models.lightfm.lightfm_utils import similar_items

import config_helper
from personalized_recommender import BasePersonalizedRecommender
from utils import sql_connector

logger = logging.getLogger(__name__)

config = config_helper.read_config()


class LightFMHybridRecommender(BasePersonalizedRecommender):
    def __init__(self) -> None:
        super().__init__()

    def load_data_from_sql(
        self, CompanyId, StartDate, EndDate, user_feature_list, item_feature_list
    ):
        user_data_query = f"""
            SELECT
                {config["light_fm"]["user_id_col"]},
                {str(user_feature_list)[1:-1].replace("'", "")}
            from
                {config["light_fm"]["user_table_name"]}
            where
                {config["light_fm"]["company_id_col"]} = {CompanyId}
        ;"""
        connection_string = config["db_config"]["db_connection_string"]
        user_data = sql_connector.get_data(
            query=user_data_query, connection_string=connection_string
        )

        user_data = pd.DataFrame(
            user_data,
            columns=[config["light_fm"]["user_id_col"]] + user_feature_list,
        )
        print("Fetched user_data!")

        # product data
        product_data_query = f"""
            SELECT
                {config["light_fm"]["product_id_col"]},
                {str(item_feature_list)[1:-1].replace("'", "")}
            from
                {config["light_fm"]["product_table_name"]}
            where
                {config["light_fm"]["company_id_col"]} = {CompanyId}
        ;"""
        connection_string = config["db_config"]["db_connection_string"]
        product_data = sql_connector.get_data(
            query=product_data_query, connection_string=connection_string
        )

        product_data = pd.DataFrame(
            product_data,
            columns=[config["light_fm"]["product_id_col"]] + item_feature_list,
        )
        print("Fetched product_data!")

        # transaction data
        transaction_data_query = f"""
            SELECT
                {config["light_fm"]["user_id_col"]},
                {config["light_fm"]["product_id_col"]},
                {config["light_fm"]["rating_col"]}
            from
                {config["light_fm"]["transaction_table_name"]}
            where
                {config["light_fm"]["company_id_col"]} = {CompanyId}
            and
                {config["light_fm"]["order_date_col"]} >= {StartDate}
            and
                {config["light_fm"]["order_date_col"]} <={EndDate}
            and
                {config["light_fm"]["quantity_col"]} > 0
            and
                {config["light_fm"]["revenue_col"]} > 0
        ;"""
        connection_string = config["db_config"]["db_connection_string"]
        transaction_data = sql_connector.get_data(
            query=transaction_data_query, connection_string=connection_string
        )

        transaction_data = pd.DataFrame(
            transaction_data,
            columns=[
                config["light_fm"]["user_id_col"],
                config["light_fm"]["product_id_col"],
                config["light_fm"]["rating_col"],
            ],
        )
        print("Fetched transaction_data!")

        return user_data, product_data, transaction_data

    def preprocess_data(
        self,
        user_data,
        product_data,
        transaction_data,
    ):
        rating_col = config["light_fm"]["rating_col"]
        transaction_data = (
            transaction_data.groupby(
                [
                    config["light_fm"]["user_id_col"],
                    config["light_fm"]["product_id_col"],
                ]
            )
            .agg({rating_col: sum})
            .reset_index()
        )

        transaction_data[rating_col] = transaction_data[rating_col].clip(
            upper=(
                transaction_data[rating_col].mean()
                + (transaction_data[rating_col].std() * 3)
            )
        )

        col_to_norm = rating_col
        new_max = 5.4
        new_min = 0.5
        transaction_data[col_to_norm] = np.round(
            (
                (transaction_data[col_to_norm] - transaction_data[col_to_norm].min())
                / (
                    transaction_data[col_to_norm].max()
                    - transaction_data[col_to_norm].min()
                )
            )
            * (new_max - new_min)
            + new_min
        ).astype(int)

        # user data
        user_data = user_data[
            user_data[config["light_fm"]["user_id_col"]].isin(
                transaction_data[config["light_fm"]["user_id_col"]].unique()
            )
        ]

        # product data
        product_data = product_data[
            product_data[config["light_fm"]["product_id_col"]].isin(
                transaction_data[config["light_fm"]["product_id_col"]].unique()
            )
        ]

        return user_data, product_data, transaction_data

    def prepare_unique_value_set(self, feature_list: list, feature_data: pd.DataFrame):
        unique_value_set = []

        col_name_list = []
        value_list = []
        for feature in feature_list:
            column_unique_values = feature_data[feature].unique()
            col_name_list += [feature] * len(column_unique_values)
            value_list += list(column_unique_values)

        for x, y in zip(col_name_list, value_list):
            res = str(x) + ":" + str(y)
            unique_value_set.append(res)
        return unique_value_set

    def fit_dataset(self, transaction_data, user_features, item_features):
        dataset = Dataset()
        dataset.fit(
            transaction_data[config["light_fm"]["user_id_col"]],
            transaction_data[config["light_fm"]["product_id_col"]],
            item_features=item_features,
            user_features=user_features,
        )

        interactions, weights = dataset.build_interactions(
            transaction_data.iloc[:, 0:3].values
        )

        return dataset, interactions, weights

    def feature_colon_value(self, feat_list, val_list):
        """
        Adds all corresponding feat_list values to val_list values
        For example:
        output = ['feat_list_1:val_list_1', 'feat_list_2:val_list_2', 'feat_list_3:val_list_3']
        """
        result = []
        ll = feat_list
        aa = val_list
        for x, y in zip(ll, aa):
            res = str(x) + ":" + str(y)
            result.append(res)
        return result

    def build_feature_tuples(self, feature_data):
        return feature_data.apply(
            lambda x: (x[0], self.feature_colon_value(x[1:].index, x[1:])), axis=1
        ).values.tolist()

    def predict(self, dataset, model, UserId, MaxResults=10):
        user_id_map, user_feature_map, item_id_map, item_feature_map = dataset.mapping()

        # predict for existing user
        user_x = user_id_map.get(UserId, None)
        if user_x is None:
            return None
        n_items = len(item_id_map.keys())  # no of users * no of items
        output = pd.Series(
            np.argsort(model.predict(user_x, np.arange(n_items)))[-MaxResults:][::-1]
        )  # means predict for all

        output = output.map(
            lambda x: [i for i in item_id_map if item_id_map[i] == x][0]
        )

        return output.values.tolist()

    def find_latest_model(self, CompanyId: int):
        BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        PATH_NAME = os.path.join("..", "model")

        all_models = [
            path
            for path in os.listdir(os.path.join(BASE_PATH, PATH_NAME))
            if str(CompanyId) in path
        ]
        if not all_models:
            return None
        latest_model = max(all_models, key=lambda x: int(x[-12:-4]))
        logger.info(f"Latest model is: {latest_model}")
        return os.path.join(BASE_PATH, PATH_NAME, latest_model)

    def load_and_predict(self, CompanyId, UserId, MaxResults=10):
        model_save_path = self.find_latest_model(CompanyId=CompanyId)
        if model_save_path is None:
            return {
                "code": False,
                "status": "failure",
                "message": "No Saved model found for given Company!",
            }

        item_features, dataset, model = joblib.load(model_save_path)
        logger.info(f"Model Loaded!")

        output = self.predict(
            dataset=dataset, model=model, UserId=UserId, MaxResults=MaxResults
        )

        if output is None:
            return {"code": False, "status": "failure", "message": "UserId Not Found!"}

        return {
            "code": True,
            "status": "success",
            "data": output,
        }

    def get_similar_items(
        self, item_features, dataset, model, ProductId, MaxResults=10
    ):
        (
            user_id_map,
            user_feature_map,
            product_id_map,
            product_feature_map,
        ) = dataset.mapping()

        item_id = product_id_map.get(ProductId, None)
        if item_id is None:
            return None

        return (
            similar_items(
                item_id=item_id,
                item_features=item_features,
                model=model,
                N=MaxResults,
            )["itemID"]
            .map(lambda x: [i for i in product_id_map if product_id_map[i] == x][0])
            .values.tolist()
        )

    def load_and_get_similar_items(self, CompanyId, ProductId, MaxResults=10):
        model_save_path = self.find_latest_model(CompanyId=CompanyId)
        if model_save_path is None:
            return {
                "code": False,
                "status": "failure",
                "message": "No Saved model found for given Company!",
            }

        item_features, dataset, model = joblib.load(model_save_path)
        logger.info(f"Model Loaded!")

        output = self.get_similar_items(
            item_features=item_features,
            dataset=dataset,
            model=model,
            ProductId=ProductId,
            MaxResults=MaxResults,
        )

        if output is None:
            return {
                "code": False,
                "status": "failure",
                "message": "ProductId Not Found!",
            }

        return {
            "code": True,
            "status": "success",
            "data": output,
        }

    def process(self, CompanyId, StartDate, EndDate):
        """LightFM main function,
        'rating_col' will be outlier neutralized and normalized from 1 to 5 as ratings

        Args:
            user_data (pd.DataFrame): user id and features
            product_data (pd.DataFrame): item id and features
            transaction_data (pd.DataFrame): unique rows with
                user-item pairs, for each pair there will be 'rating_col'
                that could be any transactional metric like Revenue(recommended), Quantity, etc
        """
        print("Inside LightFM Pipeline!")

        item_feature_list = json.loads(
            config["light_fm"]["item_feature_list"].replace("'", '"')
        )
        user_feature_list = json.loads(
            config["light_fm"]["user_feature_list"].replace("'", '"')
        )

        user_data, product_data, transaction_data = self.load_data_from_sql(
            CompanyId, StartDate, EndDate, user_feature_list, item_feature_list
        )

        if transaction_data.shape[0] == 0:
            return {
                "code": False,
                "status": "failure",
                "message": "No data found for given Inputs!",
            }
        logger.info("Data Loaded from Database!")

        # preprocessing rating col
        user_data, product_data, transaction_data = self.preprocess_data(
            user_data=user_data,
            product_data=product_data,
            transaction_data=transaction_data,
        )
        logger.info("Data Preprocessing Done!")

        # get unique values of features
        user_feature_value_set = self.prepare_unique_value_set(
            feature_list=user_feature_list, feature_data=user_data
        )
        item_feature_value_set = self.prepare_unique_value_set(
            feature_list=item_feature_list, feature_data=product_data
        )
        logger.info("Get Unique Values of Features!")

        # fit dataset
        dataset, interactions, weights = self.fit_dataset(
            transaction_data=transaction_data,
            item_features=item_feature_value_set,
            user_features=user_feature_value_set,
        )
        logger.info("Fit the dataset!")

        # get feature tuples and make interaction matrix
        user_tuples = self.build_feature_tuples(feature_data=user_data)
        user_features = dataset.build_user_features(user_tuples, normalize=False)

        item_tuples = self.build_feature_tuples(feature_data=product_data)
        item_features = dataset.build_item_features(item_tuples, normalize=False)
        logger.info("Get Feature tuples and make interaction matrix!")

        # define and fit the model
        model = LightFM(
            no_components=int(config["light_fm"]["model_n_components"]),
            loss=config["light_fm"]["model_loss"],
            learning_rate=float(config["light_fm"]["model_learning_rate"]),
            random_state=np.random.RandomState(int(config["light_fm"]["seed"])),
        )
        model.fit(
            interactions,  # sparse matrix representing whether user u and item i interacted
            user_features=user_features,  # sparse matrix for users
            item_features=item_features,  # sparse matrix for items
            sample_weight=weights,  # spase matrix with user u and item i interaction: i.e ratings
            epochs=int(config["light_fm"]["model_epochs"]),
        )
        logger.info("LightFM Model Fit!")

        # evaluate
        train_auc = auc_score(
            model,
            interactions,
            user_features=user_features,
            item_features=item_features,
        ).mean()
        logger.info(f"Hybrid training set AUC: {train_auc}")

        BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        PATH_NAME = os.path.join("..", "model")
        FILE_NAME = (
            "model_"
            + str(CompanyId)
            + datetime.strftime(datetime.now(), "_%Y%m%d")
            + ".pkl"
        )

        # if path doesn't exist, create path
        if not os.path.exists(os.path.join(BASE_PATH, PATH_NAME)):
            os.makedirs(os.path.join(BASE_PATH, PATH_NAME))

        joblib.dump(
            (item_features, dataset, model),
            os.path.join(BASE_PATH, PATH_NAME, FILE_NAME),
        )
        logger.info(f"Model saved to {os.path.join(BASE_PATH, PATH_NAME, FILE_NAME)}")

        return {
            "code": True,
            "status": "success",
            "message": "Model Trained and Saved!",
        }
