import logging
import math
import os
from datetime import datetime

import implicit
import joblib
import pandas as pd
from scipy.sparse import coo_matrix

import config_helper
from personalized_recommender import BasePersonalizedRecommender
from utils import sql_connector

# Import repo's evaluation metrics
# from implicit.evaluation import mean_average_precision_at_k, precision_at_k, AUC_at_k, ndcg_at_k

logger = logging.getLogger(__name__)
config = config_helper.read_config()

rating_col = "NumOrders"


class ImplicitRecommender(BasePersonalizedRecommender):
    def __init__(self) -> None:
        super().__init__()

    def load_data_from_sql(self, CompanyId, StartDate, EndDate):
        # transaction data
        transaction_data_query = f"""
        SELECT
            {config["implicit"]['user_id_col']},
            {config["implicit"]['product_id_col']},
            count(distinct {config["implicit"]['order_date_col']}) as {rating_col}
        from
            {config["implicit"]["transaction_table_name"]}
        where
            {config["implicit"]["company_id_col"]} = {CompanyId}
        and
            {config["implicit"]["order_date_col"]} >= {StartDate}
        and
            {config["implicit"]["order_date_col"]} <={EndDate}
        and
            {config["implicit"]["quantity_col"]} > 0
        and
            {config["implicit"]["revenue_col"]} > 0
        GROUP BY
            {config["implicit"]['user_id_col']},
            {config["implicit"]['product_id_col']}
        ;"""
        connection_string = config["db_config"]["db_connection_string"]
        transaction_data = sql_connector.get_data(
            query=transaction_data_query, connection_string=connection_string
        )

        transaction_data = pd.DataFrame(
            transaction_data,
            columns=[
                config["implicit"]["user_id_col"],
                config["implicit"]["product_id_col"],
                rating_col,
            ],
        )
        logger.info("Fetched transaction_data!")

        return transaction_data

    def preprocess_data(
        self,
        transaction_data,
    ):
        # remove extreme outliers
        transaction_data[rating_col] = (
            transaction_data[rating_col]
            .clip(
                upper=(
                    transaction_data[rating_col].mean()
                    + (transaction_data[rating_col].std() * 3)
                )
            )
            .apply(lambda x: int(math.ceil(x)))
        )

        # make user and product maps
        ALL_USERS = (
            transaction_data[config["implicit"]["user_id_col"]].unique().tolist()
        )
        ALL_PRODUCTS = (
            transaction_data[config["implicit"]["product_id_col"]].unique().tolist()
        )

        user_ids = dict(list(enumerate(ALL_USERS)))
        product_ids = dict(list(enumerate(ALL_PRODUCTS)))

        user_map = {u: uidx for uidx, u in user_ids.items()}
        product_map = {i: iidx for iidx, i in product_ids.items()}

        transaction_data["user_id"] = transaction_data[
            config["implicit"]["user_id_col"]
        ].map(user_map)
        transaction_data["product_id"] = transaction_data[
            config["implicit"]["product_id_col"]
        ].map(product_map)

        coo_train = coo_matrix(
            (
                transaction_data[rating_col].values,
                (
                    transaction_data["user_id"].values,
                    transaction_data["product_id"].values,
                ),
            ),
            shape=(len(ALL_USERS), len(ALL_PRODUCTS)),
        )

        return coo_train, user_ids, user_map, product_ids, product_map

    def find_latest_model(self, CompanyId: int):
        BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        PATH_NAME = os.path.join("..", "model")

        all_models = [
            path
            for path in os.listdir(os.path.join(BASE_PATH, PATH_NAME))
            if (
                config["personalized_recommender"]["process"]
                + "_model_"
                + str(CompanyId)
            )
            in path
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

        model, coo_train, user_map, product_ids, product_map = joblib.load(
            model_save_path
        )
        logger.info(f"Model Loaded!")

        UserId = user_map.get(UserId, None)
        if UserId:
            # predict for existing user
            user_items = coo_train.tocsr()
            recom_items, recom_score = model.recommend(
                userid=UserId, user_items=user_items[UserId], N=MaxResults
            )
            output = pd.Series(recom_items).map(product_ids).values.tolist()
            return {
                "code": True,
                "status": "success",
                "data": output,
            }
        else:
            return {
                "code": False,
                "status": "failure",
                "message": "UserId Not Found!",
            }

    def load_and_get_similar_items(self, CompanyId, ProductId, MaxResults=10):
        model_save_path = self.find_latest_model(CompanyId=CompanyId)
        if model_save_path is None:
            return {
                "code": False,
                "status": "failure",
                "message": "No Saved model found for given Company!",
            }

        model, coo_train, user_map, product_ids, product_map = joblib.load(
            model_save_path
        )
        logger.info(f"Model Loaded!")

        ProductId = product_map.get(ProductId, None)
        if ProductId:
            # predict for existing user
            similar_items, score = model.similar_items(
                itemid=ProductId, N=MaxResults, filter_items=[ProductId]
            )

            output = pd.Series(similar_items).map(product_ids).values.tolist()
            return {
                "code": True,
                "status": "success",
                "data": output,
            }
        else:
            return {
                "code": False,
                "status": "failure",
                "message": "ProductId Not Found!",
            }

        output = self.get_similar_items(
            item_features=item_features,
            dataset=dataset,
            model=model,
            ProductId=ProductId,
            MaxResults=MaxResults,
        )

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
        logger.info("Inside Implicit Pipeline!")

        transaction_data = self.load_data_from_sql(CompanyId, StartDate, EndDate)

        if transaction_data.shape[0] == 0:
            return {
                "code": False,
                "status": "failure",
                "message": "No data found for given Inputs!",
            }
        logger.info("Data Loaded from Database!")

        # preprocessing rating col
        (
            coo_train,
            user_ids,
            user_map,
            product_ids,
            product_map,
        ) = self.preprocess_data(
            transaction_data=transaction_data,
        )
        logger.info("Data Preprocessing Done!")

        # initialize a model
        model = implicit.als.AlternatingLeastSquares(
            factors=int(config["light_fm"]["model_n_components"]),
            iterations=int(config["light_fm"]["model_epochs"]),
            random_state=int(config["light_fm"]["seed"]),
        )

        # train the model on a sparse matrix of item/user/confidence weights
        model.fit(coo_train)
        logger.info("Implicit Model Fit!")

        BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        PATH_NAME = os.path.join("..", "model")
        FILE_NAME = (
            config["personalized_recommender"]["process"]
            + "_model_"
            + str(CompanyId)
            + datetime.strftime(datetime.now(), "_%Y%m%d")
            + ".pkl"
        )

        # if path doesn't exist, create path
        if not os.path.exists(os.path.join(BASE_PATH, PATH_NAME)):
            os.makedirs(os.path.join(BASE_PATH, PATH_NAME))

        joblib.dump(
            (model, coo_train, user_map, product_ids, product_map),
            os.path.join(BASE_PATH, PATH_NAME, FILE_NAME),
        )
        logger.info(f"Model saved to {os.path.join(BASE_PATH, PATH_NAME, FILE_NAME)}")

        return {
            "code": True,
            "status": "success",
            "message": "Model Trained and Saved!",
        }
