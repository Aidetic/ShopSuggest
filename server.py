import os
import sys
import time
from traceback import print_exc

BASE_PATH = os.path.dirname(os.path.abspath(__file__))
print(BASE_PATH)
sys.path.append(BASE_PATH)


from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from handlers import (
    handler_category_bestsellers_process,
    handler_find_category_bestsellers,
    handler_frequently_bought_together_inference,
    handler_frequently_bought_together_process,
    handler_personalized_recommender_process,
    handler_personalized_recommender_similar_items,
    handler_personalized_recommender_user_recoms,
)
from server_schema import (
    ItemFindCategoryBestsellers,
    ItemFindFrequentlyBoughtTogether,
    ItemFindPersonalizedRecommenderSimilarItems,
    ItemFindPersonalizedRecommenderUserRecoms,
    ItemMakeBestsellerData,
    ItemMakeFrequentlyBoughtTogether,
    ItemMakePersonalizedRecommenderModel,
)
from settings import env
from utils import logger_config

logger = logger_config(env.LOGGING_LEVEL)

app = FastAPI()

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.post("/make-frequently-bought-together")
async def make_frequently_bought_together(item: ItemMakeFrequentlyBoughtTogether):
    StartDate = item.StartDate
    EndDate = item.EndDate
    CompanyId = item.CompanyId

    # get data from database based on config file information

    try:
        response = handler_frequently_bought_together_process(
            CompanyId, StartDate, EndDate
        )
    except Exception as e:
        print(e)
        print(print_exc())
        return {
            "code": False,
            "status": "failure",
            "error": e,
        }

    return response


@app.post("/find-frequently-bought-together")
async def find_frequently_bought_together(item: ItemFindFrequentlyBoughtTogether):
    CompanyId = item.CompanyId
    ProductId = item.ProductId
    OutputConfig = item.OutputConfig

    MaxResults = OutputConfig.get("MaxResults", 6)

    if type(MaxResults) is not int:
        return {
            "code": False,
            "status": "failure",
            "message": "Please enter Valid input for MaxResults!",
        }

    try:
        response = handler_frequently_bought_together_inference(
            CompanyId=CompanyId, ProductId=ProductId, MaxResults=MaxResults
        )
    except Exception as e:
        print(e)
        print(print_exc())
        return {
            "code": False,
            "status": "failure",
            "error": e,
        }

    return response


@app.post("/make-bestseller-data")
async def make_bestseller_data(item: ItemMakeBestsellerData):
    StartDate = item.StartDate
    EndDate = item.EndDate
    CompanyId = item.CompanyId

    try:
        response = handler_category_bestsellers_process(CompanyId, StartDate, EndDate)
    except Exception as e:
        print(e)
        print(print_exc())
        return {
            "code": False,
            "status": "failure",
            "error": e,
        }

    return response


@app.post("/find-category-bestsellers")
async def find_category_bestsellers(item: ItemFindCategoryBestsellers):
    CompanyId = item.CompanyId
    CategoryLevel = item.CategoryLevel
    CategoryValue = item.CategoryValue
    BestsellerMetric = item.BestsellerMetric
    OutputConfig = item.OutputConfig

    MaxResults = OutputConfig.get("MaxResults", 6)

    if type(MaxResults) is not int:
        return {
            "code": False,
            "status": "failure",
            "message": "Please enter Valid input for MaxResults!",
        }

    try:
        response = handler_find_category_bestsellers(
            CompanyId, CategoryLevel, CategoryValue, BestsellerMetric, MaxResults
        )
    except Exception as e:
        print(e)
        print(print_exc())
        return {
            "code": False,
            "status": "failure",
            "error": e,
        }

    return response


@app.post("/make-personalized-recommender-model")
async def make_personalized_recommender_model(
    item: ItemMakePersonalizedRecommenderModel,
):
    StartDate = item.StartDate
    EndDate = item.EndDate
    CompanyId = item.CompanyId

    try:
        response = handler_personalized_recommender_process(
            CompanyId, StartDate, EndDate
        )
    except Exception as e:
        print(e)
        print(print_exc())
        return {
            "code": False,
            "status": "failure",
            "error": e,
        }

    return response


@app.post("/find-personalized-recommender-user-recoms")
async def find_personalized_recommender_user_recoms(
    item: ItemFindPersonalizedRecommenderUserRecoms,
):
    CompanyId = item.CompanyId
    UserId = item.UserId
    OutputConfig = item.OutputConfig

    MaxResults = OutputConfig.get("MaxResults", 10)

    if type(MaxResults) is not int:
        return {
            "code": False,
            "status": "failure",
            "message": "Please enter Valid input for MaxResults!",
        }

    try:
        response = handler_personalized_recommender_user_recoms(
            CompanyId=CompanyId, UserId=UserId, MaxResults=MaxResults
        )
    except Exception as e:
        print(e)
        print(print_exc())
        return {
            "code": False,
            "status": "failure",
            "error": e,
        }

    return response


@app.post("/find-personalized-recommender-similar-items")
async def find_personalized_recommender_similar_items(
    item: ItemFindPersonalizedRecommenderSimilarItems,
):
    CompanyId = item.CompanyId
    ProductId = item.ProductId
    OutputConfig = item.OutputConfig

    MaxResults = OutputConfig.get("MaxResults", 10)

    if type(MaxResults) is not int:
        return {
            "code": False,
            "status": "failure",
            "message": "Please enter Valid input for MaxResults!",
        }

    try:
        response = handler_personalized_recommender_similar_items(
            CompanyId=CompanyId, ProductId=ProductId, MaxResults=MaxResults
        )
    except Exception as e:
        print(e)
        print(print_exc())
        return {
            "code": False,
            "status": "failure",
            "error": e,
        }

    return response
