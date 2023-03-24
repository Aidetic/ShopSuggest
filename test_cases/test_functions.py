"""Test files for checking if the Function is 
returning response
"""
import os
import sys

BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_PATH)

import unittest

from server_requests import function_test_json

from category_bestsellers import CategoryBestsellers
from frequently_bought_together import FPGrowthFBT
from personalized_recommender import LightFMHybridRecommender
from settings import env
from utils import logger_config

logger = logger_config(env.LOGGING_LEVEL)

bestseller_obj = CategoryBestsellers()
fbt_obj = FPGrowthFBT()
hybrid_recom_obj = LightFMHybridRecommender()


class FunctionTests(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_lightfm_process(self):
        # inputs
        function_input = function_test_json["lightfm_process"]["input"]
        function_output = function_test_json["lightfm_process"]["output"]

        CompanyId = function_input.get("CompanyId", 0)
        StartDate = function_input.get("StartDate", 0)
        EndDate = function_input.get("EndDate", 0)

        response = hybrid_recom_obj.process(
            CompanyId=CompanyId, StartDate=StartDate, EndDate=EndDate
        )

        self.assertEqual(response, function_output)

    def test_lightfm_user_recoms(self):
        # inputs
        function_input = function_test_json["lightfm_user_recoms"]["input"]
        function_output = function_test_json["lightfm_user_recoms"]["output"]

        CompanyId = function_input.get("CompanyId", 0)
        UserId = function_input.get("UserId", 0)
        MaxResults = function_input.get("MaxResults", 0)

        response = hybrid_recom_obj.load_and_predict(
            CompanyId=CompanyId, UserId=UserId, MaxResults=MaxResults
        )

        self.assertEqual(response, function_output)

    def test_lightfm_similar_items(self):
        # inputs
        function_input = function_test_json["lightfm_similar_items"]["input"]
        function_output = function_test_json["lightfm_similar_items"]["output"]

        CompanyId = function_input.get("CompanyId", 0)
        ProductId = function_input.get("ProductId", 0)
        MaxResults = function_input.get("MaxResults", 0)

        response = hybrid_recom_obj.load_and_get_similar_items(
            CompanyId=CompanyId, ProductId=ProductId, MaxResults=MaxResults
        )

        self.assertEqual(response, function_output)

    def test_fbt_process(self):
        # inputs
        function_input = function_test_json["fbt_process"]["input"]
        function_output = function_test_json["fbt_process"]["output"]

        CompanyId = function_input.get("CompanyId", 0)
        StartDate = function_input.get("StartDate", 0)
        EndDate = function_input.get("EndDate", 0)

        response = fbt_obj.process(
            CompanyId=CompanyId, StartDate=StartDate, EndDate=EndDate
        )

        self.assertEqual(response, function_output)

    def test_fbt_predict(self):
        # inputs
        function_input = function_test_json["fbt_predict"]["input"]
        function_output = function_test_json["fbt_predict"]["output"]

        CompanyId = function_input.get("CompanyId", 0)
        ProductId = function_input.get("ProductId", 0)
        MaxResults = function_input.get("MaxResults", 0)

        response = fbt_obj.predict(
            CompanyId=CompanyId, ProductId=ProductId, MaxResults=MaxResults
        )

        self.assertEqual(response, function_output)

    def test_bestseller_process(self):
        # inputs
        function_input = function_test_json["bestseller_process"]["input"]
        function_output = function_test_json["bestseller_process"]["output"]

        CompanyId = function_input.get("CompanyId", 0)
        StartDate = function_input.get("StartDate", 0)
        EndDate = function_input.get("EndDate", 0)

        response = bestseller_obj.process(
            CompanyId=CompanyId, StartDate=StartDate, EndDate=EndDate
        )

        self.assertEqual(response, function_output)

    def test_bestseller_find(self):
        # inputs
        function_input = function_test_json["bestseller_find"]["input"]
        function_output = function_test_json["bestseller_find"]["output"]

        CompanyId = function_input.get("CompanyId", 0)
        CategoryLevel = function_input.get("CategoryLevel", 0)
        CategoryValue = function_input.get("CategoryValue", 0)
        BestsellerMetric = function_input.get("BestsellerMetric", 0)
        MaxResults = function_input.get("MaxResults", 0)

        response = bestseller_obj.find_bestseller(
            CompanyId=CompanyId,
            CategoryLevel=CategoryLevel,
            CategoryValue=CategoryValue,
            BestsellerMetric=BestsellerMetric,
            MaxResults=MaxResults,
        )

        self.assertEqual(response, function_output)


unittest.main()
