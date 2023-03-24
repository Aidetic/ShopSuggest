from personalized_recommender import LightFMHybridRecommender

obj = LightFMHybridRecommender()


def handler_personalized_recommender_process(CompanyId, StartDate, EndDate):
    response = obj.process(CompanyId, StartDate, EndDate)

    return response


def handler_personalized_recommender_user_recoms(CompanyId, UserId, MaxResults):
    response = obj.load_and_predict(
        CompanyId=CompanyId, UserId=UserId, MaxResults=MaxResults
    )

    return response


def handler_personalized_recommender_similar_items(CompanyId, ProductId, MaxResults):
    response = obj.load_and_get_similar_items(
        CompanyId=CompanyId, ProductId=ProductId, MaxResults=MaxResults
    )
    return response
