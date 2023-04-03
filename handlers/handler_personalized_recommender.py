from personalized_recommender import PersonalizedRecommender

obj = PersonalizedRecommender()


def handler_personalized_recommender_process(CompanyId, StartDate, EndDate):
    response = obj.recommender.process(CompanyId, StartDate, EndDate)

    return response


def handler_personalized_recommender_user_recoms(CompanyId, UserId, MaxResults):
    response = obj.recommender.load_and_predict(
        CompanyId=CompanyId, UserId=UserId, MaxResults=MaxResults
    )

    return response


def handler_personalized_recommender_similar_items(CompanyId, ProductId, MaxResults):
    response = obj.recommender.load_and_get_similar_items(
        CompanyId=CompanyId, ProductId=ProductId, MaxResults=MaxResults
    )
    return response
