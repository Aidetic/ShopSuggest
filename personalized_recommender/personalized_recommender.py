import logging

import config_helper
from personalized_recommender import (
    BasePersonalizedRecommender,
    ImplicitRecommender,
    LightFMHybridRecommender,
)

logger = logging.getLogger(__name__)

config = config_helper.read_config()


class PersonalizedRecommender(BasePersonalizedRecommender):
    def __init__(self) -> None:
        super().__init__()
        self.recommender = self.process()

    def process(self):
        process = config["personalized_recommender"]["process"]

        if process == "lightfm":
            obj = LightFMHybridRecommender()
        elif process == "implicit":
            obj = ImplicitRecommender()
        else:
            raise Exception("Invalid config for personalized_recommender!")
        return obj
