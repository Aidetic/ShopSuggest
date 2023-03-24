from pydantic import BaseModel


class ItemMakeFrequentlyBoughtTogether(BaseModel):
    CompanyId: int
    StartDate: int
    EndDate: int


class ItemFindFrequentlyBoughtTogether(BaseModel):
    CompanyId: int
    ProductId: int
    OutputConfig: dict


class ItemMakeBestsellerData(BaseModel):
    CompanyId: int
    StartDate: int
    EndDate: int


class ItemFindCategoryBestsellers(BaseModel):
    CompanyId: int
    CategoryLevel: str
    CategoryValue: str
    BestsellerMetric: str
    OutputConfig: dict


class ItemMakePersonalizedRecommenderModel(BaseModel):
    CompanyId: int
    StartDate: int
    EndDate: int


class ItemFindPersonalizedRecommenderUserRecoms(BaseModel):
    CompanyId: int
    UserId: int
    OutputConfig: dict


class ItemFindPersonalizedRecommenderSimilarItems(BaseModel):
    CompanyId: int
    ProductId: int
    OutputConfig: dict
