from category_bestsellers import CategoryBestsellers

obj = CategoryBestsellers()


def handler_category_bestsellers_process(CompanyId, StartDate, EndDate):
    return obj.process(CompanyId, StartDate, EndDate)


def handler_find_category_bestsellers(
    CompanyId, CategoryLevel, CategoryValue, BestsellerMetric, MaxResults
):
    return obj.find_bestseller(
        CompanyId, CategoryLevel, CategoryValue, BestsellerMetric, MaxResults
    )
