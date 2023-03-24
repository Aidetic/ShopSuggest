from frequently_bought_together import FPGrowthFBT

fbt = FPGrowthFBT()


def handler_frequently_bought_together_process(CompanyId, StartDate, EndDate):
    response = fbt.process(CompanyId, StartDate, EndDate)

    return response


def handler_frequently_bought_together_inference(CompanyId, ProductId, MaxResults):
    response = fbt.predict(
        CompanyId=CompanyId, ProductId=ProductId, MaxResults=MaxResults
    )

    return response
