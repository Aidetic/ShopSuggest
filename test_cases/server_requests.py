function_test_json = {
    "lightfm_process": {
        "input": {"CompanyId": 111111, "StartDate": 20221128, "EndDate": 20221131},
        "output": {
            "code": True,
            "status": "success",
            "message": "Model Trained and Saved!",
        },
    },
    "lightfm_user_recoms": {
        "input": {"CompanyId": 111111, "UserId": "integer_uid0", "MaxResults": 10},
        "output": {
            "code": True,
            "status": "success",
            "data": [
                "integer_pid1",
                "integer_pid2",
                "integer_pid3",
                "integer_so_on_till...pid10",
            ],
        },
    },
    "lightfm_similar_items": {
        "input": {"CompanyId": 111111, "ProductId": "integer_pid0", "MaxResults": 5},
        "output": {
            "code": True,
            "status": "success",
            "data": [
                "integer_pid1",
                "integer_pid2",
                "integer_pid3",
                "integer_pid4",
                "integer_pid5",
            ],
        },
    },
    "fbt_process": {
        "input": {"CompanyId": 111111, "StartDate": 20221128, "EndDate": 20221131},
        "output": {
            "code": True,
            "status": "success",
            "message": "FBT processed!",
        },
    },
    "fbt_predict": {
        "input": {"CompanyId": 111111, "ProductId": "integer_pid0", "MaxResults": 6},
        "output": {
            "code": True,
            "status": "success",
            "data": [
                {"rank": 1, "FBTProductId": "integer_pid1"},
                {"rank": 2, "FBTProductId": "integer_pid2"},
                {"rank": 3, "FBTProductId": "integer_pid3"},
                {"rank": 4, "FBTProductId": "integer_pid4"},
                {"rank": 5, "FBTProductId": "integer_pid5"},
                {"rank": 6, "FBTProductId": "integer_pid6"},
            ],
        },
    },
    "bestseller_process": {
        "input": {"CompanyId": 111111, "StartDate": 20221128, "EndDate": 20221131},
        "output": {
            "code": True,
            "status": "success",
            "message": "Bestseller data Processed!",
        },
    },
    "bestseller_find": {
        "input": {
            "CompanyId": 111111,
            "CategoryLevel": "Category_Column",
            "CategoryValue": "Category_Value",
            "BestsellerMetric": "Revenue",
            "MaxResults": 6,
        },
        "output": {
            "code": True,
            "status": "success",
            "data": [
                {
                    "PrimaryCategory": "Primary_Category_Value",
                    "SecondaryCategory": "Secondary_Category_Value",
                    "ProductId": "integer_pid1",
                    "Quantity": 1000,
                    "Revenue": 10000.0,
                },
                {
                    "PrimaryCategory": "Primary_Category_Value",
                    "SecondaryCategory": "Secondary_Category_Value",
                    "ProductId": "integer_pid2",
                    "Quantity": 1200,
                    "Revenue": 9000.0,
                },
                {
                    "PrimaryCategory": "Primary_Category_Value",
                    "SecondaryCategory": "Secondary_Category_Value",
                    "ProductId": "integer_pid3",
                    "Quantity": 500,
                    "Revenue": 8000.0,
                },
                {
                    "PrimaryCategory": "Primary_Category_Value",
                    "SecondaryCategory": "Secondary_Category_Value",
                    "ProductId": "integer_pid4",
                    "Quantity": 250,
                    "Revenue": 7000.0,
                },
                {
                    "PrimaryCategory": "Primary_Category_Value",
                    "SecondaryCategory": "Secondary_Category_Value",
                    "ProductId": "integer_pid5",
                    "Quantity": 400,
                    "Revenue": 6000.0,
                },
                {
                    "PrimaryCategory": "Primary_Category_Value",
                    "SecondaryCategory": "Secondary_Category_Value",
                    "ProductId": "integer_pid6",
                    "Quantity": 600,
                    "Revenue": 5000.0,
                },
            ],
        },
    },
}
