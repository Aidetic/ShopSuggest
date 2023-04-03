import configparser

# CREATE OBJECT
config_file = configparser.ConfigParser()

####################-START OF INPUTS-##########################
# preparing DB Connection String:
DBMS = "sqlite"  # "mssql", "sqlite"
DATABASE_SERVER = "<dbserver>"
DATABASE_DRIVER = "<dbdriver>"
DATABASE_NAME = "<dbname>"
DATABASE_USER = "<uname>"
DATABASE_PWD = "<pwd>"
TRUSTED_CONNECTION = None  # True / False / None


# ADD NEW SECTION AND SETTINGS
config_file["fbt_data"] = {
    "transaction_table_name": "<>",
    "company_id_col": "<>",
    "order_date_col": "<>",
    "user_id_col": "<>",
    "product_id_col": "<>",
    "min_support": 0.001,
    "min_threshold": 2,
}

config_file["bestseller_data"] = {
    "transaction_table_name": "<>",
    "product_table_name": "<>",
    "company_id_col": "<>",
    "order_date_col": "<>",
    "product_id_col": "<>",
    "quantity_col": "<>",
    "revenue_col": "<>",
    "primary_category_col": "<>",
    "secondary_category_col": "<>",
}

config_file["personalized_recommender"] = {"process": "<>"}  # lightfm, implicit

# fill only if selected in personalized_recommender
config_file["light_fm"] = {
    "transaction_table_name": "<>",
    "user_table_name": "<>",
    "product_table_name": "<>",
    "company_id_col": "<>",
    "order_date_col": "<>",
    "user_id_col": "<>",
    "product_id_col": "<>",
    "rating_col": "<>",
    "user_feature_list": [
        "user_feature_1",
        "user_feature_2",
        "user_feature_3",
        "so_on...",
    ],
    "item_feature_list": [
        "item_feature_1",
        "item_feature_2",
        "item_feature_3",
        "so_on...",
    ],
    "quantity_col": "<>",
    "revenue_col": "<>",
    "model_n_components": 30,
    "model_loss": "warp",
    "model_learning_rate": 0.05,
    "model_epochs": 10,
    "seed": 23,
}

# fill only if selected in personalized_recommender
config_file["implicit"] = {
    "transaction_table_name": "<>",
    "company_id_col": "<>",
    "order_date_col": "<>",
    "user_id_col": "<>",
    "product_id_col": "<>",
    "quantity_col": "<>",
    "revenue_col": "<>",
    "model_n_components": 30,
    "model_epochs": 10,
    "seed": 23,
}

if TRUSTED_CONNECTION is None:
    db_connection_string = "../data/aid_db.db"
####################-END OF INPUTS-##########################
elif TRUSTED_CONNECTION:
    db_connection_string = f"DRIVER={DATABASE_DRIVER};SERVER={DATABASE_SERVER};DATABASE={DATABASE_NAME};Trusted_Connection=yes;"
else:
    db_connection_string = f"DRIVER={DATABASE_DRIVER};SERVER={DATABASE_SERVER};DATABASE={DATABASE_NAME};UID={DATABASE_USER};PWD={DATABASE_PWD};"

config_file["db_config"] = {"dbms": DBMS, "db_connection_string": db_connection_string}


# SAVE CONFIG FILE
with open(r"config.ini", "w") as configfileObj:
    config_file.write(configfileObj)
    configfileObj.flush()
    configfileObj.close()

print("Config file 'config.ini' created")

# PRINT FILE CONTENT
read_file = open("config.ini", "r")
content = read_file.read()
print("Content of the config file are:\n")
print(content)
read_file.flush()
read_file.close()
