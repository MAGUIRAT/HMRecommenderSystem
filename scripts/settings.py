import os

NBR_OF_ARTICLES_PARTITIONS = 100

NBR_OF_CUSTOMERS_PARTITIONS = 1000

DATA_ROOT_DIR = os.environ.get("DATA_DIRECTORY")
MODELS_ROOT_DIR = os.environ.get("MODELS_DIRECTORY")

if DATA_ROOT_DIR is None:
    DATA_ROOT_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")

if MODELS_ROOT_DIR is None:
    MODELS_ROOT_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "models")

ARTICLES_RAW_PATH = (
    "raw",
    "articles"
)

CUSTOMERS_RAW_PATH = (
    "raw",
    "customers"
)

TRANSACTIONS_RAW_PATH = (
    "raw",
    "transactions_train"
)

ARTICLES_CLEAN_PATH = (
    "clean",
    "articles"
)

CUSTOMERS_CLEAN_PATH = (
    "clean",
    "customers"
)

TRANSACTIONS_CLEAN_PATH = (
    "clean",
    "transactions"
)

TRANSACTIONS_REPARTITIONED_BY_ARTICLE_CLEAN_PATH = (
    "clean",
    "transactions_partitioned_by_article"
)

ARTICLES_AGGREGATED_FEATURES_TRAIN_PATH = (
    "features",
    "articles",
    "aggregated",
    "train"
)

ARTICLES_AGGREGATED_FEATURES_INFERENCE_PATH = (
    "features",
    "articles",
    "aggregated",
    "inference"
)

CUSTOMERS_AGGREGATED_FEATURES_TRAIN_PATH = (
    "features",
    "customers",
    "aggregated",
    "train"
)

CUSTOMERS_AGGREGATED_FEATURES_INFERENCE_PATH = (
    "features",
    "customers",
    "aggregated",
    "inference"
)

CANDIDATES_TRAIN_PATH = (
    "candidates",
    "train"

)

CANDIDATES_INFERENCE_PATH = (
    "candidates",
    "inference"

)

RECOMMENDATION_INFERENCE_PATH = (
    "prediction",
    "inference"
)

SUBMISSION_PATH = (
    "submission",
)

SUBMISSION_FILE_NAME = "submission.csv"

MODELS_PATH = (
    "ranker",
    "xgb"
)

MODEL_FILE_NAME = "xgb.model"
