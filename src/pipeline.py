"""Data Ingestion and Preprocessing"""

from functools import reduce
import pandas as pd
import logging
import time

from features.logic import (
    as_of_date_generic,
    rfm_feature_maker,
    order_feature_maker,
    return_feature_maker
)


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

handler = logging.StreamHandler()
formatter = logging.Formatter(
    "[%(asctime)s] %(levelname)s - %(name)s - %(message)s"
)
handler.setFormatter(formatter)
logger.addHandler(handler)


def load_data(
        customers_path: str,
        orders_path: str,
        returns_path: str,
        as_of_date: str
    ) -> dict:
    """Ingest customer, order and return data."""

    start_time = time.time()
    logger.info("Starting data ingestion pipeline")
    logger.info("As-of date: %s", as_of_date)

    try:
        as_of_date = pd.to_datetime(as_of_date)

        logger.info("Loading CSV files...")
        customers = pd.read_csv(customers_path)
        orders = pd.read_csv(orders_path)
        returns = pd.read_csv(returns_path)

        logger.info("Customers shape: %s", customers.shape)
        logger.info("Orders shape: %s", orders.shape)
        logger.info("Returns shape: %s", returns.shape)

        logger.info("Filtering orders and returns to as-of-date...")
        orders_to_date = as_of_date_generic(orders, "order_date", as_of_date)
        returns_to_date = as_of_date_generic(returns, "return_date", as_of_date)

        logger.info("Orders after filter: %s", orders_to_date.shape)
        logger.info("Returns after filter: %s", returns_to_date.shape)

        logger.info("Data ingestion completed successfully")

        return {
            "customers": customers,
            "orders": orders_to_date,
            "returns": returns_to_date
        }

    except Exception as error:
        logger.exception(
            "Failed during data ingestion pipeline. \nError: %s", error)
        raise

    finally:
        logger.info(
            "Data ingestion took %.2f seconds",
            time.time() - start_time)


def build_features(
        customers: pd.DataFrame,
        orders: pd.DataFrame,
        returns: pd.DataFrame,
        as_of_date: str
    ) -> pd.DataFrame:
    """Build feature set from raw tables."""

    start_time = time.time()
    logger.info("Starting feature engineering pipeline")
    logger.info("As-of date: %s", as_of_date)

    try:
        logger.info("Applying as-of-date filtering...")
        orders_as_of_date = as_of_date_generic(
            data=orders,
            date_column="order_date",
            as_of_date=as_of_date
        )

        returns_as_of_date = as_of_date_generic(
            data=returns,
            date_column="return_date",
            as_of_date=as_of_date
        )

        logger.info("Filtered orders shape: %s", orders_as_of_date.shape)
        logger.info("Filtered returns shape: %s", returns_as_of_date.shape)

        logger.info("Generating RFM features...")
        rfm = rfm_feature_maker(customers, orders_as_of_date, as_of_date)
        logger.info("RFM shape: %s", rfm.shape)

        logger.info("Generating order features...")
        order_features = order_feature_maker(orders_as_of_date)
        logger.info("Order features shape: %s", order_features.shape)

        logger.info("Generating return features...")
        return_features = return_feature_maker(returns_as_of_date)
        logger.info("Return features shape: %s", return_features.shape)

        logger.info("Merging feature sets...")
        features = [rfm, order_features, return_features]

        all_features = reduce(
            lambda x, y: pd.merge(x, y, on="customer_id"),
            features
        )

        logger.info("Final feature matrix shape: %s", all_features.shape)
        logger.info("Feature engineering completed successfully")

        return all_features

    except Exception as error:
        logger.exception(
            "Failed during feature engineering pipeline. \nError: %s", error)
        raise

    finally:
        logger.info(
            "Feature engineering took %.2f seconds",
            time.time() - start_time)

def run_pipeline(customers_path: str, orders_path: str, returns_path: str):
    customers = pd.read_csv(customers_path)
    orders = pd.read_csv(orders_path)
    returns = pd.read_csv(returns_path)

    snapshots = ["2024-06-30", "2024-12-31"]

    for dd in snapshots:
        logger.info("Processing snapshot for as-of-date: %s", dd)
        features = build_features(customers, orders, returns, dd)

        output_path = f"../output/features_{dd}.parquet"
        features.to_parquet(output_path, index=False)

        logger.info("[INFO] Snapshot %s: %s customers", dd, len(features))

if __name__ == "__main__":
    run_pipeline(
        customers_path="../data/customers.csv",
        orders_path="../data/orders.csv",
        returns_path="../data/returns.csv"
    )
