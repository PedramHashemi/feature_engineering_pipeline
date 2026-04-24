"""Data Ingestion and Preprocessing"""

from typing import Tuple
import pandas as pd
from utils.customer_segmentation import as_of_date_generic



def load_data(
        customers_path: str,
        orders_path: str,
        returns_path: str,
        as_of_date: str
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Ingest customer, order and return data with the 

    Args:
        customers_path (str): _description_
        orders_path (str): _description_
        returns_path (str): _description_

    Returns:
        tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]: _description_
    """
    as_of_date = pd.to_datetime(as_of_date)

    customers = pd.read_csv(customers_path)
    orders = pd.read_csv(orders_path)
    returns = pd.read_csv(returns_path)

    orders_to_date = as_of_date_generic(orders, "order_date", as_of_date)
    returns_to_date = as_of_date_generic(returns, "return_date", as_of_date)

    return {
        "customers": customers,
        "orders": orders_to_date,
        "returns": returns_to_date}
