"""utils for customer segmentation."""

import pandas as pd
from scipy.stats import poisson


def as_of_date_generic(
        data: pd.DataFrame,
        date_column: str,
        as_of_date: str
    ) -> pd.DataFrame:
    """Cuts the data until as_of_date. this prevents data leakage when 
    computing features for customer segmentation.

    Args:
        data (pd.DataFrame): _description_
        date_column (str): _description_
        as_of_date (str): _description_

    Returns:
        pd.DataFrame: _description_
    """
    as_of_date = pd.to_datetime(as_of_date)
    data[date_column] = pd.to_datetime(data[date_column])

    return data[data[date_column] <= as_of_date].copy()


def rfm_feature_maker(
        customers: pd.DataFrame,
        orders: pd.DataFrame,
        as_of_date: str) -> pd.DataFrame:
    """rfm feature maker.
    Features:
        - recency_days: days since last order
        - frequency: total number of orders
        - monetary: total revenue

    Args:
        customers (pd.DataFrame): _description_
        orders (pd.DataFrame): _description_
        as_of_date (str): _description_

    Returns:
        pd.DataFrame: _description_
    """
    as_of_date = pd.to_datetime(as_of_date)

    # Aggregate orders
    agg = orders.groupby("customer_id").agg(
        last_order_date=("order_date", "max"),
        frequency=("order_id", "count"),
        monetary=("revenue", "sum")
    ).reset_index()

    # Recency
    agg["recency_days"] = (as_of_date - agg["last_order_date"]).dt.days

    # Merge with customers
    df = customers.merge(agg, on="customer_id", how="left")

    # Fill customers with no orders
    df["frequency"] = df["frequency"].fillna(0)
    df["monetary"] = df["monetary"].fillna(0)
    df["recency_days"] = df["recency_days"].fillna(
        (as_of_date - pd.to_datetime(df["signup_date"])).dt.days
    )

    return df

def return_feature_maker(returns: pd.DataFrame) -> pd.DataFrame:
    """Features base on returns dataset.

    Args:
        returns (pd.DataFrame): _description_

    Returns:
        pd.DataFrame: _description_
    """
    agg = returns.groupby("customer_id").agg(
        return_count=("return_id", "count"),
        total_return_value=("refund_amount", "sum"),
        avg_return_value=("refund_amount", "mean")
    ).reset_index()

    return agg

def order_feature_maker(orders: pd.DataFrame) -> pd.DataFrame:
    """Features base on orders dataset.

    Args:
        orders (pd.DataFrame): _description_

    Returns:
        pd.DataFrame: _description_
    """

    agg = orders.groupby("customer_id").agg(
        avg_items_per_order=("items", "mean"), # we can also use median here.
        total_items=("items", "sum"),
        avg_revenue_per_order=("revenue", "mean"),
        total_revenue=("revenue", "sum")
    ).reset_index()

    return agg

def is_dormant(mu, t_delta, threshold=.8):
    """Dormancy.
    This feature detects which customers are dormant.
    This is only applicable for customers with at least 2 orders.


    Args:
        mu (_type_): averate time between orders for a customer
        t_delta (_type_): time since last order for a customer
        threshold (float, optional): Based on this we say whether a customer 
        is dormant. Defaults to .8.

    Returns:
        _type_: _description_
    """
    pr = poisson.cdf(k=t_delta, mu=mu)
    return pr > threshold
