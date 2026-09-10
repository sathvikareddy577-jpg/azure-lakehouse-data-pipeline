"""Gold layer module"""

from .dimension_tables import create_dim_customer, create_dim_product
from .fact_tables import (
    create_daily_sales_kpi,
    create_fact_orders,
    validate_gold_aggregations,
    write_gold_table,
)

__all__ = [
    "create_dim_customer",
    "create_dim_product",
    "create_fact_orders",
    "create_daily_sales_kpi",
    "write_gold_table",
    "validate_gold_aggregations",
]
