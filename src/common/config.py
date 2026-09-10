"""Configuration management for the pipeline"""

import os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()


class Config:
    """Pipeline configuration from environment or defaults."""
    
    # Paths
    DATA_PATH = os.getenv("DATA_PATH", "data")
    RAW_DATA_PATH = os.path.join(DATA_PATH, "raw")
    CDC_DATA_PATH = os.path.join(DATA_PATH, "cdc")
    WAREHOUSE_PATH = os.path.join(DATA_PATH, "warehouse")
    
    BRONZE_PATH = os.path.join(WAREHOUSE_PATH, "bronze")
    SILVER_PATH = os.path.join(WAREHOUSE_PATH, "silver")
    GOLD_PATH = os.path.join(WAREHOUSE_PATH, "gold")
    QUARANTINE_PATH = os.path.join(WAREHOUSE_PATH, "quarantine")
    
    # Spark settings
    SPARK_APP_NAME = os.getenv("SPARK_APP_NAME", "lakehouse-pipeline")
    SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
    
    # Azure paths (for deployment)
    ADLS_STORAGE_ACCOUNT = os.getenv("ADLS_STORAGE_ACCOUNT", "lakehousedata")
    ADLS_CONTAINER = os.getenv("ADLS_CONTAINER", "warehouse")
    
    # Databricks
    DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "")
    DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN", "")
    
    # Logging
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    LOG_FILE = os.getenv("LOG_FILE", None)
    
    # Data quality settings
    MAX_NULL_RATIO = 0.05  # 5%
    DUPLICATE_THRESHOLD = 0.02  # 2%
    
    @classmethod
    def ensure_paths(cls):
        """Create necessary directories if they don't exist."""
        for path_attr in [
            "RAW_DATA_PATH", "CDC_DATA_PATH", "BRONZE_PATH",
            "SILVER_PATH", "GOLD_PATH", "QUARANTINE_PATH"
        ]:
            path = getattr(cls, path_attr)
            Path(path).mkdir(parents=True, exist_ok=True)
