"""
Data processing utilities for the fuel exports ETL pipeline.
"""

import logging
import pandas as pd
import pyarrow.parquet as pq
from typing import Dict, List, Any
from datetime import datetime


class DataProcessor:
    """Handles data transformation and processing operations."""

    @staticmethod
    def read_parquet_file(file_path: str) -> pd.DataFrame:
        """
        Read a parquet file and return as pandas DataFrame.

        Args:
            file_path: Path to the parquet file

        Returns:
            DataFrame with the parquet data
        """
        try:
            table = pq.read_table(file_path)
            df = table.to_pandas()
            logging.info(f"Successfully read {len(df)} records from {file_path}")
            return df
        except Exception as e:
            logging.error(f"Error reading parquet file {file_path}: {e}")
            raise

    @staticmethod
    def transform_for_postgres(df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform DataFrame for PostgreSQL compatibility.

        Args:
            df: Raw DataFrame from parquet file

        Returns:
            Transformed DataFrame ready for PostgreSQL insertion
        """
        df_transformed = df.copy()

        # Handle dock struct - extract bay and level
        if "dock" in df_transformed.columns:
            df_transformed["dock_bay"] = df_transformed["dock"].apply(
                lambda x: x.get("bay") if isinstance(x, dict) else None
            )
            df_transformed["dock_level"] = df_transformed["dock"].apply(
                lambda x: x.get("level") if isinstance(x, dict) else None
            )
            df_transformed = df_transformed.drop("dock", axis=1)

        # Handle services array - convert to comma-separated string
        if "services" in df_transformed.columns:
            df_transformed["services"] = df_transformed["services"].apply(
                lambda x: ",".join(x) if isinstance(x, list) and x else ""
            )

        # Ensure proper datetime handling
        if "visited_at" in df_transformed.columns:
            df_transformed["visited_at"] = pd.to_datetime(
                df_transformed["visited_at"], utc=True
            )

        if "arrival_date" in df_transformed.columns:
            df_transformed["arrival_date"] = pd.to_datetime(
                df_transformed["arrival_date"]
            ).dt.date

        # Handle decimal types - convert to float for PostgreSQL
        decimal_columns = ["price_per_unit", "total_cost"]
        for col in decimal_columns:
            if col in df_transformed.columns:
                df_transformed[col] = pd.to_numeric(
                    df_transformed[col], errors="coerce"
                )

        # Handle float columns
        float_columns = ["fuel_units", "coords_x", "coords_y"]
        for col in float_columns:
            if col in df_transformed.columns:
                df_transformed[col] = pd.to_numeric(
                    df_transformed[col], errors="coerce"
                )

        # Handle integer columns
        int_columns = ["station_id"]
        for col in int_columns:
            if col in df_transformed.columns:
                df_transformed[col] = pd.to_numeric(
                    df_transformed[col], errors="coerce", downcast="integer"
                )

        # Handle boolean columns
        bool_columns = ["is_emergency"]
        for col in bool_columns:
            if col in df_transformed.columns:
                df_transformed[col] = df_transformed[col].astype(bool)

        # Fill NaN values with appropriate defaults
        df_transformed = df_transformed.fillna(
            {
                "dock_bay": None,
                "dock_level": None,
                "services": "",
                "is_emergency": False,
            }
        )

        logging.info(f"Transformed {len(df_transformed)} records for PostgreSQL")
        return df_transformed

    @staticmethod
    def validate_data(df: pd.DataFrame) -> bool:
        """
        Validate the transformed data before insertion.

        Args:
            df: DataFrame to validate

        Returns:
            True if data is valid, False otherwise
        """
        required_columns = [
            "transaction_id",
            "station_id",
            "ship_name",
            "fuel_type",
            "visited_at",
        ]

        # Check required columns exist
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logging.error(f"Missing required columns: {missing_columns}")
            return False

        # Check for duplicate transaction IDs
        if df["transaction_id"].duplicated().any():
            logging.error("Found duplicate transaction IDs")
            return False

        # Check for reasonable data ranges
        if "fuel_units" in df.columns:
            if (df["fuel_units"] < 0).any() or (df["fuel_units"] > 10000).any():
                logging.warning("Found fuel_units outside expected range [0, 10000]")

        if "station_id" in df.columns:
            if (df["station_id"] < 1000).any() or (df["station_id"] > 9999).any():
                logging.warning("Found station_id outside expected range [1000, 9999]")

        logging.info("Data validation completed successfully")
        return True


class FileTracker:
    """Manages tracking of processed files."""

    def __init__(self, postgres_hook):
        self.postgres_hook = postgres_hook

    def get_processed_files(self) -> set:
        """
        Get set of already processed filenames.

        Returns:
            Set of processed filenames
        """
        try:
            query = "SELECT filename FROM processed_files"
            records = self.postgres_hook.get_records(query)
            return {record[0] for record in records}
        except Exception as e:
            logging.warning(f"Could not query processed files: {e}")
            return set()

    def mark_file_processed(self, filename: str) -> None:
        """
        Mark a file as processed.

        Args:
            filename: Name of the processed file
        """
        try:
            query = """
            INSERT INTO processed_files (filename, processed_at)
            VALUES (%s, %s)
            ON CONFLICT (filename) DO NOTHING
            """
            self.postgres_hook.run(query, parameters=(filename, datetime.utcnow()))
            logging.info(f"Marked file as processed: {filename}")
        except Exception as e:
            logging.error(f"Error marking file as processed {filename}: {e}")
            raise


class PostgreSQLLoader:
    """Handles loading data into PostgreSQL."""

    def __init__(self, postgres_hook, table_name: str):
        self.postgres_hook = postgres_hook
        self.table_name = table_name

    def insert_dataframe(self, df: pd.DataFrame, batch_size: int = 1000) -> None:
        """
        Insert DataFrame into PostgreSQL table.

        Args:
            df: DataFrame to insert
            batch_size: Number of records to insert in each batch
        """
        try:
            # Convert DataFrame to list of tuples
            data_tuples = [tuple(row) for row in df.values]

            # Insert in batches
            self.postgres_hook.insert_rows(
                table=self.table_name,
                rows=data_tuples,
                target_fields=list(df.columns),
                commit_every=batch_size,
            )

            logging.info(
                f"Successfully inserted {len(df)} records into {self.table_name}"
            )

        except Exception as e:
            logging.error(f"Error inserting data into {self.table_name}: {e}")
            raise
