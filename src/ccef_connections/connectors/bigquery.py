"""
BigQuery connector for CCEF connections library.

This module provides both READ and WRITE access to Google BigQuery for
data warehouse operations using service account authentication.
"""

import logging
from typing import Any, Dict, List, Optional, Literal

from google.cloud import bigquery
from google.oauth2.service_account import Credentials

from ..core.base import BaseConnection
from ..core.retry import retry_google_operation
from ..exceptions import ConnectionError, CredentialError, QueryError, WriteError

logger = logging.getLogger(__name__)

# Type for write disposition
WriteDisposition = Literal["append", "replace", "fail_if_exists"]


class BigQueryConnector(BaseConnection):
    """
    BigQuery connector for data warehouse operations.

    This connector provides both read (query) and write (insert, load) operations
    for BigQuery with service account authentication.

    Examples:
        >>> connector = BigQueryConnector(project_id='your-project')
        >>>
        >>> # Query data
        >>> results = connector.query("SELECT * FROM dataset.table LIMIT 10")
        >>>
        >>> # Insert rows
        >>> rows = [{'col1': 'val1', 'col2': 'val2'}]
        >>> connector.insert_rows('dataset.table', rows)
        >>>
        >>> # Load from DataFrame (requires pandas)
        >>> import pandas as pd
        >>> df = pd.DataFrame({'col1': [1, 2, 3], 'col2': ['a', 'b', 'c']})
        >>> connector.load_dataframe(df, 'dataset.table', if_exists='append')
    """

    def __init__(self, project_id: Optional[str] = None) -> None:
        """
        Initialize the BigQuery connector.

        Args:
            project_id: GCP project ID (optional, can be specified in credentials)
        """
        super().__init__()
        self._project_id = project_id
        self._credentials: Optional[Credentials] = None

    def connect(self) -> None:
        """
        Establish connection to BigQuery using service account credentials.

        Raises:
            CredentialError: If BigQuery credentials are missing or invalid
            ConnectionError: If connection fails
        """
        try:
            creds_dict = self._credential_manager.get_bigquery_credentials()

            # Create credentials from service account info
            self._credentials = Credentials.from_service_account_info(creds_dict)

            # Use project_id from credentials if not provided
            if self._project_id is None:
                self._project_id = creds_dict.get("project_id")

            if not self._project_id:
                raise CredentialError(
                    "Project ID must be provided either in constructor or credentials"
                )

            # Create BigQuery client
            self._client = bigquery.Client(
                credentials=self._credentials, project=self._project_id
            )
            self._is_connected = True
            logger.info(f"Successfully connected to BigQuery (project: {self._project_id})")
        except CredentialError:
            logger.error("Failed to connect to BigQuery: missing credentials")
            raise
        except Exception as e:
            logger.error(f"Failed to connect to BigQuery: {str(e)}")
            raise ConnectionError(f"Failed to connect to BigQuery: {str(e)}") from e

    def disconnect(self) -> None:
        """Close the BigQuery connection."""
        if self._client:
            self._client.close()
        self._client = None
        self._credentials = None
        self._is_connected = False
        logger.debug("Disconnected from BigQuery")

    def health_check(self) -> bool:
        """
        Check if the BigQuery connection is healthy.

        Returns:
            True if connected and client exists, False otherwise
        """
        return self._is_connected and self._client is not None

    @retry_google_operation
    def query(
        self,
        sql: str,
        params: Optional[List[Any]] = None,
        timeout: Optional[float] = None,
    ) -> bigquery.table.RowIterator:
        """
        Execute a SQL query.

        Args:
            sql: SQL query string
            params: Optional query parameters for parameterized queries
            timeout: Query timeout in seconds

        Returns:
            RowIterator with query results

        Raises:
            QueryError: If query execution fails

        Examples:
            >>> connector = BigQueryConnector(project_id='your-project')
            >>> results = connector.query("SELECT * FROM dataset.table LIMIT 10")
            >>> for row in results:
            ...     print(row['column_name'])
        """
        if not self._is_connected or self._client is None:
            self.connect()

        if self._client is None:
            raise ConnectionError("Not connected to BigQuery")

        try:
            logger.debug(f"Executing query: {sql[:100]}...")
            job_config = bigquery.QueryJobConfig()

            if params:
                job_config.query_parameters = params

            query_job = self._client.query(sql, job_config=job_config, timeout=timeout)
            results = query_job.result()
            logger.debug(f"Query completed: {results.total_rows} rows")
            return results
        except Exception as e:
            logger.error(f"Query failed: {str(e)}")
            raise QueryError(f"Query failed: {str(e)}") from e

    @retry_google_operation
    def query_to_dataframe(
        self,
        sql: str,
        params: Optional[List[Any]] = None,
        timeout: Optional[float] = None,
    ) -> Any:
        """
        Execute a SQL query and return results as a pandas DataFrame.

        Args:
            sql: SQL query string
            params: Optional query parameters
            timeout: Query timeout in seconds

        Returns:
            pandas DataFrame with query results

        Raises:
            QueryError: If query execution fails
            ImportError: If pandas is not installed

        Examples:
            >>> connector = BigQueryConnector(project_id='your-project')
            >>> df = connector.query_to_dataframe("SELECT * FROM dataset.table")
            >>> print(df.head())
        """
        try:
            import pandas as pd
        except ImportError:
            raise ImportError(
                "pandas is required for query_to_dataframe. "
                "Install with: pip install ccef-connections[pandas]"
            )

        results = self.query(sql, params, timeout)
        df = results.to_dataframe()
        logger.debug(f"Converted query results to DataFrame: {len(df)} rows")
        return df

    @retry_google_operation
    def table_exists(self, table_id: str) -> bool:
        """
        Check if a table exists.

        Args:
            table_id: Table ID in format 'dataset.table' or 'project.dataset.table'

        Returns:
            True if table exists, False otherwise

        Examples:
            >>> connector = BigQueryConnector(project_id='your-project')
            >>> if connector.table_exists('dataset.table'):
            ...     print("Table exists")
        """
        if not self._is_connected or self._client is None:
            self.connect()

        if self._client is None:
            raise ConnectionError("Not connected to BigQuery")

        try:
            full_table_id = self._get_full_table_id(table_id)
            self._client.get_table(full_table_id)
            logger.debug(f"Table exists: {table_id}")
            return True
        except Exception:
            logger.debug(f"Table does not exist: {table_id}")
            return False

    @retry_google_operation
    def insert_rows(
        self, table_id: str, rows: List[Dict[str, Any]]
    ) -> None:
        """
        Insert rows into a table using streaming insert.

        Args:
            table_id: Table ID in format 'dataset.table' or 'project.dataset.table'
            rows: List of dictionaries representing rows to insert

        Raises:
            WriteError: If insert fails

        Examples:
            >>> connector = BigQueryConnector(project_id='your-project')
            >>> rows = [
            ...     {'name': 'John', 'age': 30, 'city': 'NYC'},
            ...     {'name': 'Jane', 'age': 25, 'city': 'LA'},
            ... ]
            >>> connector.insert_rows('dataset.users', rows)
        """
        if not self._is_connected or self._client is None:
            self.connect()

        if self._client is None:
            raise ConnectionError("Not connected to BigQuery")

        try:
            full_table_id = self._get_full_table_id(table_id)
            table = self._client.get_table(full_table_id)
            errors = self._client.insert_rows_json(table, rows)

            if errors:
                error_msg = f"Insert failed with errors: {errors}"
                logger.error(error_msg)
                raise WriteError(error_msg)

            logger.info(f"Successfully inserted {len(rows)} rows into {table_id}")
        except WriteError:
            raise
        except Exception as e:
            logger.error(f"Insert failed: {str(e)}")
            raise WriteError(f"Insert failed: {str(e)}") from e

    @retry_google_operation
    def load_dataframe(
        self,
        df: Any,
        table_id: str,
        if_exists: WriteDisposition = "append",
    ) -> None:
        """
        Load a pandas DataFrame into a BigQuery table.

        Args:
            df: pandas DataFrame to load
            table_id: Table ID in format 'dataset.table' or 'project.dataset.table'
            if_exists: What to do if table exists: 'append', 'replace', 'fail_if_exists'

        Raises:
            WriteError: If load fails
            ImportError: If pandas is not installed

        Examples:
            >>> import pandas as pd
            >>> connector = BigQueryConnector(project_id='your-project')
            >>> df = pd.DataFrame({'col1': [1, 2, 3], 'col2': ['a', 'b', 'c']})
            >>> connector.load_dataframe(df, 'dataset.table', if_exists='append')
        """
        try:
            import pandas as pd
        except ImportError:
            raise ImportError(
                "pandas is required for load_dataframe. "
                "Install with: pip install ccef-connections[pandas]"
            )

        if not self._is_connected or self._client is None:
            self.connect()

        if self._client is None:
            raise ConnectionError("Not connected to BigQuery")

        try:
            full_table_id = self._get_full_table_id(table_id)

            # Convert if_exists to BigQuery write disposition
            write_disposition_map = {
                "append": bigquery.WriteDisposition.WRITE_APPEND,
                "replace": bigquery.WriteDisposition.WRITE_TRUNCATE,
                "fail_if_exists": bigquery.WriteDisposition.WRITE_EMPTY,
            }

            job_config = bigquery.LoadJobConfig(
                write_disposition=write_disposition_map[if_exists]
            )

            job = self._client.load_table_from_dataframe(
                df, full_table_id, job_config=job_config
            )
            job.result()  # Wait for job to complete

            logger.info(f"Successfully loaded {len(df)} rows into {table_id}")
        except Exception as e:
            logger.error(f"Load failed: {str(e)}")
            raise WriteError(f"Load failed: {str(e)}") from e

    @retry_google_operation
    def execute_dml(self, sql: str) -> int:
        """
        Execute a DML statement (UPDATE, DELETE, etc.).

        Args:
            sql: DML SQL statement

        Returns:
            Number of affected rows

        Raises:
            QueryError: If DML execution fails

        Examples:
            >>> connector = BigQueryConnector(project_id='your-project')
            >>> rows_affected = connector.execute_dml(
            ...     "UPDATE dataset.table SET status = 'processed' WHERE id = 123"
            ... )
            >>> print(f"Updated {rows_affected} rows")
        """
        if not self._is_connected or self._client is None:
            self.connect()

        if self._client is None:
            raise ConnectionError("Not connected to BigQuery")

        try:
            logger.debug(f"Executing DML: {sql[:100]}...")
            query_job = self._client.query(sql)
            query_job.result()  # Wait for job to complete
            rows_affected = query_job.num_dml_affected_rows or 0
            logger.info(f"DML completed: {rows_affected} rows affected")
            return rows_affected
        except Exception as e:
            logger.error(f"DML failed: {str(e)}")
            raise QueryError(f"DML failed: {str(e)}") from e

    def _get_full_table_id(self, table_id: str) -> str:
        """
        Get full table ID including project.

        Args:
            table_id: Table ID (may or may not include project)

        Returns:
            Full table ID in format 'project.dataset.table'
        """
        parts = table_id.split(".")
        if len(parts) == 2:
            # Add project if not present
            return f"{self._project_id}.{table_id}"
        return table_id

    @property
    def project_id(self) -> Optional[str]:
        """Get the current project ID."""
        return self._project_id
