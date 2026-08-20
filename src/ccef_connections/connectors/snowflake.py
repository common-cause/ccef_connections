"""
Snowflake connector for CCEF connections library.

Read access to the CCEF Snowflake replica (``CMNC_DATA``), which carries the ROI
Solutions CRM data, the Unite data model, and Fivetran mirrors of the BigQuery
datasets.

This connector replaces the ``scripts/sfquery.py`` shim that had been copied into five
separate projects (``financial-reconciliation``, ``snowflake-research``,
``roi-campaign-sources``, ``major-donor-briefs``, ``unite-dashboards``). :meth:`query`
returns ``(columns, rows)`` deliberately, matching that shim's ``run()`` so migration is
a change of import rather than a change of shape.

Configuration
-------------
Unlike most connectors here, Snowflake needs six connection settings alongside its
password, so it resolves each one as: **explicit keyword > environment variable >
built-in default**, with the password always coming from
``SNOWFLAKE_CREDENTIALS_PASSWORD``::

    SnowflakeConnector()                        # everything from the environment
    SnowflakeConnector(schema="DATA_MODEL")     # override one setting
    SnowflakeConnector(warehouse="BIG_WH")

Reading from the environment is what lets this work on Civis, which injects credentials
as environment variables with no ``.env`` on disk.

⚠ Warehouse-level statement timeout
-----------------------------------
``READER_WH`` enforces a **60-second statement timeout at the warehouse level**. That
overrides any session ``STATEMENT_TIMEOUT_IN_SECONDS``, and the ``PUBLIC`` role cannot
raise it — so the ``timeout`` argument here can only ever lower the limit, never raise
it. Passing ``timeout=900`` does nothing on ``READER_WH``; the shim this replaces
hardcoded exactly that and had no effect for months. Anything needing a bigger join must
pull narrow slices and combine them client-side, or be pre-aggregated elsewhere.

⚠ Read-only
-----------
The service account is read-only. There are no write methods here on purpose, and
``CREATE TEMPORARY TABLE`` fails — which is why client-side joins are the pattern rather
than staging tables.
"""

import logging
from typing import Any, Dict, List, Optional, Sequence, Tuple

import snowflake.connector

from ..core.base import BaseConnection
from ..core.retry import retry_snowflake_operation
from ..exceptions import (
    AuthenticationError,
    ConnectionError,
    CredentialError,
    QueryError,
)

logger = logging.getLogger(__name__)

# Every setting except account, user, role and the password has a working default.
#
# ⚠ ``role`` is deliberately absent. An unset role silently lands on whatever the
# account default is, which may not see the ``BQ_*`` mirror schemas — a query then
# fails with "object does not exist" rather than a permission error, which sends you
# looking for the wrong bug. Better to demand it.
DEFAULTS: Dict[str, str] = {
    "warehouse": "READER_WH",
    "database": "CMNC_DATA",
    "schema": "ROI",
}

# Settings with no safe default: absence is an error, not something to guess at.
REQUIRED_SETTINGS: Tuple[str, ...] = ("account", "user", "role")

# Environment variable per setting. The password is handled separately, through
# CredentialManager, because it follows the {NAME}_PASSWORD convention.
ENV_VARS: Dict[str, str] = {
    "account": "SNOWFLAKE_ACCOUNT",
    "user": "SNOWFLAKE_USER",
    "role": "SNOWFLAKE_ROLE",
    "warehouse": "SNOWFLAKE_WAREHOUSE",
    "database": "SNOWFLAKE_DATABASE",
    "schema": "SNOWFLAKE_SCHEMA",
}


class SnowflakeConnector(BaseConnection):
    """
    Snowflake connector for read access to the CCEF replica.

    Examples:
        >>> connector = SnowflakeConnector()
        >>> cols, rows = connector.query("SELECT CURRENT_ROLE()")
        >>>
        >>> # Point at a different schema for this connector only
        >>> unite = SnowflakeConnector(schema="DATA_MODEL")
        >>> cols, rows = unite.query("SELECT COUNT(*) FROM FACT_TRANSACTION")
        >>>
        >>> # Rows as dicts, or as a DataFrame (needs the pandas extra)
        >>> for row in connector.query_dicts("SELECT ROI_ID FROM ACCOUNT_PROFILE LIMIT 5"):
        ...     print(row["ROI_ID"])
        >>>
        >>> # Closes the connection on exit
        >>> with SnowflakeConnector() as conn:
        ...     cols, rows = conn.query("SELECT 1")
    """

    def __init__(
        self,
        account: Optional[str] = None,
        user: Optional[str] = None,
        role: Optional[str] = None,
        warehouse: Optional[str] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        timeout: Optional[int] = None,
    ) -> None:
        """
        Initialize the Snowflake connector.

        Every argument is optional and falls back to its ``SNOWFLAKE_*`` environment
        variable, then to a built-in default where one is safe.

        Args:
            account: Snowflake account identifier (env ``SNOWFLAKE_ACCOUNT``)
            user: Snowflake user (env ``SNOWFLAKE_USER``)
            role: Role to assume (env ``SNOWFLAKE_ROLE``). Required — see module docs
            warehouse: Warehouse (env ``SNOWFLAKE_WAREHOUSE``, default ``READER_WH``)
            database: Database (env ``SNOWFLAKE_DATABASE``, default ``CMNC_DATA``)
            schema: Default schema (env ``SNOWFLAKE_SCHEMA``, default ``ROI``)
            timeout: Session statement timeout in seconds. **Can only lower the
                warehouse limit, never raise it** — see the module docstring
        """
        super().__init__()
        self._overrides: Dict[str, Optional[str]] = {
            "account": account,
            "user": user,
            "role": role,
            "warehouse": warehouse,
            "database": database,
            "schema": schema,
        }
        self._timeout = timeout
        self._config: Optional[Dict[str, Any]] = None

    # -- configuration -------------------------------------------------------

    def _resolve_config(self) -> Dict[str, Any]:
        """
        Resolve connection settings into ``snowflake.connector.connect`` kwargs.

        Returns:
            Connect kwargs, including the password

        Raises:
            CredentialError: If the password or a required setting is missing. The
                message names the setting and the environment variable it was looked
                for in, rather than letting the driver fail less specifically
        """
        creds = self._credential_manager.get_snowflake_credentials()

        config: Dict[str, Any] = {"password": creds["password"]}
        for setting, env_var in ENV_VARS.items():
            value = self._overrides.get(setting) or creds.get(setting) or DEFAULTS.get(setting)
            config[setting] = value

        missing = [s for s in REQUIRED_SETTINGS if not config.get(s)]
        if missing:
            details = ", ".join(f"{s} (env {ENV_VARS[s]})" for s in missing)
            raise CredentialError(
                f"Snowflake connection is missing required setting(s): {details}. "
                f"Pass them to SnowflakeConnector(...) or set the environment "
                f"variables."
            )

        if self._timeout is not None:
            # Best effort only. A warehouse-level timeout overrides this, and on
            # READER_WH that ceiling is 60s and PUBLIC cannot raise it.
            config["session_parameters"] = {
                "STATEMENT_TIMEOUT_IN_SECONDS": self._timeout
            }
        return config

    @property
    def config(self) -> Dict[str, Any]:
        """
        The resolved connection settings, with the password redacted.

        Safe to log or print — useful when a query hits the wrong schema and you need
        to see what the connector actually resolved.

        Returns:
            Resolved settings, password replaced with ``"***"``
        """
        config = dict(self._config or self._resolve_config())
        if "password" in config:
            config["password"] = "***"
        return config

    # -- connection ----------------------------------------------------------

    def connect(self) -> None:
        """
        Establish the Snowflake connection.

        Raises:
            CredentialError: If the password or a required setting is missing
            AuthenticationError: If Snowflake rejects the credentials
            ConnectionError: If the connection fails for any other reason,
                including the IP allowlist
        """
        try:
            self._config = self._resolve_config()
        except CredentialError:
            logger.error("Failed to connect to Snowflake: credentials incomplete")
            raise

        try:
            self._client = snowflake.connector.connect(**self._config)
            self._is_connected = True
            logger.info(
                "Connected to Snowflake (account: %s, role: %s, warehouse: %s, "
                "database: %s, schema: %s)",
                self._config.get("account"),
                self._config.get("role"),
                self._config.get("warehouse"),
                self._config.get("database"),
                self._config.get("schema"),
            )
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {str(e)}")
            raise self._translate(e, "Failed to connect to Snowflake") from e

    def disconnect(self) -> None:
        """Close the Snowflake connection."""
        if self._client is not None:
            try:
                self._client.close()
            except Exception as e:  # a close failure must not mask the real error
                logger.debug(f"Error closing Snowflake connection: {str(e)}")
        self._client = None
        self._is_connected = False
        logger.debug("Disconnected from Snowflake")

    def health_check(self) -> bool:
        """
        Check whether the connection is usable.

        Runs ``SELECT 1`` rather than trusting the connected flag, so a session that
        has been closed server-side is reported as unhealthy.

        Returns:
            True if a trivial query succeeds, False otherwise
        """
        try:
            _, rows = self.query("SELECT 1")
            return bool(rows)
        except Exception as e:
            logger.warning(f"Snowflake health check failed: {str(e)}")
            return False

    # -- queries -------------------------------------------------------------

    @retry_snowflake_operation
    def query(
        self,
        sql: str,
        params: Optional[Sequence[Any]] = None,
        timeout: Optional[int] = None,
    ) -> Tuple[List[str], List[Tuple[Any, ...]]]:
        """
        Run a query and return its columns and rows.

        Returns ``(columns, rows)`` to match the ``sfquery.run()`` shim this connector
        replaces, so existing callers migrate by changing the import.

        Args:
            sql: SQL to execute
            params: Optional bind parameters, passed through to the driver
            timeout: Per-statement timeout in seconds. **Only lowers the warehouse
                ceiling** — see the module docstring

        Returns:
            Tuple of (column names, list of row tuples)

        Raises:
            QueryError: If the query fails, including on statement timeout
            ConnectionError: If not connected and connecting fails

        Examples:
            >>> connector = SnowflakeConnector()
            >>> cols, rows = connector.query("SELECT CURRENT_WAREHOUSE()")
            >>> cols
            ['CURRENT_WAREHOUSE()']
        """
        if not self._is_connected or self._client is None:
            self.connect()
        if self._client is None:
            raise ConnectionError("Not connected to Snowflake")

        cursor = self._client.cursor()
        try:
            logger.debug(f"Executing query: {sql[:100]}...")
            if timeout is not None:
                cursor.execute(
                    f"ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = {int(timeout)}"
                )
            cursor.execute(sql, params) if params else cursor.execute(sql)
            columns = [c[0] for c in cursor.description or []]
            rows = cursor.fetchall()
            logger.debug(f"Query returned {len(rows)} rows")
            return columns, rows
        except Exception as e:
            logger.error(f"Query failed: {str(e)}")
            raise self._translate(e, "Query failed") from e
        finally:
            cursor.close()

    def query_dicts(
        self,
        sql: str,
        params: Optional[Sequence[Any]] = None,
        timeout: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Run a query and return one dict per row, keyed by column name.

        Args:
            sql: SQL to execute
            params: Optional bind parameters
            timeout: Per-statement timeout in seconds

        Returns:
            List of dicts, one per row

        Raises:
            QueryError: If the query fails

        Examples:
            >>> connector = SnowflakeConnector()
            >>> rows = connector.query_dicts("SELECT CURRENT_ROLE() AS ROLE")
            >>> rows[0]["ROLE"]
            'PUBLIC'
        """
        columns, rows = self.query(sql, params, timeout)
        return [dict(zip(columns, row)) for row in rows]

    def query_to_dataframe(
        self,
        sql: str,
        params: Optional[Sequence[Any]] = None,
        timeout: Optional[int] = None,
    ) -> Any:
        """
        Run a query and return the results as a pandas DataFrame.

        Args:
            sql: SQL to execute
            params: Optional bind parameters
            timeout: Per-statement timeout in seconds

        Returns:
            pandas DataFrame of the results

        Raises:
            QueryError: If the query fails
            ImportError: If pandas is not installed

        Examples:
            >>> connector = SnowflakeConnector()
            >>> df = connector.query_to_dataframe("SELECT CURRENT_DATABASE() AS DB")
            >>> list(df.columns)
            ['DB']
        """
        try:
            import pandas as pd
        except ImportError:
            raise ImportError(
                "pandas is required for query_to_dataframe. "
                'Install with: pip install "ccef-connections[pandas]"'
            )

        columns, rows = self.query(sql, params, timeout)
        df = pd.DataFrame(rows, columns=columns)
        logger.debug(f"Converted query results to DataFrame: {len(df)} rows")
        return df

    def list_tables(self, schema: Optional[str] = None) -> List[str]:
        """
        List the tables and views in a schema.

        Args:
            schema: Schema to inspect. Defaults to the connector's schema

        Returns:
            Sorted table and view names

        Raises:
            QueryError: If the lookup fails

        Examples:
            >>> connector = SnowflakeConnector()
            >>> "ACCOUNT_PROFILE" in connector.list_tables()
            True
        """
        target = schema or self.config.get("schema")
        database = self.config.get("database")
        _, rows = self.query(
            f"SELECT TABLE_NAME FROM {database}.INFORMATION_SCHEMA.TABLES "
            f"WHERE TABLE_SCHEMA = %s ORDER BY TABLE_NAME",
            (target,),
        )
        return [str(r[0]) for r in rows]

    # -- error translation ---------------------------------------------------

    def _translate(self, exc: Exception, prefix: str) -> Exception:
        """
        Turn a driver error into the right library exception, with a useful hint.

        The hints matter more than the exception types. Snowflake reports several
        very different problems in ways that read like a bad credential, and each of
        these cost real debugging time before it was written down:

        - **IP allowlist.** The replica is IP-allowlisted, so connecting from a new
          network fails in a way that looks like an auth problem. It is not; the
          credential is fine and the IP needs adding.
        - **Statement timeout.** ``READER_WH`` caps statements at 60s at the
          *warehouse* level, so a query that "should" have had 900s dies at 60
          anyway.
        - **Read-only.** ``CREATE TEMPORARY TABLE`` and friends fail because the
          account cannot write, not because the SQL is wrong.

        Args:
            exc: The exception raised by the driver
            prefix: Context for the message

        Returns:
            The exception to raise (never raises on its own)
        """
        text = str(exc)
        lowered = text.lower()
        errno = getattr(exc, "errno", None)

        if errno == 250001 or "not allowed to access" in lowered or "network policy" in lowered:
            return ConnectionError(
                f"{prefix}: Snowflake refused the connection from this network. "
                f"The replica is IP-allowlisted, so this is usually the current IP "
                f"needing to be added — not a bad credential. Original error: {text}"
            )

        if errno in (390100, 390101) or "incorrect username or password" in lowered:
            return AuthenticationError(
                f"{prefix}: Snowflake rejected the credentials. Check "
                f"SNOWFLAKE_USER and SNOWFLAKE_CREDENTIALS_PASSWORD. "
                f"Original error: {text}"
            )

        if errno == 630 or "statement reached its statement or warehouse timeout" in lowered:
            return QueryError(
                f"{prefix}: the statement hit its timeout. READER_WH enforces a "
                f"60-second limit at the WAREHOUSE level, which overrides any "
                f"session timeout and cannot be raised by PUBLIC. Pull narrower "
                f"slices and join client-side, or pre-aggregate. "
                f"Original error: {text}"
            )

        if errno == 370001 or "read-only" in lowered:
            return QueryError(
                f"{prefix}: this account is read-only, so writes and "
                f"CREATE TEMPORARY TABLE are not available. Combine results "
                f"client-side instead of staging them. Original error: {text}"
            )

        if "does not exist or not authorized" in lowered:
            role = (self._config or {}).get("role") or self._overrides.get("role")
            role_note = f"role {role}" if role else f"the configured role ({ENV_VARS['role']})"
            return QueryError(
                f"{prefix}: object not found, or not visible to {role_note} "
                f"(set by {ENV_VARS['role']}). A role that cannot see a schema reports "
                f"it as a missing object rather than a permission error, so check the "
                f"role before the spelling. Original error: {text}"
            )

        if prefix.startswith("Failed to connect"):
            return ConnectionError(f"{prefix}: {text}")
        return QueryError(f"{prefix}: {text}")
