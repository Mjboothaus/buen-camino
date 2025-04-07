import sqlite3
import duckdb
from pathlib import Path
from loguru import logger
import argparse
import sys


class LocalDatabaseExplorer:
    def __init__(self, sqlite_filepath: Path = None, duckdb_filepath: Path = None):
        self.sqlite_filepath = sqlite_filepath
        self.duckdb_filepath = duckdb_filepath

    def explore_sqlite(self):
        """Explore the SQLite database."""
        if not self.sqlite_filepath or not self.sqlite_filepath.exists():
            logger.error(f"SQLite file '{self.sqlite_filepath}' does not exist.")
            return

        try:
            logger.info(f"Exploring SQLite database: {self.sqlite_filepath}")
            conn = sqlite3.connect(self.sqlite_filepath)
            cursor = conn.cursor()

            # List all tables in the SQLite database
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()
            logger.info(f"SQLite Tables: {[table[0] for table in tables]}")

            # Optionally, show contents of each table (first 5 rows)
            for table in tables:
                logger.info(f"Contents of table '{table[0]}':")
                cursor.execute(f"SELECT * FROM {table[0]} LIMIT 5;")
                rows = cursor.fetchall()
                for row in rows:
                    logger.debug(row)

            conn.close()
        except sqlite3.OperationalError as e:
            logger.error(f"SQLite OperationalError: {e}")

    def explore_sqlite_with_duckdb(self):
        """Explore SQLite database using DuckDB."""
        if not self.sqlite_filepath or not self.sqlite_filepath.exists():
            logger.error(f"SQLite file '{self.sqlite_filepath}' does not exist.")
            return

        try:
            logger.info(
                f"Exploring SQLite database using DuckDB: {self.sqlite_filepath}"
            )

            # Connect to DuckDB and load the SQLite extension
            con = duckdb.connect()
            con.execute("INSTALL sqlite;")
            con.execute("LOAD sqlite;")

            # Attach the SQLite database with an alias
            con.execute(f"ATTACH '{self.sqlite_filepath}' AS tmp_sqlite (TYPE sqlite);")

            # Switch context to the attached SQLite database
            con.execute("USE tmp_sqlite;")

            # List all tables in the SQLite database
            tables = con.execute("SHOW TABLES;").fetchall()
            logger.info(f"SQLite Tables: {[table[0] for table in tables]}")

            # Show contents of each table (first 5 rows)
            for table in tables:
                table_name = table[0]
                logger.info(f"Contents of table '{table_name}':")
                rows = con.execute(f'SELECT * FROM "{table_name}" LIMIT 5').fetchall()
                if rows:
                    for row in rows:
                        logger.debug(row)
                else:
                    logger.warning(
                        f"Table '{table_name}' is empty or has no visible rows."
                    )

        except duckdb.IOException as e:
            logger.error(
                f"DuckDB IOException: The file might be locked or already open. Error: {e}"
            )
        except Exception as e:
            logger.error(f"Unexpected error while exploring SQLite with DuckDB: {e}")

    def explore_duckdb(self):
        """Explore the DuckDB database."""
        if not self.duckdb_filepath or not self.duckdb_filepath.exists():
            logger.error(f"DuckDB file '{self.duckdb_filepath}' does not exist.")
            return

        try:
            logger.info(f"Exploring DuckDB database: {self.duckdb_filepath}")
            con = duckdb.connect(str(self.duckdb_filepath))

            # List all tables in the DuckDB database
            tables = con.execute("SHOW TABLES;").fetchall()
            logger.info(f"DuckDB Tables: {[table[0] for table in tables]}")

            # Optionally, show contents of each table (first 5 rows)
            for table in tables:
                logger.info(f"Contents of table '{table[0]}':")
                rows = con.execute(f"SELECT * FROM {table[0]} LIMIT 5;").fetchall()
                for row in rows:
                    logger.debug(row)

            con.close()
        except duckdb.IOException as e:
            logger.error(
                f"DuckDB IOException: The file might be locked or already open. Error: {e}"
            )
        except Exception as e:
            logger.error(f"Unexpected error while exploring DuckDB: {e}")

    def debug_databases(self):
        """Debug both SQLite and DuckDB databases."""
        if self.sqlite_filepath:
            # self.explore_sqlite()
            self.explore_sqlite_with_duckdb()

        if self.duckdb_filepath:
            self.explore_duckdb()


def main():
    parser = argparse.ArgumentParser(
        description="Explore local SQLite and DuckDB databases."
    )
    parser.add_argument("--sqlite", type=Path, help="Path to the SQLite database file.")
    parser.add_argument("--duckdb", type=Path, help="Path to the DuckDB database file.")
    args = parser.parse_args()

    # Configure Loguru for logging
    logger.remove()
    logger.add(sys.stdout, level="INFO", colorize=True)
    logger.add(
        "database_explorer.log", rotation="1 MB", retention="1 week", level="DEBUG"
    )

    explorer = LocalDatabaseExplorer(
        sqlite_filepath=args.sqlite, duckdb_filepath=args.duckdb
    )
    explorer.debug_databases()


if __name__ == "__main__":
    main()
