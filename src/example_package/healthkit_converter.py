import argparse
import subprocess
import sys
from pathlib import Path
from loguru import logger
import tomllib  # For parsing TOML files
import duckdb
from typing import List, Dict


class HealthKitConverter:
    def __init__(
        self,
        zip_filepath: Path,
        sqlite_filepath: Path,
        duckdb_filepath: Path,
        tables_to_keep: List[str],
    ):
        self.zip_filepath = zip_filepath
        self.sqlite_filepath = sqlite_filepath
        self.duckdb_filepath = duckdb_filepath
        self.tables_to_keep = tables_to_keep

    @classmethod
    def from_toml(cls, toml_path: Path) -> "HealthKitConverter":
        """Load configuration from a TOML file and initialize the converter."""
        if not toml_path.exists():
            raise FileNotFoundError(f"TOML configuration file not found: {toml_path}")

        with open(toml_path, "rb") as f:
            config = tomllib.load(f)

        return cls(
            zip_filepath=Path(config["paths"]["zip_filepath"]),
            sqlite_filepath=Path(config["paths"]["sqlite_filepath"]),
            duckdb_filepath=Path(config["paths"]["duckdb_filepath"]),
            tables_to_keep=config["parameters"]["tables_to_keep"],
        )

    def convert_zip_to_sqlite(self, force: bool = False):
        """Convert HealthKit export ZIP to SQLite database."""
        # Check if the ZIP file exists
        if not self.zip_filepath.exists():
            logger.error(f"The zip file '{self.zip_filepath}' does not exist.")
            sys.exit(1)

        # Check if the SQLite file exists
        if self.sqlite_filepath.exists():
            if not force:
                logger.warning(
                    f"The SQLite file '{self.sqlite_filepath}' already exists. Use '--force' to overwrite."
                )
                return  # Exit without recreating the database
            else:
                logger.info(
                    f"Overwriting existing SQLite file '{self.sqlite_filepath}'."
                )

        # Run the healthkit-to-sqlite command
        command = f"healthkit-to-sqlite {self.zip_filepath} {self.sqlite_filepath}"
        logger.info(f"Running command: {command}")
        subprocess.run(command, shell=True)

    def convert_sqlite_to_duckdb(self):
        """Convert SQLite database to DuckDB with transformations."""
        logger.info("Starting conversion from SQLite to DuckDB...")

        con = duckdb.connect(str(self.duckdb_filepath))
        con.execute("INSTALL sqlite;")
        con.execute("LOAD sqlite;")
        con.execute(f"ATTACH '{self.sqlite_filepath}' (TYPE sqlite);")

        tables = [row[0] for row in con.execute("SHOW TABLES;").fetchall()]
        logger.info(f"Tables in SQLite database: {tables}")

        tables_to_drop = set(tables) - set(self.tables_to_keep)

        for table in self.tables_to_keep:
            if table == "workouts":
                logger.info(f"Transforming table '{table}'...")
                sql = f"""
                CREATE TABLE workouts AS 
                SELECT *, uuid() AS workout_uuid 
                FROM {table};
                """
                con.execute(sql)
            elif table == "workout_points":
                logger.info(f"Copying table '{table}'...")
                con.execute(f"CREATE TABLE {table} AS SELECT * FROM {table};")
            else:
                logger.warning(f"Skipping unknown table '{table}'...")

        for table in tables_to_drop:
            logger.info(f"Dropping table '{table}'...")
            con.execute(f"DROP TABLE IF EXISTS {table};")

        logger.info("Conversion completed successfully!")

    def run(self, force: bool = False):
        """Run the full conversion pipeline."""
        self.convert_zip_to_sqlite(force=force)
        self.convert_sqlite_to_duckdb()


def parse_args() -> Dict:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Convert HealthKit export zip to DuckDB database."
    )
    parser.add_argument(
        "--zip",
        type=Path,
        help="The filepath to the HealthKit export zip.",
    )
    parser.add_argument(
        "--sqlite",
        type=Path,
        help="The filepath for the intermediate SQLite database.",
    )
    parser.add_argument(
        "--duckdb",
        type=Path,
        help="The filepath for the output DuckDB database.",
    )
    parser.add_argument(
        "--tables-to-keep",
        nargs="+",
        default=["workouts", "workout_points"],
        help="List of tables to keep in the DuckDB database.",
    )
    parser.add_argument("--toml", type=Path, help="Path to a TOML configuration file.")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing SQLite database if it exists.",
    )

    args = parser.parse_args()

    # If TOML is provided, load it and override defaults
    if args.toml:
        with open(args.toml, "rb") as f:
            config = tomllib.load(f)

        # Override arguments with TOML values where applicable
        return {
            "zip_filepath": args.zip or Path(config["paths"].get("zip_filepath")),
            "sqlite_filepath": args.sqlite
            or Path(config["paths"].get("sqlite_filepath")),
            "duckdb_filepath": args.duckdb
            or Path(config["paths"].get("duckdb_filepath")),
            "tables_to_keep": args.tables_to_keep
            or config["parameters"].get("tables_to_keep", []),
            "force": args.force,
        }

    # Ensure required arguments are provided
    if not args.zip:
        raise ValueError(
            "Error: '--zip' argument is required if no TOML file is provided."
        )
    if not args.sqlite:
        raise ValueError(
            "Error: '--sqlite' argument is required if no TOML file is provided."
        )
    if not args.duckdb:
        raise ValueError(
            "Error: '--duckdb' argument is required if no TOML file is provided."
        )

    # Use command-line arguments directly if no TOML is provided
    return {
        "zip_filepath": args.zip,
        "sqlite_filepath": args.sqlite,
        "duckdb_filepath": args.duckdb,
        "tables_to_keep": args.tables_to_keep,
        "force": args.force,
    }


def main():
    config = parse_args()

    converter = HealthKitConverter(
        zip_filepath=config["zip_filepath"],
        sqlite_filepath=config["sqlite_filepath"],
        duckdb_filepath=config["duckdb_filepath"],
        tables_to_keep=config["tables_to_keep"],
    )
    converter.run(force=config["force"])


if __name__ == "__main__":
    main()
