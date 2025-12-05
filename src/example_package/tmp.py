import duckdb
from pathlib import Path


def main():
    db_name = (
        Path.cwd() / "data/healthkit-transformed_2024_12_08.duckdb.duckdb"
    )  # Using underscores

    if db_name.exists():
        print(f"Database file {db_name} exists.")
        con = duckdb.connect(f"{db_name}.duckdb")

        print(con.sql("SHOW TABLES"))

        table_name = "workout_points"  # Could also be "workout-points"

        # If table_name has hyphens or other special chars
        query = f"SELECT * FROM {table_name} LIMIT 10"

        con.execute(query).df()
        print(f"Query executed successfully: {query}")
        con.close()
    else:
        print(f"Database file {db_name} does not exist. Please check the path.")


if __name__ == "__main__":
    main()
