import argparse
import subprocess
import sys
from pathlib import Path

from loguru import logger


def convert_healthkit_to_sqlite(zip_filepath, sqlite_filepath):
    # Check if the zip file exists
    if not zip_filepath.exists():
        logger.error(f"Error: The zip file '{zip_filepath}' does not exist.")
        sys.exit(1)
    
    # Warn if the SQLite file already exists
    if sqlite_filepath.exists():
        logger.warning(f"Warning: The SQLite file '{sqlite_filepath}' already exists. It will be overwritten.")
    
    command = f"healthkit-to-sqlite {zip_filepath} {sqlite_filepath}"
    logger.info(command)
    subprocess.run(command, shell=True)

def main():
    parser = argparse.ArgumentParser(description="Convert HealthKit export zip to SQLite database.")
    parser.add_argument("zip_filepath", type=Path, help="The filepath to the HealthKit export zip.")
    parser.add_argument("sqlite_filepath", type=Path, help="The filepath for the output SQLite database.")
    
    args = parser.parse_args()
    
    convert_healthkit_to_sqlite(args.zip_filepath, args.sqlite_filepath)

if __name__ == "__main__":
    main()