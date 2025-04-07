# Default recipe to list all available recipes
default:
    @just --list

# Run the healthkit-to-sqlite script with custom input and output paths
healthkit-to-sqlite input_zip output_db data_path="~/icloud/Data/apple_health_export":
    python src/example_package/healthkit_to_sqlite.py {{data_path}}/{{input_zip}} {{output_db}}