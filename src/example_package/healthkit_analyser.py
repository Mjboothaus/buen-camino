from pathlib import Path
from dataclasses import dataclass
import tomllib
from typing import Dict, Any, List, Optional
from functools import cache
import duckdb
import folium
import pandas as pd
from loguru import logger


# ### Configuration Management
@dataclass
class HealthKitConfig:
    db_path: Path = Path("data/healthkit-transformed.duckdb")
    sql_dir: Path = Path("sql")
    cache_dir: Path = Path("cache")
    min_duration: int = 20
    map_defaults: Dict[str, Any] = None
    cache_backend: str = "memory"  # memory|disk

    @classmethod
    def from_toml(cls, toml_path: Path = Path("config.toml")):
        with open(toml_path, "rb") as f:
            config_data = tomllib.load(f)
        return cls(
            db_path=Path(config_data["paths"]["database"]),
            sql_dir=Path(config_data["paths"]["sql"]),
            cache_dir=Path(config_data["paths"]["cache"]),
            min_duration=config_data["parameters"]["min_duration"],
            map_defaults=config_data["map_defaults"],
            cache_backend=config_data["caching"]["backend"],
        )


# ### SQL Management
class SQLManager:
    def __init__(self, sql_dir: Path):
        self.sql_dir = sql_dir
        self.queries = self._load_queries()

    def _load_queries(self) -> Dict[str, str]:
        return {fp.stem: fp.read_text() for fp in self.sql_dir.glob("*.sql")}

    def get_query(self, name: str, params: Optional[Dict] = None) -> str:
        query = self.queries[name]
        return query.format(**(params or {}))


class HealthKitAnalyser:
    def __init__(self, config: Optional[HealthKitConfig] = None):
        self.config = config or HealthKitConfig.from_toml()
        self.sql_mgr = SQLManager(self.config.sql_dir)
        self._init_cache()
        self._validate_db()

    def _init_cache(self):
        if self.config.cache_backend == "disk":
            self.config.cache_dir.mkdir(exist_ok=True)

    def _validate_db(self):
        required_tables = ["workouts", "workout_points"]
        with duckdb.connect(database=str(self.config.db_path)) as con:
            tables = con.execute("SHOW TABLES;").fetchall()
        existing_tables = [t[0] for t in tables]
        missing = set(required_tables) - set(existing_tables)
        if missing:
            logger.error(f"Missing required tables: {missing}")
            raise ValueError("Invalid HealthKit database structure")

    @cache
    def get_workouts(self, start_date: str, end_date: str) -> pd.DataFrame:
        query = self.sql_mgr.get_query(
            "workouts",
            {
                "start_date": start_date,
                "end_date": end_date,
                "min_duration": self.config.min_duration,
            },
        )
        with duckdb.connect(database=str(self.config.db_path)) as con:
            return con.execute(query).df()

    @cache
    def get_workout_points(self, workout_id: str) -> pd.DataFrame:
        query = self.sql_mgr.get_query("workout_points", {"workout_id": workout_id})
        with duckdb.connect(database=str(self.config.db_path)) as con:
            return con.execute(query).df()


# ### Visualisation Adapters with Output Method Support
class MapRenderer:
    def __init__(self, analyser: HealthKitAnalyser):
        self.analyser = analyser
        self.config = analyser.config.map_defaults

    def _base_map(self) -> folium.Map:
        return folium.Map(
            location=self.config["origin"],
            zoom_start=self.config["zoom"],
            tiles=self.config["tiles"],
        )

    def render(self, workout_ids: List[str], output_method="console"):
        """
        Render the map using the specified output method.

        Args:
            workout_ids (List[str]): List of workout IDs to render.
            output_method (str): The method to output the map (e.g., console, Jupyter, Streamlit).

        Returns:
            Depends on the output method.
        """
        m = self._base_map()

        for wid in workout_ids:
            df = self.analyser.get_workout_points(wid)
            folium.PolyLine(
                df[["latitude", "longitude"]].values.tolist(),
                color=self.config.get("line_color", "blue"),
                weight=self.config.get("line_width", 3),
            ).add_to(m)

        # Output based on method
        if output_method == "console":
            logger.info("Map generated. Use a browser to view it.")
            map_path = Path("output_map.html")
            m.save(str(map_path))
            logger.info(f"Map saved to {map_path}")

        elif output_method == "jupyter":
            from IPython.display import display

            display(m)

        elif output_method == "streamlit":
            import streamlit as st

            st.components.v1.html(m._repr_html_(), height=500)

        elif output_method == "marimo":
            # Example Marimo integration (hypothetical)
            print("Marimo integration would go here.")

        else:
            raise ValueError(f"Unsupported output method: {output_method}")


# ### Example Usage Patterns
if __name__ == "__main__":
    analyser = HealthKitAnalyser()

    # Example workout IDs (replace with actual IDs from your database)
    workouts_df = analyser.get_workouts("2025-03-11", "2025-03-15")
    workout_ids = workouts_df.id.tolist()[:3]

    renderer = MapRenderer(analyser)

    # Console Example (saves the map to an HTML file)
    renderer.render(workout_ids, output_method="console")

    # Jupyter Example (displays the map in a notebook)
    """
    renderer.render(workout_ids, output_method="jupyter")
    """

    # Streamlit Example (renders the map in a Streamlit app)
    """
    import streamlit as st
    
    st.title("HealthKit Workout Map")
    
    selected_ids = st.multiselect(
        "Select workouts",
        workout_ids,
    )
    
    if selected_ids:
        renderer.render(selected_ids, output_method="streamlit")
    """
