# %%
# ---
# title: "Three Capes Dashboard"
# author: "Michael Booth"
# format: dashboard
# nav-buttons:
#     - icon: github
#       href: https://github.com/databooth
# server: shiny
# ---

# %%

from functools import cache
from pathlib import Path

import duckdb
import folium
import pandas as pd

#import shiny

from IPython.display import display

# from shiny import render, ui

# %%
HEALTHKIT_DB = Path.cwd().parent / "data" / "healthkit-sqlite-2025-04-07.db"

# %%
WALK_SQL = """
    SELECT * FROM workout_points 
    WHERE workout_id = 'WORKOUT_ID'
    """

# %%
START_DATE = "2025-03-11"
END_DATE = "2025-03-15"
MIN_DURATION_MIN = 20
MAP_ORIGIN = [-42.8821, 147.3272]  # Hobart, Tasmania
MAP_ZOOM = 13

# %%
# -- , workout_statistics, device 

THREE_CAPES_SQL = f"""
    SELECT id, duration, sourceName, creationDate, startDate, endDate  
    FROM workouts 
    WHERE sourceName != 'AllTrails' 
    AND 
        (startDate >= '{START_DATE}' AND startDate <= '{END_DATE}' AND CAST(duration AS INTEGER) > {MIN_DURATION_MIN}) 
"""


# %%
HEALTHKIT_DB.name

# %%
if Path(HEALTHKIT_DB).exists():
    con = duckdb.connect(HEALTHKIT_DB.name.replace(".db", ".duckdb"))
    con.install_extension("sqlite")
    con.load_extension("sqlite")
    con.execute(f"ATTACH '{HEALTHKIT_DB}' AS healthkit_sqlite (TYPE sqlite);")


# %%
con.sql("SHOW DATABASES;").fetchall()

# %%
con.sql("USE healthkit_sqlite;")

# %%
con.sql("SHOW TABLES;").fetchall()

# %%
con.sql("SELECT * FROM workouts LIMIT 5;")

# %%
# stats_df = con.sql(THREE_CAPES_STATS_SQL).to_df()

# %%
workouts_df = con.sql(THREE_CAPES_SQL).to_df()

# %%
@cache
def get_walk_data(id):
    """
    Fetch and process walk data for a given ID.
    This function is cached to avoid repeated database queries for the same ID.
    """
    walk_df = con.sql(WALK_SQL.replace("WORKOUT_ID", id)).to_df()
    # Any additional processing can be done here
    return walk_df

# %%
walk_ids = workouts_df["id"].values.tolist()

# %%
def load_walk_data(walk_ids):
    for _, id in enumerate(walk_ids):
        get_walk_data(id)
    return None

# %%
load_walk_data(walk_ids)

# %%
def update_map(m, df, colour="blue", line_width=3.5):
    points = df[["latitude", "longitude"]].values.tolist()
    folium.PolyLine(points, color=colour, weight=line_width, opacity=1).add_to(m)
    folium.Marker([df["latitude"].iloc[0], df["longitude"].iloc[0]]).add_to(m)
    return m

# %% [markdown]
# # Three Capes walk Tasmania - Mapping

# %%
def create_walk_map(walk_ids, colour, line_width):
    m = folium.Map(location=MAP_ORIGIN, zoom_start=MAP_ZOOM, tiles="openstreetmap")
    for id in walk_ids:
        walk_df = get_walk_data(id)  # Use the cached function
        m = update_map(m, walk_df, colour, line_width)
    m.fit_bounds(m.get_bounds())
    return m

# %%
## {.sidebar}

# ui.input_select("line_color", "Choose Line Color", choices=["red", "blue", "green", "yellow"])
# ui.input_slider("line_width", "Select Line Width", min=1, max=10, value=3)

# %% [markdown]
# ## Three Capes map

# %%
#@render.plot
def display_map_ui():
    # try:
    #     colour = input.line_color()
    #     width = input.line_width()
    # except Exception as e:
    #     print(f"Error: {e}")
    #     colour = "blue"
    #     width = 3 
    map = create_walk_map(walk_ids, colour, width)
    display(map)

# %%
display_map_ui()

# %%
# display_map_ui() if removing decorator to check in notebook

# %%



