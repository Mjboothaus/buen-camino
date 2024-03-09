import gradio as gr
import duckdb
import pandas as pd
import folium
from functools import cache

# Assuming all necessary functions from the Marimo notebook are refactored here
# For brevity, only the main logic is adapted for Gradio


def get_db_connection():
    HEALTH_DB = "data/healthkit-sqlite-2023-11-17-fix.db"
    con = duckdb.connect(HEALTH_DB)
    con.install_extension("sqlite")
    con.load_extension("sqlite")
    return con


def generate_sql_query(table, conditions, select="*"):
    condition_str = " AND ".join([f"{k} {v}" for k, v in conditions.items()])
    return f"SELECT {select} FROM {table} WHERE {condition_str}"


def clean_dataframe(df, columns_to_keep=None):
    if columns_to_keep is not None:
        cols_to_drop = [col for col in df.columns if col not in columns_to_keep]
        df.drop(columns=cols_to_drop, inplace=True)
    return df


def fetch_workouts(con, conditions):
    sql = generate_sql_query("workouts", conditions)
    # print(f"\n{sql}\n")
    df = con.sql(sql).to_df()
    return clean_dataframe(df, columns_to_keep=["id", "startDate", "duration", "distance"])


@cache
def fetch_workout_points(con, workout_id):
    sql = f"SELECT * FROM workout_points WHERE workout_id = '{workout_id}'"
    # print(sql)
    df = con.sql(sql).to_df()
    return df


def update_map(folium, m, df):
    points = df[["latitude", "longitude"]].values.tolist()
    folium.PolyLine(points, color="blue", weight=3.5, opacity=1).add_to(m)
    folium.Marker([df["latitude"].iloc[0], df["longitude"].iloc[0]], popup="Start").add_to(m)
    return m


def plot_walks(con, folium, m, workouts_df):
    for workout_id in workouts_df["id"]:
        df = fetch_workout_points(con, workout_id)
        m = update_map(folium, m, df)
    return m


def create_map(start_date, end_date, exclude_source_name, duration_greater_than, SCALAR=1.0025):
    # This function should encapsulate the logic from the Marimo notebook
    # For simplicity, let's assume it returns a Folium map object
    # Here, you would use duckdb to connect to the database, fetch data, and generate the map
    con = get_db_connection()

    conditions = {
        "sourceName != ": f"'{exclude_source_name}'",
        "startDate >= ": f"'{start_date}'",
        "startDate <= ": f"'{end_date}'",
        "duration > ": int(duration_greater_than),
    }

    workouts_df = fetch_workouts(con, conditions)
    m = folium.Map(zoom_start=13, tiles="openstreetmap")
    m_disp = plot_walks(con, folium, m, workouts_df)
    bounds = [[x * SCALAR, y * SCALAR] for [x, y] in m_disp.get_bounds()]
    m_disp.fit_bounds(bounds)

    # Normally, you'd use the input parameters to filter data and plot on `m`
    return m_disp._repr_html_()  # Return HTML representation of the map


iface = gr.Interface(
    fn=create_map,
    inputs=[
        gr.Textbox(label="Start Date (YYYY-MM-DD)"),
        gr.Textbox(label="End Date (YYYY-MM-DD)"),
        gr.Textbox(label="Exclude Source Name"),
        gr.Slider(minimum=0, maximum=1000, step=1, label="Duration Greater Than (minutes)"),
    ],
    outputs="html",
    title="HealthKit Data Mapper",
    description="Map your HealthKit workout data based on filters.",
)


if __name__ == "__main__":
    iface.launch()
