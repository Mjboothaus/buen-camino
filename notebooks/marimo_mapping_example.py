import marimo

__generated_with = "0.3.4"
app = marimo.App(width="medium")


@app.cell
def __():
    import marimo as mo
    return mo,


@app.cell
def __(mo):
    mo.md("Here is some markdown text")
    return


@app.cell
def _():
    import duckdb
    import pandas as pd
    import folium
    import pprint
    import json
    from functools import cache
    return cache, duckdb, folium, json, pd, pprint


@app.cell
def connect_to_health_db(duckdb):
    def get_db_connection():
        HEALTH_DB = "data/healthkit-sqlite-2023-11-17-fix.db"
        con = duckdb.connect(HEALTH_DB)
        con.install_extension("sqlite")
        con.load_extension("sqlite")
        return con
    return get_db_connection,


@app.cell
def __():
    def display_db_tables(con):
        if con:
            print(con.sql("SHOW ALL TABLES;"))
    return display_db_tables,


@app.cell
def __():
    def generate_sql_query(table, conditions, select="*"):
        condition_str = " AND ".join([f"{k} {v}" for k, v in conditions.items()])
        return f"SELECT {select} FROM {table} WHERE {condition_str}"
    return generate_sql_query,


@app.cell
def __():
    def clean_dataframe(df, columns_to_keep=None):
        if columns_to_keep is not None:
            cols_to_drop = [col for col in df.columns if col not in columns_to_keep]
            df.drop(columns=cols_to_drop, inplace=True)
        # Add more cleaning
        return df
    return clean_dataframe,


@app.cell
def __():
    conditions = {
        "sourceName != ": "'AllTrails'",
        "startDate >= ": "'2023-10-10'",
        "startDate <= ": "'2023-10-17'",
        "duration > ": 100,
    }
    return conditions,


@app.cell
def __(clean_dataframe, generate_sql_query):
    def fetch_workouts(con, conditions):
        sql = generate_sql_query("workouts", conditions)
        # print(f"{sql}\n")
        df = con.sql(sql).to_df()
        return clean_dataframe(df, columns_to_keep=["id", "startDate", "duration", "distance"])
    return fetch_workouts,


@app.cell
def __(cache):
    @cache
    def fetch_workout_points(con, workout_id):
        sql = f"SELECT * FROM workout_points WHERE workout_id = '{workout_id}'"
        # print(sql)
        df = con.sql(sql).to_df()
        return df
    return fetch_workout_points,


@app.cell
def __():
    def update_map(folium, m, df):
        points = df[["latitude", "longitude"]].values.tolist()
        folium.PolyLine(points, color="blue", weight=3.5, opacity=1).add_to(m)
        folium.Marker([df["latitude"].iloc[0], df["longitude"].iloc[0]], popup="Start").add_to(m)
        return m
    return update_map,


@app.cell
def __(fetch_workout_points, update_map):
    def plot_walks(con, folium, m, workouts_df):
        for workout_id in workouts_df["id"]:
            df = fetch_workout_points(con, workout_id)
            m = update_map(folium, m, df)
        return m
    return plot_walks,


@app.cell
def __(fetch_workouts, folium, get_db_connection, plot_walks):
    def create_map(conditions, SCALAR=1.0025):
        con = get_db_connection()
        workouts_df = fetch_workouts(con, conditions)
        m = folium.Map(zoom_start=13, tiles="openstreetmap")
        m_disp = plot_walks(con, folium, m, workouts_df)
        bounds = [[x * SCALAR, y * SCALAR] for [x, y] in m_disp.get_bounds()]
        m_disp.fit_bounds(bounds)
        return m_disp
    return create_map,


@app.cell
def __(conditions, create_map):
    m_disp = create_map(conditions)
    return m_disp,


@app.cell
def __(m_disp):
    m_disp
    return


@app.cell
def __():
    return


if __name__ == "__main__":
    app.run()
