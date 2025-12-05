import marimo

__generated_with = "0.3.1"
app = marimo.App()


@app.cell
def _():
    import duckdb
    import pandas as pd
    import folium
    return duckdb, folium, pd


@app.cell
def _():
    HEALTH_DB = "data/healthkit-sqlite-2023-11-17-fix.db"
    return HEALTH_DB,


@app.cell
def _(HEALTH_DB, duckdb):
    con = duckdb.connect(HEALTH_DB)
    return con,


@app.cell
def _(con):
    con.install_extension("sqlite")
    return


@app.cell
def _(con):
    con.load_extension("sqlite")
    return


@app.cell
def __(con):
    con
    return


@app.cell
def _():
    # con.sql("SHOW TABLES")
    return


@app.cell
def _():
    camino_sql = "SELECT * from workouts WHERE sourceName != 'AllTrails' AND startDate >= '2023-10-10' AND startDate <= '2023-10-17' AND duration > 100"
    return camino_sql,


@app.cell
def _():
    camino_bilbao_sql = "SELECT * from workouts WHERE sourceName != 'AllTrails' AND startDate >= '2023-10-16' AND startDate <= '2023-10-17' AND duration < 100"
    return camino_bilbao_sql,


@app.cell
def _():
    stats_sql = "select * FROM workouts WHERE sourceName != 'AllTrails' AND startDate >= '2023-10-16' AND startDate <= '2023-10-17' AND duration < 100"
    return stats_sql,


@app.cell
def _(con, stats_sql):
    tmp_df = con.sql(stats_sql).to_df()
    return tmp_df,


@app.cell
def _():
    import pprint
    return pprint,


@app.cell
def _(pprint, tmp_df):
    pprint.pprint(tmp_df["workout_statistics"].iloc[0])
    return


@app.cell
def _():
    import json
    return json,


@app.cell
def _(json, tmp_df):
    json.loads(tmp_df["workout_statistics"].iloc[0])[0]
    return


@app.cell
def _(camino_sql, con):
    workouts_df = con.sql(camino_sql).to_df()
    return workouts_df,


@app.cell
def _():
    return


@app.cell
def _(camino_bilbao_sql, con):
    bilbao_df = con.sql(camino_bilbao_sql).to_df()
    return bilbao_df,


@app.cell
def _(bilbao_df):
    bilbao_df
    return


@app.cell
def _(workouts_df):
    workouts_df.info()
    return


@app.cell
def _(workouts_df):
    workouts_df.columns
    return


@app.cell
def _():
    drop_cols = [
        "workoutActivityType",
        "durationUnit",
        "sourceVersion",
        "metadata_HKGroupFitness",
        "metadata_HKWorkoutBrandName",
        "metadata_HKTimeZone",
        "metadata_HKCoachedWorkout",
        "metadata_HKWasUserEntered",
        "workout_events",
        "metadata_HKWeatherTemperature",
        "metadata_HKWeatherHumidity",
        "metadata_HKIndoorWorkout",
        "metadata_HKElevationAscended",
        "metadata_HKSwimmingLocationType",
        "metadata_HKAverageMETs",
        "metadata_HEALTHFIT_SUB_SPORT",
        "metadata_HEALTHFIT_ROUTE",
        "metadata_HEALTHFIT_FILE_TYPE",
        "metadata_HKMaximumSpeed",
        "metadata_HEALTHFIT_TOTAL_MOVING_TIME",
        "metadata_HEALTHFIT_TOTAL_DISTANCE",
        "metadata_HEALTHFIT_MAX_RUNNING_CADENCE",
        "metadata_HEALTHFIT_MIN_ALTITUDE",
        "metadata_HEALTHFIT_AVG_RUNNING_CADENCE",
        "metadata_HEALTHFIT_APP_BUILD",
        "metadata_HEALTHFIT_FIT_SPORT",
        "metadata_HEALTHFIT_FIT_SUB_SPORT",
        "metadata_HEALTHFIT_MAX_ALTITUDE",
        "metadata_HEALTHFIT_FIT_MANUFACTURER",
        "metadata_HEALTHFIT_TOTAL_STRIDES",
        "metadata_HEALTHFIT_FIT_SERIAL_NUMBER",
        "metadata_HEALTHFIT_SPORT",
        "metadata_HKAverageSpeed",
        "metadata_HEALTHFIT_APP_VERSION",
        "metadata_HKExternalUUID",
        "metadata_HKElevationDescended",
        "metadata_HKLapLength",
    ]
    return drop_cols,


@app.cell
def _(drop_cols, e, workouts_df):
    try:
        workouts_df.drop(labels=drop_cols, axis=1, inplace=True)
    except Exception as e:
        print(e)
    return


@app.cell
def _(workouts_df):
    workouts_df
    return


@app.cell
def _(camino_sql, con):
    walk_id = con.sql(camino_sql.replace("*", "id")).to_df()
    return walk_id,


@app.cell
def _(walk_id):
    walk_ids = walk_id["id"].values.tolist()
    return walk_ids,


@app.cell
def _(walk_ids):
    walk_ids.pop(0)  # Evening walk in San Sebastin
    return


@app.cell
def _(bilbao_df):
    bilbao_df["id"].iloc[0]
    return


@app.cell
def _(bilbao_df, walk_ids):
    walk_ids.append(bilbao_df["id"].iloc[0])
    return


@app.cell
def _():
    walk_sql = "SELECT * FROM workout_points WHERE workout_id = 'WORKOUT_ID'"
    return walk_sql,


@app.cell
def _(folium):
    m = folium.Map(location=[43.3183, -1.9812], zoom_start=12, tiles="openstreetmap")
    return m,


@app.cell
def _(folium):
    def update_map(m, df):
        points = df[["latitude", "longitude"]].values.tolist()
        folium.PolyLine(points, color="blue", weight=3.5, opacity=1).add_to(m)
        folium.Marker([df["latitude"].iloc[0], df["longitude"].iloc[0]]).add_to(m)
        return m
    return update_map,


@app.cell
def _(con, update_map, walk_sql):
    def plot_walk(m, walk_ids):
        for id in walk_ids:
            # TODO: Adjust SQL to only get Camino walks (plus extra in Bilbao) - DONE
            print(f"Getting data for walk ID: {id}")
            walk_df = con.sql(walk_sql.replace("WORKOUT_ID", id)).to_df()
            # TODO: Cache / preprocess the walk data
            m = update_map(m, walk_df)
        m.fit_bounds(m.get_bounds())
    return plot_walk,


@app.cell
def _(m, plot_walk, walk_ids):
    plot_walk(m, walk_ids)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
