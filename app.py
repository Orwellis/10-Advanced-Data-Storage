import numpy as np
import pandas as pd
import datetime as dt

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func, inspect

from flask import Flask, jsonify

engine = create_engine("sqlite:///Resources/hawaii.sqlite")

app = Flask(__name__)


@app.route("/")
def home():
    text = "Endpoints:" + \
            "\n /api/v1.0/precipitation" + \
            "\n /api/v1.0/stations" + \
            "\n /api/v1.0/tobs" + \
            "\n /api/v1.0/'start' ['YYYY-MM-DD']" + \
            "\n /api/v1.0/temp/'start'/'end'"
    return text

# * `/api/v1.0/precipitation`
#   * Convert the query results to a Dictionary using `date` as the key and `prcp` as the value.
#   * Return the JSON representation of your dictionary.
@app.route("/api/v1.0/precipitation")
def precipitation():

    conn = engine.connect()
    preciptiation = pd.read_sql(f"SELECT date, prcp FROM measurement;", conn)
    conn.close()

    precip_sorted = preciptiation.sort_values("date")
    precip_reindex = precip_sorted.set_index(["date"])
    
    data = precip_reindex.to_dict()
    return jsonify(data)

# * `/api/v1.0/stations`
#   * Return a JSON list of stations from the dataset.
@app.route("/api/v1.0/stations")
def stations():

    conn = engine.connect()
    station_df = pd.read_sql(f"SELECT station FROM station;", conn)
    conn.close()

    data = station_df.to_dict()
    return jsonify(data)

# * `/api/v1.0/tobs`
#   * query for the dates and temperature observations from a year from the last data point.
#   * Return a JSON list of Temperature Observations (tobs) for the previous year.
@app.route("/api/v1.0/tobs")
def tobs():

    conn = engine.connect()
    climate_df = pd.read_sql(f"SELECT date, prcp FROM measurement;", conn)
    conn.close()

    last_date = max(climate_df["date"])
    split_date = last_date.split("-")
    year = int(split_date[0])
    month = int(split_date[1])
    day = int(split_date[2])
    end_date = dt.date(year, month, day)
    twelve_months_prior = end_date - dt.timedelta(days=365)

    conn = engine.connect()
    climate_df = pd.read_sql(f"SELECT date, prcp FROM measurement WHERE date >= '{twelve_months_prior}'", conn)
    conn.close()

    climate_sorted = climate_df.sort_values("date")
    climate_sorted = climate_sorted.set_index(["date"])

    data = climate_sorted.to_dict()
    return jsonify(data)


# * `/api/v1.0/<start>` and `/api/v1.0/<start>/<end>`
#   * Return a JSON list of the minimum temperature, the average temperature, and the max temperature for a given start or start-end range.
#   * When given the start only, calculate `TMIN`, `TAVG`, and `TMAX` for all dates greater than and equal to the start date.
#   * When given the start and the end date, calculate the `TMIN`, `TAVG`, and `TMAX` for dates between the start and end date inclusive.
# @app.route("/api/v1.0/temp/<start>")
@app.route("/api/v1.0/temp/<start>")
def start(start):

    conn = engine.connect()
    query = f"SELECT tobs FROM measurement WHERE date >= '{start}';"
    temp_df = pd.read_sql(query, conn)
    conn.close()

    max_temp = temp_df["tobs"].max()
    min_temp = temp_df["tobs"].min()
    mean_temp = temp_df["tobs"].mean()

    data = {"TMIN": min_temp, "TMAX": max_temp, "TAVG": mean_temp}
    return jsonify(data)


@app.route("/api/v1.0/temp/<start>/<end>")
def start_end(start, end):

    conn = engine.connect()
    query = f"SELECT tobs FROM measurement WHERE date >= '{start}' AND date <= '{end}';"
    temp_df = pd.read_sql(query, conn)
    conn.close()

    max_temp = temp_df["tobs"].max()
    min_temp = temp_df["tobs"].min()
    mean_temp = temp_df["tobs"].mean()

    data = {"TMIN": min_temp, "TMAX": max_temp, "TAVG": mean_temp}
    return jsonify(data)


if __name__ == '__main__':
    app.run()