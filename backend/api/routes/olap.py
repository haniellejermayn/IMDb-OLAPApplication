# OLAP operation endpoints

# This should handle all OLAP cube operations like slice, dice, roll-up, drill-down, pivot
# Can also include endpoints that list dimensions (e.g., genres, years)

from flask import Blueprint, request, jsonify
from database import execute_query

olap_bp = Blueprint('olap', __name__)

@olap_bp.route("/runtime_trend", methods=["GET"])
def get_runtime_trend():
    group_field = request.args.get("group_field")

    sql = (
        f"SELECT dtm.{group_field}, AVG(dtl.runtimeMinutes) AS avg_runtime " + 
        "FROM fact_title_performance fp " + 
        "JOIN dim_time dtm ON fp.timeKey = dtm.timeKey " + 
        "JOIN dim_title dtl ON fp.tconst = dtl.tconst " +
        "WHERE dtm.year >= %s AND dtm.year <= %s " +
        f"GROUP BY dtm.{group_field} " + 
        f"ORDER BY dtm.{group_field} "
    )

    params = (start_year, end_year)
    data = execute_query(sql, params)

    return jsonify(data)

@olap_bp.route("/avgrating_genre", methods=["GET"])
def get_avgrating_genre():

    start_year = request.args.get("start_year", type=int)
    end_year = request.args.get("end_year", type=int)

    sql = (
        "SELECT dg.genreName, AVG(averageRating) AS avg_rating " + 
        "FROM fact_title_performance fp " +  
        "JOIN dim_title dtl ON fp.tconst = dtl.tconst " +
        "JOIN dim_time dtm ON fp.timeKey = dtm.timeKey " +
        "JOIN bridge_title_genre btg ON btg.tconst = dtl.tconst " +
        "JOIN dim_genre dg ON dg.genreKey = btg.genreKey "
        "WHERE dtm.year >= %s AND dtm.year <= %s " +
        "GROUP BY dg.genreName " + 
        "ORDER BY dg.genreName "
    )

    params = (start_year, end_year)
    data = execute_query(sql, params)

    return jsonify(data)


@olap_bp.route("/highest_rated_director", methods=["GET"])
def get_highest_rated_director():
    start_year = request.args.get("start_year", type=int)
    end_year = request.args.get("end_year", type=int)
    genre_field = request.args.get("genre_field")

    sql = (
        "SELECT dp.primaryName, AVG(fp.averageRating) AS avg_rating "
        "FROM fact_title_performance fp "
        "JOIN dim_title dtl ON fp.tconst = dtl.tconst "
        "JOIN dim_time dtm ON fp.timeKey = dtm.timeKey "
        "JOIN bridge_title_person btp ON btp.tconst = dtl.tconst "
        "JOIN dim_person dp ON dp.nconst = btp.nconst "
        "JOIN bridge_title_genre btg ON btg.tconst = dtl.tconst "
        "JOIN dim_genre dg ON dg.genreKey = btg.genreKey "
        "WHERE dtm.year >= %s AND dtm.year <= %s "
        "AND btp.category = 'director' "
        "AND dp.primaryName NOT LIKE '[Unknown%' "
    )

    # Add genre condition only if provided
    if genre_field:
        sql += f"AND dg.genreName = '{genre_field}' "

    sql += "GROUP BY dp.primaryName ORDER BY avg_rating DESC LIMIT 5"

    params = (start_year, end_year)
    data = execute_query(sql, params)

    return jsonify(data)

@olap_bp.route("/polarizing_show", methods=["GET"])
def get_polarizing_show():


    sql = (
        "SELECT parent_show.primaryTitle, AVG(fp.averageRating) AS avg_rating, STDDEV_SAMP(fp.averageRating) AS stdev_show "
        "FROM fact_title_performance fp " +
        "JOIN dim_title dtl ON fp.tconst = dtl.tconst " +
        "JOIN dim_episode de ON de.episodeTconst = dtl.tconst " +
        "JOIN dim_title parent_show ON parent_show.tconst = de.parentTconst " +
        "GROUP BY parent_show.tconst " +
        "ORDER BY stdev_show DESC "
    )

    data = execute_query(sql)

    return jsonify(data)


