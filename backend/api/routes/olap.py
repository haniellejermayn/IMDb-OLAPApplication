# OLAP operation endpoints

# This should handle all OLAP cube operations like slice, dice, roll-up, drill-down, pivot
# Can also include endpoints that list dimensions (e.g., genres, years)

from flask import Blueprint, request, jsonify
from database import execute_query

olap_bp = Blueprint('olap', __name__)

@olap_bp.route("/runtime_trend", methods=["POST"])
def get_runtime_trend():
    input_data = request.get_json()

    group_by_time = input_data.get("group_by_time")
    group_by_genre = input_data.get("group_by_genre")
    start_year = input_data.get("start_year")
    end_year = input_data.get("end_year")
    where_genre = input_data.get("where_genre")

    where_title = input_data.get("where_title")
    min_rating = input_data.get("min_rating")
    start_rating = input_data.get("start_rating")
    end_rating = input_data.get("end_rating")
    min_votes = input_data.get("min_votes")


    select_fields = [f"dtm.{group_by_time}", "AVG(dtl.runtimeMinutes) AS avg_runtime", "dtl.titleType"]

    from_clause =   ["FROM fact_title_performance fp",
                    "JOIN dim_time dtm ON fp.timeKey = dtm.timeKey",
                    "JOIN dim_title dtl ON fp.tconst = dtl.tconst"
                    ]

    where_clause =  ["dtl.runtimeMinutes IS NOT NULL"]

    group_fields =  [f"dtm.{group_by_time}", 
                    "dtl.titleType"
                    ]

    params = []
    if group_by_genre:
        select_fields.append("dg.genreName")
        from_clause.append("JOIN bridge_title_genre btg ON dtl.tconst = btg.tconst")
        from_clause.append("JOIN dim_genre dg ON btg.genreKey = dg.genreKey")
        group_fields.append("dg.genreName")

    if start_year and end_year:
        where_clause.append("dtm.year BETWEEN %s AND %s")
        params.append(start_year)
        params.append(end_year)

    if where_genre:
        where_clause.append("dg.genre = %s")
        params.append(where_genre)

    if where_title:
        where_clause.append("dtl.titleType = %s")
        params.append(where_title)

    if min_rating:
        where_clause.append("fp.averageRating >= %s")
        params.append(float(min_rating))


    if min_votes:
        where_clause.append("fp.numVotes >= %s")
        params.append(int(min_votes))

    if start_rating and end_rating:
        where_clause.append("fp.averageRating BETWEEN %s AND %s")
        params.append(start_rating)
        params.append(end_rating)



    sql = "SELECT " + ', '.join(select_fields) + " " \
        + ' '.join(from_clause) + " " \
        + "WHERE " + ' AND '.join(where_clause) + " " \
        + "GROUP BY " + ', '.join(group_fields) + " " \
        + "ORDER BY " + ', '.join(group_fields) + " DESC "


    data = execute_query(sql, tuple(params))
    return jsonify(data)




