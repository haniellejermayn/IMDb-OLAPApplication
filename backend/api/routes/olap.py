# OLAP operation endpoints

# This should handle all OLAP cube operations like slice, dice, roll-up, drill-down, pivot
# Can also include endpoints that list dimensions (e.g., genres, years)

from flask import Blueprint, request, jsonify
from database import execute_query

olap_bp = Blueprint('olap', __name__)

@olap_bp.route("/runtime_trend", methods=["POST"])
def get_runtime_trend():
    input_data = request.get_json()

    print("\n=== 📡 /runtime_trend CALLED ===")
    print("Received payload:", input_data)

    filters = input_data.get("filters", {})
    groups = input_data.get("groups", {})

    # Aggregation
    group_by_time = groups.get("time")
    group_by_genre = groups.get("genre")

    ## Filters
    start_year = filters.get("min_year_val")
    end_year = filters.get("max_year_val")
    where_genre = filters.get("genre", [])

    where_title = filters.get("title_type")
    min_rating = filters.get("min_rating")
    start_rating = filters.get("min_rating_val")
    end_rating = filters.get("max_rating_val")
    min_votes = filters.get("min_votes")


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

    if group_by_genre or (where_genre and isinstance(where_genre, list) and len(where_genre) > 0):
        from_clause.append("JOIN bridge_title_genre btg ON dtl.tconst = btg.tconst")
        from_clause.append("JOIN dim_genre dg ON btg.genreKey = dg.genreKey")

    if group_by_genre:
        select_fields.append("dg.genreName")
        group_fields.append("dg.genreName")

    if isinstance(where_genre, list) and len(where_genre) > 0:
        placeholders = ', '.join(['%s'] * len(where_genre))
        where_clause.append(f"dg.genreName IN ({placeholders})")
        params.extend(where_genre)

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
    
    print("\n--- Generated SQL ---")
    print(sql)
    print("Parameters: ", params)
    print("--- ✅ End of SQL Log ---\n")

    data = execute_query(sql, tuple(params))

    # Return SQL query, parameters, and data
    return jsonify({
        "query": sql,
        "params": params,
        "results": data
    }), 200