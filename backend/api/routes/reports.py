from scipy.stats import chi2
from flask import Blueprint, request, jsonify
from database import execute_query
import logging

reports_bp = Blueprint('reports', __name__)
logger = logging.getLogger(__name__)

# ============================================================
# UTILITY FUNCTIONS
# ============================================================

def validate_time_granularity(time_granularity, required=False):
    """Validate time_granularity parameter"""
    if required and not time_granularity:
        return {"error": "time_granularity is required"}
    
    if time_granularity and time_granularity not in ['year', 'decade', 'era']:
        return {"error": "time_granularity must be 'year', 'decade', or 'era'"}
    
    return None


def build_where_clause(where_conditions, params_list, table_aliases):
    """
    Build dynamic WHERE clause from array of conditions
    where_conditions: [{"field": "dtl.titleType", "operator": "=", "value": "movie"}]
    """
    if not where_conditions:
        return ""
    
    where_parts = []
    valid_operators = ['=', '!=', '>', '<', '>=', '<=', 'LIKE', 'IN', 'NOT IN', 'IS NULL', 'IS NOT NULL', 'BETWEEN']
    
    for condition in where_conditions:
        field = condition.get('field')
        operator = condition.get('operator', '=').upper()
        value = condition.get('value')
        
        if not field or operator not in valid_operators:
            continue
        
        if operator in ['IS NULL', 'IS NOT NULL']:
            where_parts.append(f"{field} {operator}")
        elif operator == 'BETWEEN':
            if isinstance(value, list) and len(value) == 2:
                where_parts.append(f"{field} BETWEEN %s AND %s")
                params_list.extend(value)
        elif operator in ['IN', 'NOT IN']:
            if isinstance(value, list):
                placeholders = ','.join(['%s'] * len(value))
                where_parts.append(f"{field} {operator} ({placeholders})")
                params_list.extend(value)
        else:
            where_parts.append(f"{field} {operator} %s")
            params_list.append(value)
    
    return " AND " + " AND ".join(where_parts) if where_parts else ""


def build_group_by_clause(group_by_fields, default_groups):
    """Build dynamic GROUP BY clause"""
    if group_by_fields:
        return " GROUP BY " + ", ".join(group_by_fields)
    elif default_groups:
        return " GROUP BY " + ", ".join(default_groups)
    return ""


def apply_common_filters(query, params, params_list, table_aliases):
    """
    Apply common filters used across multiple reports
    Returns updated query string
    """
    # Genre filter
    if params.get('genres'):
        genres = params.get('genres') if isinstance(params.get('genres'), list) else [params.get('genres')]
        placeholders = ','.join(['%s'] * len(genres))
        query += f" AND dg.genreName IN ({placeholders})"
        params_list.extend(genres)
    
    # Title type filter
    if params.get('title_types'):
        types = params.get('title_types') if isinstance(params.get('title_types'), list) else [params.get('title_types')]
        placeholders = ','.join(['%s'] * len(types))
        
        # Determine which table alias to use
        if 'dtl_parent' in table_aliases.values():
            query += f" AND dtl_parent.titleType IN ({placeholders})"
        else:
            query += f" AND dtl.titleType IN ({placeholders})"
        params_list.extend(types)
    
    # Year range filter
    if params.get('start_year') and params.get('end_year'):
        query += " AND dtm.year BETWEEN %s AND %s"
        params_list.extend([params.get('start_year'), params.get('end_year')])
    
    # Rating filter
    if params.get('min_rating'):
        query += " AND fp.averageRating >= %s"
        params_list.append(float(params.get('min_rating')))
    
    # Votes filter
    if params.get('min_votes'):
        query += " AND fp.numVotes >= %s"
        params_list.append(int(params.get('min_votes')))
    
    # Runtime filters
    if params.get('runtime_min'):
        query += " AND dtl.runtimeMinutes >= %s"
        params_list.append(int(params.get('runtime_min')))
    
    if params.get('runtime_max'):
        query += " AND dtl.runtimeMinutes <= %s"
        params_list.append(int(params.get('runtime_max')))
    
    # Vote range filters (for report 4)
    if params.get('vote_min'):
        query += " AND fp.numVotes >= %s"
        params_list.append(int(params.get('vote_min')))
    
    if params.get('vote_max'):
        query += " AND fp.numVotes <= %s"
        params_list.append(int(params.get('vote_max')))
    
    return query


def check_grouping_needs(params):
    """
    Determine if genre/time grouping is needed based on params
    Returns: (group_by_genre, group_by_time, time_granularity)
    """
    group_by_genre = params.get('group_by_genre', False)
    group_by_time = params.get('group_by_time', False)
    time_granularity = params.get('time_granularity', 'year')
    
    # Check custom group_by
    custom_group = params.get('group_by')
    if custom_group:
        group_by_genre = any('genreName' in field for field in custom_group)
        
        # Check for time grouping and extract granularity
        for field in custom_group:
            if 'dtm.decade' in field:
                group_by_time = True
                time_granularity = 'decade'
                break
            elif 'dtm.era' in field:
                group_by_time = True
                time_granularity = 'era'
                break
            elif 'dtm.year' in field:
                group_by_time = True
                time_granularity = 'year'
                break
    
    return group_by_genre, group_by_time, time_granularity


def needs_join(params, group_by_flag, filter_keys):
    """
    Determine if a JOIN is needed based on grouping OR filtering
    params: request parameters
    group_by_flag: boolean indicating if grouping by this dimension
    filter_keys: list of param keys that require this JOIN
    """
    if group_by_flag:
        return True
    
    return any(params.get(key) for key in filter_keys)


# ============================================================
# Report 1: Genre-Rating Association
# ============================================================
@reports_bp.route("/r1", methods=["POST"])
def genre_rating_association():
    """
    Chi-square analysis: Genre vs Rating Bins
    
    Required Inputs:
    - time_granularity: "year" | "decade" | "era" (REQUIRED)
    
    Optional WHERE (Filters):
    - genres: ["Action", "Drama"] (multi-select)
    - title_types: ["movie", "tvSeries"] (multi-select) 
    - min_votes: number
    - runtime_min: number
    - runtime_max: number
    - start_year: number
    - end_year: number
    - where: [{"field": "...", "operator": "...", "value": ...}] (dynamic)
    
    Optional GROUP BY:
    - group_by: ["dg.genreName", "rating_bin", "dtm.decade"] (overrides default)
    
    Additional:
    - calculate_chi_square: true/false (computes chi-square statistic)
    """
    try:
        params = request.get_json()
        time_granularity = params.get('time_granularity')
        
        # Validate required time_granularity
        if time_granularity:
            validation_error = validate_time_granularity(time_granularity, required=False)
            if validation_error:
                return jsonify({"status": "error", "message": validation_error["error"]}), 400
        
        calculate_chi = params.get('calculate_chi_square', False)
        
        time_field = f"dtm.{time_granularity}" if time_granularity else "'All Time'"
        
        query = f"""
        SELECT
            dg.genreName AS genre,
            CASE 
                WHEN fp.averageRating < 2 THEN 'Very Low'
                WHEN fp.averageRating < 4 THEN 'Low'
                WHEN fp.averageRating < 6 THEN 'Mid'
                WHEN fp.averageRating < 8 THEN 'High'
                ELSE 'Very High'
            END AS rating_bin,
            {time_field} AS time_period,
            COUNT(*) AS count
        FROM fact_title_performance fp
        JOIN dim_title dtl ON fp.tconst = dtl.tconst
        JOIN bridge_title_genre btg ON dtl.tconst = btg.tconst
        JOIN dim_genre dg ON btg.genreKey = dg.genreKey
        JOIN dim_time dtm ON fp.timeKey = dtm.timeKey
        WHERE 1=1
        """
        
        params_list = []
        table_aliases = {
            'fact_title_performance': 'fp',
            'dim_title': 'dtl',
            'bridge_title_genre': 'btg',
            'dim_genre': 'dg',
            'dim_time': 'dtm'
        }
        
        # Apply common filters
        query = apply_common_filters(query, params, params_list, table_aliases)
        
        # Dynamic WHERE clause
        where_clause = build_where_clause(params.get('where'), params_list, table_aliases)
        query += where_clause
        
        # Dynamic GROUP BY
        default_groups = ["dg.genreName", "rating_bin"]
        if time_granularity:
            default_groups.append(f"dtm.{time_granularity}")
        else:
            default_groups.append("time_period")
        group_by_clause = build_group_by_clause(params.get('group_by'), default_groups)
        query += group_by_clause
        
        # ORDER BY - use aliased columns
        query += " ORDER BY time_period DESC, genre, rating_bin"
        
        data = execute_query(query, tuple(params_list))
        
        # Calculate chi-square if requested
        if calculate_chi:
            chi_results = calculate_chi_square_statistic(data)
            return jsonify({
                "status": "success", 
                "data": data,
                "chi_square_by_period": chi_results 
            })
        
        return jsonify({"status": "success", "data": data})
    except Exception as e:
        logger.error(f"Error in genre_rating_association: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 400


def calculate_chi_square_statistic(data, alpha=0.05):
    """Calculate chi-square statistic for each time period separately"""
    from collections import defaultdict
    
    # Group data by time period first
    time_period_data = defaultdict(list)
    for row in data:
        time_period_data[row['time_period']].append(row)
    
    all_results = {}
    
    # Calculate chi-square for each time period
    for time_period, period_data in time_period_data.items():
        contingency = defaultdict(lambda: defaultdict(int))
        row_totals = defaultdict(int)
        col_totals = defaultdict(int)
        grand_total = 0
        
        for row in period_data:
            genre = row['genre']
            rating_bin = row['rating_bin']
            count = row['count']
            
            contingency[genre][rating_bin] = count
            row_totals[genre] += count
            col_totals[rating_bin] += count
            grand_total += count
        
        if grand_total == 0:
            all_results[time_period] = {"error": "No data for chi-square calculation"}
            continue
        
        # Check if we have enough data for chi-square
        num_rows = len(row_totals)
        num_cols = len(col_totals)
        degrees_of_freedom = (num_rows - 1) * (num_cols - 1)
        
        if degrees_of_freedom <= 0:
            all_results[time_period] = {
                "error": f"Insufficient data: need at least 2 genres and 2 rating bins (found {num_rows} genres, {num_cols} rating bins)"
            }
            continue
        
        chi_square = 0
        cell_contributions = []
        
        for genre in contingency:
            for rating_bin in contingency[genre]:
                observed = contingency[genre][rating_bin]
                expected = (row_totals[genre] * col_totals[rating_bin]) / grand_total
                
                if expected > 0:
                    contribution = ((observed - expected) ** 2) / expected
                    chi_square += contribution
                    
                    cell_contributions.append({
                        "genre": genre,
                        "rating_bin": rating_bin,
                        "observed": observed,
                        "expected": round(expected, 2),
                        "contribution": round(contribution, 4)
                    })
        
        # Compute critical value dynamically using scipy
        critical_value = chi2.ppf(1 - alpha, degrees_of_freedom)
        is_significant = bool(chi_square > critical_value)

        all_results[time_period] = {
            "chi_square_statistic": round(chi_square, 4),
            "degrees_of_freedom": degrees_of_freedom,
            "critical_value_alpha_0.05": round(critical_value, 4),
            "is_significant": is_significant,
            "interpretation": (
                "Significant association between genre and rating"
                if is_significant else
                "No significant association detected"
            ),
            "row_totals": dict(row_totals),
            "column_totals": dict(col_totals),
            "grand_total": grand_total,
            "top_contributions": sorted(cell_contributions, key=lambda x: x['contribution'], reverse=True)[:10]
        }
    
    return all_results


# ============================================================
# Report 2: Runtime Trends 
# ============================================================
@reports_bp.route("/r2", methods=["POST"])
def runtime_trends():
    """
    Runtime evolution analysis by title type over time
    
    Required Inputs:
    - time_granularity: "year" | "decade" | "era" (REQUIRED)
    
    Optional WHERE (Filters):
    - title_types: ["movie", "tvSeries"] (multi-select)
    - genres: ["Action", "Drama"] (multi-select)
    - start_year: number
    - end_year: number
    - min_rating: number
    - runtime_min: number
    - runtime_max: number
    - min_votes: number
    - where: [{"field": "...", "operator": "...", "value": ...}] (dynamic)
    """
    try:
        params = request.get_json()
        time_granularity = params.get('time_granularity')
        
        # Validate required time_granularity
        validation_error = validate_time_granularity(time_granularity, required=True)
        if validation_error:
            return jsonify({"status": "error", "message": validation_error["error"]}), 400
        
        # Always include genre join since we might need it for filtering
        need_genre_join = params.get('genres') is not None
        
        # Build SELECT - always group by time and titleType
        query = f"""
        SELECT
            dtm.{time_granularity} AS time_period,
            dtl.titleType,
            AVG(dtl.runtimeMinutes) AS avg_runtime,
            COUNT(DISTINCT dtl.tconst) AS title_count,
            AVG(fp.averageRating) AS avg_rating
        FROM dim_title dtl
        JOIN fact_title_performance fp ON dtl.tconst = fp.tconst
        JOIN dim_time dtm ON fp.timeKey = dtm.timeKey"""
        
        if need_genre_join:
            query += """
        JOIN bridge_title_genre btg ON dtl.tconst = btg.tconst
        JOIN dim_genre dg ON btg.genreKey = dg.genreKey"""
        
        query += """
        WHERE dtl.runtimeMinutes IS NOT NULL
        """
        
        params_list = []
        table_aliases = {
            'dim_title': 'dtl',
            'fact_title_performance': 'fp',
            'dim_time': 'dtm',
            'bridge_title_genre': 'btg',
            'dim_genre': 'dg'
        }
        
        # Apply common filters
        query = apply_common_filters(query, params, params_list, table_aliases)

        
        # Dynamic WHERE clause
        where_clause = build_where_clause(params.get('where'), params_list, table_aliases)
        query += where_clause
        
        # GROUP BY - always time and titleType
        query += f" GROUP BY dtm.{time_granularity}, dtl.titleType"
        
        # ORDER BY
        query += " ORDER BY time_period DESC, dtl.titleType, avg_runtime DESC"
        
        data = execute_query(query, tuple(params_list))
        return jsonify({"status": "success", "data": data})
    except Exception as e:
        logger.error(f"Error in runtime_trends: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 400

# ============================================================
# Report 3: Person Performance Analysis
# ============================================================
@reports_bp.route("/r3", methods=["POST"])
def person_performance():
    """
    Performance metrics for industry professionals by job category
    
    Required WHERE:
    - job_category: string (REQUIRED - e.g., "director", "actor", "writer")
    
    Optional GROUP BY:
    - group_by_genre: true/false (adds genre to GROUP BY)
    - group_by_time: true/false (adds time to GROUP BY)
    - time_granularity: "year" | "decade" | "era" (if group_by_time enabled)
    - group_by: ["dp.nconst", "dp.primaryName", "dg.genreName"] (custom override)
    
    Optional WHERE (Filters):
    - genres: ["Action", "Drama"] (multi-select)
    - title_types: ["movie", "tvSeries"] (multi-select)
    - start_year: number
    - end_year: number
    - min_rating: number
    - min_votes: number (filters titles with minimum votes)
    - min_titles: number (HAVING filter - minimum number of titles per person)
    - where: [{"field": "...", "operator": "...", "value": ...}] (dynamic)
    """
    try:
        params = request.get_json()
        job_category = params.get('job_category')
        
        if not job_category:
            return jsonify({"status": "error", "message": "job_category is required"}), 400
        
        # Check grouping needs
        group_by_genre, group_by_time, time_granularity = check_grouping_needs(params)
        
        # Validate time_granularity if time grouping is enabled
        if group_by_time:
            validation_error = validate_time_granularity(time_granularity, required=False)
            if validation_error:
                return jsonify({"status": "error", "message": validation_error["error"]}), 400
        
        # Check if JOINs are needed
        need_genre_join = needs_join(params, group_by_genre, ['genres'])
        need_time_join = needs_join(params, group_by_time, ['start_year', 'end_year'])
        
        # Build SELECT clause
        select_clause = """
        SELECT
            dp.nconst,
            dp.primaryName"""
        
        if group_by_genre:
            select_clause += """,
            dg.genreName"""
        
        if group_by_time:
            select_clause += f""",
            dtm.{time_granularity} AS time_period"""
        
        select_clause += """,
            AVG(fp.averageRating) AS avg_rating,
            COUNT(DISTINCT dtl.tconst) AS total_titles
        FROM dim_person dp
        JOIN bridge_title_person btp ON dp.nconst = btp.nconst
        JOIN dim_title dtl ON btp.tconst = dtl.tconst
        JOIN fact_title_performance fp ON dtl.tconst = fp.tconst"""
        
        if need_time_join:
            select_clause += """
        JOIN dim_time dtm ON fp.timeKey = dtm.timeKey"""
        
        if need_genre_join:
            select_clause += """
        JOIN bridge_title_genre btg ON dtl.tconst = btg.tconst
        JOIN dim_genre dg ON btg.genreKey = dg.genreKey"""
        
        query = select_clause + """
        WHERE btp.category = %s
        """
        
        params_list = [job_category]
        table_aliases = {
            'dim_person': 'dp',
            'bridge_title_person': 'btp',
            'dim_title': 'dtl',
            'fact_title_performance': 'fp',
            'dim_time': 'dtm',
            'bridge_title_genre': 'btg',
            'dim_genre': 'dg'
        }
        
        # Apply common filters
        query = apply_common_filters(query, params, params_list, table_aliases)
        
        # Dynamic WHERE clause
        where_clause = build_where_clause(params.get('where'), params_list, table_aliases)
        query += where_clause
        
        # Dynamic GROUP BY
        # If custom group_by is provided, check if it includes time field and replace with alias
        if params.get('group_by'):
            custom_groups = []
            for field in params.get('group_by'):
                # Replace dtm.decade/year/era with time_period alias if present
                if field in [f'dtm.{time_granularity}', 'dtm.decade', 'dtm.year', 'dtm.era']:
                    custom_groups.append('time_period')
                else:
                    custom_groups.append(field)
            group_by_clause = build_group_by_clause(custom_groups, None)
        else:
            default_groups = ["dp.nconst", "dp.primaryName"]
            if group_by_genre:
                default_groups.append("dg.genreName")
            if group_by_time:
                default_groups.append('time_period')
            group_by_clause = build_group_by_clause(None, default_groups)
        
        query += group_by_clause
        
        # HAVING clause
        having_parts = []
        if params.get('min_titles'):
            having_parts.append(f"COUNT(DISTINCT dtl.tconst) >= {int(params.get('min_titles'))}")
        
        if having_parts:
            query += " HAVING " + " AND ".join(having_parts)
        
        if group_by_genre or group_by_time:
            query += " ORDER BY avg_rating DESC LIMIT 200"
        else:
            query += " ORDER BY avg_rating DESC LIMIT 10"
        
        data = execute_query(query, tuple(params_list))
        return jsonify({"status": "success", "data": data})
    except Exception as e:
        logger.error(f"Error in person_performance: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 400


# ============================================================
# Report 4: Genre Engagement 
# ============================================================
@reports_bp.route("/r4", methods=["POST"])
def genre_engagement():
    """
    Genre engagement analysis by votes and audience interaction
    
    Optional GROUP BY:
    - time_granularity: "year" | "decade" | "era" (optional single-select)
    - group_by: ["dg.genreName", "dtm.decade"] (custom override)
    
    Optional WHERE (Filters):
    - genres: ["Action", "Drama"] (multi-select)
    - title_types: ["movie", "tvSeries"] (multi-select)
    - start_year: number
    - end_year: number
    - min_rating: number
    - vote_min: number
    - vote_max: number
    - where: [{"field": "...", "operator": "...", "value": ...}] (dynamic)
    """
    try:
        params = request.get_json()
        
        # Determine time field based on custom group_by or time_granularity param
        time_granularity = params.get('time_granularity')
        include_time = time_granularity is not None
        
        # Validate time_granularity if provided
        if time_granularity:
            validation_error = validate_time_granularity(time_granularity, required=False)
            if validation_error:
                return jsonify({"status": "error", "message": validation_error["error"]}), 400
        
        if params.get('group_by'):
            for field in params.get('group_by'):
                if 'decade' in field.lower():
                    time_granularity = 'decade'
                    include_time = True
                    break
                elif 'era' in field.lower():
                    time_granularity = 'era'
                    include_time = True
                    break
                elif 'year' in field.lower() and 'dtm.' in field:
                    time_granularity = 'year'
                    include_time = True
                    break
        
        # Build SELECT clause
        select_clause = """
        SELECT
            dg.genreName"""
        
        if include_time:
            select_clause += f""",
            dtm.{time_granularity} AS time_period"""
        
        select_clause += """,
            SUM(fp.numVotes) AS total_votes,
            COUNT(DISTINCT dtl.tconst) AS title_count,
            AVG(fp.numVotes) AS avg_votes_per_title,
            AVG(fp.averageRating) AS avg_rating
        FROM dim_genre dg
        JOIN bridge_title_genre btg ON dg.genreKey = btg.genreKey
        JOIN dim_title dtl ON btg.tconst = dtl.tconst
        JOIN fact_title_performance fp ON dtl.tconst = fp.tconst
        JOIN dim_time dtm ON fp.timeKey = dtm.timeKey
        WHERE 1=1
        """
        
        query = select_clause
        params_list = []
        table_aliases = {
            'dim_genre': 'dg',
            'bridge_title_genre': 'btg',
            'dim_title': 'dtl',
            'fact_title_performance': 'fp',
            'dim_time': 'dtm'
        }
        
        # Apply common filters
        query = apply_common_filters(query, params, params_list, table_aliases)
        
        # Dynamic WHERE clause
        where_clause = build_where_clause(params.get('where'), params_list, table_aliases)
        query += where_clause
        
        # Dynamic GROUP BY
        default_groups = ["dg.genreName"]
        if include_time:
            default_groups.append(f"dtm.{time_granularity}")
        
        group_by_clause = build_group_by_clause(params.get('group_by'), default_groups)
        query += group_by_clause
        
        # ORDER BY - use aliased columns
        if include_time:
            query += " ORDER BY time_period DESC, total_votes DESC LIMIT 200"
        else:
            query += " ORDER BY total_votes DESC LIMIT 10"
        
        data = execute_query(query, tuple(params_list))
        return jsonify({"status": "success", "data": data})
    except Exception as e:
        logger.error(f"Error in genre_engagement: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 400


# ============================================================
# Report 5: TV Series Engagement Analysis
# ============================================================
@reports_bp.route("/r5", methods=["POST"])
def tv_engagement():
    """
    TV content engagement at series/season/episode hierarchy levels
    
    Required GROUP BY:
    - tv_level: "episode" | "season" | "series" (REQUIRED)
    
    Optional GROUP BY:
    - group_by_genre: true/false (adds genre to GROUP BY)
    - group_by_time: true/false (adds time to GROUP BY)
    - time_granularity: "year" | "decade" | "era" (if group_by_time enabled)
    - group_by: custom override
    
    Optional WHERE (Filters):
    - genres: ["Action", "Drama"] (multi-select)
    - title_types: ["tvSeries", "tvMiniSeries"] (multi-select)
    - start_year: number
    - end_year: number
    - min_rating: number
    - min_votes: number
    - completion_status: "ended" | "ongoing" (filter by series completion)
    - series_name: string (LIKE search on series title)
    - season_number: number (if viewing episode level)
    - where: [{"field": "...", "operator": "...", "value": ...}] (dynamic)
    """
    try:
        params = request.get_json()
        tv_level = params.get('tv_level', 'series')
        
        if tv_level not in ['episode', 'season', 'series']:
            return jsonify({
                "status": "error", 
                "message": "tv_level is required and must be 'episode', 'season', or 'series'"
            }), 400
        
        # Check grouping needs
        group_by_genre, group_by_time, time_granularity = check_grouping_needs(params)
        
        # Validate time_granularity if time grouping is enabled
        if group_by_time:
            validation_error = validate_time_granularity(time_granularity, required=False)
            if validation_error:
                return jsonify({"status": "error", "message": validation_error["error"]}), 400
        
        # Check if JOINs are needed (grouping OR filtering)
        need_genre_join = needs_join(params, group_by_genre, ['genres'])
        need_time_join = needs_join(params, group_by_time, ['start_year', 'end_year'])
        
        params_list = []
        
        # Build query based on TV level
        if tv_level == 'series':
            select_clause = """
            SELECT
                de.parentTconst AS series_id,
                dtl_parent.primaryTitle AS series_title"""
            
            if group_by_genre:
                select_clause += """,
                dg.genreName"""
            
            if group_by_time:
                select_clause += f""",
                dtm.{time_granularity} AS time_period"""
            
            select_clause += """,
                SUM(fp.numVotes) AS total_votes,
                AVG(fp.averageRating) AS avg_rating,
                COUNT(DISTINCT de.episodeTconst) AS episode_count,
                COUNT(DISTINCT de.seasonNumber) AS season_count
            FROM dim_episode de
            JOIN dim_title dtl ON de.episodeTconst = dtl.tconst
            JOIN dim_title dtl_parent ON de.parentTconst = dtl_parent.tconst
            JOIN fact_title_performance fp ON de.episodeTconst = fp.tconst"""
            
            if need_time_join:
                select_clause += """
            JOIN dim_time dtm ON fp.timeKey = dtm.timeKey"""
            
            if need_genre_join:
                select_clause += """
            JOIN bridge_title_genre btg ON de.parentTconst = btg.tconst
            JOIN dim_genre dg ON btg.genreKey = dg.genreKey"""
            
            query = select_clause + """
            WHERE dtl.titleType = 'tvEpisode'
            """
            
            default_groups = ["de.parentTconst", "dtl_parent.primaryTitle"]
            table_aliases = {
                'dim_episode': 'de',
                'dim_title': 'dtl',
                'dim_title_parent': 'dtl_parent',
                'fact_title_performance': 'fp',
                'dim_time': 'dtm',
                'bridge_title_genre': 'btg',
                'dim_genre': 'dg'
            }
        
        elif tv_level == 'season':
            select_clause = """
            SELECT
                de.parentTconst,
                dtl_parent.primaryTitle AS series_title,
                de.seasonNumber"""
            
            if group_by_genre:
                select_clause += """,
                dg.genreName"""
            
            if group_by_time:
                select_clause += f""",
                dtm.{time_granularity} AS time_period"""
            
            select_clause += """,
                SUM(fp.numVotes) AS total_votes,
                AVG(fp.averageRating) AS avg_rating,
                COUNT(DISTINCT de.episodeTconst) AS episode_count
            FROM dim_episode de
            JOIN dim_title dtl ON de.episodeTconst = dtl.tconst
            JOIN dim_title dtl_parent ON de.parentTconst = dtl_parent.tconst
            JOIN fact_title_performance fp ON de.episodeTconst = fp.tconst"""
            
            if need_time_join:
                select_clause += """
            JOIN dim_time dtm ON fp.timeKey = dtm.timeKey"""
            
            if need_genre_join:
                select_clause += """
            JOIN bridge_title_genre btg ON de.parentTconst = btg.tconst
            JOIN dim_genre dg ON btg.genreKey = dg.genreKey"""
            
            query = select_clause + """
            WHERE dtl.titleType = 'tvEpisode'
            """
            
            default_groups = ["de.parentTconst", "dtl_parent.primaryTitle", "de.seasonNumber"]
            table_aliases = {
                'dim_episode': 'de',
                'dim_title': 'dtl',
                'dim_title_parent': 'dtl_parent',
                'fact_title_performance': 'fp',
                'dim_time': 'dtm',
                'bridge_title_genre': 'btg',
                'dim_genre': 'dg'
            }
        
        else:  # episode
            select_clause = """
            SELECT
                de.episodeTconst,
                dtl.primaryTitle AS episode_title,
                dtl_parent.primaryTitle AS series_title,
                de.seasonNumber,
                de.episodeNumber"""
            
            if group_by_genre:
                select_clause += """,
                dg.genreName"""
            
            if group_by_time:
                select_clause += f""",
                dtm.{time_granularity} AS time_period"""
            
            select_clause += """,
                fp.numVotes AS total_votes,
                fp.averageRating AS avg_rating
            FROM dim_episode de
            JOIN dim_title dtl ON de.episodeTconst = dtl.tconst
            JOIN dim_title dtl_parent ON de.parentTconst = dtl_parent.tconst
            JOIN fact_title_performance fp ON de.episodeTconst = fp.tconst"""
            
            if need_time_join:
                select_clause += """
            JOIN dim_time dtm ON fp.timeKey = dtm.timeKey"""
            
            if need_genre_join:
                select_clause += """
            JOIN bridge_title_genre btg ON de.parentTconst = btg.tconst
            JOIN dim_genre dg ON btg.genreKey = dg.genreKey"""
            
            query = select_clause + """
            WHERE dtl.titleType = 'tvEpisode'
            """
            
            default_groups = []  # No grouping for episode level by default
            table_aliases = {
                'dim_episode': 'de',
                'dim_title': 'dtl',
                'dim_title_parent': 'dtl_parent',
                'fact_title_performance': 'fp',
                'dim_time': 'dtm',
                'bridge_title_genre': 'btg',
                'dim_genre': 'dg'
            }
        
        # Apply common filters
        query = apply_common_filters(query, params, params_list, table_aliases)
        
        # Series name search
        if params.get('series_name'):
            query += " AND dtl_parent.primaryTitle LIKE %s"
            params_list.append(f"%{params.get('series_name')}%")
        
        # Season number filter
        if params.get('season_number'):
            query += " AND de.seasonNumber = %s"
            params_list.append(int(params.get('season_number')))
        
        # Completion status filter
        if params.get('completion_status'):
            status = params.get('completion_status')
            if status == 'ended':
                query += " AND dtl_parent.endYear IS NOT NULL"
            elif status == 'ongoing':
                query += " AND dtl_parent.endYear IS NULL"
        
        # Dynamic WHERE clause
        where_clause = build_where_clause(params.get('where'), params_list, table_aliases)
        query += where_clause
        
        # Dynamic GROUP BY
        if tv_level in ['series', 'season']:
            if group_by_genre:
                default_groups.append("dg.genreName")
            if group_by_time:
                default_groups.append('time_period')
            
            # Handle custom group_by with time field replacement
            if params.get('group_by'):
                custom_groups = []
                for field in params.get('group_by'):
                    # Replace dtm.decade/year/era with time_period alias if present
                    if field in [f'dtm.{time_granularity}', 'dtm.decade', 'dtm.year', 'dtm.era']:
                        custom_groups.append('time_period')
                    else:
                        custom_groups.append(field)
                group_by_clause = build_group_by_clause(custom_groups, None)
            else:
                group_by_clause = build_group_by_clause(None, default_groups)
            
            query += group_by_clause
        elif params.get('group_by'):
            # Episode level with custom grouping
            custom_groups = []
            for field in params.get('group_by'):
                # Replace dtm.decade/year/era with time_period alias if present
                if field in [f'dtm.{time_granularity}', 'dtm.decade', 'dtm.year', 'dtm.era']:
                    custom_groups.append('time_period')
                else:
                    custom_groups.append(field)
            group_by_clause = build_group_by_clause(custom_groups, None)
            query += group_by_clause
        
        if group_by_genre or group_by_time:
            query += " ORDER BY total_votes DESC LIMIT 200"
        else:
            query += " ORDER BY total_votes DESC LIMIT 10"
        
        data = execute_query(query, tuple(params_list))
        return jsonify({"status": "success", "data": data})
    except Exception as e:
        logger.error(f"Error in tv_engagement: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 400