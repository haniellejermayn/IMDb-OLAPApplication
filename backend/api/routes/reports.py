from flask import Blueprint, request, jsonify
from database import execute_query
import logging

reports_bp = Blueprint('reports', __name__)
logger = logging.getLogger(__name__)

# ============================================================
# UTILITY FUNCTIONS
# ============================================================

def build_where_clause(where_conditions, params_list, table_aliases):
    """
    Build dynamic WHERE clause from array of conditions
    where_conditions: [{"field": "dtl.titleType", "operator": "=", "value": "movie"}]
    table_aliases: dict mapping table names to their aliases for validation
    """
    if not where_conditions:
        return ""
    
    where_parts = []
    valid_operators = ['=', '!=', '>', '<', '>=', '<=', 'LIKE', 'IN', 'NOT IN', 'IS NULL', 'IS NOT NULL', 'BETWEEN']
    
    for condition in where_conditions:
        field = condition.get('field')
        operator = condition.get('operator', '=').upper()
        value = condition.get('value')
        
        if not field:
            continue
            
        if operator not in valid_operators:
            logger.warning(f"Invalid operator: {operator}")
            continue
        
        # Handle NULL operators
        if operator in ['IS NULL', 'IS NOT NULL']:
            where_parts.append(f"{field} {operator}")
        # Handle BETWEEN operator
        elif operator == 'BETWEEN':
            if isinstance(value, list) and len(value) == 2:
                where_parts.append(f"{field} BETWEEN %s AND %s")
                params_list.extend(value)
            else:
                logger.warning(f"BETWEEN requires list of 2 values for field: {field}")
        # Handle IN/NOT IN operators
        elif operator in ['IN', 'NOT IN']:
            if isinstance(value, list):
                placeholders = ','.join(['%s'] * len(value))
                where_parts.append(f"{field} {operator} ({placeholders})")
                params_list.extend(value)
            else:
                logger.warning(f"IN/NOT IN requires list value for field: {field}")
        # Handle standard operators
        else:
            where_parts.append(f"{field} {operator} %s")
            params_list.append(value)
    
    return " AND " + " AND ".join(where_parts) if where_parts else ""


def build_group_by_clause(group_by_fields, default_groups):
    """
    Build dynamic GROUP BY clause
    group_by_fields: ["dtl.titleType", "dtm.year"] or None
    default_groups: default grouping if none provided
    """
    if group_by_fields:
        return " GROUP BY " + ", ".join(group_by_fields)
    elif default_groups:
        return " GROUP BY " + ", ".join(default_groups)
    return ""


# ============================================================
# Report 1: Genre-Rating Association
# ============================================================
@reports_bp.route("/r1", methods=["POST"])
def genre_rating_association():
    """
    Chi-square analysis: Genre vs Rating Bins
    
    Required Inputs:
    - time_granularity: "year" | "decade" | "era"
    
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
        time_granularity = params.get('time_granularity', 'year')
        calculate_chi = params.get('calculate_chi_square', False)
        
        query = f"""
        SELECT
            dg.genreName AS genre,
            CASE 
                WHEN fp.averageRating < 4 THEN 'Low'
                WHEN fp.averageRating < 7 THEN 'Mid'
                ELSE 'High'
            END AS rating_bin,
            dtm.{time_granularity} AS time_period,
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
            query += f" AND dtl.titleType IN ({placeholders})"
            params_list.extend(types)
        
        # Year range filter
        if params.get('start_year') and params.get('end_year'):
            query += " AND dtm.year BETWEEN %s AND %s"
            params_list.extend([params.get('start_year'), params.get('end_year')])
        
        # Minimum votes filter
        if params.get('min_votes'):
            query += " AND fp.numVotes >= %s"
            params_list.append(int(params.get('min_votes')))
        
        # Runtime range filter
        if params.get('runtime_min'):
            query += " AND dtl.runtimeMinutes >= %s"
            params_list.append(int(params.get('runtime_min')))
        
        if params.get('runtime_max'):
            query += " AND dtl.runtimeMinutes <= %s"
            params_list.append(int(params.get('runtime_max')))
        
        # Dynamic WHERE clause
        where_clause = build_where_clause(params.get('where'), params_list, table_aliases)
        query += where_clause
        
        # Dynamic GROUP BY
        default_groups = [f"dg.genreName", "rating_bin", f"dtm.{time_granularity}"]
        group_by_clause = build_group_by_clause(params.get('group_by'), default_groups)
        query += group_by_clause
        
        # Dynamic ORDER BY based on grouping
        if params.get('group_by'):
            query += f" ORDER BY {params.get('group_by')[0]} DESC"
        else:
            query += " ORDER BY dtm.year DESC, dg.genreName"
        
        data = execute_query(query, tuple(params_list))
        
        # Calculate chi-square if requested
        if calculate_chi:
            chi_results = calculate_chi_square_statistic(data)
            return jsonify({
                "status": "success", 
                "data": data,
                "chi_square_analysis": chi_results
            })
        
        return jsonify({"status": "success", "data": data})
    except Exception as e:
        logger.error(f"Error in genre_rating_association: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 400


def calculate_chi_square_statistic(data):
    """
    Calculate chi-square statistic from contingency table data
    Formula: χ² = Σ((O - E)² / E)
    where O = observed frequency, E = expected frequency
    """
    from collections import defaultdict
    
    # Build contingency table
    contingency = defaultdict(lambda: defaultdict(int))
    row_totals = defaultdict(int)
    col_totals = defaultdict(int)
    grand_total = 0
    
    for row in data:
        genre = row['genre']
        rating_bin = row['rating_bin']
        count = row['count']
        
        contingency[genre][rating_bin] = count
        row_totals[genre] += count
        col_totals[rating_bin] += count
        grand_total += count
    
    if grand_total == 0:
        return {"error": "No data for chi-square calculation"}
    
    # Calculate chi-square statistic
    chi_square = 0
    degrees_of_freedom = (len(row_totals) - 1) * (len(col_totals) - 1)
    
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
    
    # Critical values at α = 0.05 for common df
    critical_values = {
        1: 3.841, 2: 5.991, 3: 7.815, 4: 9.488, 5: 11.070,
        6: 12.592, 8: 15.507, 10: 18.307, 15: 24.996, 20: 31.410
    }
    
    critical_value = critical_values.get(degrees_of_freedom, "See chi-square table")
    is_significant = chi_square > critical_value if isinstance(critical_value, float) else None
    
    return {
        "chi_square_statistic": round(chi_square, 4),
        "degrees_of_freedom": degrees_of_freedom,
        "critical_value_alpha_0.05": critical_value,
        "is_significant": is_significant,
        "interpretation": "Significant association between genre and rating" if is_significant else "No significant association detected" if is_significant is False else "Check chi-square table for significance",
        "row_totals": dict(row_totals),
        "column_totals": dict(col_totals),
        "grand_total": grand_total,
        "top_contributions": sorted(cell_contributions, key=lambda x: x['contribution'], reverse=True)[:10]
    }


# ============================================================
# Report 2: Runtime Trends 
# ============================================================
@reports_bp.route("/r2", methods=["POST"])
def runtime_trends():
    """
    Runtime evolution analysis by title type over time
    
    Required Inputs:
    - time_granularity: "year" | "decade" | "era"
    
    Optional GROUP BY:
    - Optional: Genre grouping (add dg.genreName to SELECT and GROUP BY)
    - group_by: ["dtm.decade", "dtl.titleType", "dg.genreName"] (custom override)
    
    Optional WHERE (Filters):
    - genres: ["Action", "Drama"] (multi-select)
    - title_types: ["movie", "tvSeries"] (multi-select)
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
        time_granularity = params.get('time_granularity', 'year')
        
        # Check if genre grouping is needed
        include_genre = params.get('group_by_genre', False) or (
            params.get('group_by') and any('genreName' in field for field in params.get('group_by'))
        )
        
        # Build SELECT with optional genre
        select_clause = f"""
        SELECT
            dtm.{time_granularity} AS time_period,
            dtl.titleType"""
        
        if include_genre:
            select_clause += """,
            dg.genreName"""
        
        select_clause += """,
            AVG(dtl.runtimeMinutes) AS avg_runtime,
            COUNT(DISTINCT dtl.tconst) AS title_count,
            AVG(fp.averageRating) AS avg_rating
        FROM dim_title dtl
        JOIN fact_title_performance fp ON dtl.tconst = fp.tconst
        JOIN dim_time dtm ON fp.timeKey = dtm.timeKey"""
        
        if include_genre:
            select_clause += """
        JOIN bridge_title_genre btg ON dtl.tconst = btg.tconst
        JOIN dim_genre dg ON btg.genreKey = dg.genreKey"""
        
        query = select_clause + """
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
        
        # Runtime range filter
        if params.get('runtime_min'):
            query += " AND dtl.runtimeMinutes >= %s"
            params_list.append(int(params.get('runtime_min')))
        
        if params.get('runtime_max'):
            query += " AND dtl.runtimeMinutes <= %s"
            params_list.append(int(params.get('runtime_max')))
        
        # Minimum votes filter
        if params.get('min_votes'):
            query += " AND fp.numVotes >= %s"
            params_list.append(int(params.get('min_votes')))
        
        # Dynamic WHERE clause
        where_clause = build_where_clause(params.get('where'), params_list, table_aliases)
        query += where_clause
        
        # Dynamic GROUP BY
        default_groups = [f"dtm.{time_granularity}", "dtl.titleType"]
        if include_genre:
            default_groups.append("dg.genreName")
        
        group_by_clause = build_group_by_clause(params.get('group_by'), default_groups)
        query += group_by_clause
        
        # Dynamic ORDER BY
        if params.get('group_by'):
            query += f" ORDER BY {params.get('group_by')[0]} DESC"
        else:
            query += f" ORDER BY dtm.{time_granularity} DESC"
        
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
    - job_category: string (required)
    
    Optional GROUP BY:
    - group_by_genre: true/false (adds genre grouping)
    - group_by_time: true/false (adds time grouping)
    - time_granularity: "year" | "decade" | "era" (if group_by_time enabled)
    - group_by: ["dp.nconst", "dp.primaryName", "dg.genreName"] (custom override)
    
    Optional WHERE (Filters):
    - genres: ["Action", "Drama"] (multi-select)
    - title_types: ["movie", "tvSeries"] (multi-select)
    - start_year: number
    - end_year: number
    - min_rating: number
    - min_votes: number
    - min_titles: number (HAVING filter)
    - where: [{"field": "...", "operator": "...", "value": ...}] (dynamic)
    """
    try:
        params = request.get_json()
        job_category = params.get('job_category')
        
        if not job_category:
            return jsonify({"status": "error", "message": "job_category is required"}), 400
        
        # Check if optional grouping is needed
        group_by_genre = params.get('group_by_genre', False)
        group_by_time = params.get('group_by_time', False)
        time_granularity = params.get('time_granularity', 'year')
        
        # Check custom group_by
        custom_group = params.get('group_by')
        if custom_group:
            group_by_genre = any('genreName' in field for field in custom_group)
            group_by_time = any('dtm.' in field for field in custom_group)
        
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
        
        if group_by_time:
            select_clause += """
        JOIN dim_time dtm ON fp.timeKey = dtm.timeKey"""
        
        if group_by_genre:
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
        
        # Dynamic WHERE clause
        where_clause = build_where_clause(params.get('where'), params_list, table_aliases)
        query += where_clause
        
        # Dynamic GROUP BY
        default_groups = ["dp.nconst", "dp.primaryName"]
        if group_by_genre:
            default_groups.append("dg.genreName")
        if group_by_time:
            default_groups.append(f"dtm.{time_granularity}")
        
        group_by_clause = build_group_by_clause(params.get('group_by'), default_groups)
        query += group_by_clause
        
        # HAVING clause (filters on aggregated columns)
        having_parts = []
        if params.get('min_titles'):
            having_parts.append(f"COUNT(DISTINCT dtl.tconst) >= {int(params.get('min_titles'))}")
        
        if having_parts:
            query += " HAVING " + " AND ".join(having_parts)
        
        query += " ORDER BY avg_rating DESC"
        query += " LIMIT 100"
        
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
        
        if params.get('group_by'):
            # Check if decade or era is in group_by
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
        
        # Vote range filter
        if params.get('vote_min'):
            query += " AND fp.numVotes >= %s"
            params_list.append(int(params.get('vote_min')))
        
        if params.get('vote_max'):
            query += " AND fp.numVotes <= %s"
            params_list.append(int(params.get('vote_max')))
        
        # Legacy min_votes support
        if params.get('min_votes'):
            query += " AND fp.numVotes >= %s"
            params_list.append(int(params.get('min_votes')))
        
        # Dynamic WHERE clause
        where_clause = build_where_clause(params.get('where'), params_list, table_aliases)
        query += where_clause
        
        # Dynamic GROUP BY
        default_groups = ["dg.genreKey", "dg.genreName"]
        if include_time:
            default_groups.append(f"dtm.{time_granularity}")
        
        group_by_clause = build_group_by_clause(params.get('group_by'), default_groups)
        query += group_by_clause
        
        # Dynamic ORDER BY
        if params.get('group_by'):
            # Order by time field if present, otherwise first field
            order_field = f"dtm.{time_granularity}" if include_time else params.get('group_by')[0]
            query += f" ORDER BY {order_field} DESC, total_votes DESC"
        elif include_time:
            query += f" ORDER BY dtm.{time_granularity} DESC, total_votes DESC"
        else:
            query += " ORDER BY total_votes DESC"
        
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
    - tv_level: "episode" | "season" | "series" (required single-select)
    
    Optional GROUP BY:
    - group_by_genre: true/false (adds genre grouping)
    - group_by_time: true/false (adds time grouping)
    - time_granularity: "year" | "decade" | "era" (if group_by_time enabled)
    - group_by: custom override
    
    Optional WHERE (Filters):
    - genres: ["Action", "Drama"] (multi-select)
    - title_types: ["tvSeries", "tvMiniSeries"] (multi-select)
    - start_year: number
    - end_year: number
    - min_rating: number
    - min_votes: number
    - series_name: string (LIKE search on series title)
    - season_number: number (if viewing episode level)
    - where: [{"field": "...", "operator": "...", "value": ...}] (dynamic)
    """
    try:
        params = request.get_json()
        tv_level = params.get('tv_level', 'series')
        
        if tv_level not in ['episode', 'season', 'series']:
            return jsonify({"status": "error", "message": "tv_level must be 'episode', 'season', or 'series'"}), 400
        
        # Check if optional grouping is needed
        group_by_genre = params.get('group_by_genre', False)
        group_by_time = params.get('group_by_time', False)
        time_granularity = params.get('time_granularity', 'year')
        
        # Check custom group_by
        custom_group = params.get('group_by')
        if custom_group:
            group_by_genre = any('genreName' in field for field in custom_group)
            group_by_time = any('dtm.' in field for field in custom_group)
        
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
                COUNT(DISTINCT de.episodeTconst) AS episode_count
            FROM dim_episode de
            JOIN dim_title dtl ON de.episodeTconst = dtl.tconst
            JOIN dim_title dtl_parent ON de.parentTconst = dtl_parent.tconst
            JOIN fact_title_performance fp ON de.episodeTconst = fp.tconst"""
            
            if group_by_time:
                select_clause += """
            JOIN dim_time dtm ON fp.timeKey = dtm.timeKey"""
            
            if group_by_genre:
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
            
            if group_by_time:
                select_clause += """
            JOIN dim_time dtm ON fp.timeKey = dtm.timeKey"""
            
            if group_by_genre:
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
            
            if group_by_time:
                select_clause += """
            JOIN dim_time dtm ON fp.timeKey = dtm.timeKey"""
            
            if group_by_genre:
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
        
        # Genre filter
        if params.get('genres'):
            genres = params.get('genres') if isinstance(params.get('genres'), list) else [params.get('genres')]
            placeholders = ','.join(['%s'] * len(genres))
            query += f" AND dg.genreName IN ({placeholders})"
            params_list.extend(genres)
        
        # Title type filter (for parent series)
        if params.get('title_types'):
            types = params.get('title_types') if isinstance(params.get('title_types'), list) else [params.get('title_types')]
            placeholders = ','.join(['%s'] * len(types))
            query += f" AND dtl_parent.titleType IN ({placeholders})"
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
        
        # Series name search
        if params.get('series_name'):
            query += " AND dtl_parent.primaryTitle LIKE %s"
            params_list.append(f"%{params.get('series_name')}%")
        
        # Season number filter (for episode level)
        if params.get('season_number'):
            query += " AND de.seasonNumber = %s"
            params_list.append(int(params.get('season_number')))
        
        # Dynamic WHERE clause
        where_clause = build_where_clause(params.get('where'), params_list, table_aliases)
        query += where_clause
        
        # Dynamic GROUP BY
        if tv_level in ['series', 'season']:
            if group_by_genre:
                default_groups.append("dg.genreName")
            if group_by_time:
                default_groups.append(f"dtm.{time_granularity}")
            
            group_by_clause = build_group_by_clause(params.get('group_by'), default_groups)
            query += group_by_clause
        elif custom_group:
            # Episode level with custom grouping
            group_by_clause = build_group_by_clause(params.get('group_by'), None)
            query += group_by_clause
        
        query += " ORDER BY total_votes DESC LIMIT 100"
        
        data = execute_query(query, tuple(params_list))
        return jsonify({"status": "success", "data": data})
    except Exception as e:
        logger.error(f"Error in tv_engagement: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 400