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
    valid_operators = ['=', '!=', '>', '<', '>=', '<=', 'LIKE', 'IN', 'NOT IN', 'IS NULL', 'IS NOT NULL']
    
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
    Analyzes how genres correlate with rating distributions
    
    Additional params:
    - where: [{"field": "dtl.titleType", "operator": "=", "value": "movie"}]
    - group_by: ["dg.genreName", "rating_bin", "dtm.decade"] (overrides default)
    """
    try:
        params = request.get_json()
        time_granularity = params.get('time_granularity', 'year')
        
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
        
        # Original filters
        if params.get('genres'):
            genres = params.get('genres') if isinstance(params.get('genres'), list) else [params.get('genres')]
            placeholders = ','.join(['%s'] * len(genres))
            query += f" AND dg.genreName IN ({placeholders})"
            params_list.extend(genres)
        
        if params.get('start_year') and params.get('end_year'):
            query += " AND dtm.year BETWEEN %s AND %s"
            params_list.extend([params.get('start_year'), params.get('end_year')])
        
        if params.get('min_votes'):
            query += " AND fp.numVotes >= %s"
            params_list.append(int(params.get('min_votes')))
        
        # Dynamic WHERE clause
        where_clause = build_where_clause(params.get('where'), params_list, table_aliases)
        query += where_clause
        
        # Dynamic GROUP BY
        default_groups = [f"dg.genreName", "rating_bin", f"dtm.{time_granularity}"]
        group_by_clause = build_group_by_clause(params.get('group_by'), default_groups)
        query += group_by_clause
        
        # Dynamic ORDER BY based on grouping
        if params.get('group_by'):
            # Use first group_by field for ordering
            query += f" ORDER BY {params.get('group_by')[0]} DESC"
        else:
            query += " ORDER BY dtm.year DESC, dg.genreName"
        
        data = execute_query(query, tuple(params_list))
        return jsonify({"status": "success", "data": data})
    except Exception as e:
        logger.error(f"Error in genre_rating_association: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 400


# ============================================================
# Report 2: Runtime Trends 
# ============================================================
@reports_bp.route("/r2", methods=["POST"])
def runtime_trends():
    """
    Runtime evolution analysis by title type over time
    Uses composite index: (titleType, runtimeMinutes)
    
    Additional params:
    - where: [{"field": "dtl.runtimeMinutes", "operator": ">=", "value": 60}]
    - group_by: ["dtm.decade", "dtl.titleType"] (overrides default)
    """
    try:
        params = request.get_json()
        time_granularity = params.get('time_granularity', 'year')
        
        query = f"""
        SELECT
            dtm.{time_granularity} AS time_period,
            dtl.titleType,
            AVG(dtl.runtimeMinutes) AS avg_runtime,
            COUNT(DISTINCT dtl.tconst) AS title_count,
            AVG(fp.averageRating) AS avg_rating
        FROM dim_title dtl
        JOIN fact_title_performance fp ON dtl.tconst = fp.tconst
        JOIN dim_time dtm ON fp.timeKey = dtm.timeKey
        WHERE dtl.runtimeMinutes IS NOT NULL
        """
        
        params_list = []
        table_aliases = {
            'dim_title': 'dtl',
            'fact_title_performance': 'fp',
            'dim_time': 'dtm'
        }
        
        # Original filters
        if params.get('title_types'):
            types = params.get('title_types') if isinstance(params.get('title_types'), list) else [params.get('title_types')]
            placeholders = ','.join(['%s'] * len(types))
            query += f" AND dtl.titleType IN ({placeholders})"
            params_list.extend(types)
        
        if params.get('start_year') and params.get('end_year'):
            query += " AND dtm.year BETWEEN %s AND %s"
            params_list.extend([params.get('start_year'), params.get('end_year')])
        
        if params.get('min_rating'):
            query += " AND fp.averageRating >= %s"
            params_list.append(float(params.get('min_rating')))
        
        # Dynamic WHERE clause
        where_clause = build_where_clause(params.get('where'), params_list, table_aliases)
        query += where_clause
        
        # Dynamic GROUP BY
        default_groups = [f"dtm.{time_granularity}", "dtl.titleType"]
        group_by_clause = build_group_by_clause(params.get('group_by'), default_groups)
        query += group_by_clause
        
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
    
    Additional params:
    - where: [{"field": "dtl.titleType", "operator": "=", "value": "movie"}]
    - group_by: ["dp.nconst", "dp.primaryName", "dtl.titleType"] (overrides default)
    """
    try:
        params = request.get_json()
        job_category = params.get('job_category')
        
        if not job_category:
            return jsonify({"status": "error", "message": "job_category is required"}), 400
        
        query = """
        SELECT
            dp.nconst,
            dp.primaryName,
            AVG(fp.averageRating) AS avg_rating,
            COUNT(DISTINCT dtl.tconst) AS total_titles
        FROM dim_person dp
        JOIN bridge_title_person btp ON dp.nconst = btp.nconst
        JOIN dim_title dtl ON btp.tconst = dtl.tconst
        JOIN fact_title_performance fp ON dtl.tconst = fp.tconst
        WHERE btp.category = %s
        """
        
        params_list = [job_category]
        table_aliases = {
            'dim_person': 'dp',
            'bridge_title_person': 'btp',
            'dim_title': 'dtl',
            'fact_title_performance': 'fp'
        }
        
        # Dynamic WHERE clause
        where_clause = build_where_clause(params.get('where'), params_list, table_aliases)
        query += where_clause
        
        # Dynamic GROUP BY
        default_groups = ["dp.nconst", "dp.primaryName"]
        group_by_clause = build_group_by_clause(params.get('group_by'), default_groups)
        query += group_by_clause
        
        # HAVING clause (filters on aggregated columns)
        having_parts = []
        if params.get('min_titles'):
            having_parts.append(f"COUNT(DISTINCT dtl.tconst) >= {int(params.get('min_titles'))}")
        
        if params.get('min_rating'):
            having_parts.append(f"AVG(fp.averageRating) >= {float(params.get('min_rating'))}")
        
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
    Uses composite index: (averageRating, numVotes, timeKey)
    
    Additional params:
    - where: [{"field": "dtl.titleType", "operator": "IN", "value": ["movie", "tvSeries"]}]
    - group_by: ["dg.genreName", "dtm.decade"] (overrides default)
    """
    try:
        params = request.get_json()
        
        # Determine time field based on custom group_by or default
        time_field = "dtm.year"  # default
        if params.get('group_by'):
            # Check if decade or era is in group_by
            for field in params.get('group_by'):
                if 'decade' in field.lower():
                    time_field = "dtm.decade"
                    break
                elif 'era' in field.lower():
                    time_field = "dtm.era"
                    break
        
        query = f"""
        SELECT
            dg.genreName,
            {time_field} AS time_period,
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
        
        params_list = []
        table_aliases = {
            'dim_genre': 'dg',
            'bridge_title_genre': 'btg',
            'dim_title': 'dtl',
            'fact_title_performance': 'fp',
            'dim_time': 'dtm'
        }
        
        # Original filters
        if params.get('genres'):
            genres = params.get('genres') if isinstance(params.get('genres'), list) else [params.get('genres')]
            placeholders = ','.join(['%s'] * len(genres))
            query += f" AND dg.genreName IN ({placeholders})"
            params_list.extend(genres)
        
        if params.get('start_year') and params.get('end_year'):
            query += " AND dtm.year BETWEEN %s AND %s"
            params_list.extend([params.get('start_year'), params.get('end_year')])
        
        if params.get('min_rating'):
            query += " AND fp.averageRating >= %s"
            params_list.append(float(params.get('min_rating')))
        
        if params.get('min_votes'):
            query += " AND fp.numVotes >= %s"
            params_list.append(int(params.get('min_votes')))
        
        # Dynamic WHERE clause
        where_clause = build_where_clause(params.get('where'), params_list, table_aliases)
        query += where_clause
        
        # Dynamic GROUP BY
        default_groups = ["dg.genreKey", "dg.genreName", time_field]
        group_by_clause = build_group_by_clause(params.get('group_by'), default_groups)
        query += group_by_clause
        
        # Dynamic ORDER BY based on grouping
        if params.get('group_by'):
            # Order by time field if present, otherwise first field
            order_field = time_field if any('dtm.' in f for f in params.get('group_by')) else params.get('group_by')[0]
            query += f" ORDER BY {order_field} DESC, total_votes DESC"
        else:
            query += f" ORDER BY {time_field} DESC, total_votes DESC"
        
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
    
    Additional params:
    - where: [{"field": "de.seasonNumber", "operator": "=", "value": 1}]
    - group_by: For series: ["de.parentTconst", "dtl_parent.titleType"]
    """
    try:
        params = request.get_json()
        tv_level = params.get('tv_level', 'series')
        
        if tv_level not in ['episode', 'season', 'series']:
            return jsonify({"status": "error", "message": "tv_level must be 'episode', 'season', or 'series'"}), 400
        
        params_list = []
        
        if tv_level == 'series':
            query = """
            SELECT
                de.parentTconst AS series_id,
                dtl_parent.primaryTitle AS series_title,
                SUM(fp.numVotes) AS total_votes,
                AVG(fp.averageRating) AS avg_rating,
                COUNT(DISTINCT de.episodeTconst) AS episode_count
            FROM dim_episode de
            JOIN dim_title dtl ON de.episodeTconst = dtl.tconst
            JOIN dim_title dtl_parent ON de.parentTconst = dtl_parent.tconst
            JOIN fact_title_performance fp ON de.episodeTconst = fp.tconst
            WHERE dtl.titleType = 'tvEpisode'
            """
            default_groups = ["de.parentTconst", "dtl_parent.primaryTitle"]
            table_aliases = {
                'dim_episode': 'de',
                'dim_title': 'dtl',
                'dim_title_parent': 'dtl_parent',
                'fact_title_performance': 'fp'
            }
        
        elif tv_level == 'season':
            query = """
            SELECT
                de.parentTconst,
                de.seasonNumber,
                SUM(fp.numVotes) AS total_votes,
                AVG(fp.averageRating) AS avg_rating,
                COUNT(DISTINCT de.episodeTconst) AS episode_count
            FROM dim_episode de
            JOIN dim_title dtl ON de.episodeTconst = dtl.tconst
            JOIN fact_title_performance fp ON de.episodeTconst = fp.tconst
            WHERE dtl.titleType = 'tvEpisode'
            """
            default_groups = ["de.parentTconst", "de.seasonNumber"]
            table_aliases = {
                'dim_episode': 'de',
                'dim_title': 'dtl',
                'fact_title_performance': 'fp'
            }
        
        else:  # episode
            query = """
            SELECT
                de.episodeTconst,
                dtl.primaryTitle AS episode_title,
                de.seasonNumber,
                de.episodeNumber,
                fp.numVotes AS total_votes,
                fp.averageRating AS avg_rating
            FROM dim_episode de
            JOIN dim_title dtl ON de.episodeTconst = dtl.tconst
            JOIN fact_title_performance fp ON de.episodeTconst = fp.tconst
            WHERE dtl.titleType = 'tvEpisode'
            """
            default_groups = []  # No grouping for episode level
            table_aliases = {
                'dim_episode': 'de',
                'dim_title': 'dtl',
                'fact_title_performance': 'fp'
            }
        
        # Original filters
        if params.get('min_rating'):
            query += " AND fp.averageRating >= %s"
            params_list.append(float(params.get('min_rating')))
        
        # Dynamic WHERE clause
        where_clause = build_where_clause(params.get('where'), params_list, table_aliases)
        query += where_clause
        
        # Dynamic GROUP BY
        if tv_level in ['series', 'season']:
            group_by_clause = build_group_by_clause(params.get('group_by'), default_groups)
            query += group_by_clause
        
        query += " ORDER BY total_votes DESC LIMIT 100"
        
        data = execute_query(query, tuple(params_list))
        return jsonify({"status": "success", "data": data})
    except Exception as e:
        logger.error(f"Error in tv_engagement: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 400