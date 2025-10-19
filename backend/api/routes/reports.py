from flask import Blueprint, request, jsonify
from database import execute_query
import logging

reports_bp = Blueprint('reports', __name__)
logger = logging.getLogger(__name__)

# ============================================================
# Report 1: Genre-Rating Association
# ============================================================
@reports_bp.route("/r1", methods=["POST"])
def genre_rating_association():
    """
    Chi-square analysis: Genre vs Rating Bins
    Analyzes how genres correlate with rating distributions
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
        
        query += f" GROUP BY dg.genreName, rating_bin, dtm.{time_granularity}"
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
        
        query += f" GROUP BY dtm.{time_granularity}, dtl.titleType"
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
        
        query += " GROUP BY dp.nconst, dp.primaryName"
        
        if params.get('min_titles'):
            query += " HAVING COUNT(DISTINCT dtl.tconst) >= %s"
            params_list.append(int(params.get('min_titles')))
        
        if params.get('min_rating'):
            query += " AND AVG(fp.averageRating) >= %s"
            params_list.append(float(params.get('min_rating')))
        
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
    """
    try:
        params = request.get_json()
        
        query = """
        SELECT
            dg.genreName,
            dtm.year,
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
        
        query += " GROUP BY dg.genreKey, dg.genreName, dtm.year"
        query += " ORDER BY dtm.year DESC, total_votes DESC"
        
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
        
        if params.get('min_rating'):
            query += " AND fp.averageRating >= %s"
            params_list.append(float(params.get('min_rating')))
        
        if tv_level in ['series', 'season']:
            query += " GROUP BY "
            if tv_level == 'series':
                query += "de.parentTconst, dtl_parent.primaryTitle"
            else:
                query += "de.parentTconst, de.seasonNumber"
        
        query += " ORDER BY total_votes DESC LIMIT 100"
        
        data = execute_query(query, tuple(params_list))
        return jsonify({"status": "success", "data": data})
    except Exception as e:
        logger.error(f"Error in tv_engagement: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 400