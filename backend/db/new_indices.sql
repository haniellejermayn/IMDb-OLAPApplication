USE imdb_star_schema;

-- ============================================================
-- Index 1: Dim_Title - for Report 2 (Runtime Trends)
-- ============================================================
ALTER TABLE dim_title 
ADD INDEX idx_title_type_runtime (titleType, runtimeMinutes);
-- ============================================================
-- Index 2: Fact_Title_Performance - for Report 4 (Genre Engagement)
-- ============================================================
ALTER TABLE fact_title_performance 
ADD INDEX idx_fact_agg_optimized (averageRating, numVotes, timeKey);

-- ============================================================
-- VERIFICATION
-- ============================================================
SELECT 'Dim_Title - Runtime Trends optimization:' AS optimization;
SHOW INDEXES FROM dim_title WHERE Key_name = 'idx_title_type_runtime';

SELECT 'Fact_Title_Performance - Genre Engagement optimization:' AS optimization;
SHOW INDEXES FROM fact_title_performance WHERE Key_name = 'idx_fact_agg_optimized';