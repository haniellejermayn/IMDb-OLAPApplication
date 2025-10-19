USE imdb_star_schema;

-- ============================================================
-- Index 1: Dim_Title - for Report 2 (Runtime Trends)
-- ============================================================

ALTER TABLE dim_title 
ADD INDEX idx_title_type_runtime (titleType, runtimeMinutes);

SHOW INDEXES FROM dim_title WHERE Key_name = 'idx_title_type_runtime';

-- ============================================================
-- Index 2: Bridge_Title_Person - for Report 3 (Person Performance)
-- ============================================================
ALTER TABLE bridge_title_person 
ADD INDEX idx_person_category_optimized (category, nconst);

SHOW INDEXES FROM bridge_title_person WHERE Key_name = 'idx_person_category_optimized';

-- ============================================================
-- Index 3: Fact_Title_Performance - for Report 4 (Genre Engagement)
-- ============================================================
ALTER TABLE fact_title_performance 
ADD INDEX idx_fact_agg_optimized (averageRating, numVotes, timeKey);

SHOW INDEXES FROM fact_title_performance WHERE Key_name = 'idx_fact_agg_optimized';

-- ============================================================
-- VERIFY ALL 3 INDEXES CREATED
-- ============================================================

SELECT 'Composite indexes created successfully' AS status;

SELECT 'Dim_Title indexes:' AS table_name;
SHOW INDEXES FROM dim_title;

SELECT 'Bridge_Title_Person indexes:' AS table_name;
SHOW INDEXES FROM bridge_title_person;

SELECT 'Fact_Title_Performance indexes:' AS table_name;
SHOW INDEXES FROM fact_title_performance;