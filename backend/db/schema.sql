-- ============================================================
-- IMDb STAR SCHEMA
-- ============================================================
DROP DATABASE IF EXISTS imdb_star_schema;
CREATE DATABASE imdb_star_schema;
USE imdb_star_schema;

-- ============================================================
-- DIMENSION TABLES
-- ============================================================

-- Dimension: Time
-- Hierarchy: year -> decade -> era
CREATE TABLE Dim_Time (
    timeKey INT PRIMARY KEY AUTO_INCREMENT,
    year INT NOT NULL,
    decade VARCHAR(10),
    era VARCHAR(50),
    UNIQUE(year),
    INDEX idx_decade (decade),
    INDEX idx_era (era)
);

-- Dimension: Title
-- Core title information
CREATE TABLE Dim_Title (
    tconst VARCHAR(20) PRIMARY KEY,
    primaryTitle VARCHAR(500),
    originalTitle VARCHAR(500),
    titleType VARCHAR(50),
    endYear INT,
    runtimeMinutes INT,
    INDEX idx_titleType (titleType),
    INDEX idx_endYear (endYear),
    INDEX idx_titleType_year (titleType, endYear),
    INDEX idx_runtime (runtimeMinutes),
    INDEX idx_primaryTitle (primaryTitle(100))
);

-- Dimension: Genre
CREATE TABLE Dim_Genre (
    genreKey INT PRIMARY KEY AUTO_INCREMENT,
    genreName VARCHAR(50) UNIQUE NOT NULL,
    INDEX idx_genreName (genreName)
);

-- Dimension: Person
CREATE TABLE Dim_Person (
    nconst VARCHAR(20) PRIMARY KEY,
    primaryName VARCHAR(200),
    INDEX idx_name (primaryName)
);

-- Dimension: Episode
-- Hierarchy: episode -> season -> series
CREATE TABLE Dim_Episode (
    episodeTconst VARCHAR(20) PRIMARY KEY,
    parentTconst VARCHAR(20),
    seasonNumber INT,
    episodeNumber INT,
    FOREIGN KEY (episodeTconst) REFERENCES Dim_Title(tconst) ON DELETE CASCADE,
    FOREIGN KEY (parentTconst) REFERENCES Dim_Title(tconst) ON DELETE CASCADE,
    INDEX idx_parent (parentTconst),
    INDEX idx_season (parentTconst, seasonNumber),
    INDEX idx_episode (parentTconst, seasonNumber, episodeNumber)
);

-- ============================================================
-- BRIDGE TABLES
-- ============================================================

-- Bridge: Title-Genre (many-to-many)
-- Handles the genres array from title.basics
-- For Reports 1, 2, 3, 4, 5
CREATE TABLE Bridge_Title_Genre (
    tconst VARCHAR(20),
    genreKey INT,
    PRIMARY KEY (tconst, genreKey),
    FOREIGN KEY (tconst) REFERENCES Dim_Title(tconst) ON DELETE CASCADE,
    FOREIGN KEY (genreKey) REFERENCES Dim_Genre(genreKey) ON DELETE CASCADE,
    INDEX idx_genre (genreKey),
    INDEX idx_genre_title (genreKey, tconst)
);

-- Bridge: Title-Person Crew
-- Handles directors/writers from title.crew and all cast/crew from title.principals
-- For Report 3 (Person Performance Analysis)
CREATE TABLE Bridge_Title_Person (
    tconst VARCHAR(20),
    nconst VARCHAR(20),
    category VARCHAR(100),
    PRIMARY KEY (tconst, nconst, category),
    FOREIGN KEY (tconst) REFERENCES Dim_Title(tconst) ON DELETE CASCADE,
    FOREIGN KEY (nconst) REFERENCES Dim_Person(nconst) ON DELETE CASCADE,
    INDEX idx_person (nconst),
    INDEX idx_category (category),
    INDEX idx_person_category (nconst, category),
    INDEX idx_category_person (category, nconst)
);

-- ============================================================
-- FACT TABLE
-- ============================================================

-- Fact Table: Title Performance
CREATE TABLE Fact_Title_Performance (
    tconst VARCHAR(20) PRIMARY KEY,
    timeKey INT,
    startYear INT, 
    averageRating DECIMAL(3,1),
    numVotes INT,
    FOREIGN KEY (tconst) REFERENCES Dim_Title(tconst) ON DELETE CASCADE,
    FOREIGN KEY (timeKey) REFERENCES Dim_Time(timeKey) ON DELETE SET NULL,
    
    -- Report 1: Genre-Rating Analysis (rating bins, time filtering)
    INDEX idx_rating_time (averageRating, timeKey),
    INDEX idx_rating_votes (averageRating, numVotes),
    
    -- Report 2, 4, 5: Time-based aggregations
    INDEX idx_time (timeKey),
    INDEX idx_startYear (startYear),
    
    -- Report 3, 4, 5: Rating filters
    INDEX idx_rating (averageRating),
    
    -- Report 4, 5: Vote-based analysis and filtering
    INDEX idx_votes (numVotes),
    INDEX idx_votes_rating (numVotes, averageRating),
    
    -- Combined filtering patterns
    INDEX idx_time_rating_votes (timeKey, averageRating, numVotes)
);

-- ============================================================
-- INDEXES RATIONALE
-- ============================================================
/*
Report 1 (Genre-Rating Association):
- Bridge_Title_Genre: idx_genre_title for genre filtering
- Fact_Title_Performance: idx_rating_time for rating bins + time grouping
- Dim_Time: idx_decade, idx_era for time granularity

Report 2 (Runtime Trends):
- Dim_Title: idx_titleType_year, idx_runtime for title type + time + runtime
- Fact_Title_Performance: idx_time, idx_rating for time grouping + rating filters
- Bridge_Title_Genre: idx_genre for optional genre grouping

Report 3 (Person Performance):
- Bridge_Title_Person: idx_category_person for job category filtering
- Fact_Title_Performance: idx_rating_votes for performance metrics
- Bridge_Title_Genre: idx_genre for optional genre grouping

Report 4 (Genre Engagement):
- Bridge_Title_Genre: idx_genre_title for genre grouping
- Fact_Title_Performance: idx_votes_rating for vote aggregation + filters
- Dim_Time: idx_decade, idx_era for time grouping

Report 5 (TV Series Engagement):
- Dim_Episode: idx_parent, idx_season for hierarchy navigation
- Fact_Title_Performance: idx_votes for engagement metrics
- Bridge_Title_Genre: idx_genre for genre filtering
*/