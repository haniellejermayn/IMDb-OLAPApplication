-- ============================================================
-- IMDb STAR SCHEMA
-- ============================================================
DROP DATABASE IF EXISTS imdb_star_schema;
CREATE DATABASE imdb_star_schema;
USE imdb_star_schema;

-- Dimension: Time
-- Hierarchy: year -> decade -> era
CREATE TABLE Dim_Time (
    timeKey INT PRIMARY KEY AUTO_INCREMENT,
    year INT NOT NULL,
    decade VARCHAR(10),
    era VARCHAR(50),
    UNIQUE(year)
);

-- Dimension: Title
-- Core title information
CREATE TABLE Dim_Title (
    tconst VARCHAR(20) PRIMARY KEY,
    primaryTitle VARCHAR(500),
    originalTitle VARCHAR(500),
    titleType VARCHAR(50),
    startYear INT,
    endYear INT,
    runtimeMinutes INT,
    INDEX idx_titleType (titleType),
    INDEX idx_startYear (startYear),
    INDEX idx_titleType_year (titleType, startYear)
);

-- Dimension: Genre
CREATE TABLE Dim_Genre (
    genreKey INT PRIMARY KEY AUTO_INCREMENT,
    genreName VARCHAR(50) UNIQUE NOT NULL
);

-- Bridge: Title-Genre (many-to-many)
-- Handles the genres array from title.basics
CREATE TABLE Bridge_Title_Genre (
    tconst VARCHAR(20),
    genreKey INT,
    PRIMARY KEY (tconst, genreKey),
    FOREIGN KEY (tconst) REFERENCES Dim_Title(tconst) ON DELETE CASCADE,
    FOREIGN KEY (genreKey) REFERENCES Dim_Genre(genreKey) ON DELETE CASCADE,
    INDEX idx_genre (genreKey)
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

-- Dimension: Person
CREATE TABLE Dim_Person (
    nconst VARCHAR(20) PRIMARY KEY,
    primaryName VARCHAR(200),
    primaryProfession VARCHAR(200),
    INDEX idx_name (primaryName)
);

-- Bridge: Person Known For Titles
-- Handles the knownForTitles array from name.basics
CREATE TABLE Bridge_Person_KnownFor (
    nconst VARCHAR(20),
    tconst VARCHAR(20),
    PRIMARY KEY (nconst, tconst),
    FOREIGN KEY (nconst) REFERENCES Dim_Person(nconst) ON DELETE CASCADE,
    FOREIGN KEY (tconst) REFERENCES Dim_Title(tconst) ON DELETE CASCADE,
    INDEX idx_title (tconst)
);

-- Bridge: Title-Person Crew
-- Handles directors/writers from title.crew and all cast/crew from title.principals
CREATE TABLE Bridge_Title_Person (
    tconst VARCHAR(20),
    nconst VARCHAR(20),
    category VARCHAR(100),
    ordering INT,
    PRIMARY KEY (tconst, nconst, ordering),
    FOREIGN KEY (tconst) REFERENCES Dim_Title(tconst) ON DELETE CASCADE,
    FOREIGN KEY (nconst) REFERENCES Dim_Person(nconst) ON DELETE CASCADE,
    INDEX idx_person (nconst),
    INDEX idx_category (category),
    INDEX idx_person_category (nconst, category)
);

-- Fact Table: Title Performance
-- Central fact table with measures
CREATE TABLE Fact_Title_Performance (
    tconst VARCHAR(20) PRIMARY KEY,
    timeKey INT,
    averageRating DECIMAL(3,1),
    numVotes INT,
    FOREIGN KEY (tconst) REFERENCES Dim_Title(tconst) ON DELETE CASCADE,
    FOREIGN KEY (timeKey) REFERENCES Dim_Time(timeKey) ON DELETE SET NULL,
    INDEX idx_rating (averageRating),
    INDEX idx_votes (numVotes),
    INDEX idx_time (timeKey),
    INDEX idx_rating_votes (averageRating, numVotes)
);