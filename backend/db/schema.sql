-- ============================================================
-- IMDb STAR SCHEMA
-- ============================================================
DROP DATABASE IF EXISTS imdb_star_schema;
CREATE DATABASE imdb_star_schema;
USE imdb_star_schema;

-- ============================================================
-- DIMENSIONS
-- ============================================================

-- TIME DIMENSION
CREATE TABLE dim_time (
  time_key INT AUTO_INCREMENT PRIMARY KEY,
  year YEAR UNIQUE NOT NULL,
  decade INT,
  century INT,
  INDEX idx_year (year),
  INDEX idx_decade (decade)
);

-- TITLE TYPE DIMENSION
CREATE TABLE dim_title_type (
  type_key INT AUTO_INCREMENT PRIMARY KEY,
  titleType VARCHAR(50) UNIQUE NOT NULL
);

-- GENRE DIMENSION
CREATE TABLE dim_genre (
  genre_key INT AUTO_INCREMENT PRIMARY KEY,
  genre_name VARCHAR(50) UNIQUE NOT NULL
);

-- PERSON DIMENSION
CREATE TABLE dim_person (
  person_key INT AUTO_INCREMENT PRIMARY KEY,
  nconst VARCHAR(20) UNIQUE NOT NULL,
  primaryName VARCHAR(255),
  birthYear YEAR,
  deathYear YEAR,
  primaryProfession VARCHAR(255),
  knownForTitles TEXT,
  INDEX idx_primaryName (primaryName(100)),
  INDEX idx_birthYear (birthYear)
);

-- TITLE DIMENSION
CREATE TABLE dim_title (
  title_key INT AUTO_INCREMENT PRIMARY KEY,
  tconst VARCHAR(20) UNIQUE NOT NULL,
  primaryTitle VARCHAR(255),
  originalTitle VARCHAR(255),
  isAdult TINYINT(1),
  startYear YEAR,
  endYear YEAR,
  runtimeMinutes INT,
  type_key INT,
  INDEX idx_tconst (tconst),
  INDEX idx_primaryTitle (primaryTitle(100)),
  INDEX idx_startYear (startYear),
  CONSTRAINT fk_dim_title_type FOREIGN KEY (type_key)
    REFERENCES dim_title_type(type_key)
    ON DELETE SET NULL
);

-- REGION / AKAS DIMENSION
-- Note: region can be NULL in practice (represents worldwide/unspecified releases)
-- Even though not documented, IMDb data contains \N for region in some cases
CREATE TABLE dim_region (
  region_key INT AUTO_INCREMENT PRIMARY KEY,
  region VARCHAR(10) COMMENT 'NULL = worldwide/unspecified region',
  language VARCHAR(10),
  types VARCHAR(100),
  attributes VARCHAR(255),
  INDEX idx_region (region),
  -- unique constraint to prevent duplicate region combinations
  UNIQUE KEY unique_region_combo (region, language, types, attributes)
);

-- EPISODE DIMENSION
CREATE TABLE dim_episode (
  episode_key INT AUTO_INCREMENT PRIMARY KEY,
  tconst VARCHAR(20) UNIQUE NOT NULL,
  parentTconst VARCHAR(20),
  seasonNumber INT,
  episodeNumber INT,
  CONSTRAINT fk_episode_parent FOREIGN KEY (parentTconst)
    REFERENCES dim_title(tconst)
    ON DELETE SET NULL
);

-- ============================================================
-- FACT TABLE
-- ============================================================

CREATE TABLE fact_title_ratings (
  rating_id BIGINT AUTO_INCREMENT PRIMARY KEY,
  tconst VARCHAR(20) NOT NULL,
  averageRating DECIMAL(3,1),
  numVotes INT,
  title_key INT,
  time_key INT,
  type_key INT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_tconst (tconst),
  INDEX idx_title (title_key),
  INDEX idx_time (time_key),
  INDEX idx_type (type_key),
  INDEX idx_rating (averageRating),
  INDEX idx_votes (numVotes),
  CONSTRAINT fk_fact_title FOREIGN KEY (title_key)
    REFERENCES dim_title(title_key)
    ON DELETE CASCADE,
  CONSTRAINT fk_fact_time FOREIGN KEY (time_key)
    REFERENCES dim_time(time_key)
    ON DELETE SET NULL,
  CONSTRAINT fk_fact_type FOREIGN KEY (type_key)
    REFERENCES dim_title_type(type_key)
    ON DELETE SET NULL
);

-- ============================================================
-- BRIDGE TABLES
-- ============================================================

-- TITLE ↔ GENRE
CREATE TABLE bridge_title_genre (
  tconst VARCHAR(20) NOT NULL,
  genre_key INT NOT NULL,
  PRIMARY KEY (tconst, genre_key),
  FOREIGN KEY (tconst) REFERENCES dim_title(tconst)
    ON DELETE CASCADE,
  FOREIGN KEY (genre_key) REFERENCES dim_genre(genre_key)
    ON DELETE CASCADE
);

-- TITLE ↔ DIRECTOR
CREATE TABLE bridge_title_director (
  tconst VARCHAR(20) NOT NULL,
  person_key INT NOT NULL,
  PRIMARY KEY (tconst, person_key),
  FOREIGN KEY (tconst) REFERENCES dim_title(tconst)
    ON DELETE CASCADE,
  FOREIGN KEY (person_key) REFERENCES dim_person(person_key)
    ON DELETE CASCADE
);

-- TITLE ↔ WRITER
CREATE TABLE bridge_title_writer (
  tconst VARCHAR(20) NOT NULL,
  person_key INT NOT NULL,
  PRIMARY KEY (tconst, person_key),
  FOREIGN KEY (tconst) REFERENCES dim_title(tconst)
    ON DELETE CASCADE,
  FOREIGN KEY (person_key) REFERENCES dim_person(person_key)
    ON DELETE CASCADE
);

-- TITLE ↔ PRINCIPAL CAST/CREW
CREATE TABLE bridge_title_principal (
  tconst VARCHAR(20) NOT NULL,
  person_key INT NOT NULL,
  category VARCHAR(100),
  job VARCHAR(255),
  characters TEXT,
  PRIMARY KEY (tconst, person_key),
  FOREIGN KEY (tconst) REFERENCES dim_title(tconst)
    ON DELETE CASCADE,
  FOREIGN KEY (person_key) REFERENCES dim_person(person_key)
    ON DELETE CASCADE
);

-- TITLE ↔ REGION / AKAS
CREATE TABLE bridge_title_region (
  tconst VARCHAR(20) NOT NULL,
  region_key INT NULL,
  title VARCHAR(255),
  isOriginalTitle TINYINT(1),
  PRIMARY KEY (tconst, region_key),
  FOREIGN KEY (tconst) REFERENCES dim_title(tconst)
    ON DELETE CASCADE,
  FOREIGN KEY (region_key) REFERENCES dim_region(region_key)
    ON DELETE CASCADE
);

-- ============================================================
-- INDEXES FOR OLAP PERFORMANCE
-- ============================================================

CREATE INDEX idx_fact_composite ON fact_title_ratings (time_key, type_key, averageRating);
CREATE INDEX idx_title_year ON dim_title (startYear, tconst);
CREATE INDEX idx_genre_lookup ON dim_genre (genre_name);