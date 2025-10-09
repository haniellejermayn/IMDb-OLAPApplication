-- ============================================================
-- IMDb STAR SCHEMA
-- ============================================================
DROP DATABASE IF EXISTS imdb_star_schema;
CREATE DATABASE imdb_star_schema;
USE imdb_star_schema;

-- ============================================================
-- TIME DIMENSION
-- ============================================================

CREATE TABLE dim_date (
  date_key INT AUTO_INCREMENT PRIMARY KEY,
  year INT NOT NULL UNIQUE,
  decade INT NOT NULL,
  century INT NOT NULL,
  INDEX idx_year (year),
  INDEX idx_decade (decade)
);

-- ============================================================
-- CORE DIMENSIONS
-- ============================================================

CREATE TABLE dim_genre (
  genre_key INT AUTO_INCREMENT PRIMARY KEY,
  genre_name VARCHAR(50) UNIQUE NOT NULL,
  INDEX idx_genre_name (genre_name)
);

CREATE TABLE dim_person (
  person_key INT AUTO_INCREMENT PRIMARY KEY,
  nconst VARCHAR(20) UNIQUE NOT NULL,
  primaryName VARCHAR(255),
  birthYear INT,
  deathYear INT,
  INDEX idx_nconst (nconst)
);

CREATE TABLE dim_title (
  title_key INT AUTO_INCREMENT PRIMARY KEY,
  tconst VARCHAR(20) UNIQUE NOT NULL,
  primaryTitle VARCHAR(255),
  originalTitle VARCHAR(255),
  isAdult BOOLEAN,
  start_date_key INT,
  end_date_key INT,
  runtimeMinutes INT,
  titleType VARCHAR(50),
  INDEX idx_tconst (tconst),
  INDEX idx_title_type (titleType),
  INDEX idx_start_date (start_date_key),
  FOREIGN KEY (start_date_key) REFERENCES dim_date(date_key),
  FOREIGN KEY (end_date_key) REFERENCES dim_date(date_key)
);

-- ============================================================
-- FACT TABLE
-- ============================================================

CREATE TABLE fact_title_ratings (
  rating_id BIGINT AUTO_INCREMENT PRIMARY KEY,
  title_key INT NOT NULL UNIQUE,
  averageRating DECIMAL(3,1),
  numVotes INT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_title (title_key),
  INDEX idx_rating (averageRating),
  INDEX idx_votes (numVotes),
  FOREIGN KEY (title_key) REFERENCES dim_title(title_key) ON DELETE CASCADE
);

-- ============================================================
-- BRIDGE TABLES
-- ============================================================

CREATE TABLE bridge_title_genre (
  title_key INT NOT NULL,
  genre_key INT NOT NULL,
  PRIMARY KEY (title_key, genre_key),
  INDEX idx_title (title_key),
  INDEX idx_genre (genre_key),
  FOREIGN KEY (title_key) REFERENCES dim_title(title_key) ON DELETE CASCADE,
  FOREIGN KEY (genre_key) REFERENCES dim_genre(genre_key) ON DELETE CASCADE
);

CREATE TABLE bridge_title_crew (
  crew_id BIGINT AUTO_INCREMENT PRIMARY KEY,
  title_key INT NOT NULL,
  person_key INT NOT NULL,
  role_type VARCHAR(50) NOT NULL,
  role_detail VARCHAR(255),
  ordering INT,
  INDEX idx_title (title_key),
  INDEX idx_person (person_key),
  INDEX idx_role (role_type),
  INDEX idx_title_role (title_key, role_type),
  UNIQUE KEY uk_crew (title_key, person_key, role_type, ordering),
  FOREIGN KEY (title_key) REFERENCES dim_title(title_key) ON DELETE CASCADE,
  FOREIGN KEY (person_key) REFERENCES dim_person(person_key) ON DELETE CASCADE
);

-- ============================================================
-- OTHER TABLES
-- ============================================================

CREATE TABLE dim_episode (
  episode_key INT AUTO_INCREMENT PRIMARY KEY,
  title_key INT NOT NULL UNIQUE,
  parent_title_key INT,
  seasonNumber INT,
  episodeNumber INT,
  INDEX idx_title (title_key),
  INDEX idx_parent (parent_title_key),
  INDEX idx_season (seasonNumber),
  FOREIGN KEY (title_key) REFERENCES dim_title(title_key) ON DELETE CASCADE,
  FOREIGN KEY (parent_title_key) REFERENCES dim_title(title_key) ON DELETE SET NULL
);

CREATE TABLE dim_akas (
  aka_key INT AUTO_INCREMENT PRIMARY KEY,
  title_key INT NOT NULL,
  title VARCHAR(255),
  region VARCHAR(10),
  isOriginalTitle BOOLEAN,
  INDEX idx_title (title_key),
  INDEX idx_region (region),
  FOREIGN KEY (title_key) REFERENCES dim_title(title_key) ON DELETE CASCADE
);