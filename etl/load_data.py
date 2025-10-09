import pandas as pd
import mysql.connector
from mysql.connector import Error
from datetime import datetime
import sys
import argparse
import time
from collections import defaultdict

class IMDBDataLoader:
    def __init__(self, db_config, data_path, truncate=True, disable_fk=True):
        self.db_config = db_config
        self.data_path = data_path
        self.conn = None
        self.cursor = None
        self.truncate = truncate
        self.disable_fk = disable_fk
        
        # Logging counters
        self.skip_stats = defaultdict(lambda: defaultdict(int))
    
    # =====================================================
    # CONNECTION MANAGEMENT
    # =====================================================
    def connect(self):
        """Establish database connection"""
        try:
            self.conn = mysql.connector.connect(**self.db_config)
            self.cursor = self.conn.cursor()
            print("✓ Connected to database")
        except Error as e:
            print(f"✗ Database connection failed: {e}")
            sys.exit(1)
    
    def close(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print("✓ Database connection closed")
    
    def disable_foreign_keys(self):
        """Temporarily disable FK checks for faster bulk loading"""
        try:
            self.cursor.execute("SET FOREIGN_KEY_CHECKS=0;")
            print("✓ Foreign key checks disabled")
        except Error as e:
            print(f"⚠ Could not disable FK checks: {e}")
    
    def enable_foreign_keys(self):
        """Re-enable FK checks after loading"""
        try:
            self.cursor.execute("SET FOREIGN_KEY_CHECKS=1;")
            print("✓ Foreign key checks re-enabled")
        except Error as e:
            print(f"⚠ Could not re-enable FK checks: {e}")
    
    # =====================================================
    # UTILITIES
    # =====================================================
    def truncate_table(self, table):
        if not self.truncate:
            return
        try:
            if self.disable_fk:
                self.cursor.execute(f"TRUNCATE TABLE {table}")
            else:
                self.cursor.execute(f"DELETE FROM {table}")
            print(f"  ↻ Cleared {table}")
        except Error as e:
            print(f"  ⚠ Could not clear {table}: {e}")
    
    def read_tsv(self, filename, nrows=None):
        """Read TSV file with proper null handling"""
        print(f"  Reading {filename}...")
        try:
            df = pd.read_csv(
                f'{self.data_path}{filename}',
                sep='\t',
                na_values=['\\N'],
                keep_default_na=True,
                low_memory=False,
                nrows=nrows
            )
            print(f"  ✓ Loaded {len(df):,} rows from {filename}")
            return df
        except Exception as e:
            print(f"  ✗ Error reading {filename}: {e}")
            return None
    
    def bulk_insert(self, table, columns, data, batch_size=50000):
        """Efficient bulk insert with batching"""
        if not data:
            print(f"  ⚠ No data to insert into {table}")
            return
        
        placeholders = ', '.join(['%s'] * len(columns))
        query = f"INSERT IGNORE INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
        
        total = len(data)
        inserted = 0
        start = time.time()
        
        try:
            for i in range(0, total, batch_size):
                batch = data[i:i + batch_size]
                self.cursor.executemany(query, batch)
                self.conn.commit()
                inserted += len(batch)
                print(f"  Progress: {inserted:,}/{total:,} rows", end='\r')
            
            elapsed = time.time() - start
            print(f"\n  ✓ Inserted {inserted:,} rows into {table} ({elapsed:.2f}s)")
        except Error as e:
            print(f"\n  ✗ Error inserting into {table}: {e}")
            self.conn.rollback()
    
    def timed(self, label, func, *args, **kwargs):
        """Helper to time each ETL stage"""
        print(f"\n[{label}]")
        start = time.time()
        result = func(*args, **kwargs)
        elapsed = time.time() - start
        print(f"✓ {label} completed in {elapsed:.2f}s")
        return result
    
    def print_skip_summary(self, table):
        """Print summary of skipped records for a table"""
        if table in self.skip_stats and self.skip_stats[table]:
            print(f"\n  📊 Skip Summary for {table}:")
            for reason, count in self.skip_stats[table].items():
                print(f"     • {reason}: {count:,} records")
            total_skipped = sum(self.skip_stats[table].values())
            print(f"     • Total skipped: {total_skipped:,} records")
    
    # =====================================================
    # DIMENSIONS
    # =====================================================
    def load_dim_time(self):
        table = "dim_time"
        self.truncate_table(table)
        
        years = list(range(1874, 2041))
        time_data = [(year, (year // 10) * 10, (year // 100) + 1) for year in years]
        
        self.bulk_insert(table, ['year', 'decade', 'century'], time_data)
    
    def load_dim_title_type(self, df_basics):
        """Dynamically populate dim_title_type from dataset"""
        table = "dim_title_type"
        self.truncate_table(table)
        
        if df_basics is None:
            print("  ⚠ No data to extract title types")
            return
        
        # Extract unique title types
        unique_types = df_basics['titleType'].dropna().unique()
        
        self.bulk_insert(table, ['titleType'], [(title_type,) for title_type in unique_types])
        print(f"  ✓ Loaded {len(unique_types)} unique title types")
    
    def load_dim_genre(self, df_basics):
        """Dynamically populate dim_genre from dataset"""
        table = "dim_genre"
        self.truncate_table(table)
        
        if df_basics is None:
            print("  ⚠ No data to extract genres")
            return
        
        # Extract all unique genres from the genres column
        all_genres = set()
        for genres_str in df_basics['genres'].dropna():
            if genres_str != '\\N':
                genres = [g.strip() for g in genres_str.split(',')]
                all_genres.update(genres)
        
        genre_data = [(genre,) for genre in sorted(all_genres)]
        
        self.bulk_insert(table, ['genre_name'], genre_data)
        print(f"  ✓ Loaded {len(all_genres)} unique genres")
    
    def load_dim_person(self, nrows=None):
        table = "dim_person"
        self.truncate_table(table)
        
        df = self.read_tsv('name.basics.tsv.gz', nrows=nrows)
        if df is None:
            return None
        
        df['birthYear'] = pd.to_numeric(df['birthYear'], errors='coerce')
        df['deathYear'] = pd.to_numeric(df['deathYear'], errors='coerce')
        
        person_data = []
        for _, row in df.iterrows():
            person_data.append((
                row['nconst'],
                row['primaryName'][:255] if pd.notna(row['primaryName']) else None,
                int(row['birthYear']) if pd.notna(row['birthYear']) else None,
                int(row['deathYear']) if pd.notna(row['deathYear']) else None,
                row['primaryProfession'][:255] if pd.notna(row['primaryProfession']) else None,
                row['knownForTitles'] if pd.notna(row['knownForTitles']) else None
            ))
        
        self.bulk_insert(
            table,
            ['nconst', 'primaryName', 'birthYear', 'deathYear', 'primaryProfession', 'knownForTitles'],
            person_data
        )
        
        return df
    
    def load_dim_title(self, df_basics=None, nrows=None):
        table = "dim_title"
        self.truncate_table(table)
        
        if df_basics is None:
            df_basics = self.read_tsv('title.basics.tsv.gz', nrows=nrows)
        if df_basics is None:
            return None
        
        df_basics['isAdult'] = df_basics['isAdult'].fillna(0).astype(int)
        df_basics['startYear'] = pd.to_numeric(df_basics['startYear'], errors='coerce')
        df_basics['endYear'] = pd.to_numeric(df_basics['endYear'], errors='coerce')
        df_basics['runtimeMinutes'] = pd.to_numeric(df_basics['runtimeMinutes'], errors='coerce')
        
        # Get type mappings
        self.cursor.execute("SELECT titleType, type_key FROM dim_title_type")
        type_map = {titleType: type_key for titleType, type_key in self.cursor.fetchall()}
        
        title_data = []
        titles_skipped = 0
        
        for _, row in df_basics.iterrows():
            if pd.isna(row['tconst']) or row['tconst'] == '\\N':
                self.skip_stats['dim_title']['missing_tconst'] += 1
                titles_skipped += 1
                continue
                
            type_key = type_map.get(row['titleType'])
            if type_key is None and pd.notna(row['titleType']):
                self.skip_stats['dim_title']['unknown_titleType'] += 1
            
            start_year = int(row['startYear']) if pd.notna(row['startYear']) else None
            end_year = int(row['endYear']) if pd.notna(row['endYear']) else None
            runtime = int(row['runtimeMinutes']) if pd.notna(row['runtimeMinutes']) else None
            
            title_data.append((
                row['tconst'],
                row['primaryTitle'][:255] if pd.notna(row['primaryTitle']) else None,
                row['originalTitle'][:255] if pd.notna(row['originalTitle']) else None,
                int(row['isAdult']),
                start_year,
                end_year,
                runtime,
                type_key  # Can be NULL
            ))
        
        self.bulk_insert(
            table,
            ['tconst', 'primaryTitle', 'originalTitle', 'isAdult', 'startYear', 'endYear', 'runtimeMinutes', 'type_key'],
            title_data
        )
        
        print(f"  ✓ Loaded {len(title_data):,} titles, skipped {titles_skipped:,} due to missing tconst")
        self.print_skip_summary('dim_title')
        return df_basics
    
    def load_dim_episode(self, nrows=None):
        table = "dim_episode"
        self.truncate_table(table)
        
        df = self.read_tsv('title.episode.tsv.gz', nrows=nrows)
        if df is None:
            return
        
        # Get valid tconst and parentTconst values from dim_title
        self.cursor.execute("SELECT tconst FROM dim_title")
        valid_tconsts = {row[0] for row in self.cursor.fetchall()}
        
        episode_data = []
        for _, row in df.iterrows():
            # Skip if episode tconst not in dim_title
            if row['tconst'] not in valid_tconsts:
                self.skip_stats['dim_episode']['tconst_not_found'] += 1
                continue
            
            # Check if parentTconst exists (if not NULL)
            parent_tconst = row['parentTconst'] if pd.notna(row['parentTconst']) else None
            if parent_tconst and parent_tconst not in valid_tconsts:
                self.skip_stats['dim_episode']['parent_tconst_not_found'] += 1
                parent_tconst = None  # Set to NULL instead of skipping entire record
            
            episode_data.append((
                row['tconst'],
                parent_tconst,
                int(row['seasonNumber']) if pd.notna(row['seasonNumber']) else None,
                int(row['episodeNumber']) if pd.notna(row['episodeNumber']) else None
            ))
        
        self.bulk_insert(table, ['tconst', 'parentTconst', 'seasonNumber', 'episodeNumber'], episode_data)
        self.print_skip_summary('dim_episode')
    
    def load_dim_akas(self, nrows=None):
        """Load dim_akas from title.akas.tsv"""
        table = "dim_akas"
        self.truncate_table(table)
        
        df = self.read_tsv('title.akas.tsv.gz', nrows=nrows)
        if df is None:
            return None
        
        # Get valid tconst values from dim_title
        self.cursor.execute("SELECT tconst FROM dim_title")
        valid_tconsts = {row[0] for row in self.cursor.fetchall()}
        
        akas_data = []
        for _, row in df.iterrows():
            # Skip if tconst not in dim_title (FK constraint)
            if row['titleId'] not in valid_tconsts:
                self.skip_stats['dim_akas']['tconst_not_found'] += 1
                continue
            
            akas_data.append((
                row['titleId'],
                row['title'][:255] if pd.notna(row['title']) else None,
                row['region'][:10] if pd.notna(row['region']) else None,
                row['language'][:10] if pd.notna(row['language']) else None,
                row['types'][:100] if pd.notna(row['types']) else None,
                row['attributes'][:255] if pd.notna(row['attributes']) else None,
                int(row['isOriginalTitle']) if pd.notna(row['isOriginalTitle']) else 0
            ))
        
        self.bulk_insert(
            table,
            ['tconst', 'title', 'region', 'language', 'types', 'attributes', 'isOriginalTitle'],
            akas_data
        )
        
        self.print_skip_summary('dim_akas')
        return df
    
    # ========================================
    # BRIDGES
    # ========================================
    
    def load_bridge_title_genre(self, df_basics):
        """Load title-genre bridge from title.basics genres column"""
        table = "bridge_title_genre"
        self.truncate_table(table)
        
        if df_basics is None:
            print("  ⚠ Skipping - no title data available")
            return
        
        # Get genre_key mappings
        self.cursor.execute("SELECT genre_name, genre_key FROM dim_genre")
        genre_map = {name: key for name, key in self.cursor.fetchall()}
        
        # Get valid tconst values
        self.cursor.execute("SELECT tconst FROM dim_title")
        valid_tconsts = {row[0] for row in self.cursor.fetchall()}
        
        bridge_data = []
        for _, row in df_basics.iterrows():
            # Skip if tconst not in dim_title (FK constraint)
            if row['tconst'] not in valid_tconsts:
                self.skip_stats['bridge_title_genre']['tconst_not_found'] += 1
                continue
            
            if pd.notna(row['genres']) and row['genres'] != '\\N':
                genres = row['genres'].split(',')
                for genre in genres:
                    genre = genre.strip()
                    if genre in genre_map:
                        bridge_data.append((row['tconst'], genre_map[genre]))
                    else:
                        self.skip_stats['bridge_title_genre']['unknown_genre'] += 1
            else:
                self.skip_stats['bridge_title_genre']['no_genres'] += 1
        
        self.bulk_insert('bridge_title_genre', ['tconst', 'genre_key'], bridge_data)
        self.print_skip_summary('bridge_title_genre')
    
    def load_bridge_title_director(self, nrows=None):
        """Load director bridge from title.crew.tsv"""
        table = "bridge_title_director"
        self.truncate_table(table)
        
        df = self.read_tsv('title.crew.tsv.gz', nrows=nrows)
        if df is None:
            return
        
        # Get person_key mapping
        self.cursor.execute("SELECT nconst, person_key FROM dim_person")
        person_map = {nconst: person_key for nconst, person_key in self.cursor.fetchall()}
        
        # Get valid tconst values
        self.cursor.execute("SELECT tconst FROM dim_title")
        valid_tconsts = {row[0] for row in self.cursor.fetchall()}
        
        bridge_data = []
        for _, row in df.iterrows():
            # Skip if tconst not in dim_title (FK constraint)
            if row['tconst'] not in valid_tconsts:
                self.skip_stats['bridge_title_director']['tconst_not_found'] += 1
                continue
            
            if pd.notna(row['directors']) and row['directors'] != '\\N':
                directors = row['directors'].split(',')
                for nconst in directors:
                    nconst = nconst.strip()
                    person_key = person_map.get(nconst)
                    if person_key:
                        bridge_data.append((row['tconst'], person_key))
                    else:
                        self.skip_stats['bridge_title_director']['unknown_person'] += 1
            else:
                self.skip_stats['bridge_title_director']['no_directors'] += 1
        
        self.bulk_insert('bridge_title_director', ['tconst', 'person_key'], bridge_data)
        self.print_skip_summary('bridge_title_director')
    
    def load_bridge_title_writer(self, nrows=None):
        """Load writer bridge from title.crew.tsv"""
        table = "bridge_title_writer"
        self.truncate_table(table)
        
        df = self.read_tsv('title.crew.tsv.gz', nrows=nrows)
        if df is None:
            return
        
        # Get person_key mapping
        self.cursor.execute("SELECT nconst, person_key FROM dim_person")
        person_map = {nconst: person_key for nconst, person_key in self.cursor.fetchall()}
        
        # Get valid tconst values
        self.cursor.execute("SELECT tconst FROM dim_title")
        valid_tconsts = {row[0] for row in self.cursor.fetchall()}
        
        bridge_data = []
        for _, row in df.iterrows():
            # Skip if tconst not in dim_title (FK constraint)
            if row['tconst'] not in valid_tconsts:
                self.skip_stats['bridge_title_writer']['tconst_not_found'] += 1
                continue
            
            if pd.notna(row['writers']) and row['writers'] != '\\N':
                writers = row['writers'].split(',')
                for nconst in writers:
                    nconst = nconst.strip()
                    person_key = person_map.get(nconst)
                    if person_key:
                        bridge_data.append((row['tconst'], person_key))
                    else:
                        self.skip_stats['bridge_title_writer']['unknown_person'] += 1
            else:
                self.skip_stats['bridge_title_writer']['no_writers'] += 1
        
        self.bulk_insert('bridge_title_writer', ['tconst', 'person_key'], bridge_data)
        self.print_skip_summary('bridge_title_writer')
    
    def load_bridge_title_principal(self, nrows=None):
        """Load principal cast/crew bridge from title.principals.tsv"""
        table = "bridge_title_principal"
        self.truncate_table(table)
        
        df = self.read_tsv('title.principals.tsv.gz', nrows=nrows)
        if df is None:
            return
        
        # Get person_key mapping
        self.cursor.execute("SELECT nconst, person_key FROM dim_person")
        person_map = {nconst: person_key for nconst, person_key in self.cursor.fetchall()}
        
        # Get valid tconst values
        self.cursor.execute("SELECT tconst FROM dim_title")
        valid_tconsts = {row[0] for row in self.cursor.fetchall()}
        
        bridge_data = []
        for _, row in df.iterrows():
            # Skip if tconst not in dim_title (FK constraint)
            if row['tconst'] not in valid_tconsts:
                self.skip_stats['bridge_title_principal']['tconst_not_found'] += 1
                continue
            
            person_key = person_map.get(row['nconst'])
            if person_key:
                bridge_data.append((
                    row['tconst'],
                    person_key,
                    row['category'][:100] if pd.notna(row['category']) else None,
                    row['job'][:255] if pd.notna(row['job']) else None,
                    row['characters'] if pd.notna(row['characters']) else None
                ))
            else:
                self.skip_stats['bridge_title_principal']['unknown_person'] += 1
        
        self.bulk_insert(
            'bridge_title_principal',
            ['tconst', 'person_key', 'category', 'job', 'characters'],
            bridge_data
        )
        self.print_skip_summary('bridge_title_principal')
    
    # ========================================
    # FACT TABLE 
    # ========================================
    
    def load_fact_title_ratings(self, nrows=None):
        """Load fact table by joining ratings with title dimensions"""
        table = "fact_title_ratings"
        self.truncate_table(table)
        
        # Read ratings
        df_ratings = self.read_tsv('title.ratings.tsv.gz', nrows=nrows)
        if df_ratings is None:
            return
        
        # Get lookup mappings from database
        print("  Building lookup tables...")
        
        # Get title_key, startYear and type_key mapping (allow NULL startYear)
        self.cursor.execute("SELECT tconst, title_key, startYear, type_key FROM dim_title")
        title_map = {tconst: (title_key, year, type_key) for tconst, title_key, year, type_key in self.cursor.fetchall()}
        
        # Get time_key mapping
        self.cursor.execute("SELECT year, time_key FROM dim_time")
        time_map = {year: time_key for year, time_key in self.cursor.fetchall()}
        
        # Prepare fact data
        fact_data = []
        
        for _, row in df_ratings.iterrows():
            tconst = row['tconst']
            
            # Skip if title not in dimension (FK constraint)
            if tconst not in title_map:
                self.skip_stats['fact_title_ratings']['title_not_found'] += 1
                continue
            
            title_key, start_year, type_key = title_map[tconst]
            
            # If startYear is NULL or not in time dimension, set time_key to NULL
            time_key = None
            if pd.notna(start_year) and start_year in time_map:
                time_key = time_map[start_year]
            elif pd.notna(start_year):
                self.skip_stats['fact_title_ratings']['year_not_in_dim_time'] += 1
            
            fact_data.append((
                tconst,
                float(row['averageRating']) if pd.notna(row['averageRating']) else None,
                int(row['numVotes']) if pd.notna(row['numVotes']) else None,
                title_key,
                time_key,
                type_key
            ))
        
        self.bulk_insert(
            'fact_title_ratings',
            ['tconst', 'averageRating', 'numVotes', 'title_key', 'time_key', 'type_key'],
            fact_data
        )
        self.print_skip_summary('fact_title_ratings')
    
    # =====================================================
    # MAIN ETL 
    # =====================================================
    
    def run_etl(self, test_mode=False):
        start_time = datetime.now()
        print("=" * 60)
        print("IMDB DATA WAREHOUSE ETL")
        print("=" * 60)
        
        nrows = 10000 if test_mode else None
        if test_mode:
            print("⚠ TEST MODE: Loading only 10,000 rows per file\n")
        
        try:
            self.connect()
            
            if self.disable_fk:
                self.disable_foreign_keys()
            
            # =====================================================
            # PHASE 1: INDEPENDENT DIMENSIONS (no dependencies)
            # =====================================================
            
            # Step 1: Load static dimensions first
            self.timed("1/10 dim_time", self.load_dim_time)
            
            # Step 2: Load people (independent of titles)
            df_person = self.timed("2/10 dim_person", self.load_dim_person, nrows)
            
            # =====================================================
            # PHASE 2: TITLE-RELATED DIMENSIONS (complex dependencies)
            # =====================================================
            
            # Step 3: Read title.basics ONCE for metadata extraction
            print("\n[3/10] Reading title.basics for metadata extraction...")
            df_basics = self.read_tsv('title.basics.tsv.gz', nrows)
            if df_basics is None:
                raise Exception("Failed to read title.basics - cannot continue")
            
            # Step 4: Extract title types FROM the basics data
            self.timed("4/10 dim_title_type (dynamic)", self.load_dim_title_type, df_basics)
            
            # Step 5: Extract genres FROM the basics data  
            self.timed("5/10 dim_genre (dynamic)", self.load_dim_genre, df_basics)
            
            # Step 6: Load titles (now that types exist)
            df_basics = self.timed("6/10 dim_title", self.load_dim_title, df_basics, nrows)
            
            # =====================================================
            # PHASE 3: TITLE-DEPENDENT DIMENSIONS (require dim_title)
            # =====================================================
            
            # Step 7: Load episodes (depends on dim_title)
            self.timed("7/10 dim_episode", self.load_dim_episode, nrows)
            
            # Step 8: Load AKAs (depends on dim_title)
            self.timed("8/10 dim_akas", self.load_dim_akas, nrows)
            
            # =====================================================
            # PHASE 4: BRIDGE TABLES (require multiple dimensions)
            # =====================================================
            
            print("\n[9/10] Loading bridge tables...")
            
            # Bridge 9: Title-Genre (needs dim_title + dim_genre)
            self.load_bridge_title_genre(df_basics)
            
            # Bridge 9: Title-Director (needs dim_title + dim_person)
            self.load_bridge_title_director(nrows)
            
            # Bridge 9: Title-Writer (needs dim_title + dim_person)  
            self.load_bridge_title_writer(nrows)
            
            # Bridge 9: Title-Principal (needs dim_title + dim_person)
            self.load_bridge_title_principal(nrows)
            
            # =====================================================
            # PHASE 5: FACT TABLE (requires all dimensions)
            # =====================================================
            
            # Step 10: Fact table (needs dim_title, dim_time, dim_title_type)
            self.timed("10/10 fact_title_ratings", self.load_fact_title_ratings, nrows)
            
            elapsed = datetime.now() - start_time
            print("\n" + "=" * 60)
            print(f"ETL COMPLETED SUCCESSFULLY in {elapsed}")
            print("=" * 60)
            
        except Exception as e:
            print(f"\n✗ ETL FAILED: {e}")
            import traceback
            traceback.print_exc()
        finally:
            if self.disable_fk:
                self.enable_foreign_keys()
            self.close()

# =====================================================
# MAIN EXECUTION (ARGPARSE)
# =====================================================
if __name__ == "__main__":
    from config import DB_CONFIG, DATA_PATH
    
    parser = argparse.ArgumentParser(description="IMDb Data Warehouse ETL Loader")
    parser.add_argument("--test", action="store_true", help="Run ETL in test mode (10,000 rows per file)")
    parser.add_argument("--truncate", action="store_true", help="Clear tables before load (full reload)")
    parser.add_argument("--check-fk", action="store_true", help="Enable foreign key checks during load")
    
    args = parser.parse_args()
    
    loader = IMDBDataLoader(
        DB_CONFIG,
        DATA_PATH,
        truncate=args.truncate,
        disable_fk=not args.check_fk
    )
    
    loader.run_etl(test_mode=args.test)