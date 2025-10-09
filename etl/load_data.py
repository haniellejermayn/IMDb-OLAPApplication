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
        self.skip_stats = defaultdict(lambda: defaultdict(int))
    
    # =====================================================
    # UTILITIES
    # =====================================================
    
    def connect(self):
        try:
            self.conn = mysql.connector.connect(**self.db_config)
            self.cursor = self.conn.cursor()
            print("✓ Connected to database")
        except Error as e:
            print(f"✗ Database connection failed: {e}")
            sys.exit(1)
    
    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print("✓ Database connection closed")
    
    def disable_foreign_keys(self):
        try:
            self.cursor.execute("SET FOREIGN_KEY_CHECKS=0;")
            print("✓ Foreign key checks disabled")
        except Error as e:
            print(f"⚠ Could not disable FK checks: {e}")
    
    def enable_foreign_keys(self):
        try:
            self.cursor.execute("SET FOREIGN_KEY_CHECKS=1;")
            print("✓ Foreign key checks re-enabled")
        except Error as e:
            print(f"⚠ Could not re-enable FK checks: {e}")
    
    def truncate_table(self, table):
        if not self.truncate:
            return
        try:
            self.cursor.execute(f"TRUNCATE TABLE {table}")
            print(f"  ↻ Cleared {table}")
        except Error as e:
            print(f"  ⚠ Could not clear {table}: {e}")
    
    def read_tsv(self, filename, nrows=None):
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
            print(f"  ✓ Loaded {len(df):,} rows")
            return df
        except Exception as e:
            print(f"  ✗ Error: {e}")
            return None
    
    def bulk_insert(self, table, columns, data, batch_size=50000):
        if not data:
            print(f"  ⚠ No data to insert")
            return
        
        placeholders = ', '.join(['%s'] * len(columns))
        query = f"INSERT IGNORE INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
        
        total = len(data)
        start = time.time()
        
        try:
            for i in range(0, total, batch_size):
                batch = data[i:i + batch_size]
                self.cursor.executemany(query, batch)
                self.conn.commit()
                print(f"  Progress: {i + len(batch):,}/{total:,}", end='\r')
            
            print(f"\n  ✓ Inserted {total:,} rows ({time.time() - start:.2f}s)")
        except Error as e:
            print(f"\n  ✗ Error: {e}")
            self.conn.rollback()
    
    def timed(self, label, func, *args):
        print(f"\n[{label}]")
        start = time.time()
        result = func(*args)
        print(f"✓ Completed in {time.time() - start:.2f}s")
        return result
    
    # =====================================================
    # LOADERS
    # =====================================================
    
    def load_dim_date(self):
        self.truncate_table("dim_date")
        data = [(y, (y // 10) * 10, (y // 100) * 100) for y in range(1874, 2033)]
        self.bulk_insert("dim_date", ['year', 'decade', 'century'], data)
    
    def load_dim_genre(self, df_basics):
        self.truncate_table("dim_genre")
        genres = set()
        for g in df_basics['genres'].dropna():
            if str(g) not in ['\\N', 'nan']:
                genres.update([x.strip() for x in str(g).split(',') if x.strip()])
        self.bulk_insert("dim_genre", ['genre_name'], [(g,) for g in sorted(genres)])
    
    def load_dim_person(self, nrows):
        self.truncate_table("dim_person")
        
        df_names = self.read_tsv('name.basics.tsv.gz', nrows)
        if df_names is None:
            return
        
        all_nconsts = set(df_names['nconst'].dropna())
        
        # Add crew IDs
        df_crew = self.read_tsv('title.crew.tsv.gz', nrows)
        if df_crew is not None:
            for col in ['directors', 'writers']:
                for val in df_crew[col].dropna():
                    if str(val) not in ['\\N', 'nan']:
                        all_nconsts.update([n.strip() for n in str(val).split(',')])
        
        # Add principal IDs
        df_principals = self.read_tsv('title.principals.tsv.gz', nrows)
        if df_principals is not None:
            all_nconsts.update(df_principals['nconst'].dropna())
        
        print(f"  Found {len(all_nconsts):,} unique people")
        
        # Build person info map
        person_info = {}
        for _, row in df_names.iterrows():
            person_info[row['nconst']] = (
                row['primaryName'][:255] if pd.notna(row['primaryName']) else None,
                int(row['birthYear']) if pd.notna(row['birthYear']) else None,
                int(row['deathYear']) if pd.notna(row['deathYear']) else None
            )
        
        data = [(nc, *person_info.get(nc, (None, None, None))) for nc in all_nconsts]
        self.bulk_insert("dim_person", ['nconst', 'primaryName', 'birthYear', 'deathYear'], data)
    
    def load_dim_title(self, nrows):
        self.truncate_table("dim_title")
        
        df = self.read_tsv('title.basics.tsv.gz', nrows)
        if df is None:
            return None
        
        # Get date mapping
        self.cursor.execute("SELECT year, date_key FROM dim_date")
        date_map = dict(self.cursor.fetchall())
        
        data = []
        for _, row in df.iterrows():
            if pd.isna(row['tconst']):
                continue
            
            start_year = int(row['startYear']) if pd.notna(row['startYear']) else None
            end_year = int(row['endYear']) if pd.notna(row['endYear']) else None
            
            data.append((
                row['tconst'],
                row['primaryTitle'][:255] if pd.notna(row['primaryTitle']) else None,
                row['originalTitle'][:255] if pd.notna(row['originalTitle']) else None,
                int(row['isAdult']) if pd.notna(row['isAdult']) else 0,
                date_map.get(start_year),
                date_map.get(end_year),
                int(row['runtimeMinutes']) if pd.notna(row['runtimeMinutes']) else None,
                row['titleType'] if pd.notna(row['titleType']) else None
            ))
        
        self.bulk_insert(
            "dim_title",
            ['tconst', 'primaryTitle', 'originalTitle', 'isAdult', 
             'start_date_key', 'end_date_key', 'runtimeMinutes', 'titleType'],
            data
        )
        return df
    
    def load_bridge_title_genre(self, df_basics):
        self.truncate_table("bridge_title_genre")
        
        self.cursor.execute("SELECT tconst, title_key FROM dim_title")
        title_map = dict(self.cursor.fetchall())
        
        self.cursor.execute("SELECT genre_name, genre_key FROM dim_genre")
        genre_map = dict(self.cursor.fetchall())
        
        data = []
        for _, row in df_basics.iterrows():
            if row['tconst'] not in title_map:
                continue
            if pd.notna(row['genres']) and str(row['genres']) not in ['\\N', 'nan']:
                genres = [g.strip() for g in str(row['genres']).split(',')]
                for g in genres:
                    if g in genre_map:
                        data.append((title_map[row['tconst']], genre_map[g]))
        
        self.bulk_insert('bridge_title_genre', ['title_key', 'genre_key'], data)
    
    def load_bridge_title_crew(self, nrows):
        self.truncate_table("bridge_title_crew")
        
        self.cursor.execute("SELECT tconst, title_key FROM dim_title")
        title_map = dict(self.cursor.fetchall())
        
        self.cursor.execute("SELECT nconst, person_key FROM dim_person")
        person_map = dict(self.cursor.fetchall())
        
        data = []
        
        # Load crew
        df_crew = self.read_tsv('title.crew.tsv.gz', nrows)
        if df_crew is not None:
            for _, row in df_crew.iterrows():
                if row['tconst'] not in title_map:
                    continue
                tk = title_map[row['tconst']]
                
                for col, role in [('directors', 'director'), ('writers', 'writer')]:
                    if pd.notna(row[col]) and str(row[col]) not in ['\\N', 'nan']:
                        for nc in [n.strip() for n in str(row[col]).split(',')]:
                            if nc in person_map:
                                data.append((tk, person_map[nc], role, None, 0))
        
        # Load principals
        df_principals = self.read_tsv('title.principals.tsv.gz', nrows)
        if df_principals is not None:
            for _, row in df_principals.iterrows():
                if row['tconst'] not in title_map or row['nconst'] not in person_map:
                    continue
                
                role_detail = None
                if pd.notna(row.get('job')):
                    role_detail = str(row['job'])[:255]
                elif pd.notna(row.get('characters')):
                    role_detail = str(row['characters'])[:255]
                
                data.append((
                    title_map[row['tconst']],
                    person_map[row['nconst']],
                    row['category'] if pd.notna(row['category']) else 'unknown',
                    role_detail,
                    int(row['ordering']) if pd.notna(row['ordering']) else 0
                ))
        
        self.bulk_insert(
            'bridge_title_crew',
            ['title_key', 'person_key', 'role_type', 'role_detail', 'ordering'],
            data
        )
    
    def load_fact_title_ratings(self, nrows):
        self.truncate_table("fact_title_ratings")
        
        df = self.read_tsv('title.ratings.tsv.gz', nrows)
        if df is None:
            return
        
        self.cursor.execute("SELECT tconst, title_key FROM dim_title")
        title_map = dict(self.cursor.fetchall())
        
        data = [
            (title_map[row['tconst']],
             float(row['averageRating']) if pd.notna(row['averageRating']) else None,
             int(row['numVotes']) if pd.notna(row['numVotes']) else None)
            for _, row in df.iterrows()
            if row['tconst'] in title_map
        ]
        
        self.bulk_insert('fact_title_ratings', ['title_key', 'averageRating', 'numVotes'], data)
    
    def load_dim_episode(self, nrows):
        self.truncate_table("dim_episode")
        
        df = self.read_tsv('title.episode.tsv.gz', nrows)
        if df is None:
            return
        
        self.cursor.execute("SELECT tconst, title_key FROM dim_title")
        title_map = dict(self.cursor.fetchall())
        
        data = []
        for _, row in df.iterrows():
            if row['tconst'] not in title_map:
                continue
            
            parent_key = title_map.get(row['parentTconst']) if pd.notna(row['parentTconst']) else None
            
            data.append((
                title_map[row['tconst']],
                parent_key,
                int(row['seasonNumber']) if pd.notna(row['seasonNumber']) else None,
                int(row['episodeNumber']) if pd.notna(row['episodeNumber']) else None
            ))
        
        self.bulk_insert('dim_episode', ['title_key', 'parent_title_key', 'seasonNumber', 'episodeNumber'], data)
    
    def load_dim_akas(self, nrows):
        self.truncate_table("dim_akas")
        
        df = self.read_tsv('title.akas.tsv.gz', nrows)
        if df is None:
            return
        
        self.cursor.execute("SELECT tconst, title_key FROM dim_title")
        title_map = dict(self.cursor.fetchall())
        
        data = [
            (title_map[row['titleId']],
             row['title'][:255] if pd.notna(row['title']) else None,
             row['region'][:10] if pd.notna(row['region']) else None,
             int(row['isOriginalTitle']) if pd.notna(row['isOriginalTitle']) else 0)
            for _, row in df.iterrows()
            if row['titleId'] in title_map
        ]
        
        self.bulk_insert('dim_akas', ['title_key', 'title', 'region', 'isOriginalTitle'], data)
    
    # =====================================================
    # MAIN
    # =====================================================
    
    def run_etl(self, test_mode=False):
        start = datetime.now()
        print("=" * 60)
        print("IMDb ETL - Star Schema")
        print("=" * 60)
        
        nrows = 10000 if test_mode else None
        if test_mode:
            print("⚠ TEST MODE: 10,000 rows per file\n")
        
        try:
            self.connect()
            if self.disable_fk:
                self.disable_foreign_keys()
            
            self.timed("1/8 dim_date", self.load_dim_date)
            
            df_basics = self.read_tsv('title.basics.tsv.gz', nrows)
            if df_basics is None:
                raise Exception("Failed to read title.basics")
            
            self.timed("2/8 dim_genre", self.load_dim_genre, df_basics)
            self.timed("3/8 dim_person", self.load_dim_person, nrows)
            self.timed("4/8 dim_title", self.load_dim_title, nrows)
            self.timed("5/8 bridge_title_genre", self.load_bridge_title_genre, df_basics)
            self.timed("6/8 bridge_title_crew", self.load_bridge_title_crew, nrows)
            self.timed("7/8 fact_title_ratings", self.load_fact_title_ratings, nrows)
            self.timed("8/8 dim_episode + dim_akas", lambda n: (self.load_dim_episode(n), self.load_dim_akas(n)), nrows)
            
            print("\n" + "=" * 60)
            print(f"✓ ETL COMPLETED in {datetime.now() - start}")
            print("=" * 60)
            
        except Exception as e:
            print(f"\n✗ FAILED: {e}")
            import traceback
            traceback.print_exc()
        finally:
            if self.disable_fk:
                self.enable_foreign_keys()
            self.close()

if __name__ == "__main__":
    from config import DB_CONFIG, DATA_PATH
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--test", action="store_true")
    parser.add_argument("--truncate", action="store_true")
    parser.add_argument("--check-fk", action="store_true")
    args = parser.parse_args()
    
    loader = IMDBDataLoader(DB_CONFIG, DATA_PATH, args.truncate, not args.check_fk)
    loader.run_etl(args.test)