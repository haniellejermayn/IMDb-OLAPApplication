import pandas as pd
import mysql.connector
from mysql.connector import Error
from datetime import datetime
import sys
import argparse
import time
from collections import defaultdict

import logging

# ==========================================
# LOGGING CONFIGURATION
# ==========================================
log_dir = Path(__file__).resolve().parent
log_filename = log_dir / f"etl_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

class IMDBDataLoader:
    def __init__(self, db_config, data_path):
        self.db_config = db_config
        self.data_path = data_path
        self.conn = None
        self.cursor = None
        self.stats = defaultdict(lambda: {'inserted': 0, 'errors': 0})
    
    # =====================================================
    # UTILITIES
    # =====================================================
    
    def connect(self):
        try:
            self.conn = mysql.connector.connect(**self.db_config)
            self.cursor = self.conn.cursor()
            logging.info("✓ Connected to database")
        except Error as e:
            logging.error(f"✗ Database connection failed: {e}")
            sys.exit(1)
    
    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logging.info("✓ Database connection closed")
    
    def disable_foreign_keys(self):
        try:
            self.cursor.execute("SET FOREIGN_KEY_CHECKS=0;")
            self.cursor.execute("SET UNIQUE_CHECKS=0;")
            self.cursor.execute("SET AUTOCOMMIT=0;")
            logging.info("✓ Constraints disabled for faster loading")
        except Error as e:
            logging.warning(f"⚠ Could not disable checks: {e}")
    
    def enable_foreign_keys(self):
        try:
            self.cursor.execute("COMMIT;")
            self.cursor.execute("SET UNIQUE_CHECKS=1;")
            self.cursor.execute("SET FOREIGN_KEY_CHECKS=1;")
            self.cursor.execute("SET AUTOCOMMIT=1;")
            logging.info("✓ Constraints re-enabled")
        except Error as e:
            logging.warning(f"⚠ Could not re-enable checks: {e}")
    
    def truncate_table(self, table):
        try:
            self.cursor.execute(f"TRUNCATE TABLE {table}")
            logging.info(f"  ↻ Cleared {table}")
        except Error as e:
            logging.warning(f"  ⚠ Could not clear {table}: {e}")

    def read_tsv(self, filename, nrows=None):
        logging.info(f"  Reading {filename}...")
        try:
            df = pd.read_csv(
                f'{self.data_path}{filename}',
                sep='\t',
                na_values=['\\N'],
                keep_default_na=True,
                low_memory=False,
                nrows=nrows,
                quoting=3,
                encoding='utf-8',
                on_bad_lines='warn'
            )
            logging.info(f"  ✓ Loaded {len(df):,} rows")
            return df
        except Exception as e:
            logging.error(f"  ✗ Error: {e}")
            return None
    
    def bulk_insert(self, table, columns, data, batch_size=50000):
        if not data:
            logging.warning(f"  ⚠ No data to insert")
            return
        
        placeholders = ', '.join(['%s'] * len(columns))
        query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
        
        total = len(data)
        inserted = 0
        start = time.time()
        
        try:
            for i in range(0, total, batch_size):
                batch = data[i:i + batch_size]
                self.cursor.executemany(query, batch)
                inserted += self.cursor.rowcount
                
                if i % (batch_size * 5) == 0:
                    self.conn.commit()
                
                print(f"  Progress: {i + len(batch):,}/{total:,}", end='\r')
            
            self.conn.commit()
            
            self.stats[table]['inserted'] = inserted
            
            logging.info(f"\n  ✓ Inserted {inserted:,} rows ({time.time() - start:.2f}s)")
        except Error as e:
            logging.error(f"\n  ✗ Error: {e}")
            self.conn.rollback()
            self.stats[table]['errors'] += 1
    
    def timed(self, label, func, *args):
        logging.info(f"\n{'='*60}\n[{label}]\n{'='*60}")
        start = time.time()
        result = func(*args)
        logging.info(f"✓ Completed in {time.time() - start:.2f}s")
        return result
    
    def print_summary(self):
        logging.info("\n" + "="*60)
        logging.info("LOAD SUMMARY")
        logging.info("="*60)
        for table, stats in sorted(self.stats.items()):
            logging.info(f"{table:30} | Inserted: {stats['inserted']:>8,}")
        logging.info("="*60)
    
    # =====================================================
    # LOADERS
    # =====================================================
    
    def load_dim_time(self):
        self.truncate_table("Dim_Time")
        
        current_year = datetime.now().year
        data = []
        
        for year in range(1874, current_year + 11):
            decade = f"{(year // 10) * 10}s"
            
            if year < 1920:
                era = "Silent Era (Pre-1920)"
            elif year < 1960:
                era = "Golden Age (1920-1959)"
            elif year < 1980:
                era = "New Hollywood (1960-1979)"
            elif year < 2000:
                era = "Blockbuster Era (1980-1999)"
            elif year < 2010:
                era = "Digital Age (2000-2009)"
            elif year < 2020:
                era = "Streaming Rise (2010-2019)"
            else:
                era = "Modern Era (2020+)"
            
            data.append((year, decade, era))
        
        self.bulk_insert("Dim_Time", ['year', 'decade', 'era'], data)
    
    def load_dim_genre(self, df_basics):
        self.truncate_table("Dim_Genre")
        
        genres = set()
        for g in df_basics['genres'].dropna():
            if str(g) not in ['\\N', 'nan']:
                genres.update([x.strip() for x in str(g).split(',') if x.strip()])
        
        data = [(g,) for g in sorted(genres)]
        self.bulk_insert("Dim_Genre", ['genreName'], data)
    
    def load_dim_person(self, nrows):
        self.truncate_table("Dim_Person")
        
        usecols = ['nconst', 'primaryName', 'primaryProfession', 'knownForTitles']
        
        logging.info(f"  Reading name.basics.tsv.gz...")
        try:
            df_names = pd.read_csv(
                f'{self.data_path}name.basics.tsv.gz',
                sep='\t',
                na_values=['\\N'],
                keep_default_na=True,
                low_memory=False,
                nrows=nrows,
                usecols=usecols,
                quoting=3,
                encoding='utf-8'
            )
            logging.info(f"  ✓ Loaded {len(df_names):,} rows")
        except Exception as e:
            logging.error(f"  ✗ Error: {e}")
            return None
        
        all_nconsts = set(df_names['nconst'].dropna())
        additional_nconsts = set()
        
        # Get IDs from crew
        df_crew = self.read_tsv('title.crew.tsv.gz', nrows)
        if df_crew is not None:
            for col in ['directors', 'writers']:
                for val in df_crew[col].dropna():
                    if str(val) not in ['\\N', 'nan']:
                        ids = [n.strip() for n in str(val).split(',') if n.strip()]
                        additional_nconsts.update([n for n in ids if n not in all_nconsts])
        
        # Get IDs from principals
        df_principals = self.read_tsv('title.principals.tsv.gz', nrows)
        if df_principals is not None:
            principal_ids = set(df_principals['nconst'].dropna())
            additional_nconsts.update([n for n in principal_ids if n not in all_nconsts])

        logging.info(f"  Found {len(all_nconsts):,} people in name.basics")
        logging.info(f"  Found {len(additional_nconsts):,} additional people in crew/principals")

        person_data = []
        for _, row in df_names.iterrows():
            person_data.append((
                row['nconst'],
                row['primaryName'][:200] if pd.notna(row['primaryName']) else None,
                str(row['primaryProfession'])[:200] if pd.notna(row['primaryProfession']) else None
            ))
        
        for nconst in additional_nconsts:
            person_data.append((nconst, f"[Unknown - {nconst}]", None))
        
        self.bulk_insert("Dim_Person", ['nconst', 'primaryName', 'primaryProfession'], person_data)
        
        return df_names
    
    def load_dim_title(self, nrows):
        self.truncate_table("Dim_Title")
        
        usecols = ['tconst', 'titleType', 'primaryTitle', 'originalTitle', 
                   'startYear', 'endYear', 'runtimeMinutes', 'genres']

        logging.info(f"  Reading title.basics.tsv.gz...")
        try:
            df = pd.read_csv(
                f'{self.data_path}title.basics.tsv.gz',
                sep='\t',
                na_values=['\\N'],
                keep_default_na=True,
                low_memory=False,
                nrows=nrows,
                usecols=usecols,
                quoting=3,
                encoding='utf-8'
            )
            logging.info(f"  ✓ Loaded {len(df):,} rows")
        except Exception as e:
            logging.error(f"  ✗ Error: {e}")
            return None
        
        if df is None:
            return None
        
        data = []
        for _, row in df.iterrows():
            if pd.isna(row['tconst']):
                continue
            
            data.append((
                row['tconst'],
                row['primaryTitle'][:500] if pd.notna(row['primaryTitle']) else None,
                row['originalTitle'][:500] if pd.notna(row['originalTitle']) else None,
                row['titleType'] if pd.notna(row['titleType']) else None,
                int(row['startYear']) if pd.notna(row['startYear']) else None,
                int(row['endYear']) if pd.notna(row['endYear']) else None,
                int(row['runtimeMinutes']) if pd.notna(row['runtimeMinutes']) else None
            ))
        
        self.bulk_insert(
            "Dim_Title",
            ['tconst', 'primaryTitle', 'originalTitle', 'titleType', 
             'startYear', 'endYear', 'runtimeMinutes'],
            data
        )
        return df
    
    def load_bridge_title_genre(self, df_basics):
        self.truncate_table("Bridge_Title_Genre")
        
        self.cursor.execute("SELECT genreName, genreKey FROM Dim_Genre")
        genre_map = dict(self.cursor.fetchall())
        
        data = []
        for _, row in df_basics.iterrows():
            if pd.isna(row['tconst']):
                continue
            
            if pd.notna(row['genres']) and str(row['genres']) not in ['\\N', 'nan']:
                genres = [g.strip() for g in str(row['genres']).split(',')]
                for g in genres:
                    if g in genre_map:
                        data.append((row['tconst'], genre_map[g]))
        
        self.bulk_insert('Bridge_Title_Genre', ['tconst', 'genreKey'], data)
    
    def load_dim_episode(self, nrows):
        self.truncate_table("Dim_Episode")
        
        df = self.read_tsv('title.episode.tsv.gz', nrows)
        if df is None:
            return
        
        self.cursor.execute("SELECT tconst FROM Dim_Title")
        valid_titles = set(row[0] for row in self.cursor.fetchall())
        
        data = []
        orphaned = 0
        for _, row in df.iterrows():
            if pd.isna(row['tconst']) or row['tconst'] not in valid_titles:
                continue
            
            parent_tconst = None
            if pd.notna(row['parentTconst']) and row['parentTconst'] in valid_titles:
                parent_tconst = row['parentTconst']
            elif pd.notna(row['parentTconst']):
                orphaned += 1
            
            data.append((
                row['tconst'],
                parent_tconst,
                int(row['seasonNumber']) if pd.notna(row['seasonNumber']) else None,
                int(row['episodeNumber']) if pd.notna(row['episodeNumber']) else None
            ))
        
        if orphaned > 0:
            logging.warning(f"  ⚠ {orphaned} episodes have missing parent series (set to NULL)")
        
        self.bulk_insert(
            'Dim_Episode', 
            ['episodeTconst', 'parentTconst', 'seasonNumber', 'episodeNumber'], 
            data
        )
    
    def load_bridge_person_knownfor(self, df_names, nrows):
        self.truncate_table("Bridge_Person_KnownFor")
        
        if df_names is None:
            usecols = ['nconst', 'primaryName', 'primaryProfession', 'knownForTitles']
            logging.info(f"  Reading name.basics.tsv.gz...")
            try:
                df_names = pd.read_csv(
                    f'{self.data_path}name.basics.tsv.gz',
                    sep='\t',
                    na_values=['\\N'],
                    keep_default_na=True,
                    low_memory=False,
                    nrows=nrows,
                    usecols=usecols,
                    quoting=3,
                    encoding='utf-8'
                )
                logging.info(f"  ✓ Loaded {len(df_names):,} rows")
            except Exception as e:
                logging.error(f"  ✗ Error: {e}")
                return
        
        self.cursor.execute("SELECT tconst FROM Dim_Title")
        valid_titles = set(row[0] for row in self.cursor.fetchall())
        
        data = []
        skipped = 0
        for _, row in df_names.iterrows():
            if pd.isna(row['nconst']):
                continue
            
            if pd.notna(row['knownForTitles']) and str(row['knownForTitles']) not in ['\\N', 'nan']:
                titles = [t.strip() for t in str(row['knownForTitles']).split(',')]
                for tconst in titles:
                    if tconst and tconst in valid_titles:
                        data.append((row['nconst'], tconst))
                    elif tconst:
                        skipped += 1
        
        if skipped > 0:
            logging.warning(f"  ⚠ Skipped {skipped} relationships (title not in dataset)")
        
        self.bulk_insert('Bridge_Person_KnownFor', ['nconst', 'tconst'], data)
    
    def load_bridge_title_person(self, nrows):
        self.truncate_table("Bridge_Title_Person")
        
        self.cursor.execute("SELECT tconst FROM Dim_Title")
        valid_titles = set(row[0] for row in self.cursor.fetchall())
        
        self.cursor.execute("SELECT nconst FROM Dim_Person")
        valid_persons = set(row[0] for row in self.cursor.fetchall())
        
        # dict to deduplicate: key = (tconst, nconst, category)
        unique_records = {}
        
        # Load from title.crew
        df_crew = self.read_tsv('title.crew.tsv.gz', nrows)
        if df_crew is not None:
            for _, row in df_crew.iterrows():
                if pd.isna(row['tconst']) or row['tconst'] not in valid_titles:
                    continue
                
                if pd.notna(row['directors']) and str(row['directors']) not in ['\\N', 'nan']:
                    for nc in [n.strip() for n in str(row['directors']).split(',')]:
                        if nc and nc in valid_persons:
                            key = (row['tconst'], nc, 'director')
                            unique_records[key] = (row['tconst'], nc, 'director')
                
                if pd.notna(row['writers']) and str(row['writers']) not in ['\\N', 'nan']:
                    for nc in [n.strip() for n in str(row['writers']).split(',')]:
                        if nc and nc in valid_persons:
                            key = (row['tconst'], nc, 'writer')
                            unique_records[key] = (row['tconst'], nc, 'writer')
        
        # Load from title.principals (will overwrite crew data if same key exists)
        usecols = ['tconst', 'nconst', 'category']

        logging.info(f"  Reading title.principals.tsv.gz...")
        try:
            df_principals = pd.read_csv(
                f'{self.data_path}title.principals.tsv.gz',
                sep='\t',
                na_values=['\\N'],
                keep_default_na=True,
                low_memory=False,
                nrows=nrows,
                usecols=usecols,
                quoting=3,
                encoding='utf-8'
            )
            logging.info(f"  ✓ Loaded {len(df_principals):,} rows")
            
            for _, row in df_principals.iterrows():
                if pd.isna(row['tconst']) or pd.isna(row['nconst']):
                    continue
                
                if row['tconst'] not in valid_titles or row['nconst'] not in valid_persons:
                    continue
                
                category = row['category'] if pd.notna(row['category']) else 'unknown'
                key = (row['tconst'], row['nconst'], category)
                unique_records[key] = (row['tconst'], row['nconst'], category)
                
        except Exception as e:
            logging.error(f"  ✗ Error reading principals: {e}")
        
        # Convert to list for bulk insert
        data = list(unique_records.values())
        
        logging.info(f"  Total unique records: {len(data):,}")
        
        self.bulk_insert(
            'Bridge_Title_Person',
            ['tconst', 'nconst', 'category'],
            data
        )
    
    def load_fact_title_performance(self, nrows):
        self.truncate_table("Fact_Title_Performance")
        
        df = self.read_tsv('title.ratings.tsv.gz', nrows)
        if df is None:
            return
        
        self.cursor.execute("SELECT year, timeKey FROM Dim_Time")
        time_map = dict(self.cursor.fetchall())
        
        self.cursor.execute("SELECT tconst, startYear FROM Dim_Title")
        title_years = dict(self.cursor.fetchall())
        
        data = []
        for _, row in df.iterrows():
            if pd.isna(row['tconst']) or row['tconst'] not in title_years:
                continue
            
            year = title_years.get(row['tconst'])
            time_key = time_map.get(year) if year else None
            
            data.append((
                row['tconst'],
                time_key,
                float(row['averageRating']) if pd.notna(row['averageRating']) else None,
                int(row['numVotes']) if pd.notna(row['numVotes']) else None
            ))
        
        self.bulk_insert(
            'Fact_Title_Performance', 
            ['tconst', 'timeKey', 'averageRating', 'numVotes'], 
            data
        )
    
    # =====================================================
    # MAIN
    # =====================================================
    
    def run_etl(self, test_mode=False):
        start = datetime.now()
        logging.info("=" * 60)
        logging.info("IMDb ETL - Star Schema (Full Refresh)")
        logging.info("=" * 60)

        nrows = 10000 if test_mode else None
        if test_mode:
            logging.warning("⚠ TEST MODE: 10,000 rows per file\n")

        try:
            self.connect()
            self.disable_foreign_keys()
            
            self.timed("1/9 Dim_Time", self.load_dim_time)
            
            usecols = ['tconst', 'titleType', 'primaryTitle', 'originalTitle', 
                       'startYear', 'endYear', 'runtimeMinutes', 'genres']
            logging.info(f"\n{'='*60}\nReading title.basics for genre extraction\n{'='*60}")
            logging.info(f"  Reading title.basics.tsv.gz...")
            try:
                df_basics = pd.read_csv(
                    f'{self.data_path}title.basics.tsv.gz',
                    sep='\t',
                    na_values=['\\N'],
                    keep_default_na=True,
                    low_memory=False,
                    nrows=nrows,
                    usecols=usecols,
                    quoting=3,
                    encoding='utf-8'
                )
                logging.info(f"  ✓ Loaded {len(df_basics):,} rows")
            except Exception as e:
                logging.error(f"  ✗ Error: {e}")
                raise Exception("Failed to read title.basics")
            
            self.timed("2/9 Dim_Genre", self.load_dim_genre, df_basics)
            df_names = self.timed("3/9 Dim_Person", self.load_dim_person, nrows)
            self.timed("4/9 Dim_Title", self.load_dim_title, nrows)
            self.timed("5/9 Bridge_Title_Genre", self.load_bridge_title_genre, df_basics)
            self.timed("6/9 Dim_Episode", self.load_dim_episode, nrows)
            self.timed("7/9 Bridge_Person_KnownFor", self.load_bridge_person_knownfor, df_names, nrows)
            self.timed("8/9 Bridge_Title_Person", self.load_bridge_title_person, nrows)
            self.timed("9/9 Fact_Title_Performance", self.load_fact_title_performance, nrows)
            
            self.print_summary()

            logging.info("\n" + "=" * 60)
            logging.info(f"✓ ETL COMPLETED in {datetime.now() - start}")
            logging.info("=" * 60)

        except Exception as e:
            logging.error(f"\n✗ FAILED: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self.enable_foreign_keys()
            self.close()

if __name__ == "__main__":
    from config import DB_CONFIG, DATA_PATH
    
    parser = argparse.ArgumentParser(description="IMDb ETL Pipeline - Full Refresh")
    parser.add_argument("--test", action="store_true", help="Run in test mode (10k rows per file)")
    args = parser.parse_args()
    
    loader = IMDBDataLoader(DB_CONFIG, DATA_PATH)
    loader.run_etl(args.test)