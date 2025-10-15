import pandas as pd
import mysql.connector
from mysql.connector import Error
from datetime import datetime
import sys
import argparse
import time
from collections import defaultdict
import numpy as np

from pathlib import Path
import os
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
    
    def convert_to_native_types(self, data):
        """Convert numpy/pandas types to native Python types"""
        converted = []
        for row in data:
            converted_row = []
            for val in row:
                # Handle pandas NA/NaT
                if pd.isna(val):
                    converted_row.append(None)
                # Convert numpy integers
                elif isinstance(val, (np.integer, np.int64, np.int32)):
                    converted_row.append(int(val))
                # Convert numpy floats
                elif isinstance(val, (np.floating, np.float64, np.float32)):
                    converted_row.append(float(val))
                # Keep native Python types as-is
                else:
                    converted_row.append(val)
            converted.append(tuple(converted_row))
        return converted
    
    def bulk_insert(self, table, columns, data, batch_size=50000):
        # Convert structured array or Series to list of tuples if needed
        if hasattr(data, 'to_records'):
            data = [tuple(x) for x in data]
        elif isinstance(data, pd.Series):
            data = data.tolist()

        if len(data) == 0:
            logging.warning(f"  ⚠ No data to insert")
            return

        # Convert to native Python types
        data = self.convert_to_native_types(data)

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

                if (i // batch_size) % 5 == 0 and i > 0:
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

            if year < 1930:
                era = "Silent Movie Era (Pre-1930s)"
            elif year < 1970:
                era = "The Golden Age of Hollywood (1930-1969)"
            elif year < 1990:
                era = "Blockbusters (1970-1989)"
            elif year < 2010:
                era = "Digital Revolution (1990-2009)"
            else:
                era = "Streaming (2010-Present)"
            
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
    
    def load_dim_person(self, nrows, df_crew=None, df_principals=None):
        self.truncate_table("Dim_Person")

        usecols = ['nconst', 'primaryName']
        logging.info(f"  Reading name.basics.tsv.gz...")
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

        # --- Collect additional nconsts from crew/principals ---
        additional_nconsts = set()

        if df_crew is None:
            df_crew = self.read_tsv('title.crew.tsv.gz', nrows)

        if df_crew is not None:
            for col in ['directors', 'writers']:
                s = df_crew[col].dropna().astype(str).str.split(',')
                s = [i.strip() for sublist in s for i in sublist if i.strip()]
                additional_nconsts.update(s)

        if df_principals is None:
            df_principals = self.read_tsv('title.principals.tsv.gz', nrows)

        if df_principals is not None:
            additional_nconsts.update(df_principals['nconst'].dropna().astype(str).tolist())

        # --- Filter out existing nconsts ---
        existing_nconsts = set(df_names['nconst'])
        additional_nconsts = additional_nconsts - existing_nconsts
        logging.info(f"  Found {len(additional_nconsts):,} additional people in crew/principals")

        # --- Prepare DataFrame for bulk insert ---
        df_additional = pd.DataFrame({
            'nconst': list(additional_nconsts),
            'primaryName': [f"[Unknown - {n}]" for n in additional_nconsts]
        })

        df_names['primaryName'] = df_names['primaryName'].fillna('').str[:200]
        df_final = pd.concat([df_names, df_additional], ignore_index=True)

        # --- Bulk insert using to_records() ---
        self.bulk_insert(
            "Dim_Person",
            ['nconst', 'primaryName'],
            df_final.to_records(index=False)
        )

    def load_dim_title(self, df_basics):
        self.truncate_table("Dim_Title")
        if df_basics is None:
            logging.error("  ✗ df_basics is None, cannot load Dim_Title")
            return

        df = df_basics[df_basics['tconst'].notna()].copy()
        
        # Convert Int64 to regular int/None BEFORE creating final DataFrame
        df['endYear'] = df['endYear'].apply(lambda x: int(x) if pd.notna(x) else None)
        df['runtimeMinutes'] = df['runtimeMinutes'].apply(lambda x: int(x) if pd.notna(x) else None)
        
        df_final = pd.DataFrame({
            'tconst': df['tconst'],
            'primaryTitle': df['primaryTitle'].fillna('').str[:500],
            'originalTitle': df['originalTitle'].fillna('').str[:500],
            'titleType': df['titleType'],
            'endYear': df['endYear'],
            'runtimeMinutes': df['runtimeMinutes']
        })
        
        self.bulk_insert('Dim_Title', df_final.columns.tolist(), df_final.to_records(index=False))

    def load_bridge_title_genre(self, df_basics):
        self.truncate_table("Bridge_Title_Genre")
        self.cursor.execute("SELECT genreName, genreKey FROM Dim_Genre")
        genre_map = dict(self.cursor.fetchall())

        df = df_basics[['tconst','genres']].dropna()
        df = df[df['tconst'].notna()]
        df = df.assign(genre=df['genres'].str.split(',')).explode('genre')
        df['genre'] = df['genre'].str.strip()
        df = df[df['genre'].isin(genre_map.keys())]
        df['genreKey'] = df['genre'].map(genre_map)

        self.bulk_insert('Bridge_Title_Genre', ['tconst','genreKey'], df[['tconst','genreKey']].to_records(index=False))

    def load_dim_episode(self, nrows):
        self.truncate_table("Dim_Episode")
        df = self.read_tsv('title.episode.tsv.gz', nrows)
        if df is None:
            return

        self.cursor.execute("SELECT tconst FROM Dim_Title")
        valid_titles = set(row[0] for row in self.cursor.fetchall())

        df = df[df['tconst'].isin(valid_titles)].copy()
        df['parentTconst'] = df['parentTconst'].where(df['parentTconst'].isin(valid_titles), None)
        df_final = df[['tconst','parentTconst','seasonNumber','episodeNumber']].copy()
        df_final = df_final.rename(columns={'tconst':'episodeTconst'})
        
        # Convert to native int/None
        df_final['seasonNumber'] = df_final['seasonNumber'].apply(lambda x: int(x) if pd.notna(x) else None)
        df_final['episodeNumber'] = df_final['episodeNumber'].apply(lambda x: int(x) if pd.notna(x) else None)

        orphaned = df['parentTconst'].isna().sum()
        if orphaned > 0:
            logging.warning(f"  ⚠ {orphaned} episodes have missing parent series (set to NULL)")

        self.bulk_insert('Dim_Episode', df_final.columns.tolist(), df_final.to_records(index=False))

    
    def load_bridge_title_person(self, nrows, df_crew=None, df_principals=None):
        self.truncate_table("Bridge_Title_Person")

        # --- Preload valid IDs ---
        self.cursor.execute("SELECT tconst FROM Dim_Title")
        valid_titles = set(row[0] for row in self.cursor.fetchall())
        self.cursor.execute("SELECT nconst FROM Dim_Person")
        valid_persons = set(row[0] for row in self.cursor.fetchall())

        # --- Process crew ---
        if df_crew is None:
            df_crew = self.read_tsv('title.crew.tsv.gz', nrows)
        
        records = []

        if df_crew is not None:
            for role in ['directors', 'writers']:
                s = df_crew[['tconst', role]].dropna()
                s = s[s['tconst'].isin(valid_titles)]
                # explode comma-separated nconsts
                s = s.assign(nconsts=s[role].str.split(',')).explode('nconsts')
                s['nconsts'] = s['nconsts'].str.strip()
                s = s[s['nconsts'].isin(valid_persons)]
                s['category'] = role[:-1] if role.endswith('s') else role
                records.append(s[['tconst','nconsts','category']].rename(columns={'nconsts':'nconst'}))

        # --- Process principals ---
        if df_principals is None:
            df_principals = self.read_tsv('title.principals.tsv.gz', nrows)

        if df_principals is not None:
            df_principals = df_principals.dropna(subset=['tconst','nconst'])
            df_principals = df_principals[
                df_principals['tconst'].isin(valid_titles) & df_principals['nconst'].isin(valid_persons)
            ]
            df_principals['category'] = df_principals['category'].fillna('unknown')
            records.append(df_principals[['tconst','nconst','category']])

        if records:
            df_final = pd.concat(records, ignore_index=True).drop_duplicates()
            logging.info(f"  Total unique records: {len(df_final):,}")
            self.bulk_insert(
                'Bridge_Title_Person',
                ['tconst', 'nconst', 'category'],
                df_final.to_records(index=False)
            )

    def load_fact_title_performance(self, df_basics, nrows=None):
        self.truncate_table("Fact_Title_Performance")
        df_ratings = self.read_tsv('title.ratings.tsv.gz', nrows)
        if df_ratings is None:
            return

        df_basics_filtered = df_basics[['tconst', 'startYear']].dropna(subset=['tconst', 'startYear'])
        df_basics_filtered['startYear'] = df_basics_filtered['startYear'].apply(lambda x: int(x) if pd.notna(x) else None)
        df_basics_filtered = df_basics_filtered.dropna(subset=['startYear'])

        # Merge ratings with startYear
        df_ratings = df_ratings.merge(df_basics_filtered, on='tconst', how='inner')

        # timeKey mapping
        self.cursor.execute("SELECT year, timeKey FROM Dim_Time")
        df_time_map = pd.DataFrame(self.cursor.fetchall(), columns=['startYear','timeKey'])

        # Merge to get timeKey
        df_final = df_ratings.merge(df_time_map, on='startYear', how='left')

        df_final = df_final[['tconst','timeKey','startYear','averageRating','numVotes']].copy()
        df_final['averageRating'] = df_final['averageRating'].astype(float)
        df_final['numVotes'] = df_final['numVotes'].apply(lambda x: int(x) if pd.notna(x) else None)

        self.bulk_insert('Fact_Title_Performance', df_final.columns.tolist(), df_final.to_records(index=False))


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
            
            logging.info(f"\n{'='*60}\nPHASE 1: Preloading Shared Data\n{'='*60}")
            
            usecols = ['tconst', 'titleType', 'primaryTitle', 'originalTitle', 
                    'startYear', 'endYear', 'runtimeMinutes', 'genres']
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

            logging.info(f"  Reading title.crew.tsv.gz...")
            df_crew = self.read_tsv('title.crew.tsv.gz', nrows)
            
            logging.info(f"  Reading title.principals.tsv.gz...")
            df_principals = self.read_tsv('title.principals.tsv.gz', nrows)
            
            logging.info(f"\n{'='*60}\nPHASE 2: Core Dimensions\n{'='*60}")
            self.timed("1/8 Dim_Time", self.load_dim_time)
            self.timed("2/8 Dim_Genre", self.load_dim_genre, df_basics)
            self.timed("3/8 Dim_Person", self.load_dim_person, nrows, df_crew, df_principals)
            self.timed("4/8 Dim_Title", self.load_dim_title, df_basics)
            
            logging.info(f"\n{'='*60}\nPHASE 3: Bridge Tables & Dependent Dimensions\n{'='*60}")
            self.timed("5/8 Bridge_Title_Genre", self.load_bridge_title_genre, df_basics)
            self.timed("6/8 Dim_Episode", self.load_dim_episode, nrows)
            self.timed("7/8 Bridge_Title_Person", self.load_bridge_title_person, nrows, df_crew, df_principals)
            
            logging.info(f"\n{'='*60}\nPHASE 4: Fact Table\n{'='*60}")
            self.timed("8/8 Fact_Title_Performance", self.load_fact_title_performance, df_basics, nrows)
            
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