# 🎬 IMDb OLAP Data Warehouse (STADVDB MCO1)

A dimensional data warehouse implementing star schema for IMDb dataset analysis, featuring ETL pipelines, OLAP operations, and query optimization strategies.

Submitted by:
* CHUA, Hanielle
* DAVID Jr., Jose
* KELSEY, Gabrielle
* TOLENTINO, Hephzi

---

## ⚙️ Setup

### 1. Requirements
- Python 3.10+
- MySQL 8.0+
- Git

### 2. Virtual Environment

```bash
python -m venv venv
source venv/Scripts/activate    # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Database Setup

```bash
mysql -u root -p < backend/db/schema.sql
```

### 4. Environment Configuration

Create a `.env` file in the project root:

```env
DB_HOST=localhost
DB_PORT=3306
DB_USER=root
DB_PASSWORD=your_password_here
DB_NAME=imdb_star_schema
```

### 5. Run Frontend

Make sure you are in **(venv)**.

```
python backend/api/app.py
```

---

## 📂 Data Files

Download all `.tsv.gz` files from [datasets.imdbws.com](https://datasets.imdbws.com/) into:

```
data/raw/
```

**Required files:**
```
title.basics.tsv.gz       (~700 MB)
title.ratings.tsv.gz      (~25 MB)
name.basics.tsv.gz        (~600 MB)
title.crew.tsv.gz         (~250 MB)
title.episode.tsv.gz      (~150 MB)
title.principals.tsv.gz   (~2 GB)
```

**Total size:** ~3.7 GB compressed, ~12 GB uncompressed

---

## 🚀 Run ETL

Activate your virtual environment, navigate to the `etl/` folder, and run:

```bash
cd etl
python load_data.py
```

This performs a **full refresh**:
- Truncates all tables
- Disables foreign key checks for performance
- Loads all dimensions, bridges, and fact tables
- Re-enables constraints

**Estimated time:** 
- Test mode: 2-5 minutes
- Full load: 8-12 hours

---

## 🧪 Run Modes

| Command | Description | Time |
|---------|-------------|------|
| `python load_data.py --test` | Load first 10K rows per file | ~2-5 min |
| `python load_data.py` | Full production load | ~8-12 hrs |

**Examples:**

```bash
# Quick test during development
python load_data.py --test

# Weekly production refresh
python load_data.py
```

---

## 🧱 Data Warehouse Schema

### **Dimensions**
- **Dim_Time** – Years, decades, eras (1874-2036)
- **Dim_Title** – Movies, TV shows, episodes (~10M records)
- **Dim_Person** – Actors, directors, writers (~11M records)
- **Dim_Genre** – 28 unique genres
- **Dim_Episode** – Episode metadata with series relationships

### **Bridges** (Many-to-Many)
- **Bridge_Title_Genre** – Title ↔ Genre relationships
- **Bridge_Title_Person** – Title ↔ Person (with role/category)
- **Bridge_Person_KnownFor** – Person ↔ Notable titles

### **Fact Table**
- **Fact_Title_Performance** – Ratings, votes, time dimension (~1.3M records)

---

## 📊 ETL Pipeline

### **Extraction**
- Reads compressed `.tsv.gz` files using pandas
- Handles null values (`\N`) and encoding issues
- Validates data integrity during load

### **Transformation**
- Splits comma-separated values (genres, crew lists)
- Generates surrogate keys for dimensions
- Validates referential integrity before insertion
- Creates placeholder records for missing foreign keys

### **Loading**
- **Full refresh strategy** (truncate-and-load)
- **Performance optimizations:**
  - Foreign key checks disabled during load
  - Unique checks disabled during load
  - Batch inserts (50K rows per batch)
  - Manual transaction commits
- **Data quality:**
  - Orphaned records handled gracefully
  - Invalid foreign keys skipped with warnings
  - Statistics logged for each table

---

## 📝 Design Decisions

### **Why Full Refresh (Truncate-and-Load)?**

1. **Source data format:** IMDb publishes complete daily snapshots, not delta files
2. **OLAP requirements:** Weekly freshness is acceptable for analytical queries
3. **Data quality:** Prevents drift, ensures referential integrity
4. **Simplicity:** Clear, repeatable process with predictable performance
5. **Academic focus:** Allows consistent baseline for query optimization testing

### **Why Disable Foreign Keys During Load?**

- **10-15x performance improvement** for bulk inserts
- Data integrity validated in Python before insertion
- Constraints re-enabled after load completes
- Standard practice for data warehouse ETL

### **Performance Optimizations**
- Batch inserts (50K rows)
- Pre-validation of foreign keys in Python
- Commit every 250K rows to balance speed and recovery
- Chunked file reading for memory efficiency

---

## 🧪 Testing

```bash
# Development testing
python load_data.py --test

# Validate schema
mysql -u root -p imdb_star_schema < tests/validate_schema.sql

# Check row counts
mysql -u root -p -e "USE imdb_star_schema; 
SELECT table_name, table_rows 
FROM information_schema.tables 
WHERE table_schema='imdb_star_schema';"
```

---

## 📈 Expected Statistics (Full Load)

| Table | Rows | Load Time |
|-------|------|-----------|
| Dim_Time | 162 | <1s |
| Dim_Genre | 28 | <1s |
| Dim_Person | ~11M | ~45 min |
| Dim_Title | ~10M | ~40 min |
| Bridge_Title_Genre | ~20M | ~60 min |
| Dim_Episode | ~7M | ~30 min |
| Bridge_Person_KnownFor | ~40M | ~90 min |
| Bridge_Title_Person | ~50M | ~120 min |
| Fact_Title_Performance | ~1.3M | ~10 min |

**Total:** ~8-12 hours (varies by hardware)

---

## 🔧 Troubleshooting

### **ETL fails with "Duplicate entry" error**
- Ensure you're running `python load_data.py` (truncate is automatic)
- Check if previous ETL was interrupted

### **"Out of memory" errors**
- Use `--test` mode for development
- Increase MySQL `innodb_buffer_pool_size`
- Close other applications during full load

---

## 📚 Project Structure

```
├── backend/
│   └── db/
│       └── schema.sql          # Star schema DDL
├── data/
│   └── raw/                    # Downloaded .tsv.gz files (gitignored)
├── etl/
│   ├── load_data.py            # Main ETL script
│   └── config.py               # Database configuration
├── requirements.txt            # Python dependencies
├── .env                        # Database credentials (gitignored)
└── README.md
```

---

## 📖 References

- [IMDb Non-Commercial Datasets](https://datasets.imdbws.com/)
- [Kimball Dimensional Modeling](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/)
- [MySQL Performance Tuning](https://dev.mysql.com/doc/refman/8.0/en/optimization.html)