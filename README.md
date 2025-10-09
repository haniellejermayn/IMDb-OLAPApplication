# 🎬 IMDb OLAP Data Warehouse (STADVDB MCO1)

Description

---

## ⚙️ Setup

1. **Requirements:** Python 3.10+, MySQL, Git
2. **Virtual Environment:**

   ```bash
   python -m venv venv
   source venv/Scripts/activate    # or venv/bin/activate (Mac/Linux)
   pip install -r requirements.txt
   ```
3. **Database:**

   ```bash
   mysql -u root -p < backend/db/schema.sql
   ```
4. **.env file (in project root):**

   ```
   DB_HOST=localhost
   DB_PORT=3306
   DB_USER=root
   DB_PASSWORD=your_password_here
   DB_NAME=imdb_star_schema
   ```

---

## 📂 Data Files

Download all `.tsv.gz` files from [datasets.imdbws.com](https://datasets.imdbws.com/) into:

```
data/raw/
```

Required files:

```
title.basics.tsv.gz
title.ratings.tsv.gz
name.basics.tsv.gz
title.akas.tsv.gz
title.crew.tsv.gz
title.episode.tsv.gz
title.principals.tsv.gz
```

---

## 🚀 Run ETL

Activate your virtual environment, go to the `etl/` folder, and run:

```bash
cd etl
python load_data.py
```

This performs a **full load** — truncates all tables, disables FK checks for faster inserts,
then reloads all dimensions, bridges, and fact tables.

---

## 🧪 Run Modes

| Mode                   | Command                               | Description                                     |
| ---------------------- | ------------------------------------- | ----------------------------------------------- |
| 🧪 **Test mode**       | `python load_data.py --test`          | Loads only first 10k rows per dataset           |
| 🚀 **Full load**       | `python load_data.py`                 | Loads all data (FK checks off for speed)        |
| 🧹 **Remove old data** | `python load_data.py --truncate`      | Clear tables before load (full reload)          |
| ✅ **FK validation**   | `python load_data.py --check-fk`      | Runs with FK checks ON (slower, ensures links)  |

Example:

```bash
python load_data.py --test
```

---

## 🧱 Table Overview

**Dimensions:**

* `dim_date` – Years, decades, centuries (for rollups)
* `dim_person` – People (actors, directors, writers, etc.)
* `dim_title` – Movies, shows, etc.
* `dim_genre` – Genres (with surrogate `genre_key`)
* `dim_episode` – Episodes linked to titles
* `dim_akas` – Alternative titles and regions

**Bridges:**

* `bridge_title_genre` – Many-to-many title ↔ genre
* `bridge_title_crew` – Crew and principal roles (director, writer, actor)

**Fact:**

* `fact_title_ratings` – Ratings, votes per title

---

## 📝 Notes

* `.env` uses your **own MySQL credentials**
* `data/raw/` is **gitignored** (download manually)
* Test mode (`--test`) is **recommended first**
* FK checks are **disabled automatically** during load for performance
