# chaz 01.04.26
# File Name: nfl_data_manager.property
# OPP version of batch_json_to_sql.py
# Usage: python nfl_data_manager.py ./NFL_2025_week_1 --db Week1_Stats.db (if you want to overwrite the default db name.)

import sqlite3
import json
import argparse
import logging
from pathlib import Path



# --- Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("nfl_pipeline.log"),  # Saves all details to a file
        logging.StreamHandler()                 # Prints info to your terminal
    ]
)
logger = logging.getLogger(__name__)

class NFLStatsDatabase:
    """Handles all SQLite database operations and schema management."""

    def __init__(self, db_name):
        self.db_name = db_name
        try:
            self.conn = sqlite3.connect(self.db_name)
            self.cursor = self.conn.cursor()
            self._setup_master_tables()
            logger.info(f"Connected to database: {db_name}")
        except sqlite3.Error as e:
            logger.error(f"Database connection failed: {e}")
            raise

    def _setup_master_tables(self):
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS games (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                matchup TEXT,
                date TEXT,
                filename TEXT
            )
        ''')
        self.conn.commit()

    def ensure_table_and_columns(self, table_name, columns):
        self.cursor.execute(f"CREATE TABLE IF NOT EXISTS [{table_name}] (id INTEGER PRIMARY KEY AUTOINCREMENT, game_id INTEGER, team TEXT)")

        self.cursor.execute(f"PRAGMA table_info([{table_name}])")
        existing = [info[1] for info in self.cursor.fetchall()]

        integer_keywords = {"yards", "tds", "long", "ints", "rec", "att", "sacks", "fumbles"}

        for col in columns:
            if col not in existing:
                col_type = "INTEGER" if any(k in col.lower() for k in integer_keywords) else "TEXT"
                logger.info(f"Adding new column: {col} ({col_type}) to table [{table_name}]")
                self.cursor.execute(f"ALTER TABLE [{table_name}] ADD COLUMN [{col}] {col_type}")

    def insert_game(self, matchup, date, filename):
        self.cursor.execute("INSERT INTO games (matchup, date, filename) VALUES (?, ?, ?)",
                            (matchup, date, filename))
        return self.cursor.lastrowid

    def insert_stats(self, table_name, game_id, team, stats_dict):
        keys = list(stats_dict.keys())
        self.ensure_table_and_columns(table_name, keys)

        values = [game_id, team]
        for k in keys:
            val = stats_dict[k]
            if isinstance(val, str) and val.isdigit():
                values.append(int(val))
            else:
                values.append(val if val is not None else "")

        placeholders = ", ".join(["?"] * len(values))
        col_names = ", ".join([f"[{c}]" for c in keys])
        query = f"INSERT INTO [{table_name}] (game_id, team, {col_names}) VALUES ({placeholders})"
        self.cursor.execute(query, values)

    def commit(self):
        self.conn.commit()

    def close(self):
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed.")

class NFLStatsImporter:
    """Handles the business logic of reading JSON files and directing the DB."""

    def __init__(self, db_manager):
        self.db = db_manager

    def process_directory(self, directory_path):
        path = Path(directory_path)
        if not path.is_dir():
            logger.error(f"Provided path is not a directory: {directory_path}")
            return

        json_files = list(path.glob("*.json"))
        logger.info(f"Starting import of {len(json_files)} files...")

        for json_file in json_files:
            try:
                self._import_single_file(json_file)
                self.db.commit()
                logger.info(f"Successfully processed: {json_file.name}")
            except Exception as e:
                # This catches errors in one file but keeps the loop running for the rest
                logger.error(f"Failed to process {json_file.name}: {e}", exc_info=True)

    def _import_single_file(self, file_path):
        with open(file_path, 'r') as f:
            data = json.load(f)

        game_info = data.get("game_info", {})
        game_id = self.db.insert_game(
            game_info.get("matchup"),
            game_info.get("date"),
            file_path.name
        )

        teams = data.get("teams", {})
        for team_name, categories in teams.items():
            for category, stats_list in categories.items():
                if not isinstance(stats_list, list): continue

                table_name = category.replace(" ", "_").lower()
                for entry in stats_list:
                    self.db.insert_stats(table_name, game_id, team_name, entry)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Professional NFL Box Score Importer")
    parser.add_argument("directory", help="Path to JSON folder")
    parser.add_argument("--db", default="NFL_season_stats.db", help="Output database name")
    args = parser.parse_args()

    db_manager = NFLStatsDatabase(args.db)
    importer = NFLStatsImporter(db_manager)

    try:
        importer.process_directory(args.directory)
    except KeyboardInterrupt:
        logger.warning("Process interrupted by user.")
    finally:
        db_manager.close()
        logger.info("Import session finished.")
