# chaz 01.04.26 | 02.13.26 Refactor code to be season agnostic (json has different formats)|02.14.26 Changed date to DATE|Format date: yyyy-mm-dd before insert.
# File Name: nfl_data_manager.property
# OPP version of batch_json_to_sql.py
# Usage: python nfl_data_manager.py ./NFL_2025_week_1 --db Week1_Stats.db (if you want to overwrite the default db name.)

import sqlite3
import json
import logging
import argparse
from pathlib import Path
from datetime import datetime

# --- Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.FileHandler("nfl_pipeline.log"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

class NFLStatsDatabase:
    def __init__(self, db_path):
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self._create_tables()

    def _create_tables(self):
        with self.conn:
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS games (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    matchup TEXT,
                    date TEXT,
                    week INTEGER,
                    filename TEXT,
                    UNIQUE(matchup, date)
                )
            """)

    def _ensure_columns(self, table_name, keys):
        """Checks if columns exist and adds them if they don't."""
        cursor = self.conn.cursor()
        cursor.execute(f"PRAGMA table_info([{table_name}])")
        existing_cols = [row[1] for row in cursor.fetchall()]

        for key in keys:
            if key not in existing_cols and key != 'player':
                try:
                    cursor.execute(f"ALTER TABLE [{table_name}] ADD COLUMN [{key}] REAL")
                    logger.info(f"Added new column [{key}] to table [{table_name}]")
                except sqlite3.OperationalError:
                    pass # Column might have been added by another process

    def insert_game(self, matchup, date, week, filename):
        cursor = self.conn.cursor()
        cursor.execute("INSERT OR IGNORE INTO games (matchup, date, week, filename) VALUES (?, ?, ?, ?)",
                       (matchup, date, week, filename))
        cursor.execute("SELECT id FROM games WHERE matchup=? AND date=?", (matchup, date))
        row = cursor.fetchone()
        return row[0] if row else None

    def insert_stats(self, table_name, game_id, team, stats_dict, player_name=None):
        clean_name = f"{table_name.lower().replace(' ', '_')}_stats"
        cursor = self.conn.cursor()

        stats_copy = stats_dict.copy()
        if player_name is None:
            player_name = stats_copy.pop('player', 'Team Total')

        keys = list(stats_copy.keys())

        # 1. Create table if it doesn't exist
        cursor.execute(f"CREATE TABLE IF NOT EXISTS [{clean_name}] (id INTEGER PRIMARY KEY AUTOINCREMENT, game_id INTEGER, team TEXT, player TEXT)")

        # 2. Add any missing columns (like 'average')
        self._ensure_columns(clean_name, keys)

        # 3. Insert the data
        placeholders = ", ".join(["?"] * (len(keys) + 3))
        col_names = ", ".join([f"[{k}]" for k in keys])
        vals = [game_id, team, player_name] + [stats_copy.get(k) for k in keys]

        cursor.execute(f"INSERT INTO [{clean_name}] (game_id, team, player, {col_names}) VALUES ({placeholders})", vals)

class NFLStatsImporter:
    def __init__(self, db_manager):
        self.db = db_manager

    def _import_file(self, file_path):
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)

            info = data.get("game_info", {})
            scores = info.get("final_score", {})
            team_names = list(scores.keys())
            matchup = f"{team_names[0]} vs {team_names[1]}" if len(team_names) >= 2 else "Unknown"

            # Date formatting
            raw_date = info.get("date", "")
            try:
                formatted_date = datetime.strptime(raw_date, "%B %d, %Y").strftime("%Y-%m-%d")
            except:
                formatted_date = raw_date

            game_id = self.db.insert_game(matchup, formatted_date, info.get("week"), file_path.name)

            if info.get("teams") and game_id:
                for team_name, categories in info["teams"].items():
                    for cat_name, val in categories.items():
                        if isinstance(val, list):
                            for p_stats in val:
                                self.db.insert_stats(cat_name, game_id, team_name, p_stats)
                        elif isinstance(val, dict):
                            self.db.insert_stats(cat_name, game_id, team_name, val, player_name="Team")

            self.db.conn.commit()
            logger.info(f"Processed: {file_path.name}")
        except Exception as e:
            logger.error(f"Failed {file_path.name}: {e}")

    def process_directory(self, folder):
        files = list(Path(folder).rglob("*.json"))
        for f in files:
            self._import_file(f)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("folder", help="Path to JSON folder")
    args = parser.parse_args()

    db_manager = NFLStatsDatabase("NFL_Seasons_Stats.db")
    importer = NFLStatsImporter(db_manager)
    importer.process_directory(args.folder)
    print("Database updated and schema evolved successfully!")
