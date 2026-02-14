# chaz 01.04.26
# File Name: nfl_data_manager.property
# OPP version of batch_json_to_sql.py
# Usage: python nfl_data_manager.py ./NFL_2025_week_1 --db Week1_Stats.db (if you want to overwrite the default db name.)

import sqlite3
import json
import logging
import argparse
from pathlib import Path

# --- Professional Logging ---
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
        """Creates the foundation tables with unique constraints to prevent duplicates."""
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

    def insert_game(self, matchup, date, week, filename):
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT OR IGNORE INTO games (matchup, date, week, filename)
            VALUES (?, ?, ?, ?)
        """, (matchup, date, week, filename))

        cursor.execute("SELECT id FROM games WHERE matchup=? AND date=?", (matchup, date))
        return cursor.fetchone()[0]

    def insert_stats(self, table_name, game_id, team, stats_dict):
        """Dynamically builds tables for Passing, Rushing, etc."""
        table_name = f"{table_name.lower()}_stats"
        cursor = self.conn.cursor()

        # Clean data: separate player name from numeric stats
        player_name = stats_dict.pop('player', 'Unknown')
        keys = list(stats_dict.keys())

        # Build dynamic SQL
        cols_def = ", ".join([f"[{k}] REAL" for k in keys])
        cursor.execute(f"CREATE TABLE IF NOT EXISTS [{table_name}] (id INTEGER, game_id INTEGER, team TEXT, player TEXT, {cols_def})")

        placeholders = ", ".join(["?"] * (len(keys) + 3))
        col_names = ", ".join([f"[{k}]" for k in keys])
        vals = [game_id, team, player_name] + [stats_dict.get(k) for k in keys]

        cursor.execute(f"INSERT INTO [{table_name}] (game_id, team, player, {col_names}) VALUES ({placeholders})", vals)

class NFLStatsImporter:
    def __init__(self, db_manager):
        self.db = db_manager

    def process_directory(self, folder_path):
        files = list(Path(folder_path).rglob("*.json"))
        logger.info(f"Scanning {folder_path}... Found {len(files)} files.")

        for f in files:
            if "schema_template" in f.name: continue
            self._import_file(f)
            self.db.conn.commit()

    def _import_file(self, file_path):
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)

            game_info = data.get("game_info", {})

            # --- 2024 vs 2025 FLEX-LOGIC ---
            # Try 2025 path first, then fall back to the 2024 final_score path
            teams_data = game_info.get("teams")
            if not teams_data and "final_score" in game_info:
                # 2024 files nest teams inside the scores of the two playing teams
                for key, value in game_info["final_score"].items():
                    if isinstance(value, dict) and "teams" in value:
                        teams_data = value["teams"]
                        break

            # Build Matchup String (e.g., "Denver Broncos vs Seattle Seahawks")
            scores = game_info.get("final_score", {})
            team_names = [k for k in scores.keys() if isinstance(scores[k], (int, dict))]
            matchup = f"{team_names[0]} vs {team_names[1]}" if len(team_names) >= 2 else "Unknown"

            # 1. Save Game
            game_id = self.db.insert_game(matchup, game_info.get("date"), game_info.get("week"), file_path.name)

            # 2. Save Stats
            if teams_data:
                for team, categories in teams_data.items():
                    for cat_name, players in categories.items():
                        if not isinstance(players, list): continue
                        for p_stats in players:
                            self.db.insert_stats(cat_name, game_id, team, p_stats)

            logger.info(f"Successfully Imported: {file_path.name}")
        except Exception as e:
            logger.error(f"Failed {file_path.name}: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("folder", help="Path to your 'nfl' directory")
    args = parser.parse_args()

    db = NFLStatsDatabase("NFL_Master_Stats.db")
    importer = NFLStatsImporter(db)
    importer.process_directory(args.folder)
