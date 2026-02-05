# chaz 01.04.26 refactor: 02.05.26
# File Name: nfl_data_manager.property
# OPP version of batch_json_to_sql.py
# Usage: python nfl_data_manager.py ./NFL_2025_week_1 --db Week1_Stats.db (if you want to overwrite the default db name.)

import sqlite3
import json
import logging
import argparse
from pathlib import Path

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
        self._create_schema()

    def _create_schema(self):
        # Create core 'games' table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS games (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                matchup TEXT,
                date TEXT,
                week INTEGER,
                UNIQUE(matchup, date)
            )
        """)
        # Create a stats table for the players (e.g., Passing/Rushing)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS player_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                game_id INTEGER,
                player TEXT,
                team TEXT,
                category TEXT,
                stat_name TEXT,
                stat_value REAL,
                FOREIGN KEY(game_id) REFERENCES games(id)
            )
        """)
        self.conn.commit()

    def close(self):
        self.conn.close()

class NFLStatsImporter:
    """Handles parsing NFL JSON data and inserting it into the database."""
    def __init__(self, db_manager):
        self.db = db_manager

    def process_directory(self, folder_path):
        files = list(Path(folder_path).rglob("*.json"))
        logging.info(f"Starting import of {len(files)} files...")

        for file in files:
            if "schema_template" in file.name:
                continue
            self._import_file(file)

    def _import_file(self, file_path):
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)

            info = data.get("game_info", {})
            matchup = info.get("matchup")
            date = info.get("date")
            week = info.get("week")

            cursor = self.db.conn.cursor()

            # Step 1: Insert game and get ID
            cursor.execute("""
                INSERT OR IGNORE INTO games (matchup, date, week)
                VALUES (?, ?, ?)
            """, (matchup, date, week))

            # Use lastrowid or fetch existing
            game_id = cursor.lastrowid
            if not game_id:
                cursor.execute("SELECT id FROM games WHERE matchup=? AND date=?", (matchup, date))
                game_id = cursor.fetchone()[0]

            # Step 2: Process teams and stats
            # (Loop through 'teams' and 'player' lists here)
            teams_data = data.get("game_info", {}).get("teams", {})

            for team_name, categories in teams_data.items():
                for category, players_list in categories.items():
                    # 'category' will be 'passing', 'rushing', etc.
                    if not isinstance(players_list, list):
                        continue

                    for stat_entry in players_list:
                        player_name = stat_entry.get("player")

                        # We iterate through the keys to catch everything
                        # (e.g., yards, touchdowns, interceptions)
                        for stat_name, stat_value in stat_entry.items():
                            if stat_name == "player": continue  # Skip the name itself

                            cursor.execute("""
                                INSERT INTO player_stats (game_id, player, team, category, stat_name, stat_value)
                                VALUES (?, ?, ?, ?, ?, ?)
                            """, (game_id, player_name, team_name, category, stat_name, stat_value))

            self.db.conn.commit()
            logging.info(f"Successfully processed: {file_path.name} (Week {week})")

        except Exception as e:
            logging.error(f"Failed {file_path.name}: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NFL Box Score Importer")
    parser.add_argument("directory", help="Path to JSON folder")
    parser.add_argument("--db", default="NFL_Seasons_Stats.db", help="DB name")
    args = parser.parse_args()

    # Initialize the database first
    db_manager = NFLStatsDatabase(args.db)

    # Pass the manager to the importer
    importer = NFLStatsImporter(db_manager)

    try:
        importer.process_directory(args.directory)
    except KeyboardInterrupt:
        logger.warning("Process interrupted.")
    finally:
        db_manager.close()
        logger.info("Import session finished.")
