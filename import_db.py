"""Import clean CSVs into a SQLite database for the minigames platform."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd

CLEAN_DIR = Path("clean")
DB_PATH = CLEAN_DIR / "football.db"

TABLES: list[tuple[str, str]] = [
    ("countries.csv", "countries"),
    ("competitions.csv", "competitions"),
    ("clubs.csv", "clubs"),
    ("national_teams.csv", "national_teams"),
    ("players_clean.csv", "players"),
    ("games_clean.csv", "games"),
    ("appearances_clean.csv", "appearances"),
    ("game_lineups_clean.csv", "game_lineups"),
    ("game_events_clean.csv", "game_events"),
    ("transfers_clean.csv", "transfers"),
    ("player_valuations_clean.csv", "player_valuations"),
    ("club_games_clean.csv", "club_games"),
]

INDEXES: list[tuple[str, str, str]] = [
    ("idx_players_player_id", "players", "player_id"),
    ("idx_games_game_id", "games", "game_id"),
    ("idx_games_competition_id", "games", "competition_id"),
    ("idx_appearances_player_id", "appearances", "player_id"),
    ("idx_appearances_game_id", "appearances", "game_id"),
    ("idx_appearances_competition_id", "appearances", "competition_id"),
    ("idx_lineups_player_id", "game_lineups", "player_id"),
    ("idx_lineups_game_id", "game_lineups", "game_id"),
    ("idx_events_player_id", "game_events", "player_id"),
    ("idx_events_game_id", "game_events", "game_id"),
    ("idx_transfers_player_id", "transfers", "player_id"),
    ("idx_valuations_player_id", "player_valuations", "player_id"),
    ("idx_club_games_game_id", "club_games", "game_id"),
    ("idx_club_games_club_id", "club_games", "club_id"),
]

CHUNK_SIZE = 100_000


def log(msg: str) -> None:
    print(msg, flush=True)


def import_csv(conn: sqlite3.Connection, csv_path: Path, table: str) -> int:
    total = 0
    first_chunk = True

    for chunk in pd.read_csv(csv_path, chunksize=CHUNK_SIZE, low_memory=False):
        chunk.to_sql(
            table,
            conn,
            if_exists="replace" if first_chunk else "append",
            index=False,
            chunksize=5000,
        )
        total += len(chunk)
        first_chunk = False
        log(f"  {table}: {total:,} linhas importadas...")

    return total


def create_indexes(conn: sqlite3.Connection) -> None:
    for name, table, column in INDEXES:
        conn.execute(f"CREATE INDEX IF NOT EXISTS {name} ON {table} ({column})")


def print_summary(conn: sqlite3.Connection) -> None:
    log("")
    log("=" * 50)
    log(f"{'Tabela':<22} {'Linhas':>10}")
    log("-" * 50)

    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()

    for (table,) in rows:
        count = conn.execute(f"SELECT COUNT(*) FROM [{table}]").fetchone()[0]
        log(f"{table:<22} {count:>10,}")

    size_mb = DB_PATH.stat().st_size / (1024 * 1024)
    log("-" * 50)
    log(f"Arquivo: {DB_PATH} ({size_mb:.1f} MB)")
    log("=" * 50)


def main() -> None:
    if not CLEAN_DIR.exists():
        raise SystemExit(f"Pasta {CLEAN_DIR}/ nao encontrada. Rode python process.py primeiro.")

    missing = [csv for csv, _ in TABLES if not (CLEAN_DIR / csv).exists()]
    if missing:
        raise SystemExit(
            "CSVs ausentes em clean/:\n  "
            + "\n  ".join(missing)
            + "\n\nRode python process.py para gerar os arquivos."
        )

    if DB_PATH.exists():
        DB_PATH.unlink()
        log(f"Removido {DB_PATH} anterior.")

    log(f"Criando {DB_PATH}...")
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    try:
        for csv_name, table in TABLES:
            csv_path = CLEAN_DIR / csv_name
            log(f"Importando {csv_name} -> {table}...")
            import_csv(conn, csv_path, table)

        log("Criando indices...")
        create_indexes(conn)
        conn.commit()
        log("Importacao concluida.")
        print_summary(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
