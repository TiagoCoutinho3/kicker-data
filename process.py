"""Process Transfermarkt raw CSVs into filtered clean datasets."""

from __future__ import annotations

import shutil
from pathlib import Path

import pandas as pd

RAW_DIR = Path("raw")
CLEAN_DIR = Path("clean")

BIG_COMPETITIONS = {"CL", "EL", "WC", "EC", "CLI", "WCWC", "FIWC", "EURO"}
VALID_LEAGUES = {"GB1", "ES1", "IT1", "L1", "FR1", "BRA1", "ARG1", "NL1", "PO1", "MEX1"}
VALID_GAMES = VALID_LEAGUES | BIG_COMPETITIONS
MARKET_VALUE_THRESHOLD = 5_000_000
STARTER_TYPE = "starting_lineup"

COMPETITION_ALIASES = {
    "WC": "FIWC",
    "EC": "EURO",
}

REFERENCE_FILES = ("clubs.csv", "competitions.csv", "countries.csv", "national_teams.csv")

Stats = dict[str, tuple[int, int]]


def log(msg: str) -> None:
    print(msg, flush=True)


def resolve_competition_ids(competition_ids: set[str]) -> set[str]:
    resolved = set(competition_ids)
    for alias, target in COMPETITION_ALIASES.items():
        if alias in competition_ids:
            resolved.add(target)
    return resolved


def count_rows(path: Path) -> int:
    with path.open("r", encoding="utf-8") as handle:
        return sum(1 for _ in handle) - 1


def save_and_track(
    df: pd.DataFrame,
    raw_name: str,
    clean_name: str,
    stats: Stats,
    before: int | None = None,
) -> None:
    before_count = before if before is not None else len(df)
    after_count = len(df)
    output_path = CLEAN_DIR / clean_name
    df.to_csv(output_path, index=False)
    stats[raw_name] = (before_count, after_count)
    log(f"  Salvo {clean_name}: {before_count:,} -> {after_count:,}")


def copy_reference(name: str, stats: Stats) -> None:
    src = RAW_DIR / name
    dst = CLEAN_DIR / name
    log(f"Copiando {name}...")
    shutil.copy2(src, dst)
    row_count = count_rows(src)
    stats[name] = (row_count, row_count)


def build_valid_player_ids(games: pd.DataFrame) -> set[int]:
    log("Identificando jogadores validos...")

    players = pd.read_csv(
        RAW_DIR / "players.csv",
        usecols=["player_id", "highest_market_value_in_eur"],
    )
    players["highest_market_value_in_eur"] = pd.to_numeric(
        players["highest_market_value_in_eur"],
        errors="coerce",
    )
    condition_1 = set(
        players.loc[
            players["highest_market_value_in_eur"] >= MARKET_VALUE_THRESHOLD,
            "player_id",
        ]
    )
    log(f"  Condicao 1 (valor >= {MARKET_VALUE_THRESHOLD:,}): {len(condition_1):,} jogadores")

    big_comp_ids = resolve_competition_ids(BIG_COMPETITIONS)
    big_comp_game_ids = set(
        games.loc[games["competition_id"].isin(big_comp_ids), "game_id"]
    )
    log(f"  Jogos em competicoes grandes: {len(big_comp_game_ids):,}")

    lineups = pd.read_csv(
        RAW_DIR / "game_lineups.csv",
        usecols=["game_id", "player_id", "type"],
    )
    condition_2 = set(
        lineups.loc[
            lineups["game_id"].isin(big_comp_game_ids)
            & (lineups["type"] == STARTER_TYPE),
            "player_id",
        ]
    )
    log(f"  Condicao 2 (titular em competicao grande): {len(condition_2):,} jogadores")

    valid_player_ids = condition_1 | condition_2
    log(f"  Total de jogadores validos: {len(valid_player_ids):,}")
    return valid_player_ids


def build_valid_game_ids(games: pd.DataFrame) -> set[int]:
    valid_comp_ids = resolve_competition_ids(VALID_GAMES)
    return set(games.loc[games["competition_id"].isin(valid_comp_ids), "game_id"])


def print_report(stats: Stats) -> None:
    log("")
    log("=" * 62)
    log(f"{'Arquivo':<28} {'Antes':>10} {'Depois':>10} {'Reducao':>10}")
    log("-" * 62)

    for raw_name, (before, after) in stats.items():
        if before == 0:
            reduction = 0.0
        else:
            reduction = (1 - after / before) * 100
        log(f"{raw_name:<28} {before:>10,} {after:>10,} {reduction:>9.1f}%")

    log("=" * 62)


def main() -> None:
    log("Iniciando processamento do dataset Transfermarkt...")
    CLEAN_DIR.mkdir(exist_ok=True)
    stats: Stats = {}

    for name in REFERENCE_FILES:
        copy_reference(name, stats)

    log("Carregando games.csv...")
    games = pd.read_csv(RAW_DIR / "games.csv", usecols=["game_id", "competition_id"])
    games_before = len(games)

    valid_player_ids = build_valid_player_ids(games)

    log("Processando players.csv...")
    players = pd.read_csv(RAW_DIR / "players.csv")
    players_clean = players[players["player_id"].isin(valid_player_ids)]
    save_and_track(players_clean, "players.csv", "players_clean.csv", stats, before=len(players))

    valid_game_ids = build_valid_game_ids(games)
    games_clean = games[games["game_id"].isin(valid_game_ids)]
    log("Processando games.csv...")
    save_and_track(games_clean, "games.csv", "games_clean.csv", stats, before=games_before)

    log("Processando appearances.csv...")
    appearances = pd.read_csv(RAW_DIR / "appearances.csv")
    valid_league_ids = resolve_competition_ids(VALID_LEAGUES)
    appearances_clean = appearances[
        appearances["player_id"].isin(valid_player_ids)
        & appearances["competition_id"].isin(valid_league_ids)
    ]
    save_and_track(
        appearances_clean,
        "appearances.csv",
        "appearances_clean.csv",
        stats,
        before=len(appearances),
    )

    log("Processando game_lineups.csv...")
    lineups = pd.read_csv(RAW_DIR / "game_lineups.csv", low_memory=False)
    lineups_clean = lineups[
        lineups["player_id"].isin(valid_player_ids)
        & lineups["game_id"].isin(valid_game_ids)
    ]
    save_and_track(
        lineups_clean,
        "game_lineups.csv",
        "game_lineups_clean.csv",
        stats,
        before=len(lineups),
    )

    log("Processando game_events.csv...")
    events = pd.read_csv(RAW_DIR / "game_events.csv")
    events_clean = events[
        events["player_id"].isin(valid_player_ids)
        & events["game_id"].isin(valid_game_ids)
    ]
    save_and_track(
        events_clean,
        "game_events.csv",
        "game_events_clean.csv",
        stats,
        before=len(events),
    )

    log("Processando transfers.csv...")
    transfers = pd.read_csv(RAW_DIR / "transfers.csv")
    transfers_clean = transfers[transfers["player_id"].isin(valid_player_ids)]
    save_and_track(
        transfers_clean,
        "transfers.csv",
        "transfers_clean.csv",
        stats,
        before=len(transfers),
    )

    log("Processando player_valuations.csv...")
    valuations = pd.read_csv(RAW_DIR / "player_valuations.csv")
    valuations_clean = valuations[valuations["player_id"].isin(valid_player_ids)]
    save_and_track(
        valuations_clean,
        "player_valuations.csv",
        "player_valuations_clean.csv",
        stats,
        before=len(valuations),
    )

    log("Processando club_games.csv...")
    club_games = pd.read_csv(RAW_DIR / "club_games.csv")
    club_games_clean = club_games[club_games["game_id"].isin(valid_game_ids)]
    save_and_track(
        club_games_clean,
        "club_games.csv",
        "club_games_clean.csv",
        stats,
        before=len(club_games),
    )

    log("Processamento concluido.")
    print_report(stats)


if __name__ == "__main__":
    main()
