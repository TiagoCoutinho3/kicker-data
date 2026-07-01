"""Unified pipeline: process raw data, generate avatars, import to football.db."""

from __future__ import annotations

import json
import random
import sqlite3
import sys
import urllib.parse
from pathlib import Path

import pandas as pd

# Paths
RAW_DIR = Path("raw")
CLEAN_DIR = Path("clean")
DB_PATH = CLEAN_DIR / "football.db"
CLUB_COLORS_FILE = Path("club_colors.json")

# Constants from process.py
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

# Database tables and indexes
TABLES: list[tuple[str, str]] = [
    ("clubs.csv", "clubs"),
    ("competitions.csv", "competitions"),
    ("countries.csv", "countries"),
    ("national_teams.csv", "national_teams"),
    ("players_clean", "players"),
    ("games_clean", "games"),
    ("appearances_clean", "appearances"),
    ("game_lineups_clean", "game_lineups"),
    ("game_events_clean", "game_events"),
    ("transfers_clean", "transfers"),
    ("player_valuations_clean", "player_valuations"),
    ("club_games_clean", "club_games"),
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


# ==================== DATA PROCESSING FUNCTIONS ====================

def resolve_competition_ids(competition_ids: set[str]) -> set[str]:
    resolved = set(competition_ids)
    for alias, target in COMPETITION_ALIASES.items():
        if alias in competition_ids:
            resolved.add(target)
    return resolved




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


def process_raw_data() -> dict:
    """Process raw CSVs and return DataFrames for import."""
    log("Iniciando processamento do dataset Transfermarkt...")
    CLEAN_DIR.mkdir(exist_ok=True)
    stats: dict[str, tuple[int, int]] = {}


    # Load games
    log("Carregando games.csv...")
    games = pd.read_csv(RAW_DIR / "games.csv", usecols=["game_id", "competition_id"])
    games_before = len(games)

    # Build valid player and game IDs
    valid_player_ids = build_valid_player_ids(games)
    valid_game_ids = build_valid_game_ids(games)

    # Process players
    log("Processando players.csv...")
    players = pd.read_csv(RAW_DIR / "players.csv")
    players_clean = players[players["player_id"].isin(valid_player_ids)]
    stats["players.csv"] = (len(players), len(players_clean))
    log(f"  Salvo players_clean.csv: {len(players):,} -> {len(players_clean):,}")

    # Process games
    games_clean = games[games["game_id"].isin(valid_game_ids)]
    stats["games.csv"] = (games_before, len(games_clean))
    log(f"  Salvo games_clean.csv: {games_before:,} -> {len(games_clean):,}")

    # Process appearances
    log("Processando appearances.csv...")
    appearances = pd.read_csv(RAW_DIR / "appearances.csv")
    valid_league_ids = resolve_competition_ids(VALID_LEAGUES)
    appearances_clean = appearances[
        appearances["player_id"].isin(valid_player_ids)
        & appearances["competition_id"].isin(valid_league_ids)
    ]
    stats["appearances.csv"] = (len(appearances), len(appearances_clean))
    log(f"  Salvo appearances_clean.csv: {len(appearances):,} -> {len(appearances_clean):,}")

    # Process game_lineups
    log("Processando game_lineups.csv...")
    lineups = pd.read_csv(RAW_DIR / "game_lineups.csv", low_memory=False)
    lineups_clean = lineups[
        lineups["player_id"].isin(valid_player_ids)
        & lineups["game_id"].isin(valid_game_ids)
    ]
    stats["game_lineups.csv"] = (len(lineups), len(lineups_clean))
    log(f"  Salvo game_lineups_clean.csv: {len(lineups):,} -> {len(lineups_clean):,}")

    # Process game_events
    log("Processando game_events.csv...")
    events = pd.read_csv(RAW_DIR / "game_events.csv")
    events_clean = events[
        events["player_id"].isin(valid_player_ids)
        & events["game_id"].isin(valid_game_ids)
    ]
    stats["game_events.csv"] = (len(events), len(events_clean))
    log(f"  Salvo game_events_clean.csv: {len(events):,} -> {len(events_clean):,}")

    # Process transfers
    log("Processando transfers.csv...")
    transfers = pd.read_csv(RAW_DIR / "transfers.csv")
    transfers_clean = transfers[transfers["player_id"].isin(valid_player_ids)]
    stats["transfers.csv"] = (len(transfers), len(transfers_clean))
    log(f"  Salvo transfers_clean.csv: {len(transfers):,} -> {len(transfers_clean):,}")

    # Process player_valuations
    log("Processando player_valuations.csv...")
    valuations = pd.read_csv(RAW_DIR / "player_valuations.csv")
    valuations_clean = valuations[valuations["player_id"].isin(valid_player_ids)]
    stats["player_valuations.csv"] = (len(valuations), len(valuations_clean))
    log(f"  Salvo player_valuations_clean.csv: {len(valuations):,} -> {len(valuations_clean):,}")

    # Process club_games
    log("Processando club_games.csv...")
    club_games = pd.read_csv(RAW_DIR / "club_games.csv")
    club_games_clean = club_games[club_games["game_id"].isin(valid_game_ids)]
    stats["club_games.csv"] = (len(club_games), len(club_games_clean))
    log(f"  Salvo club_games_clean.csv: {len(club_games):,} -> {len(club_games_clean):,}")

    log("Processamento concluido.")
    print_report(stats)

    return {
        "players": players_clean,
        "games": games_clean,
        "appearances": appearances_clean,
        "game_lineups": lineups_clean,
        "game_events": events_clean,
        "transfers": transfers_clean,
        "player_valuations": valuations_clean,
        "club_games": club_games_clean,
    }


def print_report(stats: dict) -> None:
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


# ==================== AVATAR GENERATION FUNCTIONS ====================

def load_club_colors() -> dict:
    """Load club colors from JSON file."""
    if not CLUB_COLORS_FILE.exists():
        log(f"Warning: {CLUB_COLORS_FILE} not found. Using default colors.")
        return {}
    with open(CLUB_COLORS_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def load_existing_avatars_from_db() -> dict[int, tuple[str, bool]]:
    """Load existing image URLs and manual status from football.db."""
    if not DB_PATH.exists():
        return {}
    conn = sqlite3.connect(DB_PATH)
    try:
        # Check if is_manual column exists
        has_manual_column = conn.execute(
            "PRAGMA table_info(players)"
        ).fetchall()
        has_manual = any(col[1] == 'is_manual' for col in has_manual_column)
        
        if has_manual:
            rows = conn.execute("SELECT player_id, image_url, is_manual FROM players WHERE image_url IS NOT NULL").fetchall()
            return {pid: (url, bool(manual)) for pid, url, manual in rows}
        else:
            rows = conn.execute("SELECT player_id, image_url FROM players WHERE image_url IS NOT NULL").fetchall()
            return {pid: (url, False) for pid, url in rows}
    finally:
        conn.close()


def get_color_hex(color_name: str) -> str:
    """Map Portuguese color names to hex codes."""
    color_map = {
        "Vermelho": "#e53935",
        "Azul": "#1e88e5",
        "Amarelo": "#fdd835",
        "Verde": "#43a047",
        "Preto": "#212121",
        "Branco": "#ffffff",
        "Laranja": "#fb8c00",
        "Vinho": "#7b1fa2",
        "Cinza": "#757575",
        "Violeta": "#8e24aa",
        "Rosa": "#ec407a",
        "Celeste": "#4fc3f7",
        "Xadrez": "#424242",
        "Dourado": "#ffb300",
        "Bordo": "#880e4f",
        "Roxo": "#6a1b9a",
        "Grená": "#8d6e63",
        "Azul marinho": "#0d47a1",
        "Borgonha": "#7b1fa2",
        "Marrom": "#795548"
    }
    return color_map.get(color_name, "#757575")


def update_clothing_color_in_url(url: str, new_clothing_color: str) -> str:
    """Update only the clothingColor parameter in an existing DiceBear URL."""
    if "?" not in url:
        return url
    
    base_url, query_string = url.split("?", 1)
    params = urllib.parse.parse_qs(query_string)
    
    params["clothingColor"] = [new_clothing_color]
    
    new_query = urllib.parse.urlencode(params, doseq=True)
    return f"{base_url}?{new_query}"


def get_clothing_color_from_url(url: str) -> str | None:
    """Extract the current clothingColor parameter from a DiceBear URL."""
    if "?" not in url:
        return None
    
    _, query_string = url.split("?", 1)
    params = urllib.parse.parse_qs(query_string)
    
    if "clothingColor" in params and params["clothingColor"]:
        return params["clothingColor"][0]
    
    return None


def extract_visual_params_from_url(url: str) -> dict[str, str]:
    """Extract all visual parameters from a DiceBear URL."""
    params = {
        "skin_color": None,
        "hair_color": None,
        "head_variant": None,
        "expression_variant": None,
        "clothing_color": None,
        "facial_hair_probability": None,
        "facial_hair_variant": None
    }
    
    if "?" not in url:
        return params
    
    _, query_string = url.split("?", 1)
    url_params = urllib.parse.parse_qs(query_string)
    
    if "skinColor" in url_params and url_params["skinColor"]:
        params["skin_color"] = url_params["skinColor"][0].lstrip("#")
    
    if "headContrastColor" in url_params and url_params["headContrastColor"]:
        params["hair_color"] = url_params["headContrastColor"][0].lstrip("#")
    
    if "headVariant" in url_params and url_params["headVariant"]:
        params["head_variant"] = url_params["headVariant"][0]
    
    if "expressionVariant" in url_params and url_params["expressionVariant"]:
        params["expression_variant"] = url_params["expressionVariant"][0]
    
    if "clothingColor" in url_params and url_params["clothingColor"]:
        params["clothing_color"] = url_params["clothingColor"][0].lstrip("#")
    
    if "facialHairProbability" in url_params and url_params["facialHairProbability"]:
        params["facial_hair_probability"] = url_params["facialHairProbability"][0]
    
    if "facialHairVariant" in url_params and url_params["facialHairVariant"]:
        params["facial_hair_variant"] = url_params["facialHairVariant"][0]
    
    return params


def ensure_url_params(url: str) -> str:
    """Ensure required parameters (accessoriesProbability, maskProbability) exist and are set to 0."""
    if "?" not in url:
        return url
    
    base_url, query_string = url.split("?", 1)
    params = urllib.parse.parse_qs(query_string)
    
    # Ensure required parameters exist and are set to 0
    params["accessoriesProbability"] = ["0"]
    params["maskProbability"] = ["0"]
    
    # Ensure facialHairVariant exists (set to none if missing)
    if "facialHairVariant" not in params or not params["facialHairVariant"]:
        params["facialHairVariant"] = ["none"]
    
    # Rebuild query string
    new_query = urllib.parse.urlencode(params, doseq=True)
    return f"{base_url}?{new_query}"


def detect_name_pattern(name: str) -> str | None:
    """Detect name pattern to determine avatar region based on player name."""
    if not name:
        return None
    
    name_lower = str(name).lower()
    name_parts = name_lower.split()
    
    # North Africa
    north_africa_names = {
        "abdallah", "abdel", "abdelaziz", "abdelhamid", "abdelkader", "abdelrahman", 
        "abderrazak", "abdi", "abdullah", "abed", "achraf", "adel", "adil", "ahmed", 
        "ahmadi", "ait", "ait-nouri", "akram", "amallah", "amine", "amrabat", 
        "ayoub", "aziz", "barka", "belhanda", "benali", "benatia", "bensebaini", 
        "benzema", "bennacer", "bennani", "benrahma", "bentaleb", "boufal", "bounou", 
        "bouzidi", "chadi", "cheddira", "elkaabi", "elneny", "faouzi", "faris", 
        "fathi", "fouad", "hakimi", "hakim", "hamza", "hani", "harit", "hassan", 
        "hatem", "ismael", "ismaël", "jawad", "karim", "khalid", "khalifa", 
        "khalil", "khannouss", "lotfi", "mahdi", "mahrez", "majid", "mansour", "mehdi", 
        "meriah", "mohamed", "mohammed", "mounir", "moustapha", "mustafa", "nabil", 
        "nasri", "nouredine", "nouri", "omar", "ounahi", "rafik", "rachid", "ramy", 
        "reda", "riyad", "sabiri", "saiss", "salah", "salem", "samy", "soufiane", 
        "taha", "tarek", "yacine", "yahya", "yassine", "younes", "youssef", "zakaria", 
        "ziyech", "zouheir"
    }
    
    for part in name_parts:
        if part.startswith(("el-", "al-", "ait-")):
            return "north_africa"
    
    for na_name in north_africa_names:
        if na_name in name_parts:
            return "north_africa"
    
    # Sub-Saharan Africa
    accent_exceptions = {
        "rené", "josé", "andré", "gérard", "stéphane", "frédéric", "théodore", 
        "jérôme", "sébastien", "nicolas", "philippe", "christophe", "jean", 
        "pierre", "jacques", "michel", "laurent", "xavier", "olivier", "fabien",
        "julien", "antoine", "matthieu", "guillaume", "alexandre", "thomas",
        "victor", "hugo", "louis", "gabriel", "léo", "théo", "enzo", "noé",
        "lucas", "nathan", "noah", "léo", "théo", "enzo", "noé", "mathéo",
        "timothé", "clément", "maxime", "cédric", "cyprien", "élie", "émeric",
        "étienne", "évariste", "fabrice", "félix", "florent", "françois", "gaston",
        "gaston", "gauthier", "gédéon", "georges", "gilles", "grégory", "guillaume",
        "gustave", "henri", "honoré", "hubert", "ignace", "jacques", "jean",
        "jérémie", "jonas", "joseph", "jules", "julien", "justin", "kévin",
        "laurent", "lazare", "léon", "léopold", "louis", "luc", "lucien", "lucien",
        "malo", "marc", "marcel", "martial", "mathias", "mathieu", "mathis",
        "maurice", "maxence", "maxime", "méhdi", "michel", "narcisse", "nicolas",
        "noël", "noé", "octave", "olivier", "pascal", "paul", "pierre", "philippe",
        "raphaël", "raoul", "rémi", "rené", "rémy", "richard", "robert", "robin",
        "roland", "romain", "samuel", "sébastien", "simon", "stéphane", "sylvain",
        "théo", "théodore", "thibault", "thibaut", "thomas", "timothé", "valentin",
        "victor", "vincent", "xavier", "yves", "zacharie"
    }
    
    for part in name_parts:
        if part.endswith("é") and part not in accent_exceptions:
            return "sub_saharan_africa"
    
    sub_saharan_names = {
        "abdou", "abdoulaye", "aboubakar", "adingra", "adebayo", "adebayor", "adou", 
        "agbadu", "aguerd", "aina", "akanji", "amara", "amadou", "amoah", "awoniyi", 
        "bakambu", "bakary", "bakayoko", "bamba", "bassey", "batshuayi", "bell", 
        "bissouma", "boga", "boly", "bonaventure", "camara", "kamara", "coulibaly", 
        "koulibaly", "daouda", "dembele", "dembélé", "dia", "diaby", "diakite", 
        "diakité", "diallo", "diarra", "diomande", "diomandé", "doucoure", "doucouré", 
        "dramé", "dumbuya", "eboue", "eboué", "ekong", "essien", "fofana", "gomis", 
        "gueye", "gyasi", "haidara", "ibrahima", "idrissa", "issa", "kanoute", "kanouté", 
        "kante", "kanté", "keita", "keïta", "kessie", "kessié", "konate", "konaté", 
        "kouame", "kouamé", "kudus", "lukeba", "mahmadou", "mane", "mané", "mamadou", 
        "mendy", "moukoko", "moussa", "mukiele", "ndiaye", "ndicka", "niakhate", 
        "niakhaté", "niasse", "nkoulou", "nkunku", "nsame", "nzonzi", "obafemi", 
        "onana", "openda", "osimhen", "ouattara", "ouedraogo", "ouédraogo", "ousmane", 
        "owusu", "partey", "sacko", "sakho", "saliba", "samba", "sangare", "sangaré", 
        "sarr", "sekou", "seydou", "sidibe", "sidibé", "sissoko", "souare", "souaré", 
        "soumare", "soumaré", "tapsoba", "tchouameni", "tchouaméni", "traore", "traoré", 
        "toure", "touré", "upamecano", "wan-bissaka", "weah", "yahia", "yaya", 
        "yattara", "yeboah", "youssouf", "zaha", "zambo", "zouma", "zoumana", 
        "mainoo", "saka", "eze", "olise", "balogun", "tomori", "chalobah", 
        "adarabioyo", "madueke", "akinfenwa", "iheanacho", "onyeka", "aribo", 
        "ekitike", "disasi", "kalulu", "kolo muani", "sangante", "agbadou", 
        "konsa", "guehi", "balogun", "sarr", "camavinga", "rüdiger", "tah", "maignan", 
        "thuram", "mateta", "lukaku", "doku" , "onana", "lukebakio", "brobbey",
        "gravenberch", "dumfries", "isak", "sumerville", "elanga", "aké", "rashford"
    }
    
    for ss_name in sub_saharan_names:
        if ss_name in name_parts:
            return "sub_saharan_africa"
    
    # Latino
    latino_names = {
        "aguirre", "alvarez", "álvarez", "arias", "benitez", "benítez", "cabrera", 
        "campos", "cardozo", "cardoso", "castillo", "castro", "chavez", "chávez", 
        "correa", "cortes", "cortés", "cuesta", "diaz", "díaz", "dominguez", 
        "domínguez", "duarte", "escobar", "estrada", "fernandes", "fernandez", 
        "fernández", "ferreira", "figueroa", "flores", "fuentes", "gallardo", 
        "garcia", "garcía", "gomez", "gómez", "gonzalez", "gonzález", "guerrero", 
        "gutierrez", "gutiérrez", "herrera", "ibarra", "jimenez", "jiménez", 
        "lopez", "lópez", "machado", "medina", "mendez", "méndez", 
        "mendoza", "miranda", "molina", "montes", "morales", "moreno", "munoz", 
        "muñoz", "navarro", "nunes", "oliveira", "ortega", "paredes", "pereira", 
        "perez", "pérez", "quintana", "ramirez", "ramírez", "ramos", "rendon", 
        "rendón", "reyes", "ribeiro", "rios", "ríos", "rivera", "rocha", 
        "rodrigues", "rodriguez", "rodríguez", "romero", "salazar", "sanchez", 
        "sánchez", "santana", "santos", "silva", "sosa", "souza", "suarez", 
        "suárez", "torres", "valdez", "valdés", "vargas", "vasquez", "vázquez", 
        "vera", "villarreal", "alejandro", "andres", "andrés", "angel", "ángel", 
        "carlos", "cristian", "cristiano", "daniel", "diego", "eduardo", 
        "enzo", "esteban", "facundo", "federico", "fernando", "gabriel", "gonzalo", 
        "guillermo", "hector", "héctor", "javier", "joao", "joão", "jorge", "jose", 
        "josé", "juan", "julio", "leonardo", "luis", "manuel", "marcos", "martin", 
        "martín", "mateo", "matias", "matías", "miguel", "nicolas", "nicolás", 
        "pablo", "pedro", "rafael", "ricardo", "roberto", "rodrigo", "sergio", 
        "thiago", "tomas", "tomás", "victor", "víctor"
    }
    
    for latino_name in latino_names:
        if latino_name in name_parts:
            return "latin_america"
    
    return None


def get_avatar_url(player_id: int, name: str, country: str, position: str, sub_position: str, club: str | None = None, club_colors: dict | None = None) -> tuple[str, dict[str, str]]:
    rng = random.Random(player_id)
    
    country_lower = str(country).lower().strip() if pd.notna(country) else ""
    sub_pos_lower = str(sub_position).lower().strip() if pd.notna(sub_position) else ""
    
    is_winger_or_am = "winger" in sub_pos_lower or "attacking midfield" in sub_pos_lower
    
    # Country sets
    asia_countries = {
        "japan", "south korea", "korea, south", "china", "north korea", "korea, north", "taiwan", "india", "vietnam", "thailand", 
        "indonesia", "malaysia", "singapore", "philippines", "iran", "iraq", "saudi arabia", "uae", 
        "qatar", "oman", "jordan", "lebanon", "syria", "uzbekistan", "kyrgyzstan", 
        "tajikistan", "turkmenistan", "kazakhstan", "yemen", "palestine", "kuwait", "bahrain", 
        "bangladesh", "pakistan", "afghanistan", "nepal", "bhutan", "sri lanka", "maldives", 
        "mongolia", "myanmar", "cambodia", "laos", "brunei", "timor-leste"
    }
    
    sub_saharan_africa = {
        "nigeria", "senegal", "cameroon", "cote d'ivoire", "ivory coast", "ghana", "mali", "guinea", 
        "dr congo", "congo", "angola", "south africa", "kenya", "zambia", "zimbabwe", "uganda", 
        "gabon", "togo", "benin", "burkina faso", "liberia", "sierra_leone", "sierra leone", "cape verde", 
        "guinea-bissau", "gambia", "the gambia", "niger", "chad", "central african republic", "south sudan", 
        "eritrea", "ethiopia", "somalia", "rwanda", "burundi", "tanzania", "malawi", "mozambique", 
        "namibia", "botswana", "lesotho", "eswatini", "madagascar", "mauritius", "seychelles", 
        "comoros", "equatorial guinea", "sao tome and principe", "ecuador"
    }
    
    north_africa = {"morocco", "algeria", "tunisia", "egypt", "libya", "sudan", "mauritania"}
    
    group_4_countries = {
        "argentina", "uruguay", "switzerland", "italy", "hungary", "poland", "scotland", 
        "croatia", "serbia", "bosnia-herzegovina", "slovenia", "montenegro", "north macedonia", 
        "albania", "kosovo", "bulgaria", "greece", "romania", "czech republic", "slovakia",
        "turkey", "trkiye", "türkiye"
    }
    
    nordic_leste = {
        "denmark", "sweden", "norway", "finland", "iceland", "ukraine", "estonia", "russia", "latvia", "lithuania"
    }
    
    latin_america = {
        "brazil", "mexico", "colombia", "peru", "chile", "ecuador", "venezuela", "bolivia", 
        "paraguay", "costa rica", "panama", "honduras", "el salvador", "guatemala", "nicaragua", 
        "cuba", "dominican republic", "puerto rico", "jamaica", "trinidad and tobago", "haiti"
    }
    
    europe_north_america = {
        "united states", "usa", "canada", "england", "france", "germany", "switzerland", 
        "netherlands", "netherland", "holland", "belgium", "austria", "ireland", 
        "scotland", "wales", "northern ireland", "italy", "spain", "portugal", 
        "greece", "sweden", "norway", "denmark", "finland", "iceland", "poland", 
        "czech republic", "czechia", "slovakia", "hungary", "romania", "bulgaria", 
        "croatia", "serbia", "slovenia", "bosnia and herzegovina", "montenegro", 
        "north macedonia", "albania", "kosovo", "estonia", "latvia", "lithuania", 
        "ukraine", "belarus", "russia", "moldova", "luxembourg", "liechtenstein", 
        "monaco", "san marino", "andorra", "malta", "cyprus", "israel", "armenia", 
        "azerbaijan", "georgia"
    }
    
    # Check name patterns first
    name_region = detect_name_pattern(name)
    
    if name_region and country_lower in europe_north_america:
        if name_region == "north_africa":
            country_lower = "morocco"
        elif name_region == "sub_saharan_africa":
            country_lower = "nigeria"
        elif name_region == "latin_america":
            country_lower = "argentina"
    elif country_lower in europe_north_america and not name_region:
        country_lower = "argentina"
    elif name_region and country_lower not in europe_north_america:
        name_region = None
    
    # Default options
    skin_color = "ffdbb4"
    expression = "blank"
    head = "short1"
    hair_color = "000000"
    
    if country_lower in asia_countries:
        skin_color = rng.choice(["ffdbb4", "fd9841", "edb98a", "f2c18d"])
        expression = "cute"
        head = rng.choices(["flatTop", "grayShort", "short5", "short2"], weights=[10, 30, 30, 30])[0]
        hair_color = rng.choice(["000000", "1a1a1a", "2c1b18"])
        
    elif country_lower in sub_saharan_africa:
        skin_color = rng.choice(["6f4f1d", "4a3728", "3c2e25", "a1662f", "8d5524"])
        if is_winger_or_am:
            expression = rng.choice(["smileTeethGap", "smileBig"])
        else:
            expression = rng.choice(["blank", "concerned", "explaining", "hectic"])
        head = rng.choice(["short1", "short2"])
        hair_color = rng.choice(["000000", "1a1a1a", "2c1b18"])
        
    elif country_lower in north_africa:
        skin_color = rng.choice(["d2b48c", "c68642", "ae703b", "8d5524"])
        expression = rng.choice(["blank", "concerned", "suspicious", "cheeky", "contempt"])
        head = rng.choice(["short1", "short2", "flatTop", "shaved2", "short4", "twists"])
        hair_color = rng.choice(["000000", "1a1a1a", "2c1b18", "4a3728"])
        
    elif country_lower in group_4_countries:
        skin_color = rng.choice(["ffdbb4", "f8d25c", "fd9841", "edb98a", "f2c18d"])
        expression = rng.choice(["blank", "cheeky", "contempt", "explaining", "tired", "suspicious"])
        head = rng.choice(["short1", "short2", "short4", "short5", "grayShort"])
        hair_color = rng.choice(["000000", "1a1a1a", "2c1b18"])
        
    elif country_lower in ["portugal", "spain"]:
        skin_color = rng.choice(["ffdbb4", "f8d25c", "fd9841", "edb98a", "f2c18d"])
        expression = rng.choice(["blank", "cheeky", "contempt", "explaining", "hectic", "suspicious"])
        head = rng.choice(["short1", "short2", "short4", "short5", "grayShort"])
        hair_color = rng.choice(["000000", "1a1a1a", "2c1b18"])
        
    elif country_lower in nordic_leste:
        skin_color = rng.choice(["ffdbb4", "f8d25c", "fd9841", "edb98a", "f2c18d"])
        expression_chosen = rng.choice(["blank", "cheeky", "contempt", "explaining", "hectic", "tired"])
        head_chosen = rng.choice(["short1", "short2", "short4", "short5", "grayShort", "smile"])
        if head_chosen == "smile":
            head = "short1"
            expression = "smile"
        else:
            head = head_chosen
            expression = expression_chosen
        hair_color = rng.choice(["fbe7a1", "f3e5ab", "e6c229", "d4af37"])
        
    elif country_lower in latin_america:
        skin_color = rng.choice(["fd9841", "edb98a", "f2c18d", "c68642"])
        expression = rng.choice(["smileBig", "smileTeethGap", "explaining", "cheeky", "smile"])
        head = rng.choice(["short1", "short2", "shaved2", "short4"])
        hair_color = rng.choice(["000000", "1a1a1a", "2c1b18", "3d2314"])
        
    else:
        skin_color = rng.choice(["ffdbb4", "f8d25c", "fd9841", "edb98a", "f2c18d"])
        expression = rng.choice(["blank", "cheeky", "contempt", "explaining"])
        head = rng.choice(["short1", "short2", "short4", "short5", "grayShort"])
        hair_color = rng.choice(["000000", "1a1a1a", "2c1b18"])
    
    # Get club color for clothing
    clothing_color = "#757575"
    if club and club_colors:
        club_data = club_colors.get(club)
        if club_data and "Principal" in club_data:
            color_name = club_data["Principal"]
            clothing_color = get_color_hex(color_name)
    
    # Build visual params dict
    visual_params = {
        "skin_color": skin_color,
        "hair_color": hair_color,
        "head_variant": head,
        "expression_variant": expression,
        "clothing_color": clothing_color.lstrip("#"),
        "facial_hair_probability": "0",
        "facial_hair_variant": "none"
    }
    
    encoded_params = urllib.parse.urlencode({
        "seed": str(player_id),
        "skinColor": f"#{skin_color}",
        "headContrastColor": f"#{hair_color}",
        "headVariant": head,
        "expressionVariant": expression,
        "clothingColor": clothing_color,
        "accessoriesProbability": "0",
        "maskProbability": "0",
        "facialHairProbability": "0",
        "facialHairVariant": "none"
    })
    
    url = f"https://api.dicebear.com/10.x/open-peeps/svg?{encoded_params}"
    return url, visual_params


def generate_avatars(players: pd.DataFrame, force: bool = False) -> pd.DataFrame:
    """Generate avatars for players and return updated DataFrame."""
    log(f"Carregando {CLUB_COLORS_FILE}...")
    club_colors = load_club_colors()
    
    log("Carregando avatares existentes do football.db...")
    existing_avatars = load_existing_avatars_from_db()
    log(f"  Encontrados {len(existing_avatars):,} avatares existentes")

    avatar_updates = 0
    avatar_preserved = 0
    manual_preserved = 0
    new_image_urls = []
    is_manual_flags = []
    
    # Visual columns
    skin_colors = []
    hair_colors = []
    head_variants = []
    expression_variants = []
    clothing_colors = []
    facial_hair_probs = []
    facial_hair_variants = []

    log("Gerando/atualizando avatares para todos os jogadores...")
    for _, row in players.iterrows():
        pid = int(row["player_id"])
        player_name = row["name"]
        current_club = row["current_club_name"] if pd.notna(row["current_club_name"]) else None
        
        saved_data = existing_avatars.get(pid)
        
        if saved_data:
            saved_url, is_manual = saved_data
            
            # Ensure URL has required parameters
            saved_url = ensure_url_params(saved_url)
            
            # Extract visual params from saved URL
            visual_params = extract_visual_params_from_url(saved_url)
            
            # Manual avatars: update clothing color but preserve everything else
            if is_manual:
                if current_club and current_club in club_colors:
                    club_data = club_colors[current_club]
                    if "Principal" in club_data:
                        color_name = club_data["Principal"]
                        new_clothing_color = get_color_hex(color_name)
                        current_color = get_clothing_color_from_url(saved_url)
                        # Only update if color actually changed
                        if current_color != new_clothing_color:
                            updated_url = update_clothing_color_in_url(saved_url, new_clothing_color)
                            new_image_urls.append(updated_url)
                            is_manual_flags.append(1)
                            visual_params["clothing_color"] = new_clothing_color.lstrip("#")
                            manual_preserved += 1
                            # Append visual params
                            skin_colors.append(visual_params["skin_color"])
                            hair_colors.append(visual_params["hair_color"])
                            head_variants.append(visual_params["head_variant"])
                            expression_variants.append(visual_params["expression_variant"])
                            clothing_colors.append(visual_params["clothing_color"])
                            facial_hair_probs.append(visual_params["facial_hair_probability"])
                            facial_hair_variants.append(visual_params["facial_hair_variant"])
                            continue
                
                new_image_urls.append(saved_url)
                is_manual_flags.append(1)
                manual_preserved += 1
                # Append visual params
                skin_colors.append(visual_params["skin_color"])
                hair_colors.append(visual_params["hair_color"])
                head_variants.append(visual_params["head_variant"])
                expression_variants.append(visual_params["expression_variant"])
                clothing_colors.append(visual_params["clothing_color"])
                facial_hair_probs.append(visual_params["facial_hair_probability"])
                facial_hair_variants.append(visual_params["facial_hair_variant"])
                continue
            
            # Auto avatars: regenerate if force, otherwise update clothing color if club changed
            if force:
                # Regenerate avatar completely
                avatar_url, visual_params = get_avatar_url(
                    player_id=pid,
                    name=player_name,
                    country=row["country_of_citizenship"],
                    position=row["position"],
                    sub_position=row["sub_position"],
                    club=current_club,
                    club_colors=club_colors
                )
                new_image_urls.append(avatar_url)
                is_manual_flags.append(0)
                avatar_updates += 1
                # Append visual params
                skin_colors.append(visual_params["skin_color"])
                hair_colors.append(visual_params["hair_color"])
                head_variants.append(visual_params["head_variant"])
                expression_variants.append(visual_params["expression_variant"])
                clothing_colors.append(visual_params["clothing_color"])
                facial_hair_probs.append(visual_params["facial_hair_probability"])
                facial_hair_variants.append(visual_params["facial_hair_variant"])
            else:
                # Update clothing color if club changed
                if current_club and current_club in club_colors:
                    club_data = club_colors[current_club]
                    if "Principal" in club_data:
                        color_name = club_data["Principal"]
                        new_clothing_color = get_color_hex(color_name)
                        current_color = get_clothing_color_from_url(saved_url)
                        # Only update if color actually changed
                        if current_color != new_clothing_color:
                            updated_url = update_clothing_color_in_url(saved_url, new_clothing_color)
                            new_image_urls.append(updated_url)
                            is_manual_flags.append(0)
                            visual_params["clothing_color"] = new_clothing_color.lstrip("#")
                            avatar_updates += 1
                            # Append visual params
                            skin_colors.append(visual_params["skin_color"])
                            hair_colors.append(visual_params["hair_color"])
                            head_variants.append(visual_params["head_variant"])
                            expression_variants.append(visual_params["expression_variant"])
                            clothing_colors.append(visual_params["clothing_color"])
                            facial_hair_probs.append(visual_params["facial_hair_probability"])
                            facial_hair_variants.append(visual_params["facial_hair_variant"])
                            continue
                
                new_image_urls.append(saved_url)
                is_manual_flags.append(0)
                avatar_preserved += 1
                # Append visual params
                skin_colors.append(visual_params["skin_color"])
                hair_colors.append(visual_params["hair_color"])
                head_variants.append(visual_params["head_variant"])
                expression_variants.append(visual_params["expression_variant"])
                clothing_colors.append(visual_params["clothing_color"])
                facial_hair_probs.append(visual_params["facial_hair_probability"])
                facial_hair_variants.append(visual_params["facial_hair_variant"])
        else:
            # Generate new avatar
            avatar_url, visual_params = get_avatar_url(
                player_id=pid,
                name=player_name,
                country=row["country_of_citizenship"],
                position=row["position"],
                sub_position=row["sub_position"],
                club=current_club,
                club_colors=club_colors
            )
            
            new_image_urls.append(avatar_url)
            is_manual_flags.append(0)
            avatar_updates += 1
            # Append visual params
            skin_colors.append(visual_params["skin_color"])
            hair_colors.append(visual_params["hair_color"])
            head_variants.append(visual_params["head_variant"])
            expression_variants.append(visual_params["expression_variant"])
            clothing_colors.append(visual_params["clothing_color"])
            facial_hair_probs.append(visual_params["facial_hair_probability"])
            facial_hair_variants.append(visual_params["facial_hair_variant"])

    players["image_url"] = new_image_urls
    players["is_manual"] = is_manual_flags
    players["skin_color"] = skin_colors
    players["hair_color"] = hair_colors
    players["head_variant"] = head_variants
    players["expression_variant"] = expression_variants
    players["clothing_color"] = clothing_colors
    players["facial_hair_probability"] = facial_hair_probs
    players["facial_hair_variant"] = facial_hair_variants
    
    log(f"Concluido. Avatares gerados/atualizados: {avatar_updates:,} | Preservados: {avatar_preserved:,} | Manuais preservados: {manual_preserved:,}")
    
    return players


# ==================== DATABASE IMPORT FUNCTIONS ====================

def import_dataframe_to_sql(conn: sqlite3.Connection, df: pd.DataFrame, table: str) -> int:
    """Import DataFrame to SQLite table."""
    total = 0
    first_chunk = True

    for chunk in [df[i:i+CHUNK_SIZE] for i in range(0, len(df), CHUNK_SIZE)]:
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


def import_reference_files(conn: sqlite3.Connection) -> None:
    """Import reference CSV files to database directly from raw/ directory."""
    for csv_name, table in TABLES[:4]:  # First 4 are reference files
        csv_path = RAW_DIR / csv_name
        log(f"Importando {csv_name} -> {table}...")
        df = pd.read_csv(csv_path)
        import_dataframe_to_sql(conn, df, table)


def create_indexes(conn: sqlite3.Connection) -> None:
    for name, table, column in INDEXES:
        conn.execute(f"CREATE INDEX IF NOT EXISTS {name} ON {table} ({column})")


def print_db_summary(conn: sqlite3.Connection) -> None:
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


def import_to_database(dataframes: dict) -> None:
    """Import all data to SQLite database."""
    # No need to preserve avatars - they are handled in generate_avatars
    if DB_PATH.exists():
        DB_PATH.unlink()
        log(f"Removido {DB_PATH} anterior.")

    log(f"Criando {DB_PATH}...")
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    try:
        # Import reference files
        import_reference_files(conn)

        # Import processed dataframes
        for table_name, df in dataframes.items():
            log(f"Importando {table_name} -> {table_name}...")
            import_dataframe_to_sql(conn, df, table_name)

        log("Criando indices...")
        create_indexes(conn)
        conn.commit()
        log("Importacao concluida.")
        print_db_summary(conn)
    finally:
        conn.close()


# ==================== MAIN PIPELINE ====================

def main() -> None:
    force_avatars = "--force" in sys.argv
    skip_avatars = "--skip-avatars" in sys.argv

    log("=" * 60)
    log("FOOTBALL DATA PIPELINE")
    log("=" * 60)
    log("")

    # Step 1: Process raw data
    log("STEP 1: Processando dados brutos...")
    log("-" * 60)
    dataframes = process_raw_data()
    log("")

    # Step 2: Generate avatars
    if not skip_avatars:
        log("STEP 2: Gerando avatares...")
        log("-" * 60)
        dataframes["players"] = generate_avatars(dataframes["players"], force=force_avatars)
        log("")
    else:
        log("STEP 2: Geracao de avatares pulada (--skip-avatars)")
        log("")

    # Step 3: Import to database
    log("STEP 3: Importando para football.db...")
    log("-" * 60)
    import_to_database(dataframes)
    log("")

    log("=" * 60)
    log("PIPELINE CONCLUIDO COM SUCESSO")
    log("=" * 60)


if __name__ == "__main__":
    main()
