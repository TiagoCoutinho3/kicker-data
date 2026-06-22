"""Unified pipeline: process raw data, generate avatars, import to football.db."""

from __future__ import annotations

import json
import random
import shutil
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
AVATAR_STATE_FILE = Path("players_avatar_state.json")

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


def copy_reference(name: str, stats: dict) -> None:
    src = RAW_DIR / name
    dst = CLEAN_DIR / name
    log(f"Copiando {name}...")
    shutil.copy2(src, dst)
    row_count = sum(1 for _ in src.open("r", encoding="utf-8")) - 1
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


def process_raw_data() -> dict:
    """Process raw CSVs and return DataFrames for import."""
    log("Iniciando processamento do dataset Transfermarkt...")
    CLEAN_DIR.mkdir(exist_ok=True)
    stats: dict[str, tuple[int, int]] = {}

    # Copy reference files
    for name in REFERENCE_FILES:
        copy_reference(name, stats)

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


def load_avatar_state() -> dict:
    """Load avatar state from JSON file."""
    if not AVATAR_STATE_FILE.exists():
        return {"avatars": {}, "overrides": {}}
    with open(AVATAR_STATE_FILE, "r", encoding="utf-8") as f:
        state = json.load(f)
    
    # Convert old format (just URL) to new format (with name)
    for pid, data in state["avatars"].items():
        if isinstance(data, str):
            state["avatars"][pid] = {"name": "", "url": data}
    
    return state


def save_avatar_state(state: dict) -> None:
    """Save avatar state to JSON file."""
    with open(AVATAR_STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


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
        "konsa", "guehi"
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


def get_avatar_url(player_id: int, name: str, country: str, position: str, sub_position: str, club: str | None = None, club_colors: dict | None = None) -> str:
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
        "comoros", "equatorial guinea", "sao tome and principe"
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
        skin_color = rng.choice(["ffdbb4", "f8d25c", "fd9841", "edb98a", "f2c18d"])
        expression = "cute"
        head = rng.choices(["flatTop", "grayShort", "short5", "short2"], weights=[10, 30, 30, 30])[0]
        hair_color = rng.choice(["000000", "1a1a1a", "2c1b18"])
        
    elif country_lower in sub_saharan_africa:
        skin_color = rng.choice(["6f4f1d", "4a3728", "3c2e25", "2d1d16"])
        if is_winger_or_am:
            expression = rng.choice(["smileTeethGap", "smileBig"])
        else:
            expression = rng.choice(["blank", "concerned", "explaining", "driven"])
        head = rng.choice(["short1", "short2"])
        hair_color = rng.choice(["000000", "1a1a1a", "2c1b18"])
        
    elif country_lower in north_africa:
        skin_color = rng.choice(["d2b48c", "c68642", "ae703b", "8d5524"])
        expression = rng.choice(["blank", "concerned", "suspicious", "cheeky", "contempt"])
        head = rng.choice(["short1", "short2", "flatTop", "shaved2", "short4"])
        hair_color = rng.choice(["000000", "1a1a1a", "2c1b18", "4a3728"])
        
    elif country_lower in group_4_countries:
        skin_color = rng.choice(["ffdbb4", "f8d25c", "fd9841", "edb98a", "f2c18d"])
        expression = rng.choice(["blank", "cheeky", "contempt", "explaining"])
        head = rng.choice(["short1", "short2", "short4", "short5", "grayShort"])
        hair_color = rng.choice(["000000", "1a1a1a", "2c1b18"])
        
    elif country_lower in ["portugal", "spain"]:
        skin_color = rng.choice(["ffdbb4", "f8d25c", "fd9841", "edb98a", "f2c18d"])
        expression = rng.choice(["blank", "cheeky", "contempt", "explaining"])
        head = rng.choice(["short1", "short2", "short4", "short5", "grayShort"])
        hair_color = rng.choice(["000000", "1a1a1a", "2c1b18"])
        
    elif country_lower in nordic_leste:
        skin_color = rng.choice(["ffdbb4", "f8d25c", "fd9841", "edb98a", "f2c18d"])
        expression_chosen = rng.choice(["blank", "cheeky", "contempt", "explaining"])
        head_chosen = rng.choice(["short1", "short2", "short4", "short5", "grayShort", "smile"])
        if head_chosen == "smile":
            head = "short1"
            expression = "smile"
        else:
            head = head_chosen
            expression = expression_chosen
        hair_color = rng.choice(["fbe7a1", "f3e5ab", "e6c229", "d4af37"])
        
    elif country_lower in latin_america:
        skin_color = rng.choice(["ffdbb4", "f8d25c", "fd9841", "edb98a", "f2c18d"])
        expression = rng.choice(["smileBig", "smileTeethGap", "explaining"])
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
    
    encoded_params = urllib.parse.urlencode({
        "seed": str(player_id),
        "skinColor": f"#{skin_color}",
        "headContrastColor": f"#{hair_color}",
        "headVariant": head,
        "expressionVariant": expression,
        "clothingColor": clothing_color,
        "accessoriesProbability": "0",
        "maskProbability": "0",
        "facialHairProbability": "0"
    })
    
    return f"https://api.dicebear.com/10.x/open-peeps/svg?{encoded_params}"


def generate_avatars(players: pd.DataFrame, force: bool = False) -> pd.DataFrame:
    """Generate avatars for players and return updated DataFrame."""
    log(f"Carregando {CLUB_COLORS_FILE}...")
    club_colors = load_club_colors()
    
    log(f"Carregando {AVATAR_STATE_FILE}...")
    avatar_state = load_avatar_state()

    avatar_updates = 0
    avatar_preserved = 0
    new_image_urls = []
    updated_state = {"avatars": {}, "overrides": avatar_state.get("overrides", {})}

    log("Gerando avatares para todos os jogadores...")
    for _, row in players.iterrows():
        pid = str(row["player_id"])
        player_name = row["name"]
        current_club = row["current_club_name"] if pd.notna(row["current_club_name"]) else None
        
        saved_data = avatar_state["avatars"].get(pid)
        saved_url = saved_data["url"] if saved_data and isinstance(saved_data, dict) else saved_data if saved_data else None
        has_override = pid in avatar_state["overrides"]
        
        if saved_url and not force and not has_override:
            if current_club and current_club in club_colors:
                club_data = club_colors[current_club]
                if "Principal" in club_data:
                    color_name = club_data["Principal"]
                    new_clothing_color = get_color_hex(color_name)
                    updated_url = update_clothing_color_in_url(saved_url, new_clothing_color)
                    new_image_urls.append(updated_url)
                    updated_state["avatars"][pid] = {"name": player_name, "url": updated_url}
                    avatar_updates += 1
                    continue
            
            new_image_urls.append(saved_url)
            updated_state["avatars"][pid] = {"name": player_name, "url": saved_url}
            avatar_preserved += 1
        else:
            avatar_url = get_avatar_url(
                player_id=int(pid),
                name=player_name,
                country=row["country_of_citizenship"],
                position=row["position"],
                sub_position=row["sub_position"],
                club=current_club,
                club_colors=club_colors
            )
            
            if has_override:
                override = avatar_state["overrides"][pid]
                for param, value in override.items():
                    avatar_url = update_clothing_color_in_url(avatar_url, value) if param == "clothingColor" else avatar_url
            
            new_image_urls.append(avatar_url)
            updated_state["avatars"][pid] = {"name": player_name, "url": avatar_url}
            avatar_updates += 1

    players["image_url"] = new_image_urls
    
    log(f"Salvando {AVATAR_STATE_FILE}...")
    save_avatar_state(updated_state)
    
    log(f"Concluido. Avatares gerados/atualizados: {avatar_updates:,} | Preservados: {avatar_preserved:,}")
    
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
    """Import reference CSV files to database."""
    for csv_name, table in TABLES[:4]:  # First 4 are reference files
        csv_path = CLEAN_DIR / csv_name
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
