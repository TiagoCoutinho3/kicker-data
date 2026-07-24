"""Baixa a versão mais recente do dataset do Kaggle e organiza os CSVs em raw/,
prontos pra o pipeline.py consumir.

Credencial: a lib kagglehub encontra sozinha, na seguinte ordem de prioridade:
  1. Variáveis de ambiente KAGGLE_USERNAME / KAGGLE_KEY (usado no GitHub Actions)
  2. Arquivo ~/.kaggle/kaggle.json (usado localmente)
Não precisa de nenhum código diferente pra cada ambiente -- a lib resolve isso.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import kagglehub

DATASET_REF = "davidcariboo/player-scores"
RAW_DIR = Path("raw")

# nomes de arquivo que o pipeline.py espera encontrar dentro de raw/
EXPECTED_FILES = [
    "appearances.csv",
    "club_games.csv",
    "clubs.csv",
    "competitions.csv",
    "countries.csv",
    "game_events.csv",
    "game_lineups.csv",
    "games.csv",
    "national_teams.csv",
    "player_valuations.csv",
    "players.csv",
    "transfers.csv",
]


def log(msg: str) -> None:
    print(msg, flush=True)


def main() -> None:
    log(f"Baixando dataset '{DATASET_REF}' do Kaggle (força versão mais recente)...")

    # force_download=True evita usar uma cópia em cache antiga -- sempre pega
    # a versão mais recente disponível no Kaggle nesse momento
    downloaded_path = Path(kagglehub.dataset_download(DATASET_REF, force_download=True))
    log(f"  Baixado em: {downloaded_path}")

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    missing = []
    for filename in EXPECTED_FILES:
        src = downloaded_path / filename
        if not src.exists():
            missing.append(filename)
            continue

        dst = RAW_DIR / filename
        shutil.copyfile(src, dst)
        size_mb = dst.stat().st_size / 1024 / 1024
        log(f"  {filename}: {size_mb:,.1f} MB -> {dst}")

    if missing:
        raise SystemExit(
            f"ERRO: {len(missing)} arquivo(s) esperado(s) não encontrado(s) no "
            f"dataset baixado: {missing}. O pipeline.py provavelmente vai falhar "
            f"sem eles -- confira se o dataset no Kaggle mudou de estrutura."
        )

    log("")
    log(f"OK: {len(EXPECTED_FILES)} arquivos organizados em {RAW_DIR}/")


if __name__ == "__main__":
    main()