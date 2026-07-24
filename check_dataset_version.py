"""Verifica se o dataset Kaggle tem uma versão mais recente que a última processada.
Consulta só metadado (leve, sem baixar os 344MB+ do dataset) via dataset_list().

Usa current_version_number (inteiro), não lastUpdated (timestamp) -- é mais
simples de comparar e não depende de fuso horário/formato de data.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from kaggle.api.kaggle_api_extended import KaggleApi

DATASET_REF = "davidcariboo/player-scores"
STATE_FILE = Path(".last_processed_version.json")


def log(msg: str) -> None:
    print(msg, flush=True)


def get_remote_version() -> int:
    api = KaggleApi()
    api.authenticate()
    results = api.dataset_list(search="player-scores")
    dataset = next((d for d in results if str(d.ref) == DATASET_REF), None)
    if dataset is None:
        raise SystemExit(f"Dataset {DATASET_REF} não encontrado na busca da API do Kaggle")
    return dataset.current_version_number


def get_local_version() -> int | None:
    if not STATE_FILE.exists():
        return None
    return json.loads(STATE_FILE.read_text()).get("version")


def write_github_output(name: str, value: str) -> None:
    """Escreve no arquivo especial que o GitHub Actions usa pra passar valores
    entre steps de um workflow. Fora do Actions (rodando local), isso é
    simplesmente ignorado -- GITHUB_OUTPUT não existe."""
    gh_output = os.environ.get("GITHUB_OUTPUT")
    if not gh_output:
        return
    with open(gh_output, "a", encoding="utf-8") as f:
        f.write(f"{name}={value}\n")


def main() -> None:
    remote = get_remote_version()
    local = get_local_version()

    log(f"Versão no Kaggle:        {remote}")
    log(f"Última versão processada: {local}")

    if remote != local:
        log(f"MUDOU: {local} -> {remote}")
        write_github_output("has_update", "true")
        write_github_output("remote_version", str(remote))
    else:
        log("Sem mudança, nada a fazer.")
        write_github_output("has_update", "false")
        write_github_output("remote_version", str(remote))


if __name__ == "__main__":
    main()