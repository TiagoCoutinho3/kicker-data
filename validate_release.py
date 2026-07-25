"""Valida o football.db recém-gerado ANTES de publicar como release, comparando
com o estado anterior (conhecido-bom). Dois portões de segurança independentes:

  Gate 1 (preciso, sem threshold): algum jogador com is_manual=True na release
    anterior desapareceu por completo do banco novo? Se sim, reprova.

  Gate 2 (canário geral, com threshold): a % de jogadores sem
    market_value_in_eur/highest_market_value_in_eur piorou muito em relação
    à release anterior? Se sim, reprova -- sinal de scraping degradado no
    dataset como um todo, mesmo que nenhum jogador manual tenha sido afetado
    dessa vez.

Se qualquer gate reprovar, o workflow NÃO deve publicar a nova release nem
avançar o estado de "última versão processada" -- isso é feito no YAML,
não aqui. Esse script só decide e informa; não publica nem commita nada.
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path

PREVIOUS_DB = Path("clean/football_previous.db")
NEW_DB = Path("clean/football.db")

# Gate 2: quantos pontos percentuais a % de "sem valor de mercado" pode
# piorar em relação à release anterior antes de ser considerado suspeito.
MARKET_VALUE_MISSING_DELTA_THRESHOLD = 10.0


def log(msg: str) -> None:
    print(msg, flush=True)


def get_manual_player_ids(conn: sqlite3.Connection) -> set[int]:
    rows = conn.execute("SELECT player_id FROM players WHERE is_manual = 1").fetchall()
    return {row[0] for row in rows}


def get_all_player_ids(conn: sqlite3.Connection) -> set[int]:
    rows = conn.execute("SELECT player_id FROM players").fetchall()
    return {row[0] for row in rows}


def get_missing_market_value_pct(conn: sqlite3.Connection) -> float:
    total = conn.execute("SELECT COUNT(*) FROM players").fetchone()[0]
    if total == 0:
        return 0.0
    missing = conn.execute(
        """SELECT COUNT(*) FROM players
           WHERE (market_value_in_eur IS NULL OR market_value_in_eur = '')
           AND (highest_market_value_in_eur IS NULL OR highest_market_value_in_eur = '')"""
    ).fetchone()[0]
    return missing / total * 100


def write_github_output(name: str, value: str) -> None:
    gh_output = os.environ.get("GITHUB_OUTPUT")
    if not gh_output:
        return
    with open(gh_output, "a", encoding="utf-8") as f:
        if "\n" in value:
            # sintaxe de multiline output do GitHub Actions
            f.write(f"{name}<<EOF\n{value}\nEOF\n")
        else:
            f.write(f"{name}={value}\n")


def main() -> None:
    if not PREVIOUS_DB.exists() or not NEW_DB.exists():
        raise SystemExit(
            f"Esperava encontrar {PREVIOUS_DB} e {NEW_DB} -- confira se os "
            f"passos anteriores do workflow rodaram na ordem certa."
        )

    conn_before = sqlite3.connect(PREVIOUS_DB)
    conn_after = sqlite3.connect(NEW_DB)

    failures: list[str] = []

    # --- Gate 1: jogador manual desapareceu? ---
    manual_ids_before = get_manual_player_ids(conn_before)
    all_ids_after = get_all_player_ids(conn_after)
    missing_manual_ids = manual_ids_before - all_ids_after

    log(f"Jogadores manuais na release anterior: {len(manual_ids_before)}")
    if missing_manual_ids:
        log(f"  ATENÇÃO: {len(missing_manual_ids)} jogador(es) manual(is) sumiram: {sorted(missing_manual_ids)}")
        failures.append(
            f"Gate 1 (jogadores manuais): {len(missing_manual_ids)} jogador(es) com "
            f"avatar manual desapareceram do dataset novo -- player_id(s): "
            f"{sorted(missing_manual_ids)}"
        )
    else:
        log("  OK: todos os jogadores manuais continuam presentes")

    # --- Gate 2: % de valor de mercado faltando piorou muito? ---
    pct_before = get_missing_market_value_pct(conn_before)
    pct_after = get_missing_market_value_pct(conn_after)
    delta = pct_after - pct_before

    log(f"% sem market value -- antes: {pct_before:.1f}% | depois: {pct_after:.1f}% | delta: {delta:+.1f}pp")
    if delta > MARKET_VALUE_MISSING_DELTA_THRESHOLD:
        failures.append(
            f"Gate 2 (qualidade geral): % de jogadores sem valor de mercado subiu "
            f"{delta:.1f} pontos percentuais ({pct_before:.1f}% -> {pct_after:.1f}%), "
            f"acima do limite de {MARKET_VALUE_MISSING_DELTA_THRESHOLD}pp -- possível "
            f"degradação no scraping do dataset nessa versão."
        )
    else:
        log("  OK: dentro do limite aceitável")

    conn_before.close()
    conn_after.close()

    if failures:
        log("")
        log("VALIDAÇÃO REPROVADA -- release NÃO deve ser publicada.")
        write_github_output("validation_passed", "false")
        write_github_output("failure_reasons", "\n".join(f"- {f}" for f in failures))
    else:
        log("")
        log("Validação aprovada -- release pode ser publicada.")
        write_github_output("validation_passed", "true")


if __name__ == "__main__":
    main()