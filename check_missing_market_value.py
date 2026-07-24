"""Mede quantos jogadores em raw/players.csv estão sem market_value_in_eur
e/ou highest_market_value_in_eur, pra saber se é ruído pontual ou um problema
maior nessa versão do dataset."""

import csv
from pathlib import Path

PLAYERS_CSV = Path("raw/players.csv")


def main() -> None:
    if not PLAYERS_CSV.exists():
        raise SystemExit(f"Não encontrei {PLAYERS_CSV} -- rode isso na raiz do kicker-data")

    total = 0
    sem_market_value = 0
    sem_highest = 0
    sem_os_dois = 0
    sem_os_dois_mas_com_last_season_recente = 0

    exemplos_recentes_sem_valor = []

    with PLAYERS_CSV.open(encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            total += 1
            mv = row.get("market_value_in_eur", "").strip()
            hmv = row.get("highest_market_value_in_eur", "").strip()

            if not mv:
                sem_market_value += 1
            if not hmv:
                sem_highest += 1
            if not mv and not hmv:
                sem_os_dois += 1
                try:
                    last_season = int(row.get("last_season", "0") or 0)
                except ValueError:
                    last_season = 0
                if last_season >= 2024:
                    sem_os_dois_mas_com_last_season_recente += 1
                    if len(exemplos_recentes_sem_valor) < 15:
                        exemplos_recentes_sem_valor.append(
                            (row.get("name"), row.get("current_club_name"), last_season)
                        )

    print(f"Total de jogadores no CSV: {total:,}")
    print(f"Sem market_value_in_eur:            {sem_market_value:,} ({sem_market_value/total*100:.1f}%)")
    print(f"Sem highest_market_value_in_eur:    {sem_highest:,} ({sem_highest/total*100:.1f}%)")
    print(f"Sem os dois:                        {sem_os_dois:,} ({sem_os_dois/total*100:.1f}%)")
    print()
    print(f"Sem os dois, MAS ativo recente (last_season >= 2024): "
          f"{sem_os_dois_mas_com_last_season_recente:,}")
    print("  ^ esse número é o que importa -- jogador aposentado sem valor é normal,")
    print("    jogador ATIVO sem valor é o problema real (caso do Arrascaeta)")
    print()

    if exemplos_recentes_sem_valor:
        print("Exemplos de jogadores ativos recentes sem NENHUM valor de mercado:")
        for name, club, season in exemplos_recentes_sem_valor:
            print(f"  {name} ({club}) -- last_season={season}")


if __name__ == "__main__":
    main()