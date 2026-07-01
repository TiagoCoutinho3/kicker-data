import json
from pathlib import Path

path = Path('club_colors.json')
data = json.loads(path.read_text(encoding='utf-8'))

color_map = {
    'Vermelho': '#e53935',
    'Azul': '#1e88e5',
    'Amarelo': '#fdd835',
    'Verde': '#43a047',
    'Preto': '#212121',
    'Branco': '#ffffff',
    'Laranja': '#fb8c00',
    'Vinho': '#7b1fa2',
    'Cinza': '#757575',
    'Violeta': '#8e24aa',
    'Rosa': '#ec407a',
    'Celeste': '#4fc3f7',
    'Xadrez': '#424242',
    'Dourado': '#ffb300',
    'Bordo': '#880e4f',
    'Roxo': "#661197",
    'Grená': '#8d6e63',
    'Azul marinho': '#0d47a1',
    'Borgonha': '#7b1fa2',
    'Marrom': '#795548',
}

for club, colors in data.items():
    if isinstance(colors, dict):
        for role in ('Principal', 'Goleiro'):
            if role in colors and isinstance(colors[role], str):
                value = colors[role].strip()
                if value and not value.startswith('#'):
                    colors[role] = color_map.get(value, value)

path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
print('club_colors.json atualizado com valores hex')
