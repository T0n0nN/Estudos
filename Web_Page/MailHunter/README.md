# MailHunter (sem API paga)

Coleta uma lista de estabelecimentos **da cidade** (ex.: Limeira-SP) usando **dados abertos do OpenStreetMap** e gera CSV.

Opcionalmente, quando o estabelecimento tiver `website`, o script acessa o **site do próprio negócio** (com limites e respeito a `robots.txt`) e tenta extrair e-mails publicados.

## Por que isso é diferente do Google?

- Não usa Google Places API (sem chave / sem cobrança por requisição).
- Não faz scraping de resultados do Google/Maps.
- Usa fontes abertas (OpenStreetMap) + o site do próprio negócio.

> Limitação realista: se o comércio **não publica e-mail** e **não tem site**, não existe “mágica” que ache o e-mail de forma confiável.

## Requisitos

- Python 3.10+
- Internet liberada para:
  - `nominatim.openstreetmap.org`
  - `overpass-api.de`

Instalar dependências:

```powershell
cd "C:\Users\za68397\OneDrive - Goodyear\ChatGPT\Web_Page\MailHunter"
python -m pip install -r requirements.txt
```

## Uso básico (Limeira-SP)

```powershell
python mailhunter_osm.py --city "Limeira, SP, Brazil"
```

Gera:

- `leads_osm_<timestamp>.csv`
- `leads_osm_<timestamp>_no_site.csv`

## Tentar extrair e-mails dos websites

```powershell
python mailhunter_osm.py --city "Limeira, SP, Brazil" --extract-email
```

## Variáveis de ambiente (recomendado)

Nominatim pede identificação do User-Agent.

```powershell
$env:MAILHUNTER_CONTACT = "seu-email@exemplo.com"
```

## Boas práticas

- Use o CSV para prospecção **individual** (mensagem curta e relevante).
- Respeite pedidos de remoção.
- Não faça disparos massivos.
