# Agenda via Telegram (MVP)

Objetivo: um bot simples para agendar/cancelar horários com lembrete automático.

- Banco: SQLite (embutido no Python) → **não precisa instalar nada**.
- Modo: polling (mais simples para começar).

## 1) Criar o bot no Telegram (BotFather)

1. No Telegram, abra o chat com **@BotFather**
2. Envie: `/newbot`
3. Dê um nome e um username (termina com `bot`)
4. O BotFather vai te dar um **token** (ex.: `123:ABC...`)

## 2) Preparar o ambiente (Windows)

### Opção A (mais simples): Python instalado

- Instale Python 3.11+ (Microsoft Store ou python.org)
- Abra o PowerShell dentro da pasta `Telegram_Agenda/`

Crie um venv e instale dependências:

- `python -m venv .venv`
- `./.venv/Scripts/Activate.ps1`
- `pip install -r requirements.txt`

## 3) Configurar o token do bot

No PowerShell (na mesma janela do venv):

- `$env:TELEGRAM_BOT_TOKEN = "COLE_SEU_TOKEN_AQUI"`

> Dica: isso vale só para a janela atual. Para persistir, você pode criar um arquivo `.ps1` de inicialização ou usar Variáveis de Ambiente do Windows.

## 4) Ajustar horário de funcionamento (config.json)

Edite `config.json`:

- `timezone`: `America/Sao_Paulo`
- `slot_minutes`: duração do slot (ex.: 30)
- `work_hours`: horários por dia (mon..sun)
- `holidays`: lista de datas sem atendimento (`YYYY-MM-DD`)
- `reminder_minutes_before`: lembrete (ex.: 120 = 2h antes)

## 5) Rodar o bot

- `python bot.py`

No Telegram, abra seu bot e use:

- `/start`
- Clique em **Agendar**
- Clique em **Meus horários** para cancelar

## Como isso evita manutenção

- SQLite é um arquivo local: `agenda.sqlite3`.
- Lembretes rodam por um "tick" a cada 60s olhando o banco: se reiniciar, ele volta e continua.

## Próximos upgrades (quando vender)

- Vários profissionais (barbeiros) e serviços com durações diferentes
- Página web para o cliente (link) + Telegram só para confirmações
- Migrar SQLite → Postgres (mais robusto)
- Webhook (produção) em vez de polling
