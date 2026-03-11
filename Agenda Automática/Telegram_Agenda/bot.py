"""Bot de agenda via Telegram com armazenamento local em SQLite."""

import json
import os
import sqlite3
import asyncio
from dataclasses import dataclass
from datetime import datetime, date, time, timedelta
from pathlib import Path
from typing import Iterable

from zoneinfo import ZoneInfo

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.error import NetworkError, TelegramError
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
)
from telegram.request import HTTPXRequest


ROOT = Path(__file__).resolve().parent
DB_PATH = ROOT / "agenda.sqlite3"
CONFIG_PATH = ROOT / "config.json"


WEEKDAY_KEYS = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]


@dataclass(frozen=True)
class Config:
    """Configuração carregada do arquivo config.json."""

    timezone: str
    slot_minutes: int
    days_ahead: int
    work_hours: dict
    holidays: set[str]
    allow_overbooking: bool
    reminder_minutes_before: int


def load_config() -> Config:
    """Lê o arquivo de configuração e devolve um objeto imutável."""

    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        raw = json.load(f)

    return Config(
        timezone=raw["timezone"],
        slot_minutes=int(raw["slot_minutes"]),
        days_ahead=int(raw["days_ahead"]),
        work_hours=raw["work_hours"],
        holidays=set(raw.get("holidays", [])),
        allow_overbooking=bool(raw.get("allow_overbooking", False)),
        reminder_minutes_before=int(raw.get("reminder_minutes_before", 120)),
    )


def db_connect() -> sqlite3.Connection:
    """Abre uma conexão SQLite com pragmas adequados para o bot."""

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def db_init() -> None:
    """Cria as estruturas de banco necessárias caso ainda não existam."""

    conn = db_connect()
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS appointments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                customer_name TEXT,
                start_iso TEXT NOT NULL,
                end_iso TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'booked',
                reminded INTEGER NOT NULL DEFAULT 0,
                created_at_iso TEXT NOT NULL
            );
            """
        )
        # Prevent double booking (same slot) unless overbooking is enabled.
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_appointments_unique_slot
            ON appointments(start_iso)
            WHERE status = 'booked';
            """
        )
        conn.commit()
    finally:
        conn.close()


def parse_hhmm(value: str) -> time:
    """Converte uma string HH:MM em objeto time."""

    hh, mm = value.split(":")
    return time(hour=int(hh), minute=int(mm))


def now_tz(tzinfo) -> datetime:
    """Retorna o instante atual no fuso informado."""

    return datetime.now(tzinfo)


def get_tzinfo(cfg: Config) -> ZoneInfo:
    """Resolve o fuso horário configurado e falha com mensagem clara."""

    try:
        return ZoneInfo(cfg.timezone)
    except Exception as exc:
        raise RuntimeError(f"Timezone inválido em config.json: {cfg.timezone}") from exc


def build_telegram_request() -> HTTPXRequest:
    """Cria um request HTTPX configurável para redes corporativas.

    - Respeita proxy via variáveis de ambiente (HTTP_PROXY/HTTPS_PROXY) usando trust_env.
    - Permite informar um CA bundle corporativo (para SSL inspection) via TELEGRAM_CA_BUNDLE.
      Exemplo: setar para um arquivo .pem/.crt exportado pela TI.
    """

    ca_bundle = os.environ.get("TELEGRAM_CA_BUNDLE", "").strip()
    telegram_proxy = os.environ.get("TELEGRAM_PROXY", "").strip()

    httpx_kwargs: dict = {"trust_env": True}
    if ca_bundle:
        httpx_kwargs["verify"] = ca_bundle

    # Timeouts razoáveis para polling.
    return HTTPXRequest(
        connect_timeout=20,
        read_timeout=20,
        write_timeout=20,
        pool_timeout=20,
        proxy=telegram_proxy or None,
        httpx_kwargs=httpx_kwargs,
    )


def iso(dt: datetime) -> str:
    """Serializa um datetime para ISO 8601 sem microssegundos."""

    return dt.isoformat(timespec="seconds")


def from_iso(value: str) -> datetime:
    """Converte uma string ISO 8601 em datetime."""

    return datetime.fromisoformat(value)


def daterange(start_date: date, days: int) -> Iterable[date]:
    """Gera uma sequência de datas a partir da data inicial."""

    for i in range(days):
        yield start_date + timedelta(days=i)


def is_holiday(d: date, cfg: Config) -> bool:
    """Indica se a data está marcada como feriado na configuração."""

    return d.isoformat() in cfg.holidays


def day_key(d: date) -> str:
    """Mapeia uma data para a chave de dia da semana usada no config."""

    # Monday=0
    return WEEKDAY_KEYS[d.weekday()]


def generate_slots_for_day(d: date, cfg: Config, tzinfo) -> list[tuple[datetime, datetime]]:
    """Gera os slots válidos de atendimento para um dia específico."""

    if is_holiday(d, cfg):
        return []

    hours = cfg.work_hours.get(day_key(d), [])
    if not hours:
        return []

    slots: list[tuple[datetime, datetime]] = []
    step = timedelta(minutes=cfg.slot_minutes)

    for start_hhmm, end_hhmm in hours:
        start_dt = datetime.combine(d, parse_hhmm(start_hhmm), tzinfo=tzinfo)
        end_dt = datetime.combine(d, parse_hhmm(end_hhmm), tzinfo=tzinfo)
        cursor = start_dt
        while cursor + step <= end_dt:
            slots.append((cursor, cursor + step))
            cursor += step

    return slots


def slot_is_available(start_dt: datetime, cfg: Config) -> bool:
    """Verifica se um horário está livre para reserva."""

    if cfg.allow_overbooking:
        return True

    conn = db_connect()
    try:
        row = conn.execute(
            "SELECT 1 FROM appointments WHERE start_iso = ? AND status = 'booked' LIMIT 1",
            (iso(start_dt),),
        ).fetchone()
        return row is None
    finally:
        conn.close()


def book_slot(chat_id: int, customer_name: str, start_dt: datetime, end_dt: datetime, cfg: Config) -> bool:
    """Cria um agendamento para o usuário se o slot continuar disponível."""

    conn = db_connect()
    try:
        if not cfg.allow_overbooking:
            existing = conn.execute(
                "SELECT 1 FROM appointments WHERE start_iso = ? AND status='booked' LIMIT 1",
                (iso(start_dt),),
            ).fetchone()
            if existing is not None:
                return False

        conn.execute(
            """
            INSERT INTO appointments(chat_id, customer_name, start_iso, end_iso, status, reminded, created_at_iso)
            VALUES (?, ?, ?, ?, 'booked', 0, ?)
            """,
            (chat_id, customer_name, iso(start_dt), iso(end_dt), iso(datetime.now(start_dt.tzinfo))),
        )
        conn.commit()
        return True
    finally:
        conn.close()


def list_upcoming(chat_id: int, tzinfo) -> list[sqlite3.Row]:
    """Lista os próximos agendamentos ativos do usuário."""

    conn = db_connect()
    try:
        now_iso = iso(datetime.now(tzinfo))
        return conn.execute(
            """
            SELECT id, start_iso, end_iso, status
            FROM appointments
            WHERE chat_id = ? AND status = 'booked' AND start_iso >= ?
            ORDER BY start_iso ASC
            LIMIT 10
            """,
            (chat_id, now_iso),
        ).fetchall()
    finally:
        conn.close()


def cancel_appointment(chat_id: int, appt_id: int) -> bool:
    """Cancela um agendamento do usuário pelo identificador."""

    conn = db_connect()
    try:
        cur = conn.execute(
            "UPDATE appointments SET status='canceled' WHERE id=? AND chat_id=? AND status='booked'",
            (appt_id, chat_id),
        )
        conn.commit()
        return cur.rowcount == 1
    finally:
        conn.close()


def fmt_dt(dt: datetime) -> str:
    """Formata uma data para apresentação em português."""

    return dt.strftime("%d/%m/%Y %H:%M")


def menu_keyboard() -> InlineKeyboardMarkup:
    """Monta o teclado principal do bot."""

    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Agendar", callback_data="menu:agendar")],
            [InlineKeyboardButton("Meus horários", callback_data="menu:meus")],
        ]
    )


def day_keyboard(days: list[date]) -> InlineKeyboardMarkup:
    """Monta o teclado com os dias disponíveis para agendamento."""

    buttons = []
    for d in days:
        label = d.strftime("%a %d/%m")
        buttons.append([InlineKeyboardButton(label, callback_data=f"day:{d.isoformat()}")])
    buttons.append([InlineKeyboardButton("Voltar", callback_data="menu:root")])
    return InlineKeyboardMarkup(buttons)


def slots_keyboard(d: date, slots: list[tuple[datetime, datetime]]) -> InlineKeyboardMarkup:
    """Monta o teclado com os horários livres de um dia."""

    buttons = []
    for start_dt, _end_dt in slots:
        label = f"{start_dt.strftime('%H:%M')}"
        buttons.append([InlineKeyboardButton(label, callback_data=f"slot:{d.isoformat()}:{start_dt.strftime('%H:%M')}")])
    buttons.append([InlineKeyboardButton("Voltar", callback_data="menu:agendar")])
    return InlineKeyboardMarkup(buttons)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Exibe a tela inicial e guarda o nome do usuário no contexto."""

    cfg = load_config()
    db_init()

    name = update.effective_user.full_name if update.effective_user else ""

    text = (
        "<b>Agenda</b>\n"
        "Escolha uma opção:\n"
        f"\n<small>Fuso horário: {cfg.timezone}</small>"
    )
    if update.message:
        await update.message.reply_text(text, reply_markup=menu_keyboard(), parse_mode=ParseMode.HTML)

    context.user_data["customer_name"] = name


async def cmd_agendar(update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
    """Mostra os próximos dias disponíveis para agendamento."""

    cfg = load_config()
    tzinfo = get_tzinfo(cfg)
    today = now_tz(tzinfo).date()

    days = list(daterange(today, min(cfg.days_ahead, 14)))
    # Mostra próximos 7 dias primeiro.
    days = days[:7]

    text = "Escolha o dia para agendar:" 
    if update.message:
        await update.message.reply_text(text, reply_markup=day_keyboard(days))


async def cmd_meus(update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
    """Lista os agendamentos futuros do chat atual."""

    cfg = load_config()
    tzinfo = get_tzinfo(cfg)

    appts = list_upcoming(update.effective_chat.id, tzinfo)
    if not appts:
        await update.message.reply_text("Você não tem horários marcados.")
        return

    lines = ["<b>Seus próximos horários</b>"]
    buttons = []
    for row in appts:
        start_dt = from_iso(row["start_iso"]).astimezone(tzinfo)
        lines.append(f"• #{row['id']} — {fmt_dt(start_dt)}")
        buttons.append([InlineKeyboardButton(f"Cancelar #{row['id']}", callback_data=f"cancel:{row['id']}")])

    buttons.append([InlineKeyboardButton("Voltar", callback_data="menu:root")])
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(buttons))


async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Processa callbacks de menu, seleção de dia, slot e cancelamento."""

    cfg = load_config()
    tzinfo = get_tzinfo(cfg)

    query = update.callback_query
    if not query:
        return
    await query.answer()

    data = query.data or ""

    if data == "menu:root":
        await query.edit_message_text(
            "<b>Agenda</b>\nEscolha uma opção:",
            parse_mode=ParseMode.HTML,
            reply_markup=menu_keyboard(),
        )
        return

    if data == "menu:agendar":
        today = now_tz(tzinfo).date()
        days = list(daterange(today, min(cfg.days_ahead, 14)))[:7]
        await query.edit_message_text("Escolha o dia para agendar:", reply_markup=day_keyboard(days))
        return

    if data == "menu:meus":
        appts = list_upcoming(query.message.chat_id, tzinfo)
        if not appts:
            await query.edit_message_text("Você não tem horários marcados.", reply_markup=menu_keyboard())
            return

        lines = ["<b>Seus próximos horários</b>"]
        buttons = []
        for row in appts:
            start_dt = from_iso(row["start_iso"]).astimezone(tzinfo)
            lines.append(f"• #{row['id']} — {fmt_dt(start_dt)}")
            buttons.append([InlineKeyboardButton(f"Cancelar #{row['id']}", callback_data=f"cancel:{row['id']}")])

        buttons.append([InlineKeyboardButton("Voltar", callback_data="menu:root")])
        await query.edit_message_text(
            "\n".join(lines),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(buttons),
        )
        return

    if data.startswith("day:"):
        _, day_iso = data.split(":", 1)
        d = date.fromisoformat(day_iso)
        slots = generate_slots_for_day(d, cfg, tzinfo)

        # Remove horários no passado para o dia atual.
        now = now_tz(tzinfo)
        slots = [(s, e) for (s, e) in slots if e > now]

        # Filtra horários já ocupados.
        slots = [(s, e) for (s, e) in slots if slot_is_available(s, cfg)]

        if not slots:
            await query.edit_message_text(
                f"Sem horários disponíveis em {d.strftime('%d/%m/%Y')}.\nEscolha outro dia:",
                reply_markup=day_keyboard(list(daterange(now.date(), 7))),
            )
            return

        await query.edit_message_text(
            f"Horários disponíveis em {d.strftime('%d/%m/%Y')}:",
            reply_markup=slots_keyboard(d, slots),
        )
        return

    if data.startswith("slot:"):
        _, day_iso, hhmm = data.split(":", 2)
        d = date.fromisoformat(day_iso)
        start_dt = datetime.combine(d, parse_hhmm(hhmm), tzinfo=tzinfo)
        end_dt = start_dt + timedelta(minutes=cfg.slot_minutes)

        name = context.user_data.get("customer_name") or (update.effective_user.full_name if update.effective_user else "")
        ok = book_slot(query.message.chat_id, name, start_dt, end_dt, cfg)
        if not ok:
            await query.edit_message_text(
                "Esse horário acabou de ser reservado. Escolha outro:",
                reply_markup=day_keyboard(list(daterange(now_tz(tzinfo).date(), 7))),
            )
            return

        await query.edit_message_text(
            "<b>Agendamento confirmado</b>\n"
            f"• {fmt_dt(start_dt)}\n\n"
            "Para cancelar: clique em <i>Meus horários</i>.",
            parse_mode=ParseMode.HTML,
            reply_markup=menu_keyboard(),
        )
        return

    if data.startswith("cancel:"):
        _, appt_id_str = data.split(":", 1)
        appt_id = int(appt_id_str)
        ok = cancel_appointment(query.message.chat_id, appt_id)
        if ok:
            await query.edit_message_text("Cancelado. O horário voltou a ficar disponível.", reply_markup=menu_keyboard())
        else:
            await query.edit_message_text("Não consegui cancelar (talvez já tenha sido cancelado).", reply_markup=menu_keyboard())
        return


async def reminder_tick(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Envia lembretes para agendamentos próximos e marca como enviados."""

    # Job que roda periodicamente e envia lembretes 2h antes.
    cfg = load_config()
    tzinfo = get_tzinfo(cfg)

    now = now_tz(tzinfo)
    window_start = now + timedelta(minutes=cfg.reminder_minutes_before)
    window_end = window_start + timedelta(minutes=2)

    conn = db_connect()
    try:
        rows = conn.execute(
            """
            SELECT id, chat_id, start_iso
            FROM appointments
            WHERE status='booked'
              AND reminded=0
              AND start_iso >= ?
              AND start_iso < ?
            """,
            (iso(window_start), iso(window_end)),
        ).fetchall()

        for row in rows:
            appt_id = row["id"]
            chat_id = row["chat_id"]
            start_dt = from_iso(row["start_iso"]).astimezone(tzinfo)

            try:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=(
                        "⏰ <b>Lembrete de agendamento</b>\n"
                        f"Seu horário é hoje às <b>{start_dt.strftime('%H:%M')}</b> ({start_dt.strftime('%d/%m/%Y')})."
                    ),
                    parse_mode=ParseMode.HTML,
                )
                conn.execute("UPDATE appointments SET reminded=1 WHERE id=?", (appt_id,))
                conn.commit()
            except TelegramError:
                # Se falhar (usuário bloqueou bot, etc), tenta de novo no próximo tick.
                pass
    finally:
        conn.close()


def main() -> None:
    """Inicializa a aplicação Telegram e inicia o polling."""

    token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        raise RuntimeError(
            "Defina a variável de ambiente TELEGRAM_BOT_TOKEN com o token do seu bot (BotFather)."
        )

    db_init()

    request = build_telegram_request()
    app = Application.builder().token(token).request(request).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("agendar", cmd_agendar))
    app.add_handler(CommandHandler("meus", cmd_meus))
    app.add_handler(CallbackQueryHandler(on_callback))

    # Lembrete (tick a cada 60s). Robusto mesmo com reinício.
    app.job_queue.run_repeating(reminder_tick, interval=60, first=10)

    print("Bot rodando em modo polling...")

    # Python 3.14+: asyncio não cria um event loop padrão automaticamente.
    # Algumas libs (incl. python-telegram-bot) ainda chamam get_event_loop().
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        try:
            asyncio.get_event_loop()
        except RuntimeError:
            asyncio.set_event_loop(asyncio.new_event_loop())

    try:
        app.run_polling(allowed_updates=Update.ALL_TYPES)
    except NetworkError as exc:
        msg = str(exc)
        if "CERTIFICATE_VERIFY_FAILED" in msg:
            raise RuntimeError(
                "Falha SSL ao conectar no Telegram. Isso normalmente acontece em rede corporativa com inspeção SSL/proxy.\n"
                "Soluções:\n"
                "- Tentar em outra rede (4G/Hotspot) para confirmar\n"
                "- Pedir para a TI o certificado raiz (CA) e exportar como .pem/.crt\n"
                "- Setar a variável TELEGRAM_CA_BUNDLE apontando para esse arquivo\n"
                "  Ex.: $env:TELEGRAM_CA_BUNDLE = 'C:\\caminho\\corp-ca.pem'\n"
                "- Se a rede usa proxy, configure HTTP_PROXY/HTTPS_PROXY (o bot respeita essas variáveis)"
            ) from exc
        raise


if __name__ == "__main__":
    main()
