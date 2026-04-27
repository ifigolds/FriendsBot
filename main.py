import asyncio
import html
import logging
import os
import random
import sqlite3
import threading
from contextlib import closing
from dataclasses import dataclass
from datetime import date, datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from telegram import BotCommand, Chat, InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.error import Forbidden, TelegramError
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CallbackQueryHandler,
    ChatMemberHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)


DB_PATH = Path(os.getenv("DB_PATH", "double_life.sqlite3"))
BOT_TOKEN = os.getenv("BOT_TOKEN")
PORT = int(os.getenv("PORT", "10000"))

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    level=os.getenv("LOG_LEVEL", "INFO"),
)
logger = logging.getLogger("double-life")


@dataclass(frozen=True)
class Role:
    name: str
    mission: str
    level: int = 1


ROLES = [
    Role("Провокатор", "заставить кого-то начать спор, но не перейти в токсик", 1),
    Role("Философ", "перевести разговор в глубокую тему и удержать ее 5+ сообщений", 1),
    Role("Клоун", "рассмешить минимум 3 человек", 1),
    Role("Шпион", "узнать у кого-то странную бытовую деталь, например что он ел сегодня", 1),
    Role("Тихий режиссер", "незаметно поменять тему разговора два раза", 1),
    Role("Миротворец", "погасить конфликт или спор до того, как он разрастется", 1),
    Role("Архивариус", "вспомнить старый мем или историю чата так, чтобы ее подхватили", 2),
    Role("Инфлюенсер", "заставить двоих людей повторить твою фразу или идею", 2),
    Role("Серый кардинал", "подтолкнуть другого игрока к выполнению его миссии", 3),
    Role("Хамелеон", "сыграть так, чтобы на тебя подозревали две разные роли", 4),
]

EVENTS = [
    "10 минут все говорят максимально серьезно. Кто сорвался первым, тот подозрителен.",
    "Следующие 10 минут можно подозревать только тех, кто уже писал сегодня.",
    "Все игроки получают право один раз блефануть о своей роли.",
    "В ближайшие 10 минут любое слово «кстати» считается уликой.",
    "До конца раунда за правильный /sus дают +1 бонусное очко.",
]

ACHIEVEMENTS = {
    "mask": "Мастер маскировки",
    "sherlock": "Шерлок",
    "first_win": "Первая легенда",
}

BOT_COMMANDS = [
    BotCommand("play", "просто начать играть"),
    BotCommand("join", "войти в лобби"),
    BotCommand("leave", "выйти из лобби"),
    BotCommand("startgame", "начать раунд"),
    BotCommand("status", "статус игры"),
    BotCommand("quick", "быстрая игра"),
    BotCommand("settings", "настройки чата"),
    BotCommand("roles", "все роли в личку"),
    BotCommand("sus", "подозревать игрока"),
    BotCommand("me", "моя тайная роль"),
    BotCommand("complete", "отметить миссию"),
    BotCommand("event", "случайное событие"),
    BotCommand("endgame", "завершить раунд"),
    BotCommand("score", "мой профиль"),
    BotCommand("leaderboard", "рейтинг"),
    BotCommand("season", "сезон дня"),
    BotCommand("head", "справка по игре"),
    BotCommand("clean", "убрать сообщения бота"),
]

CARD_EMOJIS = ["🎭", "🕵️", "✨", "🧠", "🔥", "💅", "👀", "🎲", "🏆", "🤌"]
FOOTER_LINES = [
    "социальный эксперимент почти без последствий",
    "театр одного чата, бюджет ноль, интрига бесценна",
    "все совпадения подозрительны",
    "если стало неловко, значит игра работает",
    "делаем вид, что всё под контролем",
]
AUTO_FINAL_TASKS: dict[int, asyncio.Task] = {}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with closing(db()) as conn:
        conn.executescript(
            """
            PRAGMA journal_mode = WAL;

            CREATE TABLE IF NOT EXISTS players (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                points INTEGER NOT NULL DEFAULT 0,
                games_played INTEGER NOT NULL DEFAULT 0,
                correct_sus INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS chats (
                chat_id INTEGER PRIMARY KEY,
                title TEXT,
                active_game_id INTEGER
            );

            CREATE TABLE IF NOT EXISTS games (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                status TEXT NOT NULL,
                started_at TEXT NOT NULL,
                ended_at TEXT
            );

            CREATE TABLE IF NOT EXISTS participants (
                game_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                role_name TEXT NOT NULL,
                mission TEXT NOT NULL,
                exposed INTEGER NOT NULL DEFAULT 0,
                completed INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (game_id, user_id)
            );

            CREATE TABLE IF NOT EXISTS suspicions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                game_id INTEGER NOT NULL,
                suspect_user_id INTEGER NOT NULL,
                target_user_id INTEGER NOT NULL,
                guessed_role TEXT NOT NULL,
                correct INTEGER,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS achievements (
                user_id INTEGER NOT NULL,
                code TEXT NOT NULL,
                earned_at TEXT NOT NULL,
                PRIMARY KEY (user_id, code)
            );

            CREATE TABLE IF NOT EXISTS bot_messages (
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (chat_id, message_id)
            );

            CREATE TABLE IF NOT EXISTS chat_settings (
                chat_id INTEGER PRIMARY KEY,
                round_minutes INTEGER NOT NULL DEFAULT 120,
                reveal_sus_immediately INTEGER NOT NULL DEFAULT 1,
                max_suspicions INTEGER NOT NULL DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS hints (
                game_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                used_at TEXT NOT NULL,
                PRIMARY KEY (game_id, user_id)
            );

            CREATE TABLE IF NOT EXISTS season_scores (
                season_day TEXT NOT NULL,
                user_id INTEGER NOT NULL,
                points INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (season_day, user_id)
            );
            """
        )
        for statement in [
            "ALTER TABLE participants ADD COLUMN note TEXT",
        ]:
            try:
                conn.execute(statement)
            except sqlite3.OperationalError:
                pass
        conn.commit()


def esc(value: object) -> str:
    return html.escape(str(value))


def mention(user_id: int, name: str) -> str:
    safe_name = esc(name or "игрок")
    return f'<a href="tg://user?id={user_id}">{safe_name}</a>'


def player_name(row: sqlite3.Row) -> str:
    return row["username"] and f'@{row["username"]}' or row["first_name"] or str(row["user_id"])


def card(title: str, *sections: str) -> str:
    clean_sections = [section for section in sections if section]
    body = "\n\n".join(clean_sections)
    emoji = random.choice(CARD_EMOJIS)
    footer = random.choice(FOOTER_LINES)
    return f"{emoji} <b>{esc(title)}</b> {emoji}\n━━━━━━━━━━━━━━\n{body}\n\n🫡 <i>{esc(footer)}</i>"


def command_line(command: str, text: str) -> str:
    return f"▫️ <code>{esc(command)}</code> — {esc(text)}"


async def reply_html(update: Update, text: str, reply_markup: InlineKeyboardMarkup | None = None) -> None:
    sent = await update.effective_message.reply_text(
        text,
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
        reply_markup=reply_markup,
    )
    remember_bot_message(sent.chat_id, sent.message_id)


async def send_html(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    text: str,
    reply_markup: InlineKeyboardMarkup | None = None,
) -> None:
    sent = await context.bot.send_message(
        chat_id=chat_id,
        text=text,
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
        reply_markup=reply_markup,
    )
    remember_bot_message(sent.chat_id, sent.message_id)


def remember_bot_message(chat_id: int, message_id: int) -> None:
    if chat_id > 0:
        return
    with closing(db()) as conn:
        conn.execute(
            "INSERT OR IGNORE INTO bot_messages (chat_id, message_id, created_at) VALUES (?, ?, ?)",
            (chat_id, message_id, now_iso()),
        )
        conn.commit()


def get_settings(chat_id: int) -> sqlite3.Row:
    with closing(db()) as conn:
        conn.execute(
            "INSERT OR IGNORE INTO chat_settings (chat_id) VALUES (?)",
            (chat_id,),
        )
        conn.commit()
        return conn.execute("SELECT * FROM chat_settings WHERE chat_id = ?", (chat_id,)).fetchone()


async def is_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    chat = update.effective_chat
    user = update.effective_user
    if not chat or not user or chat.type == Chat.PRIVATE:
        return True
    try:
        member = await context.bot.get_chat_member(chat.id, user.id)
    except TelegramError:
        return False
    return member.status in {"creator", "administrator"}


async def require_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if await is_admin(update, context):
        return True
    await reply_html(update, card("Только админы", "Эта кнопка влияет на весь чат, поэтому без анархии. Почти."))
    return False


def main_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Войти", callback_data="menu_play"),
                InlineKeyboardButton("Старт", callback_data="menu_start"),
            ],
            [
                InlineKeyboardButton("Подозревать", callback_data="menu_sus"),
                InlineKeyboardButton("Моя роль", callback_data="menu_me"),
            ],
            [
                InlineKeyboardButton("Статус", callback_data="menu_status"),
                InlineKeyboardButton("Финал", callback_data="menu_end"),
            ],
            [
                InlineKeyboardButton("Настройки", callback_data="menu_settings"),
                InlineKeyboardButton("Убрать сообщения", callback_data="menu_clean"),
            ],
        ]
    )


def role_dm_buttons(chat_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Миссия выполнена", callback_data=f"complete:{chat_id}")],
            [InlineKeyboardButton("Подсказка", callback_data=f"hint:{chat_id}")],
            [InlineKeyboardButton("Записать улику/результат", callback_data=f"note:{chat_id}")],
        ]
    )


def settings_buttons(settings: sqlite3.Row) -> InlineKeyboardMarkup:
    minutes = settings["round_minutes"]
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(("✓ " if minutes == 30 else "") + "30 мин", callback_data="settings_duration:30"),
                InlineKeyboardButton(("✓ " if minutes == 60 else "") + "60 мин", callback_data="settings_duration:60"),
            ],
            [
                InlineKeyboardButton(("✓ " if minutes == 120 else "") + "2 часа", callback_data="settings_duration:120"),
                InlineKeyboardButton(("✓ " if minutes == 240 else "") + "4 часа", callback_data="settings_duration:240"),
            ],
        ]
    )


def rules_text(short: bool = False) -> str:
    intro = (
        "Это чатовая социальная игра: все продолжают общаться как обычно, "
        "но у каждого есть тайная роль и скрытая миссия. Официально это игра, "
        "неофициально - маленький сериал с подозрительно знакомыми актерами 😌"
    )
    flow = "\n".join(
        [
            "1. Игроки пишут <code>/join</code>.",
            "2. Ведущий запускает <code>/startgame</code>.",
            "3. Я отправляю роли каждому в личку.",
            "4. Все выполняют миссии незаметно.",
            "5. Подозрения кидаются через <code>/sus @username роль</code>.",
            "6. В конце <code>/endgame</code> раскрывает роли и начисляет очки.",
        ]
    )
    commands = "\n".join(
        [
            command_line("/join", "войти в лобби"),
            command_line("/play", "самый простой вход в игру"),
            command_line("/status", "понять, что сейчас происходит"),
            command_line("/leave", "выйти из лобби до старта"),
            command_line("/startgame", "раздать роли"),
            command_line("/roles", "получить весь список ролей в личку"),
            command_line("/me", "посмотреть свою роль"),
            command_line("/sus @user роль", "проверить подозрение"),
            command_line("/complete", "отметить миссию выполненной"),
            command_line("/event", "случайное событие"),
            command_line("/score", "очки, уровень и звание"),
            command_line("/leaderboard", "таблица лидеров"),
            command_line("/endgame", "финал раунда"),
            command_line("/clean", "убрать мои сообщения из группы"),
            command_line("/head", "эта справка"),
        ]
    )
    if short:
        return card("Как играть", intro, flow)
    return card("Как играть", intro, f"<b>Ход игры</b>\n{flow}", f"<b>Команды</b>\n{commands}")


def roles_text() -> str:
    lines = []
    for role in ROLES:
        locked = "" if role.level == 1 else f" · открывается с ур. {role.level}"
        lines.append(f"▫️ <b>{esc(role.name)}</b>{esc(locked)}\n   {esc(role.mission)}")
    return card(
        "Каталог ролей",
        "Вот все роли, чтобы понимать, кого подозревать. Пользоваться этим знанием можно мудро, а можно как обычно 😌",
        "\n".join(lines),
    )


def role_by_name(role_name: str) -> Role | None:
    normalized = role_name.strip().lower()
    for role in ROLES:
        if role.name.lower() == normalized:
            return role
    return None


def role_buttons(target_user_id: int) -> InlineKeyboardMarkup:
    rows = []
    for index, role in enumerate(ROLES):
        rows.append([InlineKeyboardButton(role.name, callback_data=f"sus_role:{target_user_id}:{index}")])
    rows.append([InlineKeyboardButton("Отмена", callback_data="sus_cancel")])
    return InlineKeyboardMarkup(rows)


def player_buttons(players: list[sqlite3.Row]) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(player_name(player), callback_data=f"sus_target:{player['user_id']}")]
        for player in players
    ]
    rows.append([InlineKeyboardButton("Отмена", callback_data="sus_cancel")])
    return InlineKeyboardMarkup(rows)


def level_for(points: int) -> int:
    return max(1, points // 10 + 1)


def title_for(level: int) -> str:
    if level >= 8:
        return "Легенда двойной жизни"
    if level >= 5:
        return "Мастер маски"
    if level >= 3:
        return "Опасный актер"
    return "Новичок интриги"


def upsert_player(update: Update) -> None:
    user = update.effective_user
    if not user:
        return
    with closing(db()) as conn:
        conn.execute(
            """
            INSERT INTO players (user_id, username, first_name, created_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                username = excluded.username,
                first_name = excluded.first_name
            """,
            (user.id, user.username, user.first_name, now_iso()),
        )
        conn.commit()


def remember_chat(chat: Chat) -> None:
    if chat.type == Chat.PRIVATE:
        return
    with closing(db()) as conn:
        conn.execute(
            """
            INSERT INTO chats (chat_id, title)
            VALUES (?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET title = excluded.title
            """,
            (chat.id, chat.title or chat.full_name or str(chat.id)),
        )
        conn.commit()


def active_game(conn: sqlite3.Connection, chat_id: int) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT g.*
        FROM chats c
        JOIN games g ON g.id = c.active_game_id
        WHERE c.chat_id = ? AND g.status = 'active'
        """,
        (chat_id,),
    ).fetchone()


def choose_role(used: set[str], player_points: int) -> Role:
    level = level_for(player_points)
    available = [role for role in ROLES if role.level <= level and role.name not in used]
    if not available:
        available = [role for role in ROLES if role.name not in used] or ROLES
    return random.choice(available)


async def welcome_when_added(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    member_update = update.my_chat_member
    if not member_update:
        return

    old_status = member_update.old_chat_member.status
    new_status = member_update.new_chat_member.status
    was_out = old_status in {"left", "kicked"}
    is_in = new_status in {"member", "administrator"}
    if not was_out or not is_in:
        return

    chat = member_update.chat
    remember_chat(chat)
    text = card(
        "Я в чате. Начинаем двойную жизнь?",
        (
            "Привет всем. Я превращаю обычный чат в социальную игру с тайными ролями, "
            "миссиями, подозрениями, очками, уровнями и финальным раскрытием. "
            "Ваш чат был нормальным. Был."
        ),
        (
            "<b>Быстрый старт</b>\n"
            f"{command_line('/play', 'нажми, чтобы войти в игру без лишней философии')}\n"
            f"{command_line('/startgame', 'когда все вошли, я раздаю роли в личку')}\n"
            f"{command_line('/roles', 'полный список ролей прилетит в личку')}\n"
            f"{command_line('/sus @user роль', 'проверить подозрение')}\n"
            f"{command_line('/endgame', 'раскрыть роли и начислить очки')}\n"
            f"{command_line('/clean', 'убрать мои сообщения из группы')}"
        ),
        "Важно: перед игрой каждому нужно открыть личку со мной и нажать <code>/start</code>, иначе я не смогу отправить секретную роль. Да, бюрократия добралась даже до хаоса.",
    )
    await send_html(context, chat.id, text)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    upsert_player(update)
    await reply_html(update, rules_text(), main_menu() if update.effective_chat and update.effective_chat.type != Chat.PRIVATE else None)
    if update.effective_chat and update.effective_chat.type == Chat.PRIVATE:
        await reply_html(update, roles_text())


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await start(update, context)


async def head(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await start(update, context)


async def roles(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    upsert_player(update)
    user = update.effective_user
    chat = update.effective_chat
    if not user or not chat:
        return
    if chat.type == Chat.PRIVATE:
        await reply_html(update, roles_text())
        return
    try:
        await send_html(context, user.id, roles_text())
        await reply_html(update, card("Роли улетели в личку", "Список ролей отправлен приватно. Делайте вид, что это было исследование, а не подготовка к психологической операции 🕵️"))
    except Forbidden:
        await reply_html(update, card("Личка закрыта", f"{mention(user.id, user.first_name)}, сначала открой личку со мной и нажми <code>/start</code>. Потом <code>/roles</code> сработает как надо."))


async def play(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat = update.effective_chat
    if chat and chat.type == Chat.PRIVATE:
        await start(update, context)
        return
    await join(update, context)


async def menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await reply_html(update, card("Меню", "Выбери действие кнопкой. Команды помнить больше не обязательно."), main_menu())


async def status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat = update.effective_chat
    if not chat or chat.type == Chat.PRIVATE:
        await reply_html(update, card("Статус", "В личке статуса группы нет. Открой игровой чат и напиши <code>/status</code>."))
        return
    remember_chat(chat)
    with closing(db()) as conn:
        game = active_game(conn, chat.id)
        if game:
            count = conn.execute("SELECT COUNT(*) FROM participants WHERE game_id = ?", (game["id"],)).fetchone()[0]
            sus_count = conn.execute("SELECT COUNT(*) FROM suspicions WHERE game_id = ?", (game["id"],)).fetchone()[0]
            await reply_html(update, card("Игра идет", f"Игроков: <b>{count}</b>\nПодозрений: <b>{sus_count}</b>", "Дальше: <code>/sus</code> кнопками или <code>/endgame</code> для финала."))
            return
        lobby = conn.execute(
            "SELECT id FROM games WHERE chat_id = ? AND status = 'lobby' ORDER BY id DESC LIMIT 1",
            (chat.id,),
        ).fetchone()
        if not lobby:
            await reply_html(update, card("Игра не начата", "Игроки нажимают <code>/play</code>, потом запускаем <code>/startgame</code>."))
            return
        players = conn.execute(
            """
            SELECT pl.user_id, pl.username, pl.first_name
            FROM participants p
            JOIN players pl ON pl.user_id = p.user_id
            WHERE p.game_id = ?
            ORDER BY pl.first_name
            """,
            (lobby["id"],),
        ).fetchall()
    names = ", ".join(esc(player_name(player)) for player in players) or "пока никого"
    await reply_html(update, card("Лобби", f"Игроков: <b>{len(players)}</b>\n{names}", "Дальше: <code>/play</code> для входа, <code>/leave</code> для выхода, <code>/startgame</code> для старта."))


async def leave(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat = update.effective_chat
    user = update.effective_user
    if not chat or not user or chat.type == Chat.PRIVATE:
        await reply_html(update, card("Выход только в группе", "Из лобби выходят там же, где входили. Логично, почти философия."))
        return
    with closing(db()) as conn:
        lobby = conn.execute(
            "SELECT id FROM games WHERE chat_id = ? AND status = 'lobby' ORDER BY id DESC LIMIT 1",
            (chat.id,),
        ).fetchone()
        if not lobby:
            await reply_html(update, card("Лобби нет", "Сейчас неоткуда выходить. Свобода уже наступила."))
            return
        deleted = conn.execute(
            "DELETE FROM participants WHERE game_id = ? AND user_id = ?",
            (lobby["id"], user.id),
        ).rowcount
        conn.commit()
    if deleted:
        await reply_html(update, card("Вышел из лобби", f"{mention(user.id, user.first_name)} вышел. Драма отложена."))
    else:
        await reply_html(update, card("Ты не в лобби", "Нажми <code>/play</code>, если хочешь войти."))


async def clean(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat = update.effective_chat
    if not chat or chat.type == Chat.PRIVATE:
        await reply_html(update, card("Чистка только в группе", "В личке я и так веду себя прилично. Почти."))
        return
    if not await require_admin(update, context):
        return

    with closing(db()) as conn:
        rows = conn.execute(
            "SELECT message_id FROM bot_messages WHERE chat_id = ? ORDER BY message_id DESC LIMIT 100",
            (chat.id,),
        ).fetchall()

    deleted = 0
    for row in rows:
        try:
            await context.bot.delete_message(chat_id=chat.id, message_id=row["message_id"])
            deleted += 1
        except TelegramError:
            continue

    with closing(db()) as conn:
        conn.execute("DELETE FROM bot_messages WHERE chat_id = ?", (chat.id,))
        conn.commit()

    try:
        await context.bot.delete_message(chat_id=chat.id, message_id=update.effective_message.message_id)
    except TelegramError:
        pass

    notice = await context.bot.send_message(
        chat_id=chat.id,
        text=f"🧹 Убрал свои сообщения: {deleted}. Чат снова делает вид, что ничего не было.",
    )
    remember_bot_message(notice.chat_id, notice.message_id)
    await asyncio.sleep(5)
    try:
        await context.bot.delete_message(chat_id=chat.id, message_id=notice.message_id)
    except TelegramError:
        return
    with closing(db()) as conn:
        conn.execute(
            "DELETE FROM bot_messages WHERE chat_id = ? AND message_id = ?",
            (chat.id, notice.message_id),
        )
        conn.commit()


async def settings(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat = update.effective_chat
    if not chat or chat.type == Chat.PRIVATE:
        await reply_html(update, card("Настройки только в группе", "Настраивать нужно конкретный чат."))
        return
    if not await require_admin(update, context):
        return
    current = get_settings(chat.id)
    await reply_html(
        update,
        card("Настройки", f"Длительность раунда: <b>{current['round_minutes']} мин</b>", "Выбери длительность кнопкой."),
        settings_buttons(current),
    )


async def quick(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat = update.effective_chat
    if not chat or chat.type == Chat.PRIVATE:
        await reply_html(update, card("Быстрая игра только в группе", "В личке быстро играть странно даже для нас."))
        return
    if not await require_admin(update, context):
        return
    await play(update, context)
    await reply_html(update, card("Быстрая игра", "Лобби открыто. Через 2 минуты я попробую запустить раунд сам. Финал будет через 30 минут."), main_menu())
    context.application.create_task(quick_start_task(chat.id, context))


async def quick_start_task(chat_id: int, context: ContextTypes.DEFAULT_TYPE) -> None:
    await asyncio.sleep(120)
    with closing(db()) as conn:
        if active_game(conn, chat_id):
            return
        lobby = conn.execute(
            "SELECT id FROM games WHERE chat_id = ? AND status = 'lobby' ORDER BY id DESC LIMIT 1",
            (chat_id,),
        ).fetchone()
        if not lobby:
            await send_html(context, chat_id, card("Быстрая игра сорвалась", "Лобби так и не появилось."))
            return
        count = conn.execute("SELECT COUNT(*) FROM participants WHERE game_id = ?", (lobby["id"],)).fetchone()[0]
    if count < 2:
        await send_html(context, chat_id, card("Не стартуем", "Для быстрой игры нужно минимум 2 игрока."))
        return
    await start_game_by_chat(chat_id, context, final_minutes=30)


async def start_game_by_chat(chat_id: int, context: ContextTypes.DEFAULT_TYPE, final_minutes: int | None = None) -> None:
    with closing(db()) as conn:
        if active_game(conn, chat_id):
            return
        game = conn.execute(
            "SELECT id FROM games WHERE chat_id = ? AND status = 'lobby' ORDER BY id DESC LIMIT 1",
            (chat_id,),
        ).fetchone()
        if not game:
            return
        participants = conn.execute(
            """
            SELECT p.user_id, pl.points, pl.first_name, pl.username
            FROM participants p
            JOIN players pl ON pl.user_id = p.user_id
            WHERE p.game_id = ?
            """,
            (game["id"],),
        ).fetchall()
        used_roles: set[str] = set()
        assignments = []
        for participant in participants:
            role = choose_role(used_roles, participant["points"])
            used_roles.add(role.name)
            conn.execute(
                "UPDATE participants SET role_name = ?, mission = ? WHERE game_id = ? AND user_id = ?",
                (role.name, role.mission, game["id"], participant["user_id"]),
            )
            assignments.append((participant, role))
        conn.execute("UPDATE games SET status = 'active', started_at = ? WHERE id = ?", (now_iso(), game["id"]))
        conn.execute("UPDATE chats SET active_game_id = ? WHERE chat_id = ?", (game["id"], chat_id))
        conn.commit()
    for participant, role in assignments:
        try:
            await send_html(
                context,
                participant["user_id"],
                card("Твоя роль", f"<b>{esc(role.name)}</b>", f"<b>Миссия:</b>\n{esc(role.mission)}"),
                role_dm_buttons(chat_id),
            )
        except TelegramError:
            pass
    await send_html(context, chat_id, card("Быстрый раунд начался", "Роли отправлены. Подозрения: <code>/sus</code>."), main_menu())
    schedule_auto_final(chat_id, context, final_minutes)


async def season(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    today = date.today().isoformat()
    with closing(db()) as conn:
        rows = conn.execute(
            """
            SELECT ss.points, p.username, p.first_name, p.user_id
            FROM season_scores ss
            JOIN players p ON p.user_id = ss.user_id
            WHERE ss.season_day = ?
            ORDER BY ss.points DESC
            LIMIT 10
            """,
            (today,),
        ).fetchall()
    if not rows:
        await reply_html(update, card("Сезон дня пуст", "Сегодня ещё никто не набрал очки. Самое время начать драму."))
        return
    lines = [f"{idx}. <b>{esc(player_name(row))}</b> — {row['points']}" for idx, row in enumerate(rows, 1)]
    await reply_html(update, card("Сезон дня", "\n".join(lines)))


async def join(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    upsert_player(update)
    chat = update.effective_chat
    user = update.effective_user
    if not chat or not user:
        return
    if chat.type == Chat.PRIVATE:
        await reply_html(update, card("Лобби только в группе", "В личке я храню секретные роли и список ролей. Для входа в раунд напиши <code>/play</code> в общем чате."))
        return

    remember_chat(chat)
    with closing(db()) as conn:
        game = active_game(conn, chat.id)
        if game:
            await reply_html(update, card("Раунд уже идет", "Дождись следующего запуска, чтобы не ломать интригу текущей партии."))
            return

        pending = conn.execute(
            "SELECT id FROM games WHERE chat_id = ? AND status = 'lobby' ORDER BY id DESC LIMIT 1",
            (chat.id,),
        ).fetchone()
        if not pending:
            conn.execute(
                "INSERT INTO games (chat_id, status, started_at) VALUES (?, 'lobby', ?)",
                (chat.id, now_iso()),
            )
            pending_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        else:
            pending_id = pending["id"]

        conn.execute(
            """
            INSERT OR IGNORE INTO participants (game_id, user_id, role_name, mission)
            VALUES (?, ?, '', '')
            """,
            (pending_id, user.id),
        )
        conn.commit()

        count = conn.execute(
            "SELECT COUNT(*) FROM participants WHERE game_id = ?",
            (pending_id,),
        ).fetchone()[0]

    await reply_html(
        update,
        card(
            "Игрок в лобби",
            f"{mention(user.id, user.first_name)} присоединился к раунду. Очень подозрительно, но пока законно 😎",
            f"<b>Игроков в лобби:</b> {count}\nКогда все готовы: <code>/startgame</code>\nПахнет интригой и групповым чатом.",
            count >= 2 and "Игроков уже хватает. Можно запускать <code>/startgame</code>.",
        ),
    )
    try:
        await send_html(context, user.id, rules_text(short=True))
        await send_html(context, user.id, roles_text())
    except Forbidden:
        await reply_html(
            update,
            card(
                "Личка закрыта",
                f"{mention(user.id, user.first_name)}, открой личку со мной и нажми <code>/start</code>, чтобы получить правила, все роли и свою секретную роль на старте.",
            ),
        )


async def start_game(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    upsert_player(update)
    chat = update.effective_chat
    if not chat or chat.type == Chat.PRIVATE:
        await reply_html(update, card("Запуск только в группе", "Раунд нужно запускать там, где будет происходить игра."))
        return
    if not await require_admin(update, context):
        return

    remember_chat(chat)
    with closing(db()) as conn:
        if active_game(conn, chat.id):
            await reply_html(update, card("Раунд уже активен", "Сначала завершите текущую игру командой <code>/endgame</code>."))
            return

        game = conn.execute(
            "SELECT id FROM games WHERE chat_id = ? AND status = 'lobby' ORDER BY id DESC LIMIT 1",
            (chat.id,),
        ).fetchone()
        if not game:
            await reply_html(update, card("Лобби пустое", "Сначала игроки должны написать <code>/play</code>. Это теперь главная кнопка хаоса."))
            return

        participants = conn.execute(
            """
            SELECT p.user_id, pl.points, pl.first_name, pl.username
            FROM participants p
            JOIN players pl ON pl.user_id = p.user_id
            WHERE p.game_id = ?
            """,
            (game["id"],),
        ).fetchall()
        if len(participants) < 2:
            await reply_html(update, card("Нужно больше игроков", "Минимум для раунда - 2 человека."))
            return

        used_roles: set[str] = set()
        assignments = []
        for participant in participants:
            role = choose_role(used_roles, participant["points"])
            used_roles.add(role.name)
            conn.execute(
                """
                UPDATE participants
                SET role_name = ?, mission = ?
                WHERE game_id = ? AND user_id = ?
                """,
                (role.name, role.mission, game["id"], participant["user_id"]),
            )
            assignments.append((participant, role))

        conn.execute("UPDATE games SET status = 'active', started_at = ? WHERE id = ?", (now_iso(), game["id"]))
        conn.execute(
            """
            INSERT INTO chats (chat_id, title, active_game_id)
            VALUES (?, ?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
                title = excluded.title,
                active_game_id = excluded.active_game_id
            """,
            (chat.id, chat.title or str(chat.id), game["id"]),
        )
        conn.commit()

    failed = []
    for participant, role in assignments:
        text = card(
            "Твоя тайная роль",
            f"<b>{esc(role.name)}</b>",
            f"<b>Миссия:</b>\n{esc(role.mission)}",
            "Играй незаметно. В конце раунда чат попробует понять, кем ты был. Держи лицо, даже если миссия кричит внутри.",
        )
        try:
            await send_html(context, participant["user_id"], text, role_dm_buttons(chat.id))
        except Forbidden:
            failed.append(player_name(participant))
        except TelegramError as exc:
            logger.warning("Failed to DM user %s: %s", participant["user_id"], exc)
            failed.append(player_name(participant))

    message = card(
        "Раунд начался",
        f"Роли ушли в личку <b>{len(assignments)}</b> игрокам.",
        "Теперь общайтесь как обычно, но присматривайтесь к каждому странному повороту разговора. Если кто-то внезапно стал философом - ну вы поняли 👀",
        f"<b>Подозрение:</b> <code>/sus</code> и кнопки\n<b>Финал:</b> <code>/endgame</code>",
    )
    if failed:
        message += "\n\n" + card(
            "Не смог отправить роль",
            ", ".join(esc(name) for name in failed),
            "Этим игрокам нужно открыть личку со мной и нажать <code>/start</code>.",
        )
    await reply_html(update, message, main_menu())
    schedule_auto_final(chat.id, context)


async def me(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    upsert_player(update)
    user = update.effective_user
    if not user:
        return
    with closing(db()) as conn:
        row = conn.execute(
            """
            SELECT p.role_name, p.mission, g.chat_id
            FROM participants p
            JOIN games g ON g.id = p.game_id
            WHERE p.user_id = ? AND g.status = 'active'
            ORDER BY g.id DESC LIMIT 1
            """,
            (user.id,),
        ).fetchone()
    if not row:
        await reply_html(update, card("Роли пока нет", "Ты сейчас не участвуешь в активном раунде."))
        return
    await reply_html(update, card("Твоя роль", f"<b>{esc(row['role_name'])}</b>", f"<b>Миссия:</b>\n{esc(row['mission'])}"))


def record_suspicion(chat_id: int, suspect_user_id: int, target_user_id: int, guessed_role: str) -> tuple[str, sqlite3.Row | None]:
    with closing(db()) as conn:
        game = active_game(conn, chat_id)
        if not game:
            return "no_game", None

        target = conn.execute(
            """
            SELECT pl.user_id, pl.username, pl.first_name, p.role_name
            FROM participants p
            JOIN players pl ON pl.user_id = p.user_id
            WHERE p.game_id = ? AND pl.user_id = ?
            """,
            (game["id"], target_user_id),
        ).fetchone()
        if not target:
            return "not_found", None
        if target["user_id"] == suspect_user_id:
            return "self", target

        already = conn.execute(
            """
            SELECT id FROM suspicions
            WHERE game_id = ? AND suspect_user_id = ? AND target_user_id = ?
            """,
            (game["id"], suspect_user_id, target["user_id"]),
        ).fetchone()
        if already:
            return "already", target

        correct = int(guessed_role.lower() == target["role_name"].lower())
        conn.execute(
            """
            INSERT INTO suspicions (game_id, suspect_user_id, target_user_id, guessed_role, correct, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (game["id"], suspect_user_id, target["user_id"], guessed_role.lower(), correct, now_iso()),
        )
        if correct:
            conn.execute(
                "UPDATE participants SET exposed = 1 WHERE game_id = ? AND user_id = ?",
                (game["id"], target["user_id"]),
            )
        conn.commit()
        return ("correct" if correct else "wrong"), target


def suspicion_result_text(status: str, target: sqlite3.Row | None, guessed_role: str) -> str:
    if status == "no_game":
        return card("Игра не идет", "Сначала запустите раунд: <code>/startgame</code>.")
    if status == "not_found":
        return card("Игрок не найден", "Этого игрока нет в текущем раунде.")
    if status == "self":
        return card("Нельзя на себя", "Красиво, но подозревать себя нельзя.")
    if status == "already":
        return card("Уже было", "На одного игрока можно кинуть одно подозрение за раунд.")
    if status == "correct" and target:
        return card("Верно", f"{mention(target['user_id'], player_name(target))} действительно <b>{esc(target['role_name'])}</b>.")
    return card("Записал", f"Подозрение принято: <b>{esc(guessed_role)}</b>. Правду покажу в финале.")


async def suspect(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    upsert_player(update)
    chat = update.effective_chat
    suspect_user = update.effective_user
    if not chat or not suspect_user or chat.type == Chat.PRIVATE:
        await reply_html(update, card("Только в группе", "Подозрения работают в игровом чате."))
        return

    if not context.args or len(context.args) < 2:
        with closing(db()) as conn:
            game = active_game(conn, chat.id)
            if not game:
                await reply_html(update, card("Игра не идет", "Сначала запустите раунд: <code>/startgame</code>."))
                return
            players = conn.execute(
                """
                SELECT pl.user_id, pl.username, pl.first_name
                FROM participants p
                JOIN players pl ON pl.user_id = p.user_id
                WHERE p.game_id = ? AND pl.user_id != ?
                ORDER BY pl.first_name
                """,
                (game["id"], suspect_user.id),
            ).fetchall()
        if not players:
            await reply_html(update, card("Некого подозревать", "В раунде пока нет других игроков. Очень мирно. Слишком мирно."))
            return
        await reply_html(update, card("Кого подозреваешь?", "Выбери игрока кнопкой. Потом выберешь роль."), player_buttons(players))
        return

    target_token = context.args[0].lstrip("@").lower()
    guessed_role = " ".join(context.args[1:]).strip()
    role = role_by_name(guessed_role)
    if not role:
        await reply_html(update, card("Не знаю такую роль", "Проще напиши <code>/sus</code> и выбери роль кнопкой."))
        return

    with closing(db()) as conn:
        game = active_game(conn, chat.id)
        if not game:
            await reply_html(update, card("Игра не идет", "Сначала запустите раунд: <code>/startgame</code>."))
            return

        target = conn.execute(
            """
            SELECT pl.user_id, pl.username, pl.first_name, p.role_name
            FROM participants p
            JOIN players pl ON pl.user_id = p.user_id
            WHERE p.game_id = ?
              AND (LOWER(pl.username) = ? OR LOWER(pl.first_name) = ?)
            """,
            (game["id"], target_token, target_token),
        ).fetchone()
    if not target:
        await reply_html(update, card("Игрок не найден", "Напиши <code>/sus</code> и выбери игрока кнопкой."))
        return

    status, checked_target = record_suspicion(chat.id, suspect_user.id, target["user_id"], role.name)
    await reply_html(update, suspicion_result_text(status, checked_target, role.name))


async def suspicion_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.message or not query.from_user:
        return
    await query.answer()

    data = query.data or ""
    chat = query.message.chat
    user = query.from_user

    if data == "sus_cancel":
        await query.edit_message_text("Ок, отменили. Подозрение ушло пить чай.", parse_mode=ParseMode.HTML)
        return

    if data.startswith("sus_target:"):
        target_user_id = int(data.split(":", 1)[1])
        with closing(db()) as conn:
            game = active_game(conn, chat.id)
            if not game:
                await query.edit_message_text("Игра уже не идет. Драма закончилась раньше времени.")
                return
            target = conn.execute(
                """
                SELECT pl.user_id, pl.username, pl.first_name
                FROM participants p
                JOIN players pl ON pl.user_id = p.user_id
                WHERE p.game_id = ? AND pl.user_id = ?
                """,
                (game["id"], target_user_id),
            ).fetchone()
        if not target:
            await query.edit_message_text("Игрока уже нет в раунде.")
            return
        await query.edit_message_text(
            card("Какая роль?", f"Игрок: <b>{esc(player_name(target))}</b>\nВыбери роль кнопкой."),
            parse_mode=ParseMode.HTML,
            reply_markup=role_buttons(target_user_id),
        )
        return

    if data.startswith("sus_role:"):
        _, target_user_id_raw, role_index_raw = data.split(":")
        target_user_id = int(target_user_id_raw)
        role_index = int(role_index_raw)
        if role_index < 0 or role_index >= len(ROLES):
            await query.edit_message_text("Роль потерялась. Бывает даже у лучших.")
            return
        role = ROLES[role_index]
        status, target = record_suspicion(chat.id, user.id, target_user_id, role.name)
        await query.edit_message_text(
            suspicion_result_text(status, target, role.name),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )


async def action_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query:
        return
    await query.answer()
    data = query.data or ""
    chat = query.message.chat if query.message else update.effective_chat
    user = query.from_user

    if data == "menu_play":
        await play(update, context)
    elif data == "menu_start":
        await start_game(update, context)
    elif data == "menu_sus":
        await suspect(update, context)
    elif data == "menu_me":
        await me(update, context)
    elif data == "menu_status":
        await status(update, context)
    elif data == "menu_end":
        await end_game(update, context)
    elif data == "menu_clean":
        await clean(update, context)
    elif data == "menu_settings":
        await settings(update, context)
    elif data.startswith("complete:"):
        chat_id = int(data.split(":", 1)[1])
        await complete_private(update, context, chat_id)
    elif data.startswith("hint:"):
        chat_id = int(data.split(":", 1)[1])
        await give_hint(update, context, chat_id)
    elif data.startswith("note:"):
        chat_id = int(data.split(":", 1)[1])
        context.user_data["waiting_note_chat_id"] = chat_id
        await send_html(context, user.id, card("Напиши результат", "Одним сообщением: что получилось по миссии? Я добавлю это в финальный отчёт."))
    elif data.startswith("settings_duration:"):
        if not chat or not await is_admin(update, context):
            await send_html(context, user.id, card("Нужны права", "Настройки меняют только админы."))
            return
        minutes = int(data.split(":", 1)[1])
        with closing(db()) as conn:
            conn.execute(
                """
                INSERT INTO chat_settings (chat_id, round_minutes)
                VALUES (?, ?)
                ON CONFLICT(chat_id) DO UPDATE SET round_minutes = excluded.round_minutes
                """,
                (chat.id, minutes),
            )
            conn.commit()
        await query.edit_message_text(
            card("Настройки сохранены", f"Длительность раунда: <b>{minutes} мин</b>."),
            parse_mode=ParseMode.HTML,
        )


async def complete_private(update: Update, context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    user = update.effective_user
    if not user:
        return
    with closing(db()) as conn:
        game = active_game(conn, chat_id)
        if not game:
            await send_html(context, user.id, card("Раунд не найден", "Похоже, игра уже закончилась."))
            return
        updated = conn.execute(
            "UPDATE participants SET completed = 1 WHERE game_id = ? AND user_id = ?",
            (game["id"], user.id),
        ).rowcount
        conn.commit()
    await send_html(context, user.id, card("Миссия отмечена" if updated else "Ты не в раунде", "На финале это учтётся."))


async def give_hint(update: Update, context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    user = update.effective_user
    if not user:
        return
    with closing(db()) as conn:
        game = active_game(conn, chat_id)
        if not game:
            await send_html(context, user.id, card("Подсказки нет", "Раунд уже не активен."))
            return
        used = conn.execute("SELECT 1 FROM hints WHERE game_id = ? AND user_id = ?", (game["id"], user.id)).fetchone()
        if used:
            await send_html(context, user.id, card("Подсказка уже была", "Одна подсказка на раунд. Мы же цивилизованные."))
            return
        others = conn.execute(
            """
            SELECT p.role_name, pl.first_name, pl.username
            FROM participants p
            JOIN players pl ON pl.user_id = p.user_id
            WHERE p.game_id = ? AND p.user_id != ?
            """,
            (game["id"], user.id),
        ).fetchall()
        if not others:
            await send_html(context, user.id, card("Некого подсказывать", "Ты один в этом спектакле."))
            return
        target = random.choice(others)
        conn.execute("INSERT INTO hints (game_id, user_id, used_at) VALUES (?, ?, ?)", (game["id"], user.id, now_iso()))
        conn.commit()
    await send_html(
        context,
        user.id,
        card("Подсказка", f"Кто-то похож на роль из этой зоны: <b>{esc(target['role_name'])}</b>.\nИмя не скажу. Я загадочный, мне можно."),
    )


async def save_private_note(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat = update.effective_chat
    user = update.effective_user
    message = update.effective_message
    chat_id = context.user_data.pop("waiting_note_chat_id", None)
    if not chat_id or not chat or chat.type != Chat.PRIVATE or not user or not message or not message.text:
        return
    note = message.text.strip()[:300]
    with closing(db()) as conn:
        game = active_game(conn, chat_id)
        if game:
            conn.execute(
                "UPDATE participants SET note = ? WHERE game_id = ? AND user_id = ?",
                (note, game["id"], user.id),
            )
            conn.commit()
    await reply_html(update, card("Записал", "Добавлю это в финальный отчёт."))


async def complete(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    upsert_player(update)
    chat = update.effective_chat
    user = update.effective_user
    if not chat or not user or chat.type == Chat.PRIVATE:
        await reply_html(update, card("Отметка только в группе", "Миссию нужно отмечать в чате текущего раунда."))
        return
    with closing(db()) as conn:
        game = active_game(conn, chat.id)
        if not game:
            await reply_html(update, card("Нет активного раунда", "Сначала запустите игру через <code>/startgame</code>."))
            return
        updated = conn.execute(
            "UPDATE participants SET completed = 1 WHERE game_id = ? AND user_id = ?",
            (game["id"], user.id),
        ).rowcount
        conn.commit()
    if updated:
        await reply_html(update, card("Миссия отмечена", "На финале я начислю очки за выполнение."))
    else:
        await reply_html(update, card("Ты не в раунде", "Напиши <code>/join</code> перед следующей партией."))


async def event(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat = update.effective_chat
    if not chat or chat.type == Chat.PRIVATE:
        return
    with closing(db()) as conn:
        if not active_game(conn, chat.id):
            await reply_html(update, card("Событие пока нельзя", "Случайные события включаются только во время активного раунда."))
            return
    await reply_html(update, card("Случайное событие", esc(random.choice(EVENTS))))


def grant_achievement(conn: sqlite3.Connection, user_id: int, code: str) -> None:
    conn.execute(
        "INSERT OR IGNORE INTO achievements (user_id, code, earned_at) VALUES (?, ?, ?)",
        (user_id, code, now_iso()),
    )


def schedule_auto_final(chat_id: int, context: ContextTypes.DEFAULT_TYPE, minutes: int | None = None) -> None:
    old_task = AUTO_FINAL_TASKS.pop(chat_id, None)
    if old_task:
        old_task.cancel()
    settings = get_settings(chat_id)
    duration = minutes or int(settings["round_minutes"])
    AUTO_FINAL_TASKS[chat_id] = context.application.create_task(auto_final_task(chat_id, context, duration))


async def auto_final_task(chat_id: int, context: ContextTypes.DEFAULT_TYPE, minutes: int) -> None:
    try:
        warning_delay = max(0, (minutes - 10) * 60)
        if warning_delay:
            await asyncio.sleep(warning_delay)
            await send_html(context, chat_id, card("Финал скоро", "Через 10 минут я раскрою роли автоматически. Последний шанс вести себя нормально."))
            await asyncio.sleep(10 * 60)
        else:
            await asyncio.sleep(minutes * 60)
        text = finalize_game(chat_id)
        if text:
            await send_html(context, chat_id, text, main_menu())
    except asyncio.CancelledError:
        return


def finalize_game(chat_id: int) -> str | None:
    with closing(db()) as conn:
        game = active_game(conn, chat_id)
        if not game:
            return None

        participants = conn.execute(
            """
            SELECT p.*, pl.username, pl.first_name
            FROM participants p
            JOIN players pl ON pl.user_id = p.user_id
            WHERE p.game_id = ?
            ORDER BY pl.first_name
            """,
            (game["id"],),
        ).fetchall()
        suspicions = conn.execute(
            """
            SELECT s.*, sp.first_name AS suspect_name, sp.username AS suspect_username,
                   tp.first_name AS target_name, tp.username AS target_username
            FROM suspicions s
            JOIN players sp ON sp.user_id = s.suspect_user_id
            JOIN players tp ON tp.user_id = s.target_user_id
            WHERE s.game_id = ?
            """,
            (game["id"],),
        ).fetchall()

        scores: dict[int, int] = {p["user_id"]: 0 for p in participants}
        wrong_by_user: dict[int, int] = {}
        correct_by_user: dict[int, int] = {}
        suspicion_targets: dict[int, int] = {}
        for participant in participants:
            scores[participant["user_id"]] += 3 if participant["completed"] else 1
            if not participant["exposed"]:
                scores[participant["user_id"]] += 2
                grant_achievement(conn, participant["user_id"], "mask")

        for suspicion in suspicions:
            suspicion_targets[suspicion["target_user_id"]] = suspicion_targets.get(suspicion["target_user_id"], 0) + 1
            if suspicion["correct"]:
                scores[suspicion["suspect_user_id"]] = scores.get(suspicion["suspect_user_id"], 0) + 2
                correct_by_user[suspicion["suspect_user_id"]] = correct_by_user.get(suspicion["suspect_user_id"], 0) + 1
            else:
                wrong_by_user[suspicion["suspect_user_id"]] = wrong_by_user.get(suspicion["suspect_user_id"], 0) + 1

        for user_id, correct_count in correct_by_user.items():
            if correct_count >= 3:
                grant_achievement(conn, user_id, "sherlock")

        today = date.today().isoformat()
        for user_id, points in scores.items():
            conn.execute(
                """
                UPDATE players
                SET points = points + ?, games_played = games_played + 1,
                    correct_sus = correct_sus + ?
                WHERE user_id = ?
                """,
                (points, correct_by_user.get(user_id, 0), user_id),
            )
            conn.execute(
                """
                INSERT INTO season_scores (season_day, user_id, points)
                VALUES (?, ?, ?)
                ON CONFLICT(season_day, user_id) DO UPDATE SET points = points + excluded.points
                """,
                (today, user_id, points),
            )
            if points >= 5:
                grant_achievement(conn, user_id, "first_win")

        conn.execute("UPDATE games SET status = 'finished', ended_at = ? WHERE id = ?", (now_iso(), game["id"]))
        conn.execute("UPDATE chats SET active_game_id = NULL WHERE chat_id = ?", (chat_id,))
        conn.commit()

    AUTO_FINAL_TASKS.pop(chat_id, None)
    by_user = {p["user_id"]: p for p in participants}
    mvp_id = max(scores, key=scores.get) if scores else None
    best_detective_id = max(correct_by_user, key=correct_by_user.get) if correct_by_user else None
    most_suspicious_id = max(suspicion_targets, key=suspicion_targets.get) if suspicion_targets else None
    hidden = [p for p in participants if not p["exposed"]]
    stealth_id = hidden[0]["user_id"] if hidden else None
    shame_id = max(wrong_by_user, key=wrong_by_user.get) if wrong_by_user else None

    reveal_lines = []
    for participant in participants:
        name = mention(participant["user_id"], player_name(participant))
        status = "миссия выполнена" if participant["completed"] else "миссия не отмечена"
        exposed = "раскрыт" if participant["exposed"] else "сохранил маску"
        note = participant["note"] and f"\n  Улика: {esc(participant['note'])}" or ""
        reveal_lines.append(
            f"▫️ {name}: <b>{esc(participant['role_name'])}</b>\n"
            f"   {esc(status)}, {esc(exposed)}, <b>+{scores[participant['user_id']]}</b>{note}"
        )

    awards = []
    if mvp_id:
        awards.append(f"🏆 MVP: {mention(mvp_id, player_name(by_user[mvp_id]))} (+{scores[mvp_id]})")
    if best_detective_id:
        awards.append(f"🕵️ Лучший детектив: {mention(best_detective_id, player_name(by_user[best_detective_id]))}")
    if most_suspicious_id and most_suspicious_id in by_user:
        awards.append(f"👀 Самый подозрительный: {mention(most_suspicious_id, player_name(by_user[most_suspicious_id]))}")
    if stealth_id:
        awards.append(f"🥷 Самый незаметный: {mention(stealth_id, player_name(by_user[stealth_id]))}")
    if shame_id and shame_id in by_user:
        awards.append(f"💀 Позорное упоминание: {mention(shame_id, player_name(by_user[shame_id]))} за смелые, но неверные тыки")

    suspicion_lines = []
    for suspicion in suspicions:
        mark = "верно" if suspicion["correct"] else "мимо"
        suspicion_lines.append(
            f"▫️ {esc(suspicion['suspect_name'] or suspicion['suspect_username'] or 'Игрок')} -> "
            f"{esc(suspicion['target_name'] or suspicion['target_username'] or 'игрок')}: "
            f"{esc(suspicion['guessed_role'])} ({esc(mark)})"
        )

    return card(
        "Финал раунда",
        awards and "<b>Награды</b>\n" + "\n".join(awards),
        "<b>Роли</b>\n" + "\n".join(reveal_lines),
        suspicion_lines and "<b>Подозрения</b>\n" + "\n".join(suspicion_lines),
    )


async def end_game(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    upsert_player(update)
    chat = update.effective_chat
    if not chat or chat.type == Chat.PRIVATE:
        await reply_html(update, card("Финал только в группе", "Завершать раунд нужно там, где он проходил."))
        return
    if not await require_admin(update, context):
        return
    text = finalize_game(chat.id)
    if not text:
        await reply_html(update, card("Раунда нет", "Активного раунда сейчас нет."))
        return
    await reply_html(update, text, main_menu())


async def score(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    upsert_player(update)
    user = update.effective_user
    if not user:
        return
    with closing(db()) as conn:
        row = conn.execute("SELECT * FROM players WHERE user_id = ?", (user.id,)).fetchone()
        achievements = conn.execute("SELECT code FROM achievements WHERE user_id = ?", (user.id,)).fetchall()
    points = row["points"] if row else 0
    lvl = level_for(points)
    unlocked = ", ".join(ACHIEVEMENTS[a["code"]] for a in achievements if a["code"] in ACHIEVEMENTS) or "пока нет"
    await reply_html(
        update,
        card(
            "Профиль игрока",
            f"<b>Очки:</b> {points}\n<b>Уровень:</b> {lvl}\n<b>Звание:</b> {esc(title_for(lvl))}",
            f"<b>Достижения:</b> {esc(unlocked)}",
        ),
    )


async def leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    with closing(db()) as conn:
        rows = conn.execute(
            "SELECT * FROM players ORDER BY points DESC, correct_sus DESC LIMIT 10"
        ).fetchall()
    if not rows:
        await reply_html(update, card("Рейтинг пуст", "Сыграйте первый раунд, и здесь появятся легенды чата."))
        return
    lines = []
    for index, row in enumerate(rows, start=1):
        lvl = level_for(row["points"])
        lines.append(f"{index}. <b>{esc(player_name(row))}</b> - {row['points']} очков, ур. {lvl}")
    await reply_html(update, card("Топ игроков", "\n".join(lines)))


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path not in {"/", "/health"}:
            self.send_response(404)
            self.end_headers()
            return
        body = b'{"ok":true,"service":"double-life"}'
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        logger.debug("health: " + format, *args)


def run_health_server() -> None:
    server = ThreadingHTTPServer(("0.0.0.0", PORT), HealthHandler)
    logger.info("Health server listening on port %s", PORT)
    server.serve_forever()


async def post_init(application: Application) -> None:
    await application.bot.set_my_commands(BOT_COMMANDS)
    thread = threading.Thread(target=run_health_server, daemon=True)
    thread.start()


def build_app() -> Application:
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN environment variable is required")

    application = ApplicationBuilder().token(BOT_TOKEN).post_init(post_init).build()
    application.add_handler(ChatMemberHandler(welcome_when_added, ChatMemberHandler.MY_CHAT_MEMBER))
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_cmd))
    application.add_handler(CommandHandler("head", head))
    application.add_handler(MessageHandler(filters.Regex(r"^/хед(@\w+)?$"), head))
    application.add_handler(CommandHandler("menu", menu))
    application.add_handler(CommandHandler("play", play))
    application.add_handler(CommandHandler("roles", roles))
    application.add_handler(CommandHandler("status", status))
    application.add_handler(CommandHandler("leave", leave))
    application.add_handler(CommandHandler("clean", clean))
    application.add_handler(CommandHandler("settings", settings))
    application.add_handler(CommandHandler("quick", quick))
    application.add_handler(CommandHandler("join", join))
    application.add_handler(CommandHandler("startgame", start_game))
    application.add_handler(CommandHandler("me", me))
    application.add_handler(CommandHandler("sus", suspect))
    application.add_handler(CommandHandler("complete", complete))
    application.add_handler(CommandHandler("event", event))
    application.add_handler(CommandHandler("endgame", end_game))
    application.add_handler(CommandHandler("score", score))
    application.add_handler(CommandHandler("leaderboard", leaderboard))
    application.add_handler(CommandHandler("season", season))
    application.add_handler(CallbackQueryHandler(suspicion_button, pattern=r"^sus_"))
    application.add_handler(CallbackQueryHandler(action_button, pattern=r"^(menu_|complete:|hint:|note:|settings_)"))
    application.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & ~filters.COMMAND, save_private_note))
    return application


def main() -> None:
    init_db()
    application = build_app()
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
