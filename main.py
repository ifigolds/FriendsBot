import asyncio
import html
import logging
import os
import re
import sqlite3
import threading
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from telegram import BotCommand, Chat, InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.error import Forbidden, TelegramError
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)


DB_PATH = Path(os.getenv("DB_PATH", "contact_game.sqlite3"))
BOT_TOKEN = os.getenv("BOT_TOKEN")
PORT = int(os.getenv("PORT", "10000"))
COUNTDOWN_SECONDS = 5
WORD_RE = re.compile(r"^[а-яё]+$", re.IGNORECASE)
CONTACT_TASKS: dict[int, asyncio.Task] = {}

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    level=os.getenv("LOG_LEVEL", "INFO"),
)
logger = logging.getLogger("contact-bot")


@dataclass(frozen=True)
class PendingInput:
    kind: str
    chat_id: int
    clue_id: int | None = None


BOT_COMMANDS = [
    BotCommand("contact", "меню игры Контакт"),
    BotCommand("say", "админ: сказать от лица бота"),
]


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def esc(value: object) -> str:
    return html.escape(str(value))


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
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS games (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                status TEXT NOT NULL,
                host_id INTEGER NOT NULL,
                leader_id INTEGER,
                secret_word TEXT,
                revealed_len INTEGER NOT NULL DEFAULT 0,
                current_clue_id INTEGER,
                winner_id INTEGER,
                created_at TEXT NOT NULL,
                started_at TEXT,
                ended_at TEXT
            );

            CREATE TABLE IF NOT EXISTS participants (
                game_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                joined_at TEXT NOT NULL,
                PRIMARY KEY (game_id, user_id)
            );

            CREATE TABLE IF NOT EXISTS clues (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                game_id INTEGER NOT NULL,
                asker_id INTEGER NOT NULL,
                definition TEXT NOT NULL,
                intended_word TEXT NOT NULL,
                status TEXT NOT NULL,
                leader_guess TEXT,
                created_at TEXT NOT NULL,
                resolved_at TEXT
            );

            CREATE TABLE IF NOT EXISTS contact_words (
                clue_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                guessed_word TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (clue_id, user_id)
            );
            """
        )
        conn.commit()


def card(title: str, *parts: str) -> str:
    body = "\n\n".join(part for part in parts if part)
    return f"<b>{esc(title)}</b>\n\n{body}"


def mention(user_id: int, name: str) -> str:
    return f'<a href="tg://user?id={user_id}">{esc(name or "игрок")}</a>'


def player_name(row: sqlite3.Row) -> str:
    if row["username"]:
        return f"@{row['username']}"
    return row["first_name"] or str(row["user_id"])


async def reply_html(
    update: Update,
    text: str,
    reply_markup: InlineKeyboardMarkup | None = None,
) -> None:
    await update.effective_message.reply_text(
        text,
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
        reply_markup=reply_markup,
    )


async def send_html(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    text: str,
    reply_markup: InlineKeyboardMarkup | None = None,
) -> None:
    await context.bot.send_message(
        chat_id=chat_id,
        text=text,
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
        reply_markup=reply_markup,
    )


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


def current_game(conn: sqlite3.Connection, chat_id: int) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT *
        FROM games
        WHERE chat_id = ? AND status IN ('lobby', 'active')
        ORDER BY id DESC
        LIMIT 1
        """,
        (chat_id,),
    ).fetchone()


def current_prefix(game: sqlite3.Row) -> str:
    word = (game["secret_word"] or "").lower()
    return word[: game["revealed_len"]]


def validate_secret_word(word: str) -> str | None:
    normalized = word.strip().lower()
    if not normalized:
        return "Слово пустое."
    if " " in normalized or "-" in normalized:
        return "Нужно одно слово без пробелов."
    if not WORD_RE.fullmatch(normalized):
        return "Нужны только русские буквы."
    if len(normalized) < 2:
        return "Слово должно быть длиннее одной буквы."
    return None


async def is_admin(context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_id: int) -> bool:
    try:
        member = await context.bot.get_chat_member(chat_id, user_id)
    except TelegramError:
        return False
    return member.status in {"creator", "administrator"}


async def require_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    chat = update.effective_chat
    user = update.effective_user
    if not chat or not user or chat.type == Chat.PRIVATE:
        return False
    if await is_admin(context, chat.id, user.id):
        return True
    await reply_html(update, card("Нужен админ", "Это действие меняет игру для всего чата."))
    return False


def group_menu(has_game: bool) -> InlineKeyboardMarkup:
    if not has_game:
        return InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("Создать игру", callback_data="menu:create")],
                [InlineKeyboardButton("Что это?", callback_data="menu:help")],
            ]
        )
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Войти", callback_data="menu:join"),
                InlineKeyboardButton("Выйти", callback_data="menu:leave"),
            ],
            [
                InlineKeyboardButton("Выбрать ведущего", callback_data="menu:pick_leader"),
                InlineKeyboardButton("Слово в личку", callback_data="menu:set_word"),
            ],
            [
                InlineKeyboardButton("Старт", callback_data="menu:start"),
                InlineKeyboardButton("Статус", callback_data="menu:status"),
            ],
            [
                InlineKeyboardButton("Задать вопрос", callback_data="menu:ask"),
                InlineKeyboardButton("Контакт", callback_data="menu:contact"),
            ],
            [
                InlineKeyboardButton("Отбить", callback_data="menu:block"),
                InlineKeyboardButton("Назвать слово", callback_data="menu:guess"),
            ],
            [
                InlineKeyboardButton("Завершить", callback_data="menu:end"),
                InlineKeyboardButton("Что это?", callback_data="menu:help"),
            ],
        ]
    )


def help_text() -> str:
    return card(
        "Контакт",
        "Одна команда: <code>/contact</code>.",
        "Ведущий задает слово в личке, бот открывает первую букву, участники кидают определения, жмут «Контакт», а бот считает до пяти и открывает следующую букву, если контакт сошелся.",
        "Тон бота короткий и живой, но механику он держит строго.",
    )


def private_text() -> str:
    return card(
        "Личка бота",
        "Сюда ведущий отправляет секретное слово.",
        "Сюда участник отправляет слово для вопроса: <code>слово :: определение</code>.",
        "Сюда контактирующий отправляет слово, которое он понял.",
    )


def status_text(conn: sqlite3.Connection, game: sqlite3.Row) -> str:
    players = conn.execute(
        """
        SELECT p.user_id, pl.username, pl.first_name
        FROM participants p
        JOIN players pl ON pl.user_id = p.user_id
        WHERE p.game_id = ?
        ORDER BY pl.first_name
        """,
        (game["id"],),
    ).fetchall()
    leader = None
    if game["leader_id"]:
        leader = conn.execute(
            "SELECT user_id, username, first_name FROM players WHERE user_id = ?",
            (game["leader_id"],),
        ).fetchone()
    clue = None
    if game["current_clue_id"]:
        clue = conn.execute(
            "SELECT * FROM clues WHERE id = ?",
            (game["current_clue_id"],),
        ).fetchone()
    parts = [
        f"<b>Состояние:</b> {esc(game['status'])}",
        f"<b>Игроков:</b> {len(players)}",
        f"<b>Ведущий:</b> {esc(player_name(leader)) if leader else 'не выбран'}",
    ]
    if game["status"] == "active":
        parts.append(f"<b>Открыто:</b> {esc(current_prefix(game))}")
    if clue and clue["status"] == "open":
        parts.append(f"<b>Текущий вопрос:</b> {esc(clue['definition'])}")
    if players:
        parts.append("<b>Состав:</b> " + ", ".join(esc(player_name(player)) for player in players))
    return card("Статус игры", "\n".join(parts))


async def contact_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    upsert_player(update)
    chat = update.effective_chat
    if not chat:
        return
    if chat.type == Chat.PRIVATE:
        await reply_html(update, private_text())
        return
    with closing(db()) as conn:
        game = current_game(conn, chat.id)
        text = help_text() if not game else status_text(conn, game)
    await reply_html(update, text, group_menu(bool(game)))


async def create_game(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat = update.effective_chat
    user = update.effective_user
    if not chat or not user or chat.type == Chat.PRIVATE:
        return
    with closing(db()) as conn:
        if current_game(conn, chat.id):
            await reply_html(update, card("Игра уже есть", "Открой меню: <code>/contact</code>."))
            return
        conn.execute(
            """
            INSERT INTO games (chat_id, status, host_id, created_at)
            VALUES (?, 'lobby', ?, ?)
            """,
            (chat.id, user.id, now_iso()),
        )
        conn.commit()
    await reply_html(update, card("Игра создана", "Теперь игроки жмут «Войти», потом выбираете ведущего и стартуете."), group_menu(True))


async def join_game(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat = update.effective_chat
    user = update.effective_user
    if not chat or not user or chat.type == Chat.PRIVATE:
        return
    with closing(db()) as conn:
        game = current_game(conn, chat.id)
        if not game or game["status"] != "lobby":
            await reply_html(update, card("Лобби нет", "Сначала кто-то должен создать игру."))
            return
        conn.execute(
            """
            INSERT OR IGNORE INTO participants (game_id, user_id, joined_at)
            VALUES (?, ?, ?)
            """,
            (game["id"], user.id, now_iso()),
        )
        conn.commit()
        count = conn.execute("SELECT COUNT(*) FROM participants WHERE game_id = ?", (game["id"],)).fetchone()[0]
    await reply_html(update, card("Ты в игре", f"Игроков в лобби: <b>{count}</b>."), group_menu(True))


async def leave_game(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat = update.effective_chat
    user = update.effective_user
    if not chat or not user or chat.type == Chat.PRIVATE:
        return
    with closing(db()) as conn:
        game = current_game(conn, chat.id)
        if not game or game["status"] != "lobby":
            await reply_html(update, card("Выйти неоткуда", "Лобби сейчас нет."))
            return
        deleted = conn.execute(
            "DELETE FROM participants WHERE game_id = ? AND user_id = ?",
            (game["id"], user.id),
        ).rowcount
        if game["leader_id"] == user.id:
            conn.execute("UPDATE games SET leader_id = NULL WHERE id = ?", (game["id"],))
        conn.commit()
    await reply_html(update, card("Готово", "Ты вышел из лобби." if deleted else "Тебя не было в лобби."), group_menu(True))


async def pick_leader(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update, context):
        return
    chat = update.effective_chat
    if not chat:
        return
    with closing(db()) as conn:
        game = current_game(conn, chat.id)
        if not game or game["status"] != "lobby":
            await reply_html(update, card("Лобби нет", "Сначала создайте игру и соберите игроков."))
            return
        players = conn.execute(
            """
            SELECT p.user_id, pl.username, pl.first_name
            FROM participants p
            JOIN players pl ON pl.user_id = p.user_id
            WHERE p.game_id = ?
            ORDER BY pl.first_name
            """,
            (game["id"],),
        ).fetchall()
    if not players:
        await reply_html(update, card("Игроков нет", "Сначала кто-то должен войти в игру."))
        return
    keyboard = InlineKeyboardMarkup(
        [[InlineKeyboardButton(player_name(player), callback_data=f"leader:{player['user_id']}")] for player in players]
    )
    await reply_html(update, card("Выбери ведущего", "Кнопкой ниже."), keyboard)


async def set_leader(update: Update, context: ContextTypes.DEFAULT_TYPE, leader_id: int) -> None:
    if not await require_admin(update, context):
        return
    chat = update.effective_chat
    if not chat:
        return
    with closing(db()) as conn:
        game = current_game(conn, chat.id)
        if not game or game["status"] != "lobby":
            await reply_html(update, card("Поздно", "Лобби уже не в том состоянии."))
            return
        exists = conn.execute(
            "SELECT 1 FROM participants WHERE game_id = ? AND user_id = ?",
            (game["id"], leader_id),
        ).fetchone()
        if not exists:
            await reply_html(update, card("Не вышло", "Этот человек не в лобби."))
            return
        conn.execute("UPDATE games SET leader_id = ? WHERE id = ?", (leader_id, game["id"]))
        conn.commit()
        leader = conn.execute(
            "SELECT user_id, username, first_name FROM players WHERE user_id = ?",
            (leader_id,),
        ).fetchone()
    await reply_html(
        update,
        card("Ведущий выбран", f"Теперь ведущий: {mention(leader_id, player_name(leader))}. Пусть откроет личку с ботом и нажмет кнопку «Слово в личку»."),
        group_menu(True),
    )


async def ask_secret_word(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat = update.effective_chat
    user = update.effective_user
    if not chat or not user or chat.type == Chat.PRIVATE:
        return
    with closing(db()) as conn:
        game = current_game(conn, chat.id)
        if not game or game["status"] != "lobby":
            await reply_html(update, card("Лобби нет", "Сначала создайте игру."))
            return
        if game["leader_id"] != user.id:
            await reply_html(update, card("Не ты ведущий", "Секретное слово отправляет только ведущий."))
            return
    context.user_data["pending_input"] = PendingInput("secret", chat.id)
    await send_html(
        context,
        user.id,
        card(
            "Пришли секретное слово",
            "Одно русское существительное в нижнем регистре.",
            "Бот хранит его у себя и будет открывать буквы по ходу игры.",
        ),
    )
    await reply_html(update, card("Личка открыта", "Я жду слово от ведущего в личных сообщениях."))


async def save_secret_word(update: Update, context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    user = update.effective_user
    message = update.effective_message
    if not user or not message or not message.text:
        return
    error = validate_secret_word(message.text)
    if error:
        await reply_html(update, card("Не подходит", error))
        return
    word = message.text.strip().lower()
    with closing(db()) as conn:
        game = current_game(conn, chat_id)
        if not game or game["leader_id"] != user.id or game["status"] != "lobby":
            await reply_html(update, card("Уже неактуально", "Лобби изменилось, попробуй открыть меню заново."))
            return
        conn.execute(
            "UPDATE games SET secret_word = ?, revealed_len = 1 WHERE id = ?",
            (word, game["id"]),
        )
        conn.commit()
    await reply_html(update, card("Слово сохранено", f"Первая буква: <b>{esc(word[:1])}</b>. Теперь можно запускать раунд."))
    await send_html(context, chat_id, card("Слово у бота", "Ведущий передал слово. Можно жать «Старт»."))


async def start_round(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat = update.effective_chat
    if not chat or chat.type == Chat.PRIVATE:
        return
    if not await require_admin(update, context):
        return
    with closing(db()) as conn:
        game = current_game(conn, chat.id)
        if not game or game["status"] != "lobby":
            await reply_html(update, card("Старт невозможен", "Сейчас нет готового лобби."))
            return
        if not game["leader_id"]:
            await reply_html(update, card("Нет ведущего", "Сначала выбери ведущего."))
            return
        if not game["secret_word"]:
            await reply_html(update, card("Нет слова", "Ведущий еще не отправил слово в личку."))
            return
        count = conn.execute("SELECT COUNT(*) FROM participants WHERE game_id = ?", (game["id"],)).fetchone()[0]
        if count < 2:
            await reply_html(update, card("Мало игроков", "Нужно минимум два участника кроме бота."))
            return
        conn.execute(
            "UPDATE games SET status = 'active', started_at = ? WHERE id = ?",
            (now_iso(), game["id"]),
        )
        conn.commit()
        leader = conn.execute(
            "SELECT user_id, username, first_name FROM players WHERE user_id = ?",
            (game["leader_id"],),
        ).fetchone()
        prefix = game["secret_word"][:1]
    await reply_html(
        update,
        card(
            "Раунд начался",
            f"Ведущий: {mention(leader['user_id'], player_name(leader))}",
            f"Открыто: <b>{esc(prefix)}</b>",
            "Теперь участники задают определения, а ведущий пытается их отбить.",
        ),
        group_menu(True),
    )


async def prompt_clue(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat = update.effective_chat
    user = update.effective_user
    if not chat or not user or chat.type == Chat.PRIVATE:
        return
    with closing(db()) as conn:
        game = current_game(conn, chat.id)
        if not game or game["status"] != "active":
            await reply_html(update, card("Раунд не идет", "Сначала запустите игру."))
            return
        if game["leader_id"] == user.id:
            await reply_html(update, card("Ведущий не спрашивает", "Сейчас твоя работа отбивать вопросы."))
            return
        joined = conn.execute(
            "SELECT 1 FROM participants WHERE game_id = ? AND user_id = ?",
            (game["id"], user.id),
        ).fetchone()
        if not joined:
            await reply_html(update, card("Ты не в игре", "Сначала войди в лобби следующей игры."))
            return
        if game["current_clue_id"]:
            clue = conn.execute("SELECT status FROM clues WHERE id = ?", (game["current_clue_id"],)).fetchone()
            if clue and clue["status"] == "open":
                await reply_html(update, card("Вопрос уже есть", "Сначала нужно доиграть текущий вопрос."))
                return
        prefix = current_prefix(game)
    context.user_data["pending_input"] = PendingInput("clue", chat.id)
    await send_html(
        context,
        user.id,
        card(
            "Пришли вопрос",
            f"Формат: <code>слово :: определение</code>",
            f"Оба должны начинаться с <b>{esc(prefix)}</b>.",
        ),
    )
    await reply_html(update, card("Жду в личке", "Пришли слово и определение боту в личные сообщения."))


async def save_clue(update: Update, context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    user = update.effective_user
    message = update.effective_message
    if not user or not message or not message.text:
        return
    if "::" not in message.text:
        await reply_html(update, card("Нужен формат", "Напиши так: <code>слово :: определение</code>"))
        return
    intended, definition = [part.strip() for part in message.text.split("::", 1)]
    if not intended or not definition:
        await reply_html(update, card("Нужно оба поля", "И слово, и определение."))
        return
    intended = intended.lower()
    if not WORD_RE.fullmatch(intended):
        await reply_html(update, card("Слово не подходит", "Нужно одно русское слово без пробелов."))
        return
    with closing(db()) as conn:
        game = current_game(conn, chat_id)
        if not game or game["status"] != "active":
            await reply_html(update, card("Раунд уже ушел", "Игра изменилась, вопрос не сохранил."))
            return
        prefix = current_prefix(game)
        if not intended.startswith(prefix):
            await reply_html(update, card("Не тот префикс", f"Слово должно начинаться с <b>{esc(prefix)}</b>."))
            return
        clue_id = conn.execute(
            """
            INSERT INTO clues (game_id, asker_id, definition, intended_word, status, created_at)
            VALUES (?, ?, ?, ?, 'open', ?)
            """,
            (game["id"], user.id, definition, intended, now_iso()),
        ).lastrowid
        conn.execute("UPDATE games SET current_clue_id = ? WHERE id = ?", (clue_id, game["id"]))
        conn.commit()
    await reply_html(update, card("Вопрос отправлен", "Бросил определение в чат. Теперь ждем контакт или ответ ведущего."))
    await send_html(
        context,
        chat_id,
        card("Новый вопрос", f"От {mention(user.id, user.first_name)}:\n{esc(definition)}"),
        group_menu(True),
    )


async def prompt_contact(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat = update.effective_chat
    user = update.effective_user
    if not chat or not user or chat.type == Chat.PRIVATE:
        return
    with closing(db()) as conn:
        game = current_game(conn, chat.id)
        if not game or game["status"] != "active" or not game["current_clue_id"]:
            await reply_html(update, card("Сейчас нечего контактить", "Нужен активный вопрос."))
            return
        clue = conn.execute("SELECT * FROM clues WHERE id = ?", (game["current_clue_id"],)).fetchone()
        if not clue or clue["status"] != "open":
            await reply_html(update, card("Сейчас нечего контактить", "Нужен активный вопрос."))
            return
        if clue["asker_id"] == user.id:
            await reply_html(update, card("Ты автор вопроса", "Автор ждет совпадение от другого игрока."))
            return
        if game["leader_id"] == user.id:
            await reply_html(update, card("Ты ведущий", "Тебе надо отбивать, а не контактить."))
            return
    context.user_data["pending_input"] = PendingInput("contact", chat.id, clue["id"])
    await send_html(
        context,
        user.id,
        card("Напиши слово", "Одно слово, которое ты понял из вопроса. Если оно совпадет с авторским, откроем следующую букву."),
    )
    if clue["id"] not in CONTACT_TASKS:
        CONTACT_TASKS[clue["id"]] = context.application.create_task(run_contact_countdown(chat.id, clue["id"], context))
        await reply_html(update, card("Контакт", f"Пошел отсчет: <b>{COUNTDOWN_SECONDS}</b> секунд."))
    else:
        await reply_html(update, card("Контакт", "Ты добавился в контакт. Слово жду в личке."))


async def save_contact_word(update: Update, context: ContextTypes.DEFAULT_TYPE, chat_id: int, clue_id: int) -> None:
    user = update.effective_user
    message = update.effective_message
    if not user or not message or not message.text:
        return
    word = message.text.strip().lower()
    if not WORD_RE.fullmatch(word):
        await reply_html(update, card("Не подходит", "Нужно одно русское слово."))
        return
    with closing(db()) as conn:
        clue = conn.execute("SELECT * FROM clues WHERE id = ?", (clue_id,)).fetchone()
        game = current_game(conn, chat_id)
        if not game or not clue or game["current_clue_id"] != clue_id or clue["status"] != "open":
            await reply_html(update, card("Поздно", "Этот контакт уже не активен."))
            return
        conn.execute(
            """
            INSERT INTO contact_words (clue_id, user_id, guessed_word, created_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(clue_id, user_id) DO UPDATE SET guessed_word = excluded.guessed_word
            """,
            (clue_id, user.id, word, now_iso()),
        )
        conn.commit()
    await reply_html(update, card("Слово записал", f"Для контакта у тебя стоит: <b>{esc(word)}</b>."))


async def prompt_block(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat = update.effective_chat
    user = update.effective_user
    if not chat or not user or chat.type == Chat.PRIVATE:
        return
    with closing(db()) as conn:
        game = current_game(conn, chat.id)
        if not game or game["status"] != "active" or not game["current_clue_id"]:
            await reply_html(update, card("Отбивать нечего", "Сейчас нет активного вопроса."))
            return
        if game["leader_id"] != user.id:
            await reply_html(update, card("Ты не ведущий", "Отбивает только ведущий."))
            return
    context.user_data["pending_input"] = PendingInput("block", chat.id, game["current_clue_id"])
    await send_html(
        context,
        user.id,
        card("Какое слово отбиваешь?", "Напиши слово, которое, по твоему, имел в виду спрашивающий."),
    )
    await reply_html(update, card("Жду в личке", "Слово ведущего жду в личных сообщениях."))


async def save_block_word(update: Update, context: ContextTypes.DEFAULT_TYPE, chat_id: int, clue_id: int) -> None:
    user = update.effective_user
    message = update.effective_message
    if not user or not message or not message.text:
        return
    word = message.text.strip().lower()
    if not WORD_RE.fullmatch(word):
        await reply_html(update, card("Не подходит", "Нужно одно русское слово."))
        return
    with closing(db()) as conn:
        clue = conn.execute("SELECT * FROM clues WHERE id = ?", (clue_id,)).fetchone()
        game = current_game(conn, chat_id)
        if not game or game["leader_id"] != user.id or not clue or clue["status"] != "open":
            await reply_html(update, card("Поздно", "Этот вопрос уже ушел дальше."))
            return
        conn.execute(
            "UPDATE clues SET leader_guess = ?, status = ?, resolved_at = ? WHERE id = ?",
            (word, "blocked" if word == clue["intended_word"] else "open", now_iso() if word == clue["intended_word"] else None, clue_id),
        )
        if word == clue["intended_word"]:
            conn.execute("UPDATE games SET current_clue_id = NULL WHERE id = ?", (game["id"],))
        conn.commit()
    if word == clue["intended_word"]:
        task = CONTACT_TASKS.pop(clue_id, None)
        if task:
            task.cancel()
        await reply_html(update, card("Отбил", f"Да, это не <b>{esc(word)}</b>."))
        await send_html(context, chat_id, card("Ведущий отбил", f"Нет, это не <b>{esc(word)}</b>."))
    else:
        await reply_html(update, card("Не сошлось", "Бот не считает это отбитием. Контакт еще жив."))


async def prompt_secret_guess(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat = update.effective_chat
    user = update.effective_user
    if not chat or not user or chat.type == Chat.PRIVATE:
        return
    with closing(db()) as conn:
        game = current_game(conn, chat.id)
        if not game or game["status"] != "active":
            await reply_html(update, card("Раунд не идет", "Сейчас нечего отгадывать."))
            return
    context.user_data["pending_input"] = PendingInput("guess", chat.id)
    await send_html(context, user.id, card("Назови слово", "Напиши полное слово. Если угадаешь, игра закончится."))
    await reply_html(update, card("Жду в личке", "Полное слово жду в личных сообщениях."))


async def save_secret_guess(update: Update, context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    user = update.effective_user
    message = update.effective_message
    if not user or not message or not message.text:
        return
    word = message.text.strip().lower()
    with closing(db()) as conn:
        game = current_game(conn, chat_id)
        if not game or game["status"] != "active":
            await reply_html(update, card("Поздно", "Раунд уже закончился."))
            return
        if word == (game["secret_word"] or "").lower():
            conn.execute(
                "UPDATE games SET status = 'finished', winner_id = ?, ended_at = ? WHERE id = ?",
                (user.id, now_iso(), game["id"]),
            )
            conn.commit()
            task = CONTACT_TASKS.pop(game["current_clue_id"], None) if game["current_clue_id"] else None
            if task:
                task.cancel()
            await reply_html(update, card("Угадал", f"Это действительно <b>{esc(word)}</b>."))
            await send_html(
                context,
                chat_id,
                card("Игра окончена", f"{mention(user.id, user.first_name)} угадал слово: <b>{esc(word)}</b>.\nСледующим ведущим обычно становится он."),
            )
            return
    await reply_html(update, card("Мимо", "Это не загаданное слово."))


async def run_contact_countdown(chat_id: int, clue_id: int, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        await asyncio.sleep(COUNTDOWN_SECONDS)
        await resolve_contact(chat_id, clue_id, context)
    except asyncio.CancelledError:
        return
    finally:
        CONTACT_TASKS.pop(clue_id, None)


async def resolve_contact(chat_id: int, clue_id: int, context: ContextTypes.DEFAULT_TYPE) -> None:
    with closing(db()) as conn:
        clue = conn.execute("SELECT * FROM clues WHERE id = ?", (clue_id,)).fetchone()
        if not clue or clue["status"] != "open":
            return
        game = conn.execute("SELECT * FROM games WHERE id = ?", (clue["game_id"],)).fetchone()
        if not game or game["status"] != "active" or game["current_clue_id"] != clue_id:
            return
        submissions = conn.execute(
            """
            SELECT user_id, guessed_word
            FROM contact_words
            WHERE clue_id = ?
            """,
            (clue_id,),
        ).fetchall()
        correct = [row for row in submissions if row["guessed_word"] == clue["intended_word"]]
        if correct:
            new_len = min(len(game["secret_word"]), game["revealed_len"] + 1)
            conn.execute(
                "UPDATE games SET revealed_len = ?, current_clue_id = NULL WHERE id = ?",
                (new_len, game["id"]),
            )
            conn.execute(
                "UPDATE clues SET status = 'contact', resolved_at = ? WHERE id = ?",
                (now_iso(), clue_id),
            )
            conn.commit()
            prefix = game["secret_word"][:new_len]
            if new_len >= len(game["secret_word"]):
                conn.execute(
                    "UPDATE games SET status = 'finished', ended_at = ? WHERE id = ?",
                    (now_iso(), game["id"]),
                )
                conn.commit()
                await send_html(
                    context,
                    chat_id,
                    card("Слово открылось целиком", f"<b>{esc(game['secret_word'])}</b>\nРаунд закончен."),
                )
                return
            await send_html(
                context,
                chat_id,
                card("Контакт сошелся", f"Открываю следующую букву.\nТеперь: <b>{esc(prefix)}</b>"),
                group_menu(True),
            )
            return
        conn.execute(
            "UPDATE clues SET status = 'failed', resolved_at = ? WHERE id = ?",
            (now_iso(), clue_id),
        )
        conn.execute("UPDATE games SET current_clue_id = NULL WHERE id = ?", (game["id"],))
        conn.commit()
    await send_html(context, chat_id, card("Контакт не сошелся", "Слова не совпали. Придумывайте новый вопрос."), group_menu(True))


async def finish_game(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat = update.effective_chat
    if not chat or chat.type == Chat.PRIVATE:
        return
    if not await require_admin(update, context):
        return
    with closing(db()) as conn:
        game = current_game(conn, chat.id)
        if not game:
            await reply_html(update, card("Игры нет", "Сейчас нечего завершать."))
            return
        conn.execute(
            "UPDATE games SET status = 'finished', ended_at = ? WHERE id = ?",
            (now_iso(), game["id"]),
        )
        conn.commit()
        leader = None
        if game["leader_id"]:
            leader = conn.execute(
                "SELECT user_id, username, first_name FROM players WHERE user_id = ?",
                (game["leader_id"],),
            ).fetchone()
    if game["current_clue_id"]:
        task = CONTACT_TASKS.pop(game["current_clue_id"], None)
        if task:
            task.cancel()
    word = game["secret_word"] or "не задано"
    leader_text = player_name(leader) if leader else "не выбран"
    await reply_html(
        update,
        card("Игра завершена", f"Ведущий: <b>{esc(leader_text)}</b>\nСлово: <b>{esc(word)}</b>"),
    )


async def handle_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.message:
        return
    await query.answer()
    data = query.data or ""
    action = data.split(":", 1)[1] if ":" in data else ""
    if action == "help":
        await query.edit_message_text(help_text(), parse_mode=ParseMode.HTML, reply_markup=group_menu(True))
        return
    if action == "create":
        await create_game(update, context)
    elif action == "join":
        await join_game(update, context)
    elif action == "leave":
        await leave_game(update, context)
    elif action == "pick_leader":
        await pick_leader(update, context)
    elif action == "set_word":
        await ask_secret_word(update, context)
    elif action == "start":
        await start_round(update, context)
    elif action == "status":
        await contact_command(update, context)
    elif action == "ask":
        await prompt_clue(update, context)
    elif action == "contact":
        await prompt_contact(update, context)
    elif action == "block":
        await prompt_block(update, context)
    elif action == "guess":
        await prompt_secret_guess(update, context)
    elif action == "end":
        await finish_game(update, context)


async def handle_leader_pick(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.message:
        return
    await query.answer()
    leader_id = int((query.data or "").split(":", 1)[1])
    await set_leader(update, context, leader_id)


async def private_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    upsert_player(update)
    pending: PendingInput | None = context.user_data.pop("pending_input", None)
    if not pending:
        await reply_html(update, private_text())
        return
    if pending.kind == "secret":
        await save_secret_word(update, context, pending.chat_id)
    elif pending.kind == "clue":
        await save_clue(update, context, pending.chat_id)
    elif pending.kind == "contact" and pending.clue_id:
        await save_contact_word(update, context, pending.chat_id, pending.clue_id)
    elif pending.kind == "block" and pending.clue_id:
        await save_block_word(update, context, pending.chat_id, pending.clue_id)
    elif pending.kind == "guess":
        await save_secret_guess(update, context, pending.chat_id)


async def say(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat = update.effective_chat
    message = update.effective_message
    if not chat or chat.type == Chat.PRIVATE:
        await reply_html(update, card("Только в группе", "Эта команда пишет от лица бота в общем чате."))
        return
    if not await require_admin(update, context):
        return
    text = " ".join(context.args).strip()
    if not text:
        await reply_html(update, card("Нужен текст", "Формат: <code>/say текст</code>"))
        return
    try:
        await context.bot.delete_message(chat.id, message.message_id)
    except TelegramError:
        pass
    await context.bot.send_message(chat.id, text)


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path not in {"/", "/health"}:
            self.send_response(404)
            self.end_headers()
            return
        body = b'{"ok":true,"service":"contact-bot"}'
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
    application.add_handler(CommandHandler("contact", contact_command))
    application.add_handler(CommandHandler("say", say))
    application.add_handler(CallbackQueryHandler(handle_menu, pattern=r"^menu:"))
    application.add_handler(CallbackQueryHandler(handle_leader_pick, pattern=r"^leader:"))
    application.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & ~filters.COMMAND, private_message))
    return application


def main() -> None:
    init_db()
    application = build_app()
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
