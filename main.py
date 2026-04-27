import asyncio
import html
import logging
import os
import random
import sqlite3
import threading
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from telegram import BotCommand, Chat, Update
from telegram.constants import ParseMode
from telegram.error import Forbidden, TelegramError
from telegram.ext import (
    Application,
    ApplicationBuilder,
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
    BotCommand("startgame", "начать раунд"),
    BotCommand("roles", "все роли в личку"),
    BotCommand("sus", "подозревать игрока"),
    BotCommand("me", "моя тайная роль"),
    BotCommand("complete", "отметить миссию"),
    BotCommand("event", "случайное событие"),
    BotCommand("endgame", "завершить раунд"),
    BotCommand("score", "мой профиль"),
    BotCommand("leaderboard", "рейтинг"),
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
            """
        )
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


async def reply_html(update: Update, text: str) -> None:
    sent = await update.effective_message.reply_text(
        text,
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )
    remember_bot_message(sent.chat_id, sent.message_id)


async def send_html(context: ContextTypes.DEFAULT_TYPE, chat_id: int, text: str) -> None:
    sent = await context.bot.send_message(
        chat_id=chat_id,
        text=text,
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
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
    await reply_html(update, rules_text())
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


async def clean(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat = update.effective_chat
    if not chat or chat.type == Chat.PRIVATE:
        await reply_html(update, card("Чистка только в группе", "В личке я и так веду себя прилично. Почти."))
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
            await send_html(context, participant["user_id"], text)
        except Forbidden:
            failed.append(player_name(participant))
        except TelegramError as exc:
            logger.warning("Failed to DM user %s: %s", participant["user_id"], exc)
            failed.append(player_name(participant))

    message = card(
        "Раунд начался",
        f"Роли ушли в личку <b>{len(assignments)}</b> игрокам.",
        "Теперь общайтесь как обычно, но присматривайтесь к каждому странному повороту разговора. Если кто-то внезапно стал философом - ну вы поняли 👀",
        f"<b>Подозрение:</b> <code>/sus @username роль</code>\n<b>Финал:</b> <code>/endgame</code>",
    )
    if failed:
        message += "\n\n" + card(
            "Не смог отправить роль",
            ", ".join(esc(name) for name in failed),
            "Этим игрокам нужно открыть личку со мной и нажать <code>/start</code>.",
        )
    await reply_html(update, message)


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


async def suspect(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    upsert_player(update)
    chat = update.effective_chat
    suspect_user = update.effective_user
    if not chat or not suspect_user or chat.type == Chat.PRIVATE:
        await reply_html(update, card("Подозрение только в группе", "Формат: <code>/sus @username роль</code>"))
        return

    if not context.args or len(context.args) < 2:
        await reply_html(update, card("Нужен формат", "Напиши так: <code>/sus @username роль</code>"))
        return

    target_token = context.args[0].lstrip("@").lower()
    guessed_role = " ".join(context.args[1:]).strip().lower()

    with closing(db()) as conn:
        game = active_game(conn, chat.id)
        if not game:
            await reply_html(update, card("Нет активного раунда", "Сначала запустите игру через <code>/startgame</code>."))
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
            await reply_html(update, card("Игрок не найден", "Не вижу такого участника в текущем раунде. Надежнее всего использовать username."))
            return
        if target["user_id"] == suspect_user.id:
            await reply_html(update, card("Красиво, но нет", "Самоподозрение звучит драматично, но очков за него не будет."))
            return

        already = conn.execute(
            """
            SELECT id FROM suspicions
            WHERE game_id = ? AND suspect_user_id = ? AND target_user_id = ?
            """,
            (game["id"], suspect_user.id, target["user_id"]),
        ).fetchone()
        if already:
            await reply_html(update, card("Подозрение уже есть", "На одного игрока можно сделать одно подозрение за раунд."))
            return

        correct = int(guessed_role == target["role_name"].lower())
        conn.execute(
            """
            INSERT INTO suspicions (game_id, suspect_user_id, target_user_id, guessed_role, correct, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (game["id"], suspect_user.id, target["user_id"], guessed_role, correct, now_iso()),
        )
        if correct:
            conn.execute(
                "UPDATE participants SET exposed = 1 WHERE game_id = ? AND user_id = ?",
                (game["id"], target["user_id"]),
            )
        conn.commit()

    if correct:
        await reply_html(
            update,
            card(
                "Попадание",
                f"{mention(target['user_id'], player_name(target))} раскрыт.",
                f"<b>Роль:</b> {esc(target['role_name'])}",
            ),
        )
    else:
        await reply_html(update, card("Подозрение записано", "Пока не раскрываю правду. Финал все расставит по местам."))


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


async def end_game(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    upsert_player(update)
    chat = update.effective_chat
    if not chat or chat.type == Chat.PRIVATE:
        await reply_html(update, card("Финал только в группе", "Завершать раунд нужно там, где он проходил."))
        return

    with closing(db()) as conn:
        game = active_game(conn, chat.id)
        if not game:
            await reply_html(update, card("Раунда нет", "Активного раунда сейчас нет."))
            return

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
            SELECT s.*, sp.first_name AS suspect_name, tp.first_name AS target_name
            FROM suspicions s
            JOIN players sp ON sp.user_id = s.suspect_user_id
            JOIN players tp ON tp.user_id = s.target_user_id
            WHERE s.game_id = ?
            """,
            (game["id"],),
        ).fetchall()

        scores: dict[int, int] = {p["user_id"]: 0 for p in participants}
        for participant in participants:
            scores[participant["user_id"]] += 3 if participant["completed"] else 1
            if not participant["exposed"]:
                scores[participant["user_id"]] += 2
                grant_achievement(conn, participant["user_id"], "mask")

        correct_by_user: dict[int, int] = {}
        for suspicion in suspicions:
            if suspicion["correct"]:
                scores[suspicion["suspect_user_id"]] = scores.get(suspicion["suspect_user_id"], 0) + 2
                correct_by_user[suspicion["suspect_user_id"]] = correct_by_user.get(suspicion["suspect_user_id"], 0) + 1

        for user_id, correct_count in correct_by_user.items():
            if correct_count >= 3:
                grant_achievement(conn, user_id, "sherlock")

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
            if points >= 5:
                grant_achievement(conn, user_id, "first_win")

        conn.execute("UPDATE games SET status = 'finished', ended_at = ? WHERE id = ?", (now_iso(), game["id"]))
        conn.execute("UPDATE chats SET active_game_id = NULL WHERE chat_id = ?", (chat.id,))
        conn.commit()

    reveal_lines = []
    for participant in participants:
        name = mention(participant["user_id"], player_name(participant))
        status = "миссия выполнена" if participant["completed"] else "миссия не отмечена"
        exposed = "раскрыт" if participant["exposed"] else "сохранил маску"
        reveal_lines.append(
            f"• {name}\n"
            f"  <b>{esc(participant['role_name'])}</b> - {esc(participant['mission'])}\n"
            f"  {esc(status)}, {esc(exposed)}, <b>+{scores[participant['user_id']]}</b>"
        )

    suspicion_lines = []
    for suspicion in suspicions:
        mark = "верно" if suspicion["correct"] else "мимо"
        suspicion_lines.append(
            f"• {esc(suspicion['suspect_name'] or 'Игрок')} -> "
            f"{esc(suspicion['target_name'] or 'игрок')}: "
            f"{esc(suspicion['guessed_role'])} ({esc(mark)})"
        )

    await reply_html(
        update,
        card(
            "Финал раунда",
            "<b>Роли раскрыты</b>\n" + "\n".join(reveal_lines),
            suspicion_lines and "<b>Подозрения</b>\n" + "\n".join(suspicion_lines),
        ),
    )


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
    application.add_handler(CommandHandler("play", play))
    application.add_handler(CommandHandler("roles", roles))
    application.add_handler(CommandHandler("clean", clean))
    application.add_handler(CommandHandler("join", join))
    application.add_handler(CommandHandler("startgame", start_game))
    application.add_handler(CommandHandler("me", me))
    application.add_handler(CommandHandler("sus", suspect))
    application.add_handler(CommandHandler("complete", complete))
    application.add_handler(CommandHandler("event", event))
    application.add_handler(CommandHandler("endgame", end_game))
    application.add_handler(CommandHandler("score", score))
    application.add_handler(CommandHandler("leaderboard", leaderboard))
    return application


def main() -> None:
    init_db()
    application = build_app()
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
