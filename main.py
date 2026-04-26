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

from telegram import Chat, Update
from telegram.constants import ParseMode
from telegram.error import Forbidden, TelegramError
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
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
    Role("Архивариус", "вспомнить старый мем/историю чата так, чтобы ее подхватили", 2),
    Role("Инфлюенсер", "заставить двоих людей повторить твою фразу или идею", 2),
    Role("Серый кардинал", "подтолкнуть другого игрока к выполнению его миссии", 3),
    Role("Хамелеон", "сыграть так, чтобы на тебя подозревали две разные роли", 4),
]

EVENTS = [
    "10 минут все говорят максимально серьезно. Кто сорвался первым, тот подозрителен.",
    "Следующие 10 минут можно подозревать только тех, кто уже писал сегодня.",
    "Все игроки получают право один раз блефануть о своей роли.",
    "В ближайшие 10 минут любое слово 'кстати' считается уликой.",
    "До конца раунда за правильный /sus дают +1 бонусное очко.",
]

ACHIEVEMENTS = {
    "mask": "Мастер маскировки",
    "sherlock": "Шерлок",
    "first_win": "Первая легенда",
}


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
            """
        )
        conn.commit()


def mention(user_id: int, name: str) -> str:
    safe_name = html.escape(name or "игрок")
    return f'<a href="tg://user?id={user_id}">{safe_name}</a>'


def player_name(row: sqlite3.Row) -> str:
    return row["username"] and f'@{row["username"]}' or row["first_name"] or str(row["user_id"])


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


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    upsert_player(update)
    text = (
        "Ты в игре 'Двойная жизнь'.\n\n"
        "Добавь меня в групповой чат и используй:\n"
        "/join - войти в раунд\n"
        "/startgame - раздать тайные роли\n"
        "/sus @user роль - подозрение\n"
        "/me - моя роль\n"
        "/endgame - раскрыть роли и начислить очки\n"
    )
    await update.effective_message.reply_text(text)


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await start(update, context)


async def join(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    upsert_player(update)
    chat = update.effective_chat
    user = update.effective_user
    if not chat or not user:
        return
    if chat.type == Chat.PRIVATE:
        await update.effective_message.reply_text("Вступать надо в групповом чате. В личке я храню твои роли.")
        return

    remember_chat(chat)
    with closing(db()) as conn:
        game = active_game(conn, chat.id)
        if game:
            await update.effective_message.reply_text("Раунд уже идет. Дождись следующего, чтобы не ломать интригу.")
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

    await update.effective_message.reply_html(
        f"{mention(user.id, user.first_name)} в игре. Игроков в лобби: {count}."
    )


async def start_game(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    upsert_player(update)
    chat = update.effective_chat
    if not chat or chat.type == Chat.PRIVATE:
        await update.effective_message.reply_text("Запускать раунд нужно в группе.")
        return

    remember_chat(chat)
    with closing(db()) as conn:
        if active_game(conn, chat.id):
            await update.effective_message.reply_text("Раунд уже активен.")
            return

        game = conn.execute(
            "SELECT id FROM games WHERE chat_id = ? AND status = 'lobby' ORDER BY id DESC LIMIT 1",
            (chat.id,),
        ).fetchone()
        if not game:
            await update.effective_message.reply_text("Пока нет игроков. Сначала напишите /join.")
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
            await update.effective_message.reply_text("Нужно минимум 2 игрока. Один человек может жить двойной жизнью, но это уже грустновато.")
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
        text = (
            f"Твоя тайная роль: {role.name}\n"
            f"Миссия: {role.mission}\n\n"
            "Играй незаметно. В конце раунда группа попробует понять, кто ты."
        )
        try:
            await context.bot.send_message(participant["user_id"], text)
        except Forbidden:
            failed.append(player_name(participant))
        except TelegramError as exc:
            logger.warning("Failed to DM user %s: %s", participant["user_id"], exc)
            failed.append(player_name(participant))

    message = f"Раунд начался. Роли ушли в личку {len(assignments)} игрокам."
    if failed:
        message += "\nНе смог написать: " + ", ".join(failed) + ". Им нужно открыть личку с ботом и нажать /start."
    await update.effective_message.reply_text(message)


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
        await update.effective_message.reply_text("У тебя сейчас нет активной роли.")
        return
    await update.effective_message.reply_text(f"Твоя роль: {row['role_name']}\nМиссия: {row['mission']}")


async def suspect(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    upsert_player(update)
    chat = update.effective_chat
    suspect_user = update.effective_user
    if not chat or not suspect_user or chat.type == Chat.PRIVATE:
        await update.effective_message.reply_text("Подозревать надо в группе: /sus @username роль")
        return

    if not context.args or len(context.args) < 2:
        await update.effective_message.reply_text("Формат: /sus @username роль")
        return

    target_token = context.args[0].lstrip("@").lower()
    guessed_role = " ".join(context.args[1:]).strip().lower()

    with closing(db()) as conn:
        game = active_game(conn, chat.id)
        if not game:
            await update.effective_message.reply_text("Сейчас нет активного раунда.")
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
            await update.effective_message.reply_text("Не нашел такого игрока в этом раунде. Лучше используй username.")
            return
        if target["user_id"] == suspect_user.id:
            await update.effective_message.reply_text("Самоподозрение звучит глубоко, но очков за него нет.")
            return

        already = conn.execute(
            """
            SELECT id FROM suspicions
            WHERE game_id = ? AND suspect_user_id = ? AND target_user_id = ?
            """,
            (game["id"], suspect_user.id, target["user_id"]),
        ).fetchone()
        if already:
            await update.effective_message.reply_text("Ты уже подозревал этого игрока в текущем раунде.")
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
        await update.effective_message.reply_html(
            f"Попадание. {mention(target['user_id'], player_name(target))} раскрыт: {html.escape(target['role_name'])}."
        )
    else:
        await update.effective_message.reply_text("Подозрение записано. Узнаем правду в конце раунда.")


async def complete(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    upsert_player(update)
    chat = update.effective_chat
    user = update.effective_user
    if not chat or not user or chat.type == Chat.PRIVATE:
        await update.effective_message.reply_text("Отмечать миссию нужно в группе.")
        return
    with closing(db()) as conn:
        game = active_game(conn, chat.id)
        if not game:
            await update.effective_message.reply_text("Сейчас нет активного раунда.")
            return
        updated = conn.execute(
            "UPDATE participants SET completed = 1 WHERE game_id = ? AND user_id = ?",
            (game["id"], user.id),
        ).rowcount
        conn.commit()
    if updated:
        await update.effective_message.reply_text("Миссия отмечена как выполненная. На финале начислю очки.")
    else:
        await update.effective_message.reply_text("Ты не участвуешь в этом раунде.")


async def event(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat = update.effective_chat
    if not chat or chat.type == Chat.PRIVATE:
        return
    with closing(db()) as conn:
        if not active_game(conn, chat.id):
            await update.effective_message.reply_text("События включаются только во время активного раунда.")
            return
    await update.effective_message.reply_text("Случайное событие: " + random.choice(EVENTS))


def grant_achievement(conn: sqlite3.Connection, user_id: int, code: str) -> None:
    conn.execute(
        "INSERT OR IGNORE INTO achievements (user_id, code, earned_at) VALUES (?, ?, ?)",
        (user_id, code, now_iso()),
    )


async def end_game(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    upsert_player(update)
    chat = update.effective_chat
    if not chat or chat.type == Chat.PRIVATE:
        await update.effective_message.reply_text("Завершать раунд нужно в группе.")
        return

    with closing(db()) as conn:
        game = active_game(conn, chat.id)
        if not game:
            await update.effective_message.reply_text("Активного раунда нет.")
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

    lines = ["Финал раунда. Роли раскрыты:"]
    for participant in participants:
        name = mention(participant["user_id"], player_name(participant))
        status = "выполнил" if participant["completed"] else "не отметил миссию"
        exposed = "раскрыт" if participant["exposed"] else "не раскрыт"
        lines.append(
            f"{name} - <b>{html.escape(participant['role_name'])}</b>: "
            f"{html.escape(participant['mission'])} ({status}, {exposed}, +{scores[participant['user_id']]})"
        )

    if suspicions:
        lines.append("\nПодозрения:")
        for suspicion in suspicions:
            mark = "верно" if suspicion["correct"] else "мимо"
            lines.append(
                f"{html.escape(suspicion['suspect_name'] or 'Игрок')} -> "
                f"{html.escape(suspicion['target_name'] or 'игрок')}: "
                f"{html.escape(suspicion['guessed_role'])} ({mark})"
            )

    await update.effective_message.reply_html("\n".join(lines), disable_web_page_preview=True)


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
    await update.effective_message.reply_text(
        f"Очки: {points}\nУровень: {lvl}\nЗвание: {title_for(lvl)}\nДостижения: {unlocked}"
    )


async def leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    with closing(db()) as conn:
        rows = conn.execute(
            "SELECT * FROM players ORDER BY points DESC, correct_sus DESC LIMIT 10"
        ).fetchall()
    if not rows:
        await update.effective_message.reply_text("Рейтинг пока пуст.")
        return
    lines = ["Топ игроков:"]
    for index, row in enumerate(rows, start=1):
        lvl = level_for(row["points"])
        lines.append(f"{index}. {player_name(row)} - {row['points']} очков, ур. {lvl}")
    await update.effective_message.reply_text("\n".join(lines))


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
    thread = threading.Thread(target=run_health_server, daemon=True)
    thread.start()


def build_app() -> Application:
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN environment variable is required")

    application = ApplicationBuilder().token(BOT_TOKEN).post_init(post_init).build()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_cmd))
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
