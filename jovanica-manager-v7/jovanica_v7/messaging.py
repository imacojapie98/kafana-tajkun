"""Persistent, nonblocking Telegram multi-bubble message delivery.

This module has no Telegram, model-provider, or database credentials.
Its caller owns incoming update processing, access/opt-out checks, and the
Wiro model request. Call interrupt when a new incoming fan message arrives,
then enqueue the latest completed response using its unique generation/job id.
Call tick regularly in the bot's main loop; do not sleep in that loop.

Safety invariants:
* The original ai_history/purchases tables are never modified here.
* No message is resent when a send has an uncertain outcome.
* An interrupted response never sends its remaining, unsent bubbles.
* All writes are on the caller-provided SQLite connection.
"""

from __future__ import annotations

import sqlite3
import time
from collections.abc import Callable, Mapping
from typing import Any

MAX_BUBBLES = 3
MAX_MESSAGE_LENGTH = 3900  # Telegram sendMessage text limit is 4096 characters.


def normalize_bubbles(result: Mapping[str, Any] | str | list[str], *, limit: int = MAX_BUBBLES) -> list[str]:
    """Normalize Luna JSON or legacy replies; reject malformed/oversized output.

    Supported input: {"messages": ["a", "b"]}, {"replies": ...},
    {"reply": "a"}, a single string, or a list of strings. We never split
    customer-visible text on punctuation or newlines: model controls bubbles.
    """
    if isinstance(result, Mapping):
        if "messages" in result:
            result = result["messages"]
        elif "replies" in result:
            result = result["replies"]
        else:
            result = result.get("reply", [])
    if isinstance(result, str):
        result = [result]
    if not isinstance(result, list) or not result or len(result) > limit:
        raise ValueError("Expected between 1 and %s message bubbles" % limit)
    cleaned = []
    for message in result:
        if not isinstance(message, str):
            raise ValueError("Each message bubble must be a string")
        message = message.strip()
        if not message or len(message) > MAX_MESSAGE_LENGTH:
            raise ValueError("Message bubble is empty or too long")
        cleaned.append(message)
    return cleaned


def typing_seconds(message: str, *, min_seconds: float = 0.7, max_seconds: float = 4.0) -> float:
    """Predictable nonblocking typing duration; configurable by the caller."""
    if min_seconds <= 0 or max_seconds < min_seconds:
        raise ValueError("Invalid timing configuration")
    return round(min(max_seconds, max(min_seconds, 0.4 + len(message) / 38.0)), 2)


class BubbleOutbox:
    """SQLite outbox; sequential tick is intended for one bot process.

    api has the same signature as the existing base.api(method, **data).
    A database-backed job id is required for idempotency. On new fan messages,
    call interrupt BEFORE accepting and processing the new input.
    """

    def __init__(self, db: sqlite3.Connection):
        self.db = db
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS v7_bubble_threads (
            cid TEXT NOT NULL, chat TEXT NOT NULL, generation INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY(cid, chat));
        CREATE TABLE IF NOT EXISTS v7_bubble_jobs (
            cid TEXT NOT NULL, chat TEXT NOT NULL, response_key TEXT NOT NULL,
            generation INTEGER NOT NULL, created_at REAL NOT NULL,
            PRIMARY KEY(cid, chat, response_key));
        CREATE TABLE IF NOT EXISTS v7_bubble_outbox (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cid TEXT NOT NULL, chat TEXT NOT NULL, response_key TEXT NOT NULL,
            generation INTEGER NOT NULL, position INTEGER NOT NULL, text TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending', due_at REAL,
            telegram_message_id TEXT, created_at REAL NOT NULL,
            UNIQUE(cid, chat, response_key, position));
        CREATE INDEX IF NOT EXISTS v7_bubble_pending
            ON v7_bubble_outbox(cid,chat,generation,status,position);
        """)

    def _generation(self, cid: str, chat: str) -> int:
        row = self.db.execute(
            "SELECT generation FROM v7_bubble_threads WHERE cid=? AND chat=?",
            (cid, chat)).fetchone()
        return int(row[0]) if row else 0

    def interrupt(self, cid: str, chat: str) -> int:
        """Invalidate outstanding unsent bubbles when a new fan message arrives."""
        with self.db:
            self.db.execute(
                "INSERT OR IGNORE INTO v7_bubble_threads(cid,chat,generation) VALUES(?,?,0)",
                (cid, chat))
            self.db.execute(
                "UPDATE v7_bubble_threads SET generation=generation+1 WHERE cid=? AND chat=?",
                (cid, chat))
            self.db.execute(
                "UPDATE v7_bubble_outbox SET status='cancelled' "
                "WHERE cid=? AND chat=? AND status IN ('pending','typing')",
                (cid, chat))
        return self._generation(cid, chat)

    def enqueue(self, cid: str, chat: str, response_key: str,
                result: Mapping[str, Any] | str | list[str], now: float | None = None) -> int:
        """Atomically replace unsent bubbles; repeating a job id does nothing."""
        if not response_key or not isinstance(response_key, str):
            raise ValueError("Stable response_key required")
        bubbles = normalize_bubbles(result)
        stamp = time.time() if now is None else float(now)
        with self.db:
            prior = self.db.execute(
                "SELECT generation FROM v7_bubble_jobs "
                "WHERE cid=? AND chat=? AND response_key=?", (cid, chat, response_key)
            ).fetchone()
            if prior:
                return int(prior[0])
            generation = self.interrupt(cid, chat)
            self.db.execute(
                "INSERT INTO v7_bubble_jobs(cid,chat,response_key,generation,created_at) "
                "VALUES(?,?,?,?,?)", (cid, chat, response_key, generation, stamp))
            self.db.executemany(
                "INSERT INTO v7_bubble_outbox"
                "(cid,chat,response_key,generation,position,text,status,created_at) "
                "VALUES(?,?,?,?,?,?,'pending',?)",
                [(cid, chat, response_key, generation, i, text, stamp)
                 for i, text in enumerate(bubbles)])
        return generation

    def recover(self) -> None:
        """On process restart, never resend an in-flight Telegram send."""
        with self.db:
            self.db.execute("UPDATE v7_bubble_outbox SET status='pending',due_at=NULL "
                            "WHERE status='typing'")
            sending = self.db.execute(
                "SELECT cid,chat,response_key,position FROM v7_bubble_outbox "
                "WHERE status='sending'").fetchall()
            for cid, chat, key, position in sending:
                self.db.execute(
                    "UPDATE v7_bubble_outbox SET status='uncertain' "
                    "WHERE cid=? AND chat=? AND response_key=? AND position=?",
                    (cid, chat, key, position))
                self.db.execute(
                    "UPDATE v7_bubble_outbox SET status='cancelled' "
                    "WHERE cid=? AND chat=? AND response_key=? AND position>? "
                    "AND status IN ('pending','typing')", (cid, chat, key, position))

    def tick(self, api: Callable[..., Any], *, allowed: Callable[[str, str], bool],
             now: float | None = None) -> dict[str, Any] | None:
        """Perform at most one API action; call often, never block on delays.

        Only the first unresolved bubble per thread is eligible. The caller
        should run the existing event-drain step before each tick. Telegram
        API failures during sendMessage are uncertain and never auto-retried.
        """
        stamp = time.time() if now is None else float(now)
        row = self.db.execute("""
            SELECT o.id,o.cid,o.chat,o.generation,o.position,o.text,o.status,o.due_at,
                   o.response_key
            FROM v7_bubble_outbox o JOIN v7_bubble_threads t
              ON t.cid=o.cid AND t.chat=o.chat AND t.generation=o.generation
            WHERE o.status IN ('pending','typing')
              AND NOT EXISTS (
                SELECT 1 FROM v7_bubble_outbox p
                WHERE p.cid=o.cid AND p.chat=o.chat AND p.generation=o.generation
                  AND p.position<o.position AND p.response_key=o.response_key
                  AND p.status<>'sent')
            ORDER BY o.created_at,o.id LIMIT 1
        """).fetchone()
        if not row:
            return None
        oid, cid, chat, generation, position, text, status, due_at, key = row
        if not allowed(cid, chat):
            self.interrupt(cid, chat)
            return {"event": "cancelled", "chat": chat}
        if generation != self._generation(cid, chat):
            return None
        kwargs = {"business_connection_id": cid, "chat_id": int(chat)}
        if status == 'pending':
            delay = typing_seconds(text)
            with self.db:
                self.db.execute("UPDATE v7_bubble_outbox SET status='typing',due_at=? "
                                "WHERE id=? AND status='pending'", (stamp+delay, oid))
            try:
                api('sendChatAction', action='typing', **kwargs)
            except Exception:
                # Typing status is cosmetic. Failure must not duplicate a paid send.
                pass
            return {"event": "typing", "chat": chat, "delay_seconds": delay, "position": position}
        if status == 'typing' and due_at is not None and stamp < due_at:
            return None
        with self.db:
            self.db.execute("UPDATE v7_bubble_outbox SET status='sending' "
                            "WHERE id=? AND status='typing'", (oid,))
        try:
            message = api('sendMessage', text=text, **kwargs)
            if not isinstance(message, dict) or 'message_id' not in message:
                raise ValueError("Telegram did not return a message_id")
        except Exception:
            with self.db:
                self.db.execute("UPDATE v7_bubble_outbox SET status='uncertain' WHERE id=?", (oid,))
                self.db.execute("UPDATE v7_bubble_outbox SET status='cancelled' "
                                "WHERE cid=? AND chat=? AND response_key=? AND position>? "
                                "AND status IN ('pending','typing')", (cid, chat, key, position))
            return {"event": "uncertain", "chat": chat, "position": position}
        with self.db:
            self.db.execute("UPDATE v7_bubble_outbox SET status='sent',telegram_message_id=? "
                            "WHERE id=?", (str(message['message_id']), oid))
        return {"event": "sent", "chat": chat, "position": position,
                "telegram_message_id": message['message_id'], "text": text}

    def snapshot(self, cid: str, chat: str) -> list[tuple[int, str, str]]:
        return self.db.execute(
            "SELECT position,text,status FROM v7_bubble_outbox "
            "WHERE cid=? AND chat=? ORDER BY id", (cid, chat)
        ).fetchall()
