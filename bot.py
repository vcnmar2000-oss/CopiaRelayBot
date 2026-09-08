"""k! relay bot — deletes messages containing watched words and reposts them via webhook."""

import asyncio
import io
import json
import logging
import os
import re
import time
import weakref
from pathlib import Path
from collections import defaultdict
from datetime import datetime, timedelta

import discord
from discord.ext import commands
from dotenv import load_dotenv

PREFIX = "k!"
AUTHORIZED_USER_IDS = {
    1464639433276915825,  # Your main account
    1414684138996236344,  # Your alt account
}

WORDS_PATH = Path(__file__).with_name("words.json")
MAX_WORD_LEN = 100
MESSAGE_LIMIT = 2000
MAX_RELAY_BYTES = 25 * 1024 * 1024
DENIAL_COOLDOWN = 30.0

# ============================================================
# SPAM TRACKING — optimized for speed
# ============================================================
spam_tracker = defaultdict(list)
SPAM_WINDOW = 180   # 3 minutes
SPAM_LIMIT = 10     # 10 filtered messages per 3 minutes
TIMEOUT_MINUTES = 3 # 3-minute timeout

def is_spamming(user_id):
    """Returns True if user has exceeded the spam limit."""
    now = time.time()
    # Fast cleanup
    user_times = spam_tracker.get(user_id)
    if user_times:
        # Only clean if we need to
        cutoff = now - SPAM_WINDOW
        spam_tracker[user_id] = [t for t in user_times if t > cutoff]
    
    if len(spam_tracker[user_id]) >= SPAM_LIMIT:
        return True
    
    spam_tracker[user_id].append(now)
    return False
# ============================================================

REPLY_NONE = "none"
REPLY_KNOWN = "known"
REPLY_DELETED = "deleted"
REPLY_UNKNOWN = "unknown"

log = logging.getLogger("relay")


def normalize(word):
    return " ".join(str(word).split()).lower()


class StoreWriteError(Exception):
    pass


class WordStore:
    def __init__(self, path):
        self.path = path
        self.words = []
        self._pattern = None
        self._lock = asyncio.Lock()
        self.degraded = False

    @staticmethod
    def _compile(words):
        if not words:
            return None
        ordered = sorted(words, key=len, reverse=True)
        alts = [r"\s+".join(re.escape(tok) for tok in w.split()) for w in ordered]
        return re.compile(r"(?<!\w)(?:" + "|".join(alts) + r")(?!\w)", re.IGNORECASE)

    def load(self):
        if self.path.exists():
            try:
                data = json.loads(self.path.read_text(encoding="utf-8"))
                seen = set()
                words = []
                for raw in data.get("words", []):
                    w = normalize(raw)
                    if w and w not in seen:
                        seen.add(w)
                        words.append(w)
                self.words = words
            except (json.JSONDecodeError, OSError, AttributeError, TypeError):
                backup = self.path.with_suffix(".json.corrupt")
                try:
                    os.replace(self.path, backup)
                    log.exception("could not read %s — moved it to %s, starting empty",
                                  self.path, backup)
                except OSError:
                    log.exception("could not read or preserve %s — starting empty", self.path)
                self.degraded = True
                self.words = []
        self._pattern = self._compile(self.words)

    def _write(self, words):
        tmp = self.path.with_suffix(".json.tmp")
        tmp.write_text(
            json.dumps({"words": words}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        os.replace(tmp, self.path)

    async def _commit(self, words):
        try:
            await asyncio.to_thread(self._write, words)
        except OSError as exc:
            log.exception("failed to write %s", self.path)
            raise StoreWriteError(str(exc)) from exc
        self.words = words
        self._pattern = self._compile(words)

    async def add(self, words):
        added, dupes, rejected = [], [], []
        async with self._lock:
            existing = set(self.words)
            for w in (normalize(w) for w in words):
                if not w:
                    continue
                if len(w) > MAX_WORD_LEN:
                    rejected.append(w)
                elif w in existing or w in added:
                    dupes.append(w)
                else:
                    added.append(w)
            if added:
                await self._commit(self.words + added)
        return added, dupes, rejected

    async def remove(self, words):
        removed, missing = [], []
        async with self._lock:
            pending = list(self.words)
            for w in (normalize(w) for w in words):
                if w in pending:
                    pending.remove(w)
                    removed.append(w)
                else:
                    missing.append(w)
            if removed:
                await self._commit(pending)
        return removed, missing

    async def clear(self):
        async with self._lock:
            count = len(self.words)
            await self._commit([])
        return count

    def find(self, content):
        if not self._pattern or not content:
            return None
        m = self._pattern.search(content)
        return " ".join(m.group(0).split()).lower() if m else None


store = WordStore(WORDS_PATH)

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(
    command_prefix=PREFIX,
    intents=intents,
    help_command=None,
    allowed_mentions=discord.AllowedMentions.none(),
)

_channel_locks = weakref.WeakValueDictionary()
_denial_sent_at = {}
_webhook_cache = {}


def channel_lock(channel_id):
    lock = _channel_locks.get(channel_id)
    if lock is None:
        lock = asyncio.Lock()
        _channel_locks[channel_id] = lock
    return lock


async def get_or_create_webhook(channel):
    if channel.id in _webhook_cache:
        try:
            webhook = _webhook_cache[channel.id]
            await webhook.fetch()
            return webhook
        except discord.NotFound:
            del _webhook_cache[channel.id]
        except discord.HTTPException:
            pass

    webhooks = await channel.webhooks()
    for webhook in webhooks:
        if webhook.name == "RelayBot":
            _webhook_cache[channel.id] = webhook
            return webhook

    webhook = await channel.create_webhook(name="RelayBot")
    _webhook_cache[channel.id] = webhook
    return webhook


def parse_word_list(raw):
    raw = raw.strip()
    if raw.startswith("(") and raw.endswith(")"):
        raw = raw[1:-1]
    out, seen = [], set()
    for part in raw.split(","):
        word = normalize(part)
        if word and word not in seen:
            seen.add(word)
            out.append(word)
    return out


def split_message(text, limit=MESSAGE_LIMIT):
    chunks, current = [], ""
    for line in text.split("\n"):
        while len(line) > limit:
            if current:
                chunks.append(current)
                current = ""
            chunks.append(line[:limit])
            line = line[limit:]
        candidate = f"{current}\n{line}" if current else line
        if len(candidate) > limit:
            chunks.append(current)
            current = line
        else:
            current = candidate
    if current:
        chunks.append(current)
    return chunks or [""]


def forwarded_text(message):
    snapshots = getattr(message, "message_snapshots", None) or []
    return "\n".join(
        s.content for s in snapshots if getattr(s, "content", "")
    )


def scannable_text(message):
    return "\n".join(p for p in (message.content, forwarded_text(message)) if p)


def is_forward(message):
    ref = message.reference
    return ref is not None and getattr(ref, "type", None) == discord.MessageReferenceType.forward


async def collect_attachments(message):
    if not message.attachments:
        return []
    budget = min(message.guild.filesize_limit, MAX_RELAY_BYTES)
    if sum(a.size for a in message.attachments) > budget:
        log.warning("attachments exceed the %d byte relay budget — not relaying", budget)
        return None
    files = []
    for att in message.attachments:
        try:
            data = await asyncio.wait_for(att.read(), timeout=60)
        except (discord.HTTPException, discord.NotFound, asyncio.TimeoutError, OSError):
            log.exception("could not download %s — not relaying", att.filename)
            return None
        files.append(
            discord.File(
                io.BytesIO(data),
                filename=att.filename,
                spoiler=att.is_spoiler(),
                description=att.description,
            )
        )
    return files


async def resolve_reply_target(message):
    ref = message.reference
    if ref is None or ref.message_id is None or is_forward(message):
        return "none", None, None
    resolved = ref.resolved
    if isinstance(resolved, discord.Message):
        return "known", resolved.author.id, ref.message_id
    if isinstance(resolved, discord.DeletedReferencedMessage):
        return "deleted", None, ref.message_id
    try:
        target = await message.channel.fetch_message(ref.message_id)
    except discord.NotFound:
        return "deleted", None, ref.message_id
    except (discord.Forbidden, discord.HTTPException):
        return "unknown", None, None
    return "known", target.author.id, ref.message_id


def relay_blocked_reason(message):
    if message.thread is not None:
        return "it starts a thread"
    if isinstance(message.channel, discord.Thread) and message.channel.id == message.id:
        return "it is a thread/forum starter post"
    try:
        perms = message.channel.permissions_for(message.guild.me)
    except discord.ClientException:
        return "the parent channel is not cached, so permissions are unknown"
    can_send = (
        perms.send_messages_in_threads
        if isinstance(message.channel, discord.Thread)
        else perms.send_messages
    )
    if not perms.manage_messages:
        return "missing Manage Messages"
    if not can_send:
        return "missing Send Messages"
    if message.attachments and not perms.attach_files:
        return "missing Attach Files and the message has attachments"
    return None


async def relay(message, matched):
    blocked = relay_blocked_reason(message)
    if blocked:
        log.warning("not relaying a message in #%s: %s", message.channel, blocked)
        return

    async with channel_lock(message.channel.id):
        # ============================================================
        # FAST PATH: Check if it's a reply
        # ============================================================
        state, reply_author_id, reply_message_id = await resolve_reply_target(message)

        # ============================================================
        # COLLECT ATTACHMENTS (if any)
        # ============================================================
        files = await collect_attachments(message)
        if files is None:
            return

        # ============================================================
        # BUILD BODY TEXT — ALWAYS include text if there's any
        # ============================================================
        body = message.content or ""
        has_attachments = bool(message.attachments)
        has_bad_word = bool(matched)
        
        # ============================================================
        # MESSAGE LENGTH LIMIT — prevent spam walls
        # ============================================================
        MAX_MESSAGE_LENGTH = 400  # Max characters before truncation
        if len(body) > MAX_MESSAGE_LENGTH:
            body = body[:MAX_MESSAGE_LENGTH] + "... (truncated)"
        
        # Always process text if there's a bad word OR if there's no bad word but there are attachments
        if has_bad_word or has_attachments:
            fwd = forwarded_text(message)
            if fwd:
                quoted = "\n".join("> " + line for line in fwd.splitlines())
                body = f"{body}\n{quoted}" if body else quoted
            if message.stickers:
                body += "\n*(stickers: " + ", ".join(s.name for s in message.stickers) + ")*"
        
        # If no bad word and no attachments, and no text, it's an empty message — skip
        if not body and not has_attachments and not has_bad_word:
            return

        # ============================================================
        # FAST REPLY CONTEXT (ONLY immediate parent — NO chain)
        # ============================================================
        final_content = ""
        
        # If there's a reply, fetch ONLY the immediate parent
        if state == "known" and reply_message_id and reply_author_id:
            try:
                original_msg = await message.channel.fetch_message(reply_message_id)
                original_content = original_msg.content or "[No text content]"
                original_author = original_msg.author.display_name
                
                # Check if the parent was ALSO a reply — if so, we don't fetch its parent
                if original_msg.reference and original_msg.reference.message_id:
                    # Only show the parent, not the chain
                    original_content = f"(replying to another message) {original_content}"
                
                # Build the final content with reply context
                if has_bad_word or body:
                    final_content = f"**@{original_author}:** {original_content}\n\n**{message.author.display_name}:** {body}"
                else:
                    final_content = f"**@{original_author}:** {original_content}\n\n**{message.author.display_name}** sent:"
            except (discord.NotFound, discord.HTTPException):
                if has_bad_word or body:
                    final_content = f"**[Deleted message]**\n\n**{message.author.display_name}:** {body}"
                else:
                    final_content = f"**[Deleted message]**\n\n**{message.author.display_name}** sent:"
        else:
            # No reply
            if has_bad_word or body:
                # Show the text
                final_content = body
            else:
                # Attachments only, no reply
                final_content = f"**{message.author.display_name}** sent:"

        # ============================================================
        # SPAM CHECK (quick)
        # ============================================================
        if is_spamming(message.author.id):
            try:
                await message.author.timeout(timedelta(minutes=TIMEOUT_MINUTES))
                await message.channel.send(
                    f"⏰ {message.author.mention} — you've been timed out for {TIMEOUT_MINUTES} minutes for spamming filtered words.",
                    delete_after=10
                )
                await message.delete()
            except discord.Forbidden:
                await message.channel.send(
                    f"⚠️ {message.author.mention} — please stop spamming filtered words.",
                    delete_after=5
                )
                await message.delete()
            return
        # ============================================================

        # ============================================================
        # GET WEBHOOK (cached)
        # ============================================================
        try:
            webhook = await get_or_create_webhook(message.channel)
        except (discord.Forbidden, discord.HTTPException):
            log.exception("could not get/create webhook in #%s", message.channel)
            try:
                await message.channel.send(
                    f"📨 {message.author.display_name} sent:" if not has_bad_word and not body else f"📨 {message.author.display_name} said:\n{final_content}",
                    files=files,
                    allowed_mentions=discord.AllowedMentions.none(),
                )
                await message.delete()
            except discord.HTTPException:
                log.exception("fallback send also failed")
            return

        # ============================================================
        # SEND VIA WEBHOOK (NO DELAY)
        # ============================================================
        try:
            await webhook.send(
                content=final_content,
                username=message.author.display_name,
                avatar_url=message.author.display_avatar.url,
                files=files,
                allowed_mentions=discord.AllowedMentions.none(),
            )
        except discord.HTTPException:
            log.exception("failed to send relay via webhook")
            try:
                await message.channel.send(
                    f"📨 {message.author.display_name} sent:" if not has_bad_word and not body else f"📨 {message.author.display_name} said:\n{final_content}",
                    files=files,
                    allowed_mentions=discord.AllowedMentions.none(),
                )
            except discord.HTTPException:
                log.exception("fallback send also failed")
                return

        # Delete the original message
        try:
            await message.delete()
        except discord.NotFound:
            pass
        except (discord.Forbidden, discord.HTTPException):
            log.exception("relayed but could not delete the original in #%s", message.channel)
        log.info("relayed a message from %s (matched %r, attachments=%s)", message.author, matched, has_attachments)


def authorized():
    async def predicate(ctx):
        return ctx.author.id in AUTHORIZED_USER_IDS
    return commands.check(predicate)


async def reply_chunked(ctx, text):
    for chunk in split_message(text):
        await ctx.send(chunk, allowed_mentions=discord.AllowedMentions.none())


@bot.event
async def on_ready():
    log.info("logged in as %s — watching %d word(s)", bot.user, len(store.words))
    if store.degraded:
        log.warning("the word list failed to load and started empty — check words.json.corrupt")


@bot.event
async def on_message(message):
    if message.author.bot or message.webhook_id is not None:
        return
    await bot.process_commands(message)
    if message.guild is None:
        return
    if message.content.startswith(PREFIX):
        return
    
    # ============================================================
    # CHECK FOR FILTERED WORDS OR ATTACHMENTS
    # ============================================================
    matched = store.find(scannable_text(message))
    has_attachments = bool(message.attachments)
    
    # Relay if: (message has a bad word) OR (message has images/videos)
    if matched or has_attachments:
        await relay(message, matched)
    # ============================================================


@bot.event
async def on_raw_message_edit(payload):
    content = payload.data.get("content")
    if content is None:
        return
    cached = payload.cached_message
    if cached is not None and cached.content == content:
        return
    if content.startswith(PREFIX):
        return
    message = payload.message
    if message is None or message.guild is None:
        return
    if message.author.bot or message.webhook_id is not None:
        return
    matched = store.find(scannable_text(message))
    if matched:
        await relay(message, matched)


@bot.command(name="addwords")
@authorized()
async def addwords(ctx, *, raw: str = ""):
    words = parse_word_list(raw)
    if not words:
        await ctx.reply(f"Usage: `{PREFIX}addwords (word one, word two, ...)`", mention_author=False)
        return
    try:
        added, dupes, rejected = await store.add(words)
    except StoreWriteError as exc:
        await ctx.reply(f"Could not save the word list, nothing was changed: `{exc}`", mention_author=False)
        return
    lines = []
    if added:
        lines.append(f"Added {len(added)}: " + ", ".join(f"`{w}`" for w in added))
    if dupes:
        lines.append("Already on the list: " + ", ".join(f"`{w}`" for w in dupes))
    if rejected:
        lines.append(
            f"Rejected (over {MAX_WORD_LEN} characters): "
            + ", ".join(f"`{w}`" for w in rejected)
        )
    await reply_chunked(ctx, "\n".join(lines))


@bot.command(name="removewords")
@authorized()
async def removewords(ctx, *, raw: str = ""):
    words = parse_word_list(raw)
    if not words:
        await ctx.reply(f"Usage: `{PREFIX}removewords (word one, word two, ...)`", mention_author=False)
        return
    try:
        removed, missing = await store.remove(words)
    except StoreWriteError as exc:
        await ctx.reply(f"Could not save the word list, nothing was changed: `{exc}`", mention_author=False)
        return
    lines = []
    if removed:
        lines.append(f"Removed {len(removed)}: " + ", ".join(f"`{w}`" for w in removed))
    if missing:
        lines.append("Not on the list: " + ", ".join(f"`{w}`" for w in missing))
    await reply_chunked(ctx, "\n".join(lines))


@bot.command(name="listwords")
@authorized()
async def listwords(ctx):
    if not store.words:
        await ctx.reply("The word list is empty.", mention_author=False)
        return
    await reply_chunked(
        ctx,
        f"**{len(store.words)} watched word(s):**\n" + ", ".join(f"`{w}`" for w in store.words),
    )


@bot.command(name="clearwords")
@authorized()
async def clearwords(ctx):
    try:
        count = await store.clear()
    except StoreWriteError as exc:
        await ctx.reply(f"Could not save the word list, nothing was changed: `{exc}`", mention_author=False)
        return
    await ctx.reply(f"Cleared {count} word(s).", mention_author=False)


@bot.command(name="help")
@authorized()
async def help_command(ctx):
    await ctx.reply(
        f"`{PREFIX}addwords (a, b, c)` — watch these words\n"
        f"`{PREFIX}removewords (a, b)` — stop watching them\n"
        f"`{PREFIX}listwords` — show the list\n"
        f"`{PREFIX}clearwords` — empty the list\n\n"
        "Any message containing a watched word is deleted and reposted by me, "
        "credited to its author and to whoever they were replying to.",
        mention_author=False,
    )


@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.CommandNotFound):
        return
    if isinstance(error, commands.CheckFailure):
        now = time.monotonic()
        last = _denial_sent_at.get(ctx.author.id, 0.0)
        if now - last >= DENIAL_COOLDOWN:
            _denial_sent_at[ctx.author.id] = now
            await ctx.reply("You are not authorized to use this command.", mention_author=False)
        return
    log.exception("command %s failed", ctx.command, exc_info=error)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    )
    load_dotenv()
    token = os.getenv("DISCORD_TOKEN")
    if not token:
        raise SystemExit("DISCORD_TOKEN is not set — put it in a .env file or the host's env vars.")
    store.load()
    bot.run(token, log_handler=None)


if __name__ == "__main__":
    main()
