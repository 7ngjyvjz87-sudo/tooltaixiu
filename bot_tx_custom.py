#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
KIDZVN Telegram Bot — Prediction + Nạp Xu + VIP

FIX:
    - telegram.error.TimedOut lúc initialize / get_me
    - tăng connect/read/write/pool timeout
    - HTTP/1.1 + connection pool
    - tự retry khi mạng chập chờn
    - hỗ trợ proxy (TELEGRAM_PROXY / HTTPS_PROXY / HTTP_PROXY)

Chạy:
    pip install "python-telegram-bot>=21.6" requests httpx
    python kidzvn_bot.py

Điền BOT_TOKEN và ADMIN_IDS bên dưới trước khi chạy.
Proxy (nếu Telegram bị chặn / timeout liên tục):
    set TELEGRAM_PROXY=http://127.0.0.1:7890
    hoặc socks5://127.0.0.1:1080  (cần: pip install "httpx[socks]")
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import random
import re
import sqlite3
import string
import threading
import time
from collections import defaultdict, Counter
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

import requests
from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputMediaPhoto,
    MessageEntity,
    Update,
)
from telegram.constants import ParseMode
from telegram.error import BadRequest, Forbidden, NetworkError, RetryAfter, TelegramError, TimedOut
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)
from telegram.request import HTTPXRequest

# =============================================================================
# CẤU HÌNH — SỬA TẠI ĐÂY
# =============================================================================
BOT_TOKEN = os.getenv("BOT_TOKEN", "8995256203:AAH9UvKhKVDIv-_nS_OSjxu-O1wpquN3ks0")
ADMIN_IDS = [
    int(x.strip())
    for x in os.getenv("ADMIN_IDS", "8886381571").split(",")
    if x.strip().isdigit()
]
# Nếu muốn hardcode admin, bỏ comment dòng dưới:
# ADMIN_IDS = [123456789]

BOT_USERNAME = "KidzVnPremiumBot"
# Windows Python thường thiếu tzdata → fallback UTC+7 (giờ Việt Nam)
try:
    from zoneinfo import ZoneInfo
    TZ = ZoneInfo("Asia/Ho_Chi_Minh")
except Exception:
    TZ = timezone(timedelta(hours=7))
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "kidzvn_data.sqlite3")

BANK_API = "https://thueapibank.vn/historyapimbv2/28744227af0a41f68fbc81176be02097"
BANK_NAME = "MBBank"
BANK_STK = "0815585256"
BANK_OWNER = "TRUONG MINH NHAT"
QR_BASE = f"https://img.vietqr.io/image/MB-{BANK_STK}-compact2.png"

MIN_NAP = 10_000
MAX_NAP = 100_000_000
NAP_TIMEOUT_SEC = 45          # thời gian quét ngân hàng khi bấm Kiểm Tra
NAP_POLL_EVERY = 3
FEEDBACK_REWARD_XU = 5        # 5.000đ = 5 xu

# Timeout mạng Telegram (giây) — mặc định PTB chỉ 5s nên dễ TimedOut
TG_CONNECT_TIMEOUT = float(os.getenv("TG_CONNECT_TIMEOUT", "60"))
TG_READ_TIMEOUT = float(os.getenv("TG_READ_TIMEOUT", "60"))
TG_WRITE_TIMEOUT = float(os.getenv("TG_WRITE_TIMEOUT", "60"))
TG_POOL_TIMEOUT = float(os.getenv("TG_POOL_TIMEOUT", "10"))
TG_POLL_TIMEOUT = int(os.getenv("TG_POLL_TIMEOUT", "30"))  # long-polling getUpdates
TG_START_RETRIES = int(os.getenv("TG_START_RETRIES", "30"))
TG_RETRY_SLEEP = float(os.getenv("TG_RETRY_SLEEP", "5"))

# Danh mục Tài Xỉu
TX_API = {
    "SunWin": "http://160.22.107.178:8282/api/KidzVn/SUNWIN/tx",
    "HitClub": "http://160.22.107.178:8282/api/KidzVn/HITCLUB/tx",
    "Lc79": "http://160.22.107.178:8282/api/KidzVn/LC79/tx",
    "BetVip": "http://160.22.107.178:8282/api/KidzVn/BETVIP/tx",
    "B52": "http://160.22.107.178:8282/api/KidzVn/B52/tx",
    "MAX789": "http://160.22.107.178:8282/api/KidzVn/MAX789/tx",
    "RIKVIP": "http://160.22.107.178:8282/api/KidzVn/RIKVIP/tx",
}

# Danh mục Tài Xỉu MD5
MD5_API = {
    "HitClub": "http://160.22.107.178:8282/api/KidzVn/HITCLUB/md5",
    "Lc79": "http://160.22.107.178:8282/api/KidzVn/LC79/md5",
    "BetVip": "http://160.22.107.178:8282/api/KidzVn/BETVIP/md5",
    "B52": "http://160.22.107.178:8282/api/KidzVn/B52/md5",
    "MAX789": "http://160.22.107.178:8282/api/KidzVn/MAX789/md5",
    "LUCK8": "http://160.22.107.178:8282/api/KidzVn/LUCK8/md5",
    "RIKVIP": "http://160.22.107.178:8282/api/KidzVn/RIKVIP/md5",
}

# Danh mục SICBO — dự đoán lấy sẵn từ API
SICBO_API = {
    "789CLUB": "http://160.22.107.178:8181/api/sicbo789/sicbo/latest",
    "SHNWWIN": "http://160.22.107.178:8181/api/sicbosun/sicbo/latest",
    "HITCLUB": "http://160.22.107.178:8181/api/sicbophuquy/sicbo/latest",
    "LIVE SUNWIN": "http://160.22.107.178:8181/api/sicbolivesun/sicbo/latest",
    "LIVE 789CLUB": "http://160.22.107.178:8181/api/sicbolive789/sicbo/latest",
}

API_BY_CAT = {
    "TX": TX_API,
    "MD5": MD5_API,
    "SICBO": SICBO_API,
}

CAT_LABEL = {
    "TX": "TX",
    "MD5": "MD5",
    "SICBO": "SICBO",
}

# Gói VIP: callback -> (label, xu, timedelta)
VIP_PACKS = {
    "vip_1d": ("1 Ngày", 25, timedelta(days=1)),
    "vip_3d": ("3 Ngày", 80, timedelta(days=3)),
    "vip_7d": ("1 Tuần", 125, timedelta(days=7)),
    "vip_30d": ("1 Tháng", 250, timedelta(days=30)),
    "vip_60d": ("2 Tháng", 100, timedelta(days=60)),
}

# =============================================================================
# CUSTOM EMOJI PREMIUM
# =============================================================================
E = {
    "bot": "5361674603519286961",
    "title2": "5359284999679910348",
    "game": "6127306846777643058",
    "hi": "5974048815789903111",
    "id": "5888781182249738113",
    "xu": "5336828333676054102",
    "vip": "4956640152211424211",
    "fb": "6032773375347137720",
    "step": "5361674603519286961",
    "nap": "5345790388529550373",
    "mua": "5343568975609484236",
    "soft": "5345952837077585381",
    "danh": "5346319790493420136",
    "channel": "6061915686228199245",
    "admin": "5350697100902425824",
    "choose": "5872771279337033184",
    "btn_nap": "5215420556089776398",
    "btn_mua": "4956640152211424211",
    "btn_soft": "5244983043621411868",
    "btn_gift": "5262635651265743212",
    "btn_fb": "6032773375347137720",
    "btn_me": "5870994129244131212",
    "btn_hd": "5260249105443077239",
    "btn_lh": "5361927035927140948",
    "home_nap": "5336828333676054102",
    "clock24": "5424972470023104089",
    "input": "5359523546458496837",
    "note": "5965173154203573599",
    "qr": "6318864007381911770",
    "info": "5253906216135975487",
    "bank": "6098277484946526465",
    "owner": "5974048815789903111",
    "money": "5208449313067258557",
    "desc": "5310301769547394597",
    "time": "5987576060101793849",
    "check": "5346269127059196142",
    "home": "5873147866364514353",
    "checking": "5208449313067258557",
    "amt2": "4965219701572503640",
    "nd": "6147668775626872917",
    "wait": "6147610325416941553",
    "ok": "5415599983799844725",
    "botname": "5287684458881756303",
    "notice": "5415599983799844725",
    "user": "5870994129244131212",
    "fail": "5346264771962360171",
    "reason": "6136549045167987177",
    "mua_home": "6154680825133666027",
    "p1": "5352680095892905659",
    "p3": "5224607267797606837",
    "hint": "5427168083074628963",
    "sodu": "4972482444025398275",
    "soft_home": "5435945750687149196",
    "pick": "6136549045167987177",
    "tx": "6262375824410809540",
    "sun": "6147768354443631803",
    "hit": "6150031492970978840",
    "lc79": "6147612709123793069",
    "bet": "6147625104399408688",
    "b52": "6107064485522973097",
    "max789": "6147851492125584735",
    "rik": "6107153399935934941",
    "luck8": "6107295095201999218",
    "doi": "6141147146960774082",
    "sicbo": "6262375824410809540",
    "pred": "5361870050301057412",
    "game2": "5217822164362739968",
    "phien": "5397782960512444700",
    "kq": "5264919878082509254",
    "du": "5965174442693764562",
    "tl": "5395695537687123235",
    "rr": "5420323339723881652",
    "gift_in": "5949360570678384181",
    "hsd": "6323361327767099558",
    "fb_t": "5273748264963814363",
    "fb_1": "5260535596941582167",
    "fb_2": "5431737185248223332",
    "fb_3": "5395695537687123235",
    "fb_4": "5870565671896617027",
    "bc1": "6199303727157875771",
    "bc2": "6199492924762234414",
    "bc3": "6199680357135029643",
    "bc4": "6199223630312773028",
    "bc5": "6199304788014797265",
    "bc6": "6199306548951389272",
}


# Placeholder PHẢI là emoji thật (2 unit UTF-16). Dấu "•" làm Telegram bỏ custom emoji.
_PREMIUM_PH = "🟢"


def ce(eid: str, fallback: str = _PREMIUM_PH) -> str:
    """Gắn custom emoji premium — convert sang MessageEntity lúc gửi."""
    ph = fallback if fallback and fallback != "•" else _PREMIUM_PH
    return f'<tg-emoji emoji-id="{eid}">{ph}</tg-emoji>'


def e(key: str, fallback: str = _PREMIUM_PH) -> str:
    return ce(E.get(key, "5361674603519286961"), fallback)


def _u16(s: str) -> int:
    """Độ dài UTF-16 Telegram dùng cho offset entity."""
    return len(s.encode("utf-16-le")) // 2


def compile_markup(html: str) -> Tuple[str, List[MessageEntity]]:
    """
    Đổi HTML (b/i/code/spoiler/tg-emoji) thành text + entities.
    Custom emoji PHẢI gửi bằng entity, không gửi parse_mode HTML suông.
    """
    out: List[str] = []
    stack: List[Tuple[str, int, Optional[str]]] = []
    ents: List[MessageEntity] = []
    i, n = 0, len(html)
    while i < n:
        if html.startswith("&amp;", i):
            out.append("&")
            i += 5
            continue
        if html.startswith("&lt;", i):
            out.append("<")
            i += 4
            continue
        if html.startswith("&gt;", i):
            out.append(">")
            i += 4
            continue
        if html[i] == "<":
            m_open = re.match(r'<tg-emoji emoji-id="(\d+)">', html[i:])
            if m_open:
                stack.append(("custom_emoji", _u16("".join(out)), m_open.group(1)))
                i += m_open.end()
                continue
            if html.startswith("</tg-emoji>", i):
                if stack and stack[-1][0] == "custom_emoji":
                    _typ, start, eid = stack.pop()
                    ln = _u16("".join(out)) - start
                    if eid and ln > 0:
                        ents.append(
                            MessageEntity(
                                type="custom_emoji",
                                offset=start,
                                length=ln,
                                custom_emoji_id=str(eid),
                            )
                        )
                i += 11
                continue
            m_tag = re.match(r"</?(b|i|code|tg-spoiler)>", html[i:])
            if m_tag:
                raw = m_tag.group(0)
                name = m_tag.group(1)
                typ = {"b": "bold", "i": "italic", "code": "code", "tg-spoiler": "spoiler"}[name]
                pos = _u16("".join(out))
                if not raw.startswith("</"):
                    stack.append((typ, pos, None))
                else:
                    for j in range(len(stack) - 1, -1, -1):
                        if stack[j][0] == typ:
                            _t, start, _e = stack.pop(j)
                            ln = pos - start
                            if ln > 0:
                                ents.append(MessageEntity(type=typ, offset=start, length=ln))
                            break
                i += m_tag.end()
                continue
            j = html.find(">", i)
            i = n if j < 0 else j + 1
            continue
        out.append(html[i])
        i += 1
    pos = _u16("".join(out))
    while stack:
        typ, start, eid = stack.pop()
        ln = pos - start
        if ln <= 0:
            continue
        if typ == "custom_emoji" and eid:
            ents.append(
                MessageEntity(
                    type="custom_emoji",
                    offset=start,
                    length=ln,
                    custom_emoji_id=str(eid),
                )
            )
        else:
            ents.append(MessageEntity(type=typ, offset=start, length=ln))
    return "".join(out), ents


def ibtn(text: str, data: str, eid: Optional[str] = None) -> InlineKeyboardButton:
    """Inline button + icon premium (Bot API 9.4 / PTB mới)."""
    extra = {"icon_custom_emoji_id": str(eid)} if eid else {}
    try:
        return InlineKeyboardButton(text=text, callback_data=data, api_kwargs=extra or None)
    except TypeError:
        return InlineKeyboardButton(text=text, callback_data=data)


def now_vn() -> datetime:
    return datetime.now(TZ)


def fmt_dt(dt: Optional[datetime] = None) -> str:
    d = dt or now_vn()
    if d.tzinfo is None:
        d = d.replace(tzinfo=TZ)
    return d.astimezone(TZ).strftime("%d/%m/%Y %H:%M:%S")


def fmt_money(n: int) -> str:
    return f"{int(n):,}".replace(",", ".")


def esc(s: Any) -> str:
    return (
        str(s)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def copy_txt(s: Any) -> str:
    return f"<code>{esc(s)}</code>"


def bold(s: Any) -> str:
    return f"<b>{esc(s)}</b>"


def italic(s: Any) -> str:
    return f"<i>{esc(s)}</i>"


def spoiler(s: Any) -> str:
    return f"<tg-spoiler>{esc(s)}</tg-spoiler>"


def resolve_proxy() -> Optional[str]:
    """Ưu tiên TELEGRAM_PROXY, sau đó HTTPS_PROXY / HTTP_PROXY / ALL_PROXY."""
    for key in ("TELEGRAM_PROXY", "HTTPS_PROXY", "HTTP_PROXY", "ALL_PROXY", "https_proxy", "http_proxy"):
        val = (os.getenv(key) or "").strip()
        if val:
            return val
    return None


def build_httpx_request(*, for_get_updates: bool = False) -> HTTPXRequest:
    """HTTP client PTB với timeout dài — tránh TimedOut lúc get_me / polling."""
    proxy = resolve_proxy()
    read_to = max(TG_READ_TIMEOUT, TG_POLL_TIMEOUT + 15.0) if for_get_updates else TG_READ_TIMEOUT
    kwargs: Dict[str, Any] = {
        "connect_timeout": TG_CONNECT_TIMEOUT,
        "read_timeout": read_to,
        "write_timeout": TG_WRITE_TIMEOUT,
        "pool_timeout": TG_POOL_TIMEOUT,
        "connection_pool_size": 16 if for_get_updates else 32,
        "http_version": "1.1",
    }
    if proxy:
        kwargs["proxy"] = proxy
    try:
        return HTTPXRequest(**kwargs)
    except TypeError:
        kwargs.pop("http_version", None)
        try:
            return HTTPXRequest(**kwargs)
        except TypeError:
            kwargs.pop("proxy", None)
            return HTTPXRequest(
                connect_timeout=TG_CONNECT_TIMEOUT,
                read_timeout=read_to,
                write_timeout=TG_WRITE_TIMEOUT,
                pool_timeout=TG_POOL_TIMEOUT,
            )


# =============================================================================
# DATABASE
# =============================================================================
_db_lock = threading.RLock()


def db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA foreign_keys=ON")
    return con


def db_init() -> None:
    with _db_lock:
        con = db()
        con.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                chat_id     INTEGER PRIMARY KEY,
                username    TEXT,
                first_name  TEXT,
                last_name   TEXT,
                full_name   TEXT,
                language    TEXT,
                is_premium  INTEGER DEFAULT 0,
                xu          INTEGER DEFAULT 0,
                vip_until   INTEGER DEFAULT 0,
                banned_until INTEGER DEFAULT 0,
                created_at  INTEGER,
                updated_at  INTEGER
            );
            CREATE TABLE IF NOT EXISTS deposits (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id     INTEGER,
                amount      INTEGER,
                content     TEXT UNIQUE,
                status      TEXT,
                txn_id      TEXT,
                created_at  INTEGER,
                paid_at     INTEGER
            );
            CREATE TABLE IF NOT EXISTS used_txns (
                txn_id      TEXT PRIMARY KEY,
                deposit_id  INTEGER,
                amount      INTEGER,
                description TEXT,
                used_at     INTEGER
            );
            CREATE TABLE IF NOT EXISTS purchases (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id     INTEGER,
                pack        TEXT,
                xu_cost     INTEGER,
                days_label  TEXT,
                created_at  INTEGER
            );
            CREATE TABLE IF NOT EXISTS giftcodes (
                code        TEXT PRIMARY KEY,
                hours       INTEGER,
                max_uses    INTEGER,
                used_count  INTEGER DEFAULT 0,
                expire_at   INTEGER,
                created_at  INTEGER
            );
            CREATE TABLE IF NOT EXISTS gift_uses (
                code        TEXT,
                chat_id     INTEGER,
                used_at     INTEGER,
                PRIMARY KEY (code, chat_id)
            );
            CREATE TABLE IF NOT EXISTS keys_tbl (
                key_code    TEXT PRIMARY KEY,
                days        INTEGER,
                chat_id     INTEGER,
                used        INTEGER DEFAULT 0,
                created_at  INTEGER,
                used_at     INTEGER
            );
            CREATE TABLE IF NOT EXISTS feedback (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id     INTEGER,
                content     TEXT,
                photos      TEXT,
                status      TEXT,
                created_at  INTEGER
            );
            CREATE TABLE IF NOT EXISTS game_hist (
                game        TEXT,
                category    TEXT,
                phien       INTEGER,
                ketqua      TEXT,
                raw         TEXT,
                ts          INTEGER,
                PRIMARY KEY (game, category, phien)
            );
            CREATE TABLE IF NOT EXISTS settings (
                k           TEXT PRIMARY KEY,
                v           TEXT
            );
            """
        )
        con.commit()
        con.close()


def q(sql: str, args: Tuple = (), fetch: str = "none"):
    with _db_lock:
        con = db()
        cur = con.execute(sql, args)
        if fetch == "one":
            row = cur.fetchone()
            con.close()
            return row
        if fetch == "all":
            rows = cur.fetchall()
            con.close()
            return rows
        con.commit()
        lid = cur.lastrowid
        con.close()
        return lid


# =============================================================================
# USER HELPERS
# =============================================================================
def full_name_of(u) -> str:
    parts = [getattr(u, "first_name", None) or "", getattr(u, "last_name", None) or ""]
    name = " ".join(p for p in parts if p).strip()
    return name or (getattr(u, "username", None) and f"@{u.username}") or "Người dùng"


def upsert_user(u) -> Dict[str, Any]:
    cid = int(u.id)
    fn = full_name_of(u)
    ts = int(time.time())
    row = q("SELECT * FROM users WHERE chat_id=?", (cid,), "one")
    if row is None:
        q(
            "INSERT INTO users(chat_id,username,first_name,last_name,full_name,language,is_premium,xu,vip_until,banned_until,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,0,0,?,?)",
            (
                cid,
                getattr(u, "username", None),
                getattr(u, "first_name", None),
                getattr(u, "last_name", None),
                fn,
                getattr(u, "language_code", None),
                1 if getattr(u, "is_premium", False) else 0,
                0,
                ts,
                ts,
            ),
        )
    else:
        q(
            "UPDATE users SET username=?, first_name=?, last_name=?, full_name=?, language=?, is_premium=?, updated_at=? WHERE chat_id=?",
            (
                getattr(u, "username", None),
                getattr(u, "first_name", None),
                getattr(u, "last_name", None),
                fn,
                getattr(u, "language_code", None),
                1 if getattr(u, "is_premium", False) else 0,
                ts,
                cid,
            ),
        )
    return get_user(cid)


def get_user(cid: int) -> Optional[Dict[str, Any]]:
    row = q("SELECT * FROM users WHERE chat_id=?", (int(cid),), "one")
    return dict(row) if row else None


def all_user_ids() -> List[int]:
    return [int(r["chat_id"]) for r in q("SELECT chat_id FROM users", (), "all")]


def is_admin(cid: int) -> bool:
    return int(cid) in ADMIN_IDS


def ban_status(u: Dict[str, Any]) -> Tuple[bool, str]:
    until = int(u.get("banned_until") or 0)
    if until == 0:
        return False, ""
    if until < 0:
        return True, "Vĩnh viễn"
    if until > int(time.time()):
        return True, datetime.fromtimestamp(until, TZ).strftime("%d/%m/%Y %H:%M")
    return False, ""


def vip_status_text(u: Dict[str, Any]) -> str:
    until = int(u.get("vip_until") or 0)
    if until == 0:
        return "Chưa Kích Hoạt"
    if until < 0:
        return "Đã Kích Hoạt (Vĩnh viễn)"
    if until > int(time.time()):
        return "Đã Kích Hoạt"
    return "Đã Hết Hạn"


def vip_active(u: Dict[str, Any]) -> bool:
    until = int(u.get("vip_until") or 0)
    if until < 0:
        return True
    return until > int(time.time())


def add_xu(cid: int, amount: int) -> int:
    q("UPDATE users SET xu = xu + ?, updated_at=? WHERE chat_id=?", (int(amount), int(time.time()), int(cid)))
    u = get_user(cid)
    return int(u["xu"]) if u else 0


def set_vip(cid: int, delta: Optional[timedelta] = None, forever: bool = False) -> int:
    u = get_user(cid)
    if forever:
        q("UPDATE users SET vip_until=-1, updated_at=? WHERE chat_id=?", (int(time.time()), int(cid)))
        return -1
    now = int(time.time())
    cur = int(u["vip_until"] or 0) if u else 0
    base = cur if cur > now else now
    new = base + int(delta.total_seconds())
    q("UPDATE users SET vip_until=?, updated_at=? WHERE chat_id=?", (new, now, int(cid)))
    return new


# =============================================================================
# STATE / RUNTIME
# =============================================================================
STATE: Dict[int, str] = {}
STATE_DATA: Dict[int, Dict[str, Any]] = defaultdict(dict)
LAST_MSG: Dict[int, int] = {}          # chat_id -> message_id bot gần nhất (để xoá)
WATCHERS: Dict[int, Dict[str, Any]] = {}  # chat_id -> {game, cat, msg_id, last_phien}
MEDIA_GROUP: Dict[str, Dict[str, Any]] = {}
CHECKING: Dict[int, bool] = {}


async def safe_delete(bot, chat_id: int, mid: Optional[int] = None) -> None:
    mid = mid or LAST_MSG.get(chat_id)
    if not mid:
        return
    try:
        await bot.delete_message(chat_id, mid)
    except Exception:
        pass


async def send_html(bot, chat_id: int, text: str, kb=None, photo: Optional[str] = None, delete_old: bool = True):
    """Gửi tin có bold/italic/code + custom emoji premium (entities, không parse_mode)."""
    if delete_old:
        await safe_delete(bot, chat_id)
    plain, ents = compile_markup(text)
    try:
        if photo:
            msg = await bot.send_photo(
                chat_id,
                photo=photo,
                caption=plain[:1024],
                caption_entities=ents,
                reply_markup=kb,
                read_timeout=TG_READ_TIMEOUT,
                write_timeout=TG_WRITE_TIMEOUT,
                connect_timeout=TG_CONNECT_TIMEOUT,
                pool_timeout=TG_POOL_TIMEOUT,
            )
        else:
            msg = await bot.send_message(
                chat_id,
                plain,
                entities=ents,
                reply_markup=kb,
                disable_web_page_preview=True,
                read_timeout=TG_READ_TIMEOUT,
                write_timeout=TG_WRITE_TIMEOUT,
                connect_timeout=TG_CONNECT_TIMEOUT,
                pool_timeout=TG_POOL_TIMEOUT,
            )
        LAST_MSG[chat_id] = msg.message_id
        return msg
    except TimedOut as ex:
        logging.warning("send TimedOut: %s", ex)
        try:
            msg = await bot.send_message(chat_id, plain[:4000], reply_markup=kb)
            LAST_MSG[chat_id] = msg.message_id
            return msg
        except Exception:
            return None
    except BadRequest as ex:
        logging.warning("send entities fail: %s", ex)
        try:
            msg = await bot.send_message(
                chat_id,
                plain,
                parse_mode=ParseMode.HTML,
                reply_markup=kb,
                disable_web_page_preview=True,
            )
            LAST_MSG[chat_id] = msg.message_id
            return msg
        except Exception:
            try:
                msg = await bot.send_message(chat_id, plain[:4000], reply_markup=kb)
                LAST_MSG[chat_id] = msg.message_id
                return msg
            except Exception:
                return None
    except Forbidden:
        return None
    except Exception as ex:
        logging.warning("send_html error: %s", ex)
        return None


async def send_markup(bot, chat_id: int, html: str, kb=None, photo=None, video=None):
    """Gửi 1 tin (broadcast / feedback) giữ nguyên custom emoji."""
    plain, ents = compile_markup(html)
    if photo:
        return await bot.send_photo(chat_id, photo, caption=plain[:1024], caption_entities=ents, reply_markup=kb)
    if video:
        return await bot.send_video(chat_id, video, caption=plain[:1024], caption_entities=ents, reply_markup=kb)
    return await bot.send_message(chat_id, plain, entities=ents, reply_markup=kb, disable_web_page_preview=True)


# =============================================================================
# KEYBOARDS
# =============================================================================
def kb_home(cid: int) -> InlineKeyboardMarkup:
    rows = [
        [
            ibtn("Nhập Key", "nhap_key", E["btn_nap"]),
            ibtn("Mua Gói", "mua_goi", E["btn_mua"]),
        ],
        [
            ibtn("Phần Mềm", "phan_mem", E["btn_soft"]),
            ibtn("Giftcode", "giftcode", E["btn_gift"]),
        ],
        [
            ibtn("Gửi Feedback", "feedback", E["btn_fb"]),
            ibtn("Cá Nhân", "ca_nhan", E["btn_me"]),
        ],
        [
            ibtn("Hướng Dẫn", "huong_dan", E["btn_hd"]),
            ibtn("Liên Hệ - Hỗ Trợ", "lien_he", E["btn_lh"]),
        ],
    ]
    if is_admin(cid):
        rows.append([ibtn("ADMIN PANEL", "admin_panel")])
    return InlineKeyboardMarkup(rows)


def kb_home_only() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[ibtn("Trang Chủ", "home", E["home"])]])


def kb_nap_check() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[ibtn("Kiểm Tra", "nap_check", E["check"]), ibtn("Trang Chủ", "home", E["home"])]]
    )


def kb_mua() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [ibtn("1 Ngày", "vip_1d", E["p1"]), ibtn("3 Ngày", "vip_3d", E["p3"])],
            [ibtn("1 Tuần", "vip_7d", E["p3"]), ibtn("1 Tháng", "vip_30d", E["p3"])],
            [ibtn("2 Tháng", "vip_60d", E["p3"])],
            [ibtn("Trang Chủ", "home", E["home"])],
        ]
    )


def kb_thieu_xu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[ibtn("Nhập Key", "nhap_key", E["btn_nap"]), ibtn("Trang Chủ", "home", E["home"])]]
    )


def kb_soft() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [ibtn("Tài Xỉu", "soft_tx", E["tx"]), ibtn("Tài Xỉu Md5", "soft_md5", E["tx"])],
            [ibtn("Sicbo", "soft_sicbo", E["sicbo"])],
            [ibtn("Trang Chủ", "home", E["home"])],
        ]
    )


def kb_tx_games() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [ibtn("SunWin", "play|TX|SunWin", E["sun"]), ibtn("HitClub", "play|TX|HitClub", E["hit"])],
            [ibtn("Lc79", "play|TX|Lc79", E["lc79"]), ibtn("BetVip", "play|TX|BetVip", E["bet"])],
            [ibtn("B52", "play|TX|B52", E["b52"]), ibtn("MAX789", "play|TX|MAX789", E["max789"])],
            [ibtn("RIKVIP", "play|TX|RIKVIP", E["rik"])],
            [ibtn("Trang Chủ", "home", E["home"])],
        ]
    )


def kb_md5_games() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [ibtn("HitClub", "play|MD5|HitClub", E["hit"]), ibtn("Lc79", "play|MD5|Lc79", E["lc79"])],
            [ibtn("BetVip", "play|MD5|BetVip", E["bet"]), ibtn("B52", "play|MD5|B52", E["b52"])],
            [ibtn("MAX789", "play|MD5|MAX789", E["max789"]), ibtn("LUCK8", "play|MD5|LUCK8", E["luck8"])],
            [ibtn("RIKVIP", "play|MD5|RIKVIP", E["rik"])],
            [ibtn("Trang Chủ", "home", E["home"])],
        ]
    )


def kb_sicbo_games() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [ibtn("789CLUB", "play|SICBO|789CLUB", E["tx"]), ibtn("SHNWWIN", "play|SICBO|SHNWWIN", E["sun"])],
            [ibtn("HITCLUB", "play|SICBO|HITCLUB", E["hit"]), ibtn("LIVE SUNWIN", "play|SICBO|LIVE SUNWIN", E["sun"])],
            [ibtn("LIVE 789CLUB", "play|SICBO|LIVE 789CLUB", E["tx"])],
            [ibtn("Trang Chủ", "home", E["home"])],
        ]
    )


def kb_pred() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[ibtn("Trang Chủ", "home", E["home"]), ibtn("Đổi Game", "phan_mem", E["doi"])]]
    )


def kb_me() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [ibtn("LS Nạp", "ls_nap"), ibtn("LS Mua Gói", "ls_mua")],
            [ibtn("Trang Chủ", "home", E["home"])],
        ]
    )


def kb_admin() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [ibtn("❌ BAND USER", "ad_ban"), ibtn("✅ UNBAN", "ad_unban")],
            [ibtn("👤 Danh Sách User", "ad_users:0"), ibtn("📊 Thống Kê", "ad_stats")],
            [ibtn("🔔 Gửi thông báo", "ad_bc")],
            [ibtn("🔑 Tạo Mã Key", "ad_key"), ibtn("Quản Lý Key", "ad_keys:0")],
            [ibtn("🎁 Tạo GiftCode", "ad_gift"), ibtn("Danh Sách Giftcode", "ad_gifts:0")],
            [ibtn("➕ Cộng Xu", "ad_addxu"), ibtn("➖ Trừ Xu", "ad_subxu")],
            [ibtn("👑 Set VIP", "ad_setvip"), ibtn("♻️ ResetBot", "ad_reset")],
            [ibtn("Trang Chủ", "home", E["home"])],
        ]
    )


# =============================================================================
# MÀN HÌNH
# =============================================================================
def text_start(u: Dict[str, Any], tg_user) -> str:
    return (
        f"{e('bot')} {bold('BOT AI KIDZVN - PREDICTION NHÀ CÁI')} {e('title2')}\n"
        f"{e('game')} {bold('TÀI XỈU - TÀI XỈU MD5 - SICBO - BACCRAT')} {e('game')}\n"
        f"{'━'*22}\n"
        f"{e('hi')} Xin chào, {bold(full_name_of(tg_user))}!\n"
        f"{e('id')} ID: {copy_txt(u['chat_id'])}\n"
        f"{e('xu')} Số Dư Xu: {bold(fmt_money(u['xu']) + ' xu')}\n"
        f"{italic('(1.000đ = 1 xu)')}\n"
        f"{e('vip')} Trạng Thái Vip : {bold(vip_status_text(u))}\n"
        f"{'━'*22}\n"
        f"{e('fb')} {bold('GỬI FEEDBACK KHI ĐÁNH THẮNG, HỢP LỆ NHẬN NGAY 5.000đ')}\n"
        f"{'━'*22}\n"
        f"{e('step')} {bold('4 BƯỚC BẮT ĐẦU')}\n"
        f"{e('nap')} Nhập Key\n"
        f"{e('mua')} Mua Gói\n"
        f"{e('soft')} VÀO KHU VỰC PHẦN MỀM\n"
        f"{e('danh')} ĐÁNH THEO BOT\n"
        f"{'━'*22}\n"
        f"{e('channel')} KÊNH THÔNG BÁO:   {bold('@KidzVn')}\n"
        f"{e('admin')} ADMIN HỖ TRỢ: {bold('@DucToanVn')}\n"
        f"{'━'*22}\n"
        f"{e('choose')} {bold('CHỌN CHỨC NĂNG BÊN DƯỚI!')}"
    )


def text_nap_home() -> str:
    return (
        f"{e('home_nap')} {bold('TRANG CHỦ NẠP XU - 24/7')} {e('clock24')}\n"
        f"{'━'*22}\n"
        f"{e('input')} {bold('Nhập số tiền muốn nạp (VNĐ):')}\n"
        f"•  {copy_txt('10000')} hoặc {copy_txt('10k')} → {bold('10xu')}\n"
        f"•  {copy_txt('50000')} hoặc {copy_txt('50k')} → {bold('50xu')}\n"
        f"•  {copy_txt('100000')} hoặc {copy_txt('100k')} → {bold('100xu')}\n"
        f"{'━'*22}\n"
        f"{e('note')} {italic('Tối thiểu 10.000đ — tối đa 100.000.000đ')}\n"
        f"{'━'*22}"
    )


# =============================================================================
# NẠP TIỀN / BANK
# =============================================================================
def parse_amount(text: str) -> Optional[int]:
    t = text.strip().lower().replace(" ", "").replace(",", "").replace(".", "")
    t = t.replace("vnđ", "").replace("vnd", "").replace("đ", "")
    m = re.fullmatch(r"(\d+)(k|m)?", t)
    if not m:
        return None
    n = int(m.group(1))
    suf = m.group(2)
    if suf == "k":
        n *= 1000
    elif suf == "m":
        n *= 1_000_000
    return n


def gen_content() -> str:
    alphabet = string.ascii_letters + string.digits
    while True:
        code = "KIDZVN" + "".join(random.choice(alphabet) for _ in range(10))
        exist = q("SELECT 1 FROM deposits WHERE content=?", (code,), "one")
        if not exist:
            return code


def qr_url(amount: int, content: str) -> str:
    return (
        f"{QR_BASE}?amount={int(amount)}"
        f"&addInfo={quote(content)}"
        f"&accountName={quote(BANK_OWNER)}"
    )


def fetch_bank() -> List[Dict[str, Any]]:
    try:
        r = requests.get(BANK_API, timeout=12)
        data = r.json()
    except Exception as ex:
        logging.warning("bank api error: %s", ex)
        return []
    txs = data.get("transactions") or data.get("data") or []
    if isinstance(data, list):
        txs = data
    out = []
    for t in txs:
        if not isinstance(t, dict):
            continue
        amt_raw = t.get("amount") or t.get("transferAmount") or t.get("Amount") or 0
        try:
            amt = int(str(amt_raw).replace(",", "").replace(".", "").replace(" ", ""))
        except Exception:
            continue
        desc = str(t.get("description") or t.get("content") or t.get("Description") or "")
        tid = str(t.get("transactionID") or t.get("id") or t.get("refNo") or t.get("trxId") or "")
        typ = str(t.get("type") or t.get("transferType") or t.get("CD") or "IN").upper()
        date = str(t.get("transactionDate") or t.get("when") or "")
        out.append({"amount": amt, "description": desc, "txn_id": tid, "type": typ, "date": date, "raw": t})
    return out


def match_deposit(amount: int, content: str) -> Optional[Dict[str, Any]]:
    """Khớp nội dung (ưu tiên) + số tiền. 1 txn chỉ duyệt 1 lần."""
    content_u = content.upper()
    for tx in fetch_bank():
        typ = tx["type"]
        if typ in ("OUT", "D", "-"):
            continue
        desc_u = (tx["description"] or "").upper().replace(" ", "")
        content_compact = content_u.replace(" ", "")
        used = False
        if tx["txn_id"]:
            used = bool(q("SELECT 1 FROM used_txns WHERE txn_id=?", (tx["txn_id"],), "one"))
        if used:
            continue
        has_nd = content_compact in desc_u or content_u in (tx["description"] or "").upper()
        has_amt = int(tx["amount"]) == int(amount)
        # Duyệt khi khớp nội dung (mã unique) — đồng thời ưu tiên đúng số tiền
        if has_nd and has_amt:
            return tx
        if has_nd:
            return tx
    return None


# =============================================================================
# GAME API — LẤY PHIÊN + DỰ ĐOÁN + TỈ LỆ TỪ API
# =============================================================================
def norm_tx(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip().upper()
    s = (
        s.replace("À", "A")
        .replace("Ả", "A")
        .replace("Ã", "A")
        .replace("Á", "A")
        .replace("Ạ", "A")
        .replace("Ỉ", "I")
        .replace("Í", "I")
        .replace("Ì", "I")
        .replace("Ĩ", "I")
        .replace("Ị", "I")
    )
    if s in ("T", "TAI", "BIG", "TAI XIU TAI", "1"):
        return "T"
    if s in ("X", "XIU", "SMALL", "2"):
        return "X"
    if "TAI" in s:
        return "T"
    if "XIU" in s:
        return "X"
    return None


def pretty_tx(v: Any) -> str:
    n = norm_tx(v)
    if n == "T":
        return "TÀI"
    if n == "X":
        return "XỈU"
    s = str(v or "").strip()
    return s.upper() if s else "—"


def pretty_conf(v: Any) -> str:
    if v is None or v == "":
        return "—"
    s = str(v).strip()
    if not s:
        return "—"
    if s.endswith("%"):
        return s
    try:
        f = float(s.replace(",", "."))
        if f <= 1:
            f *= 100
        return f"{f:.1f}".rstrip("0").rstrip(".") + "%"
    except Exception:
        return s


def _walk_find(obj: Any, keys: List[str]) -> Optional[Any]:
    if isinstance(obj, dict):
        low = {str(k).lower(): v for k, v in obj.items()}
        for k in keys:
            if k.lower() in low:
                val = low[k.lower()]
                if val is not None and val != "":
                    return val
        for v in obj.values():
            r = _walk_find(v, keys)
            if r is not None:
                return r
    elif isinstance(obj, list) and obj:
        return _walk_find(obj[0], keys)
    return None


def _to_phien(v: Any) -> Optional[int]:
    if v is None or v == "":
        return None
    try:
        return int(str(v).replace(",", "").replace(" ", "").split(".")[0])
    except Exception:
        m = re.search(r"\d+", str(v))
        return int(m.group(0)) if m else None


def parse_game_payload(data: Any) -> Dict[str, Any]:
    """Parse thống nhất TX / MD5 / SICBO — ưu tiên field API sẵn có."""
    phien_now = _to_phien(
        _walk_find(
            data,
            [
                "phien_hientai",
                "phien_hien_tai",
                "phienHienTai",
                "phien_du_doan",
                "next_phien",
                "phien",
                "Phien",
                "session",
                "phien_id",
                "sid",
                "issue",
                "period",
            ],
        )
    )
    phien_prev = _to_phien(
        _walk_find(
            data,
            ["phienTruoc", "phien_truoc", "phien_cu", "last_phien", "phien"],
        )
    )
    dudoan_raw = _walk_find(
        data,
        ["du_doan", "dudoan", "pred", "prediction", "duDoan", "ket_qua_du_doan"],
    )
    tile_raw = _walk_find(
        data,
        ["do_tin_cay", "ty_le", "tylethang", "confidence", "conf", "tile"],
    )
    kq = _walk_find(
        data,
        ["ketQua", "ketqua", "ket_qua", "KetQua", "result", "kq", "dice_result", "tx", "tai_xiu", "prize"],
    )
    ket = norm_tx(kq)
    if ket is None:
        d1 = _walk_find(data, ["xuc_xac_1", "d1", "dice1", "xx1"])
        d2 = _walk_find(data, ["xuc_xac_2", "d2", "dice2", "xx2"])
        d3 = _walk_find(data, ["xuc_xac_3", "d3", "dice3", "xx3"])
        try:
            tong = int(d1) + int(d2) + int(d3)
            ket = "X" if tong <= 10 else "T"
        except Exception:
            ket = None
    if phien_now is None:
        phien_now = int(time.time()) % 10_000_000
    return {
        "phien": phien_now,
        "phien_prev": phien_prev,
        "ketqua": ket,
        "dudoan": pretty_tx(dudoan_raw) if dudoan_raw is not None else "—",
        "tile": pretty_conf(tile_raw),
        "raw": data,
    }


def fetch_game(cat: str, game: str) -> Optional[Dict[str, Any]]:
    api_map = API_BY_CAT.get(cat) or {}
    url = api_map.get(game)
    if not url:
        return None
    try:
        r = requests.get(url, timeout=8)
        data = r.json()
        return parse_game_payload(data)
    except Exception as ex:
        logging.warning("game api %s/%s: %s", cat, game, ex)
        return None


def hist_of(cat: str, game: str, limit: int = 80) -> List[str]:
    rows = q(
        "SELECT ketqua FROM game_hist WHERE game=? AND category=? AND ketqua IS NOT NULL ORDER BY phien DESC LIMIT ?",
        (game, cat, limit),
        "all",
    )
    seq = [r["ketqua"] for r in rows if r["ketqua"] in ("T", "X")]
    seq.reverse()
    return seq


def save_hist(cat: str, game: str, phien: int, ketqua: Optional[str], raw: Any) -> None:
    if ketqua not in ("T", "X"):
        return
    q(
        "INSERT OR IGNORE INTO game_hist(game,category,phien,ketqua,raw,ts) VALUES(?,?,?,?,?,?)",
        (game, cat, int(phien), ketqua, json.dumps(raw, ensure_ascii=False, default=str)[:2000], int(time.time())),
    )


def text_predict(game: str, cat: str, phien: Any, dudoan: str, tile: str) -> str:
    dm = CAT_LABEL.get(cat, cat)
    return (
        f"{e('pred')} {bold('DỰ ĐOÁN TỰ ĐỘNG')} {e('clock24')}\n"
        f"{'━'*22}\n"
        f"{e('game2')} Game: {bold(game)} {bold(dm)}\n"
        f"{e('phien')} Phiên: {copy_txt(phien)}\n"
        f"{'━'*22}\n"
        f"{e('kq')} {bold('KẾT QUẢ DỰ ĐOÁN:')}\n"
        f"    {e('du')} {bold(dudoan)}\n"
        f"    {e('tl')} Tỉ Lệ: {bold(tile)}\n"
        f"{'━'*22}\n"
        f"{e('wait')} {italic('Đang chờ phiên tiếp theo...')}"
    )


# =============================================================================
# HANDLERS
# =============================================================================
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    u = update.effective_user
    chat = update.effective_chat
    rec = upsert_user(u)
    banned, until = ban_status(rec)
    if banned and not is_admin(u.id):
        await update.message.reply_text(f"Tài khoản bị khoá.\nHạn: {until}")
        return
    STATE[u.id] = "idle"
    WATCHERS.pop(u.id, None)
    await send_html(context.bot, chat.id, text_start(rec, u), kb_home(u.id))


async def cmd_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    u = update.effective_user
    if not is_admin(u.id):
        await cmd_start(update, context)
        return
    await admin_router(context.bot, u.id, "admin_panel", None)


async def show_home(bot, user, chat_id: int) -> None:
    rec = upsert_user(user)
    STATE[user.id] = "idle"
    WATCHERS.pop(user.id, None)
    await send_html(bot, chat_id, text_start(rec, user), kb_home(user.id))


async def require_vip(bot, cid: int, rec: Dict[str, Any]) -> bool:
    if vip_active(rec) or is_admin(cid):
        return True
    await send_html(
        bot,
        cid,
        f"{e('reason')} {bold('Cần VIP để vào phần mềm.')}\nHãy mua gói trước.",
        kb_thieu_xu(),
    )
    return False


async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    qh = update.callback_query
    await qh.answer()
    user = qh.from_user
    cid = user.id
    rec = upsert_user(user)
    banned, until = ban_status(rec)
    if banned and not is_admin(cid) and qh.data != "lien_he":
        await qh.answer(f"Bị khoá tới {until}", show_alert=True)
        return
    data = qh.data or ""
    bot = context.bot

    if data == "home":
        await show_home(bot, user, cid)
        return
    if data == "nhap_key":
        STATE[cid] = "wait_key"
        await send_html(
            bot,
            cid,
            (
                f"{e('home_nap')} {bold('KÍCH HOẠT MÃ KEY VIP')} {e('clock24')}\n"
                f"{'━'*22}\n"
                f"{e('input')} {bold('Vui lòng gửi mã Key của bạn vào khung chat:')}\n"
                f"{'━'*22}\n"
                f"{e('note')} {italic('Nhập mã key hợp lệ để kích hoạt VIP ngay lập tức.')}"
            ),
            kb_home_only(),
        )
        return
    if data == "nap_xu":
        STATE[cid] = "wait_nap"
        await send_html(bot, cid, text_nap_home(), kb_home_only())
        return
    if data == "nap_check":
        await do_nap_check(bot, cid, rec)
        return
    if data == "mua_goi":
        await send_html(
            bot,
            cid,
            (
                f"{e('mua_home')} {bold('TRANG CHỦ MUA GÓI VIP - 24/7')}\n"
                f"{'━'*22}\n"
                f"{e('p1')} 1 ngày  —  {bold('25xu')}\n"
                f"{e('p3')} 3 ngày  —  {bold('80xu')}\n"
                f"{e('p3')} 1 tuần  —  {bold('125xu')}\n"
                f"{e('p3')} 1 tháng  —  {bold('250xu')}\n"
                f"{e('p3')} 2 tháng  —  {bold('100xu')}\n"
                f"{'━'*22}\n"
                f"{e('hint')} {italic('Nạp xu → Mua gói → Vào game!')}"
            ),
            kb_mua(),
        )
        return
    if data in VIP_PACKS:
        await buy_pack(bot, cid, rec, data)
        return
    if data == "phan_mem":
        WATCHERS.pop(cid, None)
        await send_html(
            bot,
            cid,
            (
                f"{e('soft_home')} {bold('Khu Vực Chọn Phần Mềm Dự Đoán')}\n"
                f"{'━'*22}\n"
                f"{e('pick')} {italic('Vui lòng chọn Mục muốn dự đoán')}"
            ),
            kb_soft(),
        )
        return
    if data == "soft_tx":
        if not await require_vip(bot, cid, rec):
            return
        await send_html(bot, cid, f"{e('tx')} {bold('TÀI XỈU')} — chọn nhà cái", kb_tx_games())
        return
    if data == "soft_md5":
        if not await require_vip(bot, cid, rec):
            return
        await send_html(bot, cid, f"{e('tx')} {bold('TÀI XỈU MD5')} — chọn nhà cái", kb_md5_games())
        return
    if data == "soft_sicbo":
        if not await require_vip(bot, cid, rec):
            return
        await send_html(bot, cid, f"{e('sicbo')} {bold('SICBO')} — chọn nhà cái", kb_sicbo_games())
        return
    if data.startswith("play|"):
        parts = data.split("|", 2)
        if len(parts) != 3:
            return
        _, cat, game = parts
        if not await require_vip(bot, cid, rec):
            return
        await start_watch(bot, cid, cat, game)
        return
    if data == "giftcode":
        STATE[cid] = "wait_gift"
        await send_html(
            bot,
            cid,
            (
                f"{e('gift_in')} {bold('Nhập GiftCode')}\n"
                f"{'━'*22}\n"
                f"{e('du')} {italic('Gửi mã giftcode của bạn để nhận quà')}"
            ),
            kb_home_only(),
        )
        return
    if data == "feedback":
        STATE[cid] = "wait_fb"
        STATE_DATA[cid] = {}
        await send_html(
            bot,
            cid,
            (
                f"{e('fb_t')} {bold('GỬI FEEDBACK hoặc LỖI')}\n"
                f"{'━'*22}\n"
                f"{e('fb_1')} Gửi ý kiến/lỗi bạn gặp phải:\n"
                f"{e('fb_2')} FeedBack/Lỗi được duyệt: {bold('+5.000đ')}\n"
                f"{e('fb_3')} Có thể gửi kèm ảnh minh chứng\n"
                f"{e('fb_4')} Nhiều ảnh gửi cùng lúc → gộp thành 1 album!\n"
                f"{'━'*22}"
            ),
            kb_home_only(),
        )
        return
    if data == "ca_nhan":
        await show_profile(bot, user, rec)
        return
    if data == "ls_nap":
        await show_ls_nap(bot, cid)
        return
    if data == "ls_mua":
        await show_ls_mua(bot, cid)
        return
    if data == "huong_dan":
        await send_html(
            bot,
            cid,
            (
                f"{e('btn_hd')} {bold('HƯỚNG DẪN SỬ DỤNG')}\n"
                f"{'━'*22}\n"
                f"1. {bold('Nạp Xu')} — chuyển khoản đúng {italic('số tiền')} và {italic('nội dung')}.\n"
                f"2. Bấm {bold('Kiểm Tra')} để bot quét API ngân hàng.\n"
                f"3. {bold('Mua Gói')} VIP bằng xu.\n"
                f"4. Vào {bold('Phần Mềm')} chọn game / nhà cái.\n"
                f"5. Đánh theo dự đoán AI KIDZVN.\n"
                f"{'━'*22}\n"
                f"{e('note')} {italic('1.000đ = 1 xu. Dự đoán không đảm bảo 100%.')}"
            ),
            kb_home_only(),
        )
        return
    if data == "lien_he":
        await send_html(
            bot,
            cid,
            (
                f"{e('btn_lh')} {bold('LIÊN HỆ - HỖ TRỢ')}\n"
                f"{'━'*22}\n"
                f"{e('channel')} Kênh: {bold('@KidzVn')}\n"
                f"{e('admin')} Admin: {bold('@DucToanVn')}\n"
                f"{e('botname')} Bot: {bold('@' + BOT_USERNAME)}"
            ),
            kb_home_only(),
        )
        return

    # Feedback admin
    if data.startswith("fbok:") or data.startswith("fbno:"):
        if not is_admin(cid):
            await qh.answer("Không có quyền", show_alert=True)
            return
        ok = data.startswith("fbok:")
        fid = int(data.split(":")[1])
        row = q("SELECT * FROM feedback WHERE id=?", (fid,), "one")
        if not row:
            await qh.answer("Không tìm thấy", show_alert=True)
            return
        if row["status"] != "pending":
            await qh.answer("Đã xử lý rồi", show_alert=True)
            return
        q("UPDATE feedback SET status=? WHERE id=?", ("approved" if ok else "rejected", fid))
        if ok:
            add_xu(row["chat_id"], FEEDBACK_REWARD_XU)
            try:
                await bot.send_message(
                    row["chat_id"],
                    f"✅ Feedback đã được duyệt. Bạn nhận +{FEEDBACK_REWARD_XU} xu.",
                )
            except Exception:
                pass
        await qh.edit_message_reply_markup(
            reply_markup=InlineKeyboardMarkup(
                [[ibtn("ĐÃ DUYỆT" if ok else "ĐÃ HUỶ", "noop")]]
            )
        )
        return

    if data == "noop":
        return

    # Admin
    if data == "admin_panel" or data.startswith("ad_"):
        if not is_admin(cid):
            await qh.answer("Không có quyền", show_alert=True)
            return
        await admin_router(bot, cid, data, qh)
        return


async def buy_pack(bot, cid: int, rec: Dict[str, Any], pack_id: str) -> None:
    label, cost, delta = VIP_PACKS[pack_id]
    rec = get_user(cid) or rec
    if int(rec["xu"]) < cost:
        await send_html(
            bot,
            cid,
            (
                f"{e('fail')} {bold('Số dư xu không đủ')}\n"
                f"{'━'*22}\n"
                f"{e('xu')} Số dư: {bold(fmt_money(rec['xu']) + ' xu')}\n"
                f"{e('mua')} Cần: {bold(str(cost) + ' xu')} ({label})"
            ),
            kb_thieu_xu(),
        )
        return
    add_xu(cid, -cost)
    set_vip(cid, delta=delta)
    rec = get_user(cid)
    q(
        "INSERT INTO purchases(chat_id,pack,xu_cost,days_label,created_at) VALUES(?,?,?,?,?)",
        (cid, pack_id, cost, label, int(time.time())),
    )
    await send_html(
        bot,
        cid,
        (
            f"{e('ok')} {bold('Mua Gói Vip Thành Công')}\n"
            f"{'━'*22}\n"
            f"{e('sodu')} Số Dư Xu : {bold(fmt_money(rec['xu']) + ' xu')}\n"
            f"   {e('time')} Thời Gian : {copy_txt(fmt_dt())}\n"
            f"{'━'*22}\n"
            f"{e('vip')} Gói: {bold(label)} — {bold(vip_status_text(rec))}"
        ),
        kb_home_only(),
    )


async def show_profile(bot, tg_user, rec: Dict[str, Any]) -> None:
    until = int(rec.get("vip_until") or 0)
    if until < 0:
        vip_h = "Vĩnh viễn"
    elif until == 0:
        vip_h = "—"
    else:
        vip_h = datetime.fromtimestamp(until, TZ).strftime("%d/%m/%Y %H:%M:%S")
    uname = f"@{tg_user.username}" if tg_user.username else "—"
    txt = (
        f"{e('btn_me')} {bold('THÔNG TIN CÁ NHÂN')}\n"
        f"{'━'*22}\n"
        f"{e('id')} ID: {copy_txt(tg_user.id)}\n"
        f"{e('hi')} Tên: {bold(full_name_of(tg_user))}\n"
        f"{e('user')} Username: {bold(uname)}\n"
        f"{e('note')} Language: {copy_txt(getattr(tg_user, 'language_code', None) or '—')}\n"
        f"{e('vip')} Premium TG: {bold('Có' if getattr(tg_user, 'is_premium', False) else 'Không')}\n"
        f"{'━'*22}\n"
        f"{e('xu')} Số Dư Xu: {bold(fmt_money(rec['xu']) + ' xu')}\n"
        f"{e('vip')} VIP: {bold(vip_status_text(rec))}\n"
        f"{e('time')} HSD VIP: {copy_txt(vip_h)}\n"
        f"{e('time')} Tham gia: {copy_txt(datetime.fromtimestamp(rec['created_at'], TZ).strftime('%d/%m/%Y %H:%M'))}\n"
        f"{'━'*22}"
    )
    await send_html(bot, tg_user.id, txt, kb_me())


async def show_ls_nap(bot, cid: int) -> None:
    rows = q(
        "SELECT * FROM deposits WHERE chat_id=? ORDER BY id DESC LIMIT 15",
        (cid,),
        "all",
    )
    if not rows:
        await send_html(bot, cid, f"{e('amt2')} {italic('Chưa có lịch sử nạp.')}", kb_me())
        return
    lines = [f"{e('amt2')} {bold('LỊCH SỬ NẠP')}", "━" * 22]
    for r in rows:
        st = r["status"]
        lines.append(
            f"{e('money')} {fmt_money(r['amount'])}đ | {esc(r['content'])} | {esc(st)}"
        )
    await send_html(bot, cid, "\n".join(lines), kb_me())


async def show_ls_mua(bot, cid: int) -> None:
    rows = q(
        "SELECT * FROM purchases WHERE chat_id=? ORDER BY id DESC LIMIT 15",
        (cid,),
        "all",
    )
    if not rows:
        await send_html(bot, cid, f"{e('mua')} {italic('Chưa có lịch sử mua gói.')}", kb_me())
        return
    lines = [f"{e('mua')} {bold('LỊCH SỬ MUA GÓI')}", "━" * 22]
    for r in rows:
        t = datetime.fromtimestamp(r["created_at"], TZ).strftime("%d/%m/%Y %H:%M")
        lines.append(f"{e('p3')} {esc(r['days_label'])} (-{r['xu_cost']}xu) • {t}")
    await send_html(bot, cid, "\n".join(lines), kb_me())


# ----- Nạp: tạo bill -----
async def create_bill(bot, cid: int, amount: int) -> None:
    content = gen_content()
    q(
        "INSERT INTO deposits(chat_id,amount,content,status,created_at) VALUES(?,?,?,?,?)",
        (cid, amount, content, "pending", int(time.time())),
    )
    STATE[cid] = "wait_check"
    STATE_DATA[cid] = {"amount": amount, "content": content}
    xu = amount // 1000
    caption = (
        f"{e('qr')} {bold('Thông Tin Chuyển Khoản - Nạp Xu')} {e('info')}\n"
        f"{'━'*22}\n"
        f"{e('bank')} Ngân Hàng : {bold(BANK_NAME)}\n"
        f"Số Tài Khoản : {copy_txt(BANK_STK)}\n"
        f"{e('owner')} Chủ Tài Khoản : {bold(BANK_OWNER)}\n"
        f"{'━'*22}\n"
        f"{e('money')} Số Tiền : {copy_txt(fmt_money(amount) + 'đ')}\n"
        f"{e('desc')} Nội Dung (bắt buộc) : {copy_txt(content)}\n"
        f"{italic(f'→ nhận {xu} xu')}\n"
        f"{'━'*22}\n"
        f"{e('time')} Thời Gian : {copy_txt(fmt_dt())}\n"
        f"{'━'*22}\n"
        f"{e('note')} {italic('chuyển xong vui lòng click vào Kiểm Tra để bot check ngân hàng.')}"
    )
    await send_html(bot, cid, caption, kb_nap_check(), photo=qr_url(amount, content))


async def do_nap_check(bot, cid: int, rec: Dict[str, Any]) -> None:
    data = STATE_DATA.get(cid) or {}
    amount = data.get("amount")
    content = data.get("content")
    if not amount or not content:
        row = q(
            "SELECT * FROM deposits WHERE chat_id=? AND status='pending' ORDER BY id DESC LIMIT 1",
            (cid,),
            "one",
        )
        if not row:
            await send_html(bot, cid, f"{e('reason')} Không có đơn nạp đang chờ.", kb_home_only())
            return
        amount, content = row["amount"], row["content"]
        STATE_DATA[cid] = {"amount": amount, "content": content}

    if CHECKING.get(cid):
        return
    CHECKING[cid] = True
    try:
        start = time.time()
        found = None
        while time.time() - start < NAP_TIMEOUT_SEC:
            elapsed = int(time.time() - start)
            await send_html(
                bot,
                cid,
                (
                    f"{e('checking')} {bold('Đang Kiểm Tra Giao Dịch Khớp')}\n"
                    f"{'━'*22}\n"
                    f"{e('amt2')} Số tiền : {copy_txt(fmt_money(amount) + 'đ')}\n"
                    f"{e('nd')} Nội dung : {copy_txt(content)}\n"
                    f"{'━'*22}\n"
                    f"{e('wait')} Đang Kiểm Tra Giao Dịch ({elapsed})s"
                ),
                kb_nap_check(),
            )
            found = match_deposit(amount, content)
            if found:
                break
            await asyncio.sleep(NAP_POLL_EVERY)

        if not found:
            await send_html(
                bot,
                cid,
                (
                    f"{e('fail')} {bold('Nạp Tiền Bị Huỷ')}\n"
                    f"{'━'*22}\n"
                    f"{e('reason')} Lý Do : {italic('Không thấy thông tin khớp với thông tin nạp tiền trong bot, vui lòng liên hệ admin hỗ trợ')}\n"
                    f"{'━'*22}\n"
                    f"{e('botname')} Bot : {bold('@' + BOT_USERNAME)}"
                ),
                kb_nap_check(),
            )
            return

        # chốt 1 lần
        dep = q("SELECT * FROM deposits WHERE content=?", (content,), "one")
        if dep and dep["status"] == "paid":
            await send_html(bot, cid, f"{e('ok')} Đơn này đã được duyệt trước đó.", kb_home_only())
            return
        txn_id = found.get("txn_id") or f"ND-{content}"
        if q("SELECT 1 FROM used_txns WHERE txn_id=?", (txn_id,), "one"):
            await send_html(bot, cid, f"{e('fail')} Giao dịch đã được dùng.", kb_home_only())
            return
        q(
            "INSERT OR IGNORE INTO used_txns(txn_id,deposit_id,amount,description,used_at) VALUES(?,?,?,?,?)",
            (txn_id, dep["id"] if dep else 0, amount, found.get("description"), int(time.time())),
        )
        q(
            "UPDATE deposits SET status='paid', txn_id=?, paid_at=? WHERE content=?",
            (txn_id, int(time.time()), content),
        )
        add_xu(cid, int(amount) // 1000)
        rec = get_user(cid)
        success_user = (
            f"{bold('Nạp Tiền Thành Công')}\n"
            f"{'━'*22}\n"
            f"{e('amt2')} Số Tiền : {copy_txt(fmt_money(amount) + 'đ')}\n"
            f"        {e('nd')} Mã Giao Dịch : {copy_txt(txn_id)}\n"
            f"        {e('time')} Thời Gian : {copy_txt(fmt_dt())}\n"
            f"{'━'*22}\n"
            f"{e('xu')} Số dư: {bold(fmt_money(rec['xu']) + ' xu')}\n"
            f"{e('botname')} Bot : {bold('@' + BOT_USERNAME)}"
        )
        await send_html(bot, cid, success_user, kb_home_only())
        STATE[cid] = "idle"

        notice = (
            f"{e('notice')} {bold('THÔNG BÁO NẠP TIỀN KIDZVN')}\n"
            f"{'━'*22}\n"
            f"{e('user')} Khách Hàng : {bold(rec.get('full_name') or cid)}\n"
            f"       {e('amt2')} Số Tiền : {copy_txt(fmt_money(amount) + 'đ')}\n"
            f"       {e('nd')} Nội Dung : {copy_txt(content)}\n"
            f"       {e('time')} Thời Gian : {copy_txt(fmt_dt())}\n"
            f"{'━'*22}\n"
            f"{e('botname')} Bot : {bold('@' + BOT_USERNAME)}"
        )
        # gửi admin + toàn bộ user
        targets = set(all_user_ids()) | set(ADMIN_IDS)
        for uid in targets:
            if uid == cid:
                continue
            try:
                await send_markup(bot, uid, notice)
            except Exception:
                pass
    finally:
        CHECKING[cid] = False


# ----- Watch / predict (GET từ API) -----
async def start_watch(bot, cid: int, cat: str, game: str) -> None:
    payload = fetch_game(cat, game)
    if not payload:
        await send_html(
            bot,
            cid,
            (
                f"{e('fail')} {bold('Không lấy được dữ liệu API')}\n"
                f"{'━'*22}\n"
                f"{e('game2')} Game: {bold(game)} {bold(CAT_LABEL.get(cat, cat))}\n"
                f"{e('reason')} {italic('Vui lòng thử lại sau.')}"
            ),
            kb_pred(),
        )
        return
    phien = payload["phien"]
    if payload.get("ketqua") and payload.get("phien_prev"):
        save_hist(cat, game, int(payload["phien_prev"]), payload["ketqua"], payload.get("raw"))
    elif payload.get("ketqua"):
        save_hist(cat, game, int(phien) - 1, payload["ketqua"], payload.get("raw"))
    msg = await send_html(
        bot,
        cid,
        text_predict(game, cat, phien, payload["dudoan"], payload["tile"]),
        kb_pred(),
    )
    WATCHERS[cid] = {
        "game": game,
        "cat": cat,
        "msg_id": msg.message_id if msg else None,
        "last_phien": int(phien),
    }


async def predictor_loop(app: Application) -> None:
    await asyncio.sleep(3)
    while True:
        try:
            groups: Dict[Tuple[str, str], List[int]] = defaultdict(list)
            for uid, w in list(WATCHERS.items()):
                groups[(w["cat"], w["game"])].append(uid)
            for (cat, game), uids in groups.items():
                payload = fetch_game(cat, game)
                if not payload:
                    continue
                phien = int(payload["phien"])
                if payload.get("ketqua") and payload.get("phien_prev"):
                    save_hist(cat, game, int(payload["phien_prev"]), payload["ketqua"], payload.get("raw"))
                for uid in uids:
                    w = WATCHERS.get(uid)
                    if not w:
                        continue
                    if phien <= int(w.get("last_phien") or 0):
                        continue
                    w["last_phien"] = phien
                    try:
                        # Xoá tin cũ + gửi tin mới khi API ra phiên mới
                        await send_html(
                            app.bot,
                            uid,
                            text_predict(game, cat, phien, payload["dudoan"], payload["tile"]),
                            kb_pred(),
                            delete_old=True,
                        )
                    except Exception as ex:
                        logging.warning("push pred %s: %s", uid, ex)
        except Exception as ex:
            logging.warning("predictor_loop: %s", ex)
        await asyncio.sleep(5)


# ----- Text / media -----
async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.message.text:
        return
    user = update.effective_user
    cid = user.id
    rec = upsert_user(user)
    banned, until = ban_status(rec)
    if banned and not is_admin(cid):
        await update.message.reply_text(f"Tài khoản bị khoá.\nHạn: {until}")
        return
    text = update.message.text.strip()
    st = STATE.get(cid, "idle")
    bot = context.bot

    if st == "wait_key":
        key_code = text.upper().strip()
        row = q("SELECT * FROM keys_tbl WHERE key_code=? AND used=0", (key_code,), "one")
        if not row:
            await send_html(
                bot,
                cid,
                f"{e('fail')} {bold('Mã Key không tồn tại hoặc đã được sử dụng!')}",
                kb_home_only(),
            )
            return
        days = int(row["days"])
        now = int(time.time())
        q("UPDATE keys_tbl SET used=1, chat_id=?, used_at=? WHERE key_code=?", (cid, now, key_code))
        if days == 0:
            set_vip(cid, forever=True)
            hsd_text = "Vĩnh viễn"
        else:
            set_vip(cid, delta=timedelta(days=days))
            updated_user = get_user(cid)
            hsd_text = datetime.fromtimestamp(updated_user["vip_until"], TZ).strftime("%d/%m/%Y %H:%M")
        STATE[cid] = "idle"
        await send_html(
            bot,
            cid,
            (
                f"{e('ok')} {bold('Kích Hoạt Key Thành Công!')}\n"
                f"{'━'*22}\n"
                f"{e('du')} Mã Key : {copy_txt(key_code)}\n"
                f"   {e('time')} Hạn VIP : {copy_txt(hsd_text)}\n"
                f"{'━'*22}\n"
                f"{e('vip')} {bold(vip_status_text(get_user(cid)))}"
            ),
            kb_home_only(),
        )
        return

    if st == "wait_nap":
        amt = parse_amount(text)
        if amt is None or amt < MIN_NAP or amt > MAX_NAP:
            await send_html(
                bot,
                cid,
                f"{e('note')} Số tiền không hợp lệ.\nTối thiểu {fmt_money(MIN_NAP)}đ — tối đa {fmt_money(MAX_NAP)}đ",
                kb_home_only(),
            )
            return
        # làm tròn theo 1000đ = 1 xu
        amt = (amt // 1000) * 1000
        if amt < MIN_NAP:
            amt = MIN_NAP
        await create_bill(bot, cid, amt)
        return

    if st == "wait_gift":
        await redeem_gift(bot, cid, text.upper().strip())
        return

    if st == "wait_fb":
        await submit_feedback(bot, cid, user, text, [])
        return

    if st.startswith("ad_") and is_admin(cid):
        await admin_text(bot, cid, st, text, update)
        return

    # mặc định về home nếu gõ linh tinh
    await show_home(bot, user, cid)


async def on_photo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    cid = user.id
    rec = upsert_user(user)
    if ban_status(rec)[0] and not is_admin(cid):
        return
    st = STATE.get(cid, "idle")
    msg = update.message
    if st == "wait_fb":
        cap = msg.caption or ""
        fid = msg.photo[-1].file_id if msg.photo else None
        mgid = msg.media_group_id
        if mgid:
            g = MEDIA_GROUP.setdefault(mgid, {"uids": cid, "files": [], "cap": cap, "ts": time.time()})
            if fid:
                g["files"].append(fid)
            if cap:
                g["cap"] = cap
            await asyncio.sleep(1.3)
            g2 = MEDIA_GROUP.get(mgid)
            if not g2:
                return
            if time.time() - g2["ts"] < 1.2:
                return
            files = list(g2["files"])
            c = g2.get("cap") or ""
            MEDIA_GROUP.pop(mgid, None)
            await submit_feedback(context.bot, cid, user, c, files)
        else:
            await submit_feedback(context.bot, cid, user, cap, [fid] if fid else [])
        return
    if st == "ad_bc" and is_admin(cid):
        await admin_broadcast(context.bot, cid, msg.caption or "", photo=msg.photo[-1].file_id if msg.photo else None)
        return


async def on_video(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    cid = user.id
    st = STATE.get(cid, "idle")
    msg = update.message
    if st == "ad_bc" and is_admin(cid) and msg.video:
        await admin_broadcast(context.bot, cid, msg.caption or "", video=msg.video.file_id)
        return
    if st == "wait_fb":
        cap = msg.caption or ""
        await submit_feedback(context.bot, cid, user, cap, [])


async def redeem_gift(bot, cid: int, code: str) -> None:
    row = q("SELECT * FROM giftcodes WHERE code=?", (code,), "one")
    if not row:
        await send_html(bot, cid, f"{e('fail')} Mã giftcode không tồn tại.", kb_home_only())
        return
    now = int(time.time())
    if int(row["expire_at"]) and int(row["expire_at"]) < now:
        await send_html(bot, cid, f"{e('fail')} Mã đã hết hạn.", kb_home_only())
        return
    used = q("SELECT 1 FROM gift_uses WHERE code=? AND chat_id=?", (code, cid), "one")
    if used:
        await send_html(bot, cid, f"{e('fail')} Bạn đã nhập mã này rồi.", kb_home_only())
        return
    if int(row["used_count"]) >= int(row["max_uses"]):
        await send_html(bot, cid, f"{e('fail')} Mã đã hết lượt nhập.", kb_home_only())
        return
    q("INSERT INTO gift_uses(code,chat_id,used_at) VALUES(?,?,?)", (code, cid, now))
    q("UPDATE giftcodes SET used_count=used_count+1 WHERE code=?", (code,))
    hours = int(row["hours"])
    if hours > 0:
        set_vip(cid, delta=timedelta(hours=hours))
    hsd = datetime.fromtimestamp(row["expire_at"], TZ).strftime("%d/%m/%Y %H:%M") if row["expire_at"] else "—"
    STATE[cid] = "idle"
    await send_html(
        bot,
        cid,
        (
            f"{e('gift_in')} {bold('Kích Hoạt Mã Nhận Quà Thành Công')}\n"
            f"{'━'*22}\n"
            f"{e('du')} Mã Nhận Quà : {copy_txt(code)}\n"
            f"   {e('hsd')} HSD : {copy_txt(hsd)}\n"
            f"{'━'*22}\n"
            f"{e('vip')} {bold(vip_status_text(get_user(cid)))}"
        ),
        kb_home_only(),
    )


async def submit_feedback(bot, cid: int, user, content: str, photos: List[str]) -> None:
    fid = q(
        "INSERT INTO feedback(chat_id,content,photos,status,created_at) VALUES(?,?,?,?,?)",
        (cid, content or "", json.dumps(photos), "pending", int(time.time())),
    )
    STATE[cid] = "idle"
    await send_html(bot, cid, f"{e('ok')} Đã gửi feedback. Chờ admin duyệt.", kb_home_only())
    body = (
        f"{bold('FeedBack Đóng Góp')}\n"
        f"{'━'*22}\n"
        f"Người Dùng : {bold(full_name_of(user))} ({copy_txt(cid)})\n"
        f"Nội Dung : {esc(content) if content else italic('Không có')}\n"
        f"Thời Gian : {copy_txt(fmt_dt())}\n"
        f"{'━'*22}"
    )
    kb = InlineKeyboardMarkup(
        [[ibtn("Duyệt", f"fbok:{fid}"), ibtn("Huỷ", f"fbno:{fid}")]]
    )
    for admin in ADMIN_IDS:
        try:
            if photos:
                if len(photos) == 1:
                    await send_markup(bot, admin, body, kb=kb, photo=photos[0])
                else:
                    ptext, pents = compile_markup(body)
                    media = [InputMediaPhoto(photos[0], caption=ptext[:1024], caption_entities=pents)]
                    media += [InputMediaPhoto(p) for p in photos[1:10]]
                    await bot.send_media_group(admin, media)
                    await bot.send_message(admin, "Chọn xử lý:", reply_markup=kb)
            else:
                await send_markup(bot, admin, "KHÔNG CÓ ẢNH\n\n" + body, kb=kb)
        except Exception as ex:
            logging.warning("send fb admin: %s", ex)


# =============================================================================
# ADMIN
# =============================================================================
async def admin_router(bot, cid: int, data: str, qh) -> None:
    if data == "admin_panel":
        STATE[cid] = "idle"
        await send_html(
            bot,
            cid,
            f"⚙️ {bold('ADMIN PANEL')}\n{'━'*18}\n\n➡️ Chọn chức năng bên dưới",
            kb_admin(),
        )
        return
    if data == "ad_ban":
        STATE[cid] = "ad_ban"
        await send_html(bot, cid, "❌ Vui lòng nhập <code>&lt;chat_id&gt; &lt;số ngày band (0 = vĩnh viễn)&gt;</code>", kb_admin())
        return
    if data == "ad_unban":
        STATE[cid] = "ad_unban"
        await send_html(bot, cid, "✅ Vui lòng nhập <code>chat_id</code> cần mở khoá", kb_admin())
        return
    if data.startswith("ad_users"):
        page = int(data.split(":")[1]) if ":" in data else 0
        await admin_list_users(bot, cid, page)
        return
    if data == "ad_bc":
        STATE[cid] = "ad_bc"
        await send_html(
            bot,
            cid,
            "🔔 Vui lòng nhập nội dung muốn thông báo\n➡️ có thể gửi kèm ảnh hoặc video hoặc không",
            kb_admin(),
        )
        return
    if data == "ad_key":
        STATE[cid] = "ad_key"
        await send_html(bot, cid, "🔑 Vui lòng nhập <code>&lt;số ngày (0 = vĩnh viễn)&gt; &lt;chat_id&gt;</code>", kb_admin())
        return
    if data.startswith("ad_keys"):
        page = int(data.split(":")[1]) if ":" in data else 0
        await admin_list_keys(bot, cid, page)
        return
    if data == "ad_gift":
        STATE[cid] = "ad_gift"
        await send_html(bot, cid, "🎁 Vui lòng nhập <code>&lt;số giờ&gt; &lt;số thiết bị được nhập&gt;</code>", kb_admin())
        return
    if data.startswith("ad_gifts"):
        page = int(data.split(":")[1]) if ":" in data else 0
        await admin_list_gifts(bot, cid, page)
        return
    if data == "ad_addxu":
        STATE[cid] = "ad_addxu"
        await send_html(bot, cid, "➕ Nhập <code>&lt;chat_id&gt; &lt;số xu&gt;</code>", kb_admin())
        return
    if data == "ad_subxu":
        STATE[cid] = "ad_subxu"
        await send_html(bot, cid, "➖ Nhập <code>&lt;chat_id&gt; &lt;số xu&gt;</code>", kb_admin())
        return
    if data == "ad_setvip":
        STATE[cid] = "ad_setvip"
        await send_html(bot, cid, "👑 Nhập <code>&lt;chat_id&gt; &lt;số ngày (0 = vĩnh viễn)&gt;</code>", kb_admin())
        return
    if data == "ad_stats":
        uc = q("SELECT COUNT(*) c FROM users", (), "one")["c"]
        paid = q("SELECT COUNT(*) c, COALESCE(SUM(amount),0) s FROM deposits WHERE status='paid'", (), "one")
        vipn = 0
        now = int(time.time())
        for r in q("SELECT vip_until FROM users", (), "all"):
            v = int(r["vip_until"] or 0)
            if v < 0 or v > now:
                vipn += 1
        await send_html(
            bot,
            cid,
            (
                f"📊 {bold('THỐNG KÊ')}\n{'━'*18}\n"
                f"👤 User: {bold(uc)}\n"
                f"👑 VIP đang hoạt động: {bold(vipn)}\n"
                f"💰 Đơn nạp OK: {bold(paid['c'])} — {bold(fmt_money(paid['s']) + 'đ')}\n"
                f"👁 Watchers: {bold(len(WATCHERS))}"
            ),
            kb_admin(),
        )
        return
    if data == "ad_reset":
        STATE.clear()
        STATE_DATA.clear()
        WATCHERS.clear()
        CHECKING.clear()
        await send_html(bot, cid, "♻️ Đã reset state / watcher / pending FSM. Dữ liệu user giữ nguyên.", kb_admin())
        return


async def admin_list_users(bot, cid: int, page: int) -> None:
    rows = q("SELECT * FROM users ORDER BY created_at DESC", (), "all")
    per = 8
    total = len(rows)
    pages = max(1, (total + per - 1) // per)
    page = max(0, min(page, pages - 1))
    chunk = rows[page * per : page * per + per]
    lines = [f"👤 {bold('DANH SÁCH USER')} ({total}) — trang {page+1}/{pages}", "━" * 18]
    for r in chunk:
        lines.append(
            f"• {copy_txt(r['chat_id'])} {esc(r['full_name'] or '')} | {r['xu']}xu | {vip_status_text(dict(r))}"
        )
    nav = []
    if page > 0:
        nav.append(ibtn("◀", f"ad_users:{page-1}"))
    if page < pages - 1:
        nav.append(ibtn("▶", f"ad_users:{page+1}"))
    kb = [nav, [ibtn("ADMIN PANEL", "admin_panel")]]
    await send_html(bot, cid, "\n".join(lines), InlineKeyboardMarkup(kb))


async def admin_list_keys(bot, cid: int, page: int) -> None:
    rows = q("SELECT * FROM keys_tbl ORDER BY created_at DESC", (), "all")
    if not rows:
        await send_html(bot, cid, "Chưa có key.", kb_admin())
        return
    per = 8
    pages = max(1, (len(rows) + per - 1) // per)
    page = max(0, min(page, pages - 1))
    chunk = rows[page * per : page * per + per]
    lines = [f"🔑 {bold('QUẢN LÝ KEY')}", "━" * 18]
    for r in chunk:
        st = "USED" if r["used"] else "LIVE"
        lines.append(f"{copy_txt(r['key_code'])} | {r['days']}d | {r['chat_id']} | {st}")
    nav = []
    if page > 0:
        nav.append(ibtn("◀", f"ad_keys:{page-1}"))
    if page < pages - 1:
        nav.append(ibtn("▶", f"ad_keys:{page+1}"))
    await send_html(bot, cid, "\n".join(lines), InlineKeyboardMarkup([nav, [ibtn("ADMIN PANEL", "admin_panel")]]))


async def admin_list_gifts(bot, cid: int, page: int) -> None:
    rows = q("SELECT * FROM giftcodes ORDER BY created_at DESC", (), "all")
    if not rows:
        await send_html(bot, cid, "Chưa có giftcode.", kb_admin())
        return
    per = 8
    pages = max(1, (len(rows) + per - 1) // per)
    page = max(0, min(page, pages - 1))
    chunk = rows[page * per : page * per + per]
    lines = [f"🎁 {bold('DANH SÁCH GIFTCODE')}", "━" * 18]
    for r in chunk:
        hsd = datetime.fromtimestamp(r["expire_at"], TZ).strftime("%d/%m %H:%M") if r["expire_at"] else "—"
        lines.append(f"{copy_txt(r['code'])} | {r['used_count']}/{r['max_uses']} | {hsd}")
    nav = []
    if page > 0:
        nav.append(ibtn("◀", f"ad_gifts:{page-1}"))
    if page < pages - 1:
        nav.append(ibtn("▶", f"ad_gifts:{page+1}"))
    await send_html(bot, cid, "\n".join(lines), InlineKeyboardMarkup([nav, [ibtn("ADMIN PANEL", "admin_panel")]]))


async def admin_text(bot, cid: int, st: str, text: str, update: Update) -> None:
    parts = text.split()
    try:
        if st == "ad_ban":
            if len(parts) < 2:
                await send_html(bot, cid, "Sai cú pháp. <code>chat_id số_ngày</code>", kb_admin())
                return
            tid, days = int(parts[0]), int(parts[1])
            if days == 0:
                q("UPDATE users SET banned_until=-1 WHERE chat_id=?", (tid,))
                hsd = "Vĩnh viễn"
            else:
                until = int(time.time()) + days * 86400
                q("UPDATE users SET banned_until=? WHERE chat_id=?", (until, tid))
                hsd = datetime.fromtimestamp(until, TZ).strftime("%d/%m/%Y %H:%M")
            WATCHERS.pop(tid, None)
            await send_html(bot, cid, f"❌ Đã band {copy_txt(tid)} — {esc(hsd)}", kb_admin())
            try:
                await bot.send_message(tid, f"Tài khoản bị khoá. Hạn: {hsd}")
            except Exception:
                pass
        elif st == "ad_unban":
            tid = int(parts[0])
            q("UPDATE users SET banned_until=0 WHERE chat_id=?", (tid,))
            await send_html(bot, cid, f"✅ Đã unban {copy_txt(tid)}", kb_admin())
        elif st == "ad_bc":
            await admin_broadcast(bot, cid, text)
        elif st == "ad_key":
            if len(parts) < 2:
                await send_html(bot, cid, "Sai cú pháp. <code>số_ngày chat_id</code>", kb_admin())
                return
            days, tid = int(parts[0]), int(parts[1])
            key = "KIDZ-" + "".join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))
            q(
                "INSERT INTO keys_tbl(key_code,days,chat_id,used,created_at,used_at) VALUES(?,?,?,?,?,?)",
                (key, days, tid, 1, int(time.time()), int(time.time())),
            )
            if days == 0:
                set_vip(tid, forever=True)
                hsd = "Vĩnh viễn"
            else:
                set_vip(tid, delta=timedelta(days=days))
                u = get_user(tid)
                hsd = datetime.fromtimestamp(u["vip_until"], TZ).strftime("%d/%m/%Y %H:%M") if u and u["vip_until"] > 0 else "—"
            u = get_user(tid)
            name = (u or {}).get("full_name") or tid
            await send_html(
                bot,
                cid,
                (
                    f"🔑 {bold('Tạo Mã Key Thành Công')}\n\n"
                    f"🔑 Mã Key : {copy_txt(key)}\n"
                    f"⏰ Hạn Sử Dụng : {copy_txt(hsd)}\n"
                    f"👤 Người Dùng : {bold(name)} ({copy_txt(tid)})"
                ),
                kb_admin(),
            )
            try:
                await bot.send_message(tid, f"🔑 Bạn được kích hoạt VIP.\nHSD: {hsd}")
            except Exception:
                pass
        elif st == "ad_gift":
            if len(parts) < 2:
                await send_html(bot, cid, "Sai cú pháp. <code>số_giờ số_thiết_bị</code>", kb_admin())
                return
            hours, slots = int(parts[0]), int(parts[1])
            code = "GIFT-" + "".join(random.choice(string.ascii_uppercase + string.digits) for _ in range(8))
            exp = int(time.time()) + max(hours, 1) * 3600
            q(
                "INSERT INTO giftcodes(code,hours,max_uses,used_count,expire_at,created_at) VALUES(?,?,?,?,?,?)",
                (code, hours, max(1, slots), 0, exp, int(time.time())),
            )
            await send_html(
                bot,
                cid,
                (
                    f"🎁 {bold('Mã Quà Tặng Đã Được Tạo')}\n\n"
                    f"➡️ Mã Quà Tặng : {copy_txt(code)}\n"
                    f"⏰ Hạn Sử Dụng : {copy_txt(datetime.fromtimestamp(exp, TZ).strftime('%d/%m/%Y %H:%M'))}\n"
                    f"👤 Lượt Nhập : {bold(f'0/{slots}')}"
                ),
                kb_admin(),
            )
        elif st == "ad_addxu":
            tid, xu = int(parts[0]), int(parts[1])
            bal = add_xu(tid, xu)
            await send_html(bot, cid, f"➕ Cộng {xu} xu cho {copy_txt(tid)} — dư {bold(bal)}", kb_admin())
        elif st == "ad_subxu":
            tid, xu = int(parts[0]), int(parts[1])
            bal = add_xu(tid, -abs(xu))
            await send_html(bot, cid, f"➖ Trừ {xu} xu của {copy_txt(tid)} — dư {bold(bal)}", kb_admin())
        elif st == "ad_setvip":
            tid, days = int(parts[0]), int(parts[1])
            if days == 0:
                set_vip(tid, forever=True)
                hsd = "Vĩnh viễn"
            else:
                set_vip(tid, delta=timedelta(days=days))
                u = get_user(tid)
                hsd = datetime.fromtimestamp(u["vip_until"], TZ).strftime("%d/%m/%Y %H:%M")
            await send_html(bot, cid, f"👑 Set VIP {copy_txt(tid)} — {esc(hsd)}", kb_admin())
        STATE[cid] = "idle"
    except Exception as ex:
        await send_html(bot, cid, f"Lỗi: {esc(ex)}", kb_admin())


async def admin_broadcast(bot, admin_id: int, text: str, photo: Optional[str] = None, video: Optional[str] = None) -> None:
    head = "".join(e(k) for k in ("bc1", "bc2", "bc3", "bc4", "bc5", "bc6"))
    body = f"{head}\n{'━'*22}\n{esc(text) if text else ''}"
    ids = all_user_ids()
    ok = fail = 0
    for uid in ids:
        try:
            if photo:
                await send_markup(bot, uid, body, photo=photo)
            elif video:
                await send_markup(bot, uid, body, video=video)
            else:
                await send_markup(bot, uid, body)
            ok += 1
        except Exception:
            fail += 1
        await asyncio.sleep(0.04)
    STATE[admin_id] = "idle"
    await send_html(bot, admin_id, f"🔔 Đã gửi xong. OK: {ok} — Fail: {fail}", kb_admin())


# =============================================================================
# MAIN
# =============================================================================
async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    err = context.error
    if isinstance(err, (TimedOut, NetworkError)):
        logging.warning("Network/timeout (bỏ qua, bot tiếp tục): %s", err)
        return
    if isinstance(err, RetryAfter):
        logging.warning("Flood control RetryAfter %.1fs", getattr(err, "retry_after", 1))
        return
    logging.exception("Unhandled: %s", err)


def build_application() -> Application:
    req = build_httpx_request(for_get_updates=False)
    req_upd = build_httpx_request(for_get_updates=True)
    builder = (
        Application.builder()
        .token(BOT_TOKEN)
        .request(req)
        .get_updates_request(req_upd)
        .concurrent_updates(True)
    )
    # một số bản PTB còn nhận timeout trực tiếp trên builder
    for name, val in (
        ("connect_timeout", TG_CONNECT_TIMEOUT),
        ("read_timeout", TG_READ_TIMEOUT),
        ("write_timeout", TG_WRITE_TIMEOUT),
        ("pool_timeout", TG_POOL_TIMEOUT),
        ("get_updates_connect_timeout", TG_CONNECT_TIMEOUT),
        ("get_updates_read_timeout", max(TG_READ_TIMEOUT, TG_POLL_TIMEOUT + 15.0)),
        ("get_updates_pool_timeout", TG_POOL_TIMEOUT),
    ):
        fn = getattr(builder, name, None)
        if callable(fn):
            try:
                builder = fn(val)
            except Exception:
                pass
    return builder.build()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    if not BOT_TOKEN or BOT_TOKEN.startswith("YOUR_"):
        raise SystemExit("Điền BOT_TOKEN trong file hoặc biến môi trường BOT_TOKEN")
    if not ADMIN_IDS:
        logging.warning("ADMIN_IDS trống — không ai vào được ADMIN PANEL. Set env ADMIN_IDS=123,456")

    proxy = resolve_proxy()
    if proxy:
        logging.info("Dùng proxy Telegram: %s", proxy)
    else:
        logging.info("Không set proxy. Timeout connect/read = %.0fs/%.0fs", TG_CONNECT_TIMEOUT, TG_READ_TIMEOUT)

    db_init()
    app = build_application()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("admin", cmd_admin))
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(MessageHandler(filters.PHOTO, on_photo))
    app.add_handler(MessageHandler(filters.VIDEO, on_video))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))
    app.add_error_handler(on_error)

    async def _post(a: Application) -> None:
        a.create_task(predictor_loop(a))
        me = None
        try:
            me = a.bot.username
        except Exception:
            pass
        logging.info("KidzVN bot started%s", f" (@{me})" if me else "")

    app.post_init = _post

    attempt = 0
    while True:
        attempt += 1
        try:
            logging.info("Khởi động polling (lần %s)...", attempt)
            app.run_polling(
                allowed_updates=Update.ALL_TYPES,
                drop_pending_updates=True,
                timeout=TG_POLL_TIMEOUT,
                poll_interval=1.0,
                bootstrap_retries=8,
                close_loop=False,
            )
            break
        except KeyboardInterrupt:
            logging.info("Dừng bot (Ctrl+C)")
            break
        except SystemExit:
            raise
        except (TimedOut, NetworkError, OSError) as ex:
            if attempt >= TG_START_RETRIES:
                logging.error("Hết số lần thử (%s). Lỗi cuối: %s", TG_START_RETRIES, ex)
                logging.error(
                    "Nếu vẫn TimedOut: mạng không vào được api.telegram.org. "
                    "Bật VPN hoặc set TELEGRAM_PROXY rồi chạy lại."
                )
                raise
            wait = min(TG_RETRY_SLEEP * attempt, 30.0)
            logging.warning("TimedOut/NetworkError: %s — thử lại sau %.0fs (%s/%s)", ex, wait, attempt, TG_START_RETRIES)
            time.sleep(wait)
            try:
                app = build_application()
                app.add_handler(CommandHandler("start", cmd_start))
                app.add_handler(CommandHandler("admin", cmd_admin))
                app.add_handler(CallbackQueryHandler(on_callback))
                app.add_handler(MessageHandler(filters.PHOTO, on_photo))
                app.add_handler(MessageHandler(filters.VIDEO, on_video))
                app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))
                app.add_error_handler(on_error)
                app.post_init = _post
            except Exception as rebuild_ex:
                logging.warning("rebuild app: %s", rebuild_ex)
        except TelegramError as ex:
            logging.exception("TelegramError: %s", ex)
            if attempt >= TG_START_RETRIES:
                raise
            time.sleep(TG_RETRY_SLEEP)


if __name__ == "__main__":
    main()
