"""
بوت تيليثون + يوزربوت — الإصدار الكامل مع جميع الأوامر
"""

import os
import json
import time
import logging
import asyncio
import aiofiles

from telethon import TelegramClient, events, Button, functions, types, utils
from telethon.sessions import StringSession
from telethon.errors import (
    SessionPasswordNeededError,
    PhoneCodeInvalidError,
    PasswordHashInvalidError,
    UserAlreadyParticipantError,
    ChatAdminRequiredError,
    UserNotParticipantError,
    FloodWaitError,
)

try:
    from pytgcalls import PyTgCalls
    from pytgcalls.types import MediaStream, AudioQuality, VideoQuality
    CALLS_AVAILABLE = True
except ImportError:
    CALLS_AVAILABLE = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# الإعدادات
# ─────────────────────────────────────────────────────────────
API_ID         = int(os.getenv("API_ID", "0"))
API_HASH       = os.getenv("API_HASH", "")
BOT_TOKEN      = os.getenv("BOT_TOKEN", "")
OWNER_ID       = int(os.getenv("OWNER_ID", "0"))
SESSION_STRING = os.getenv("SESSION_STRING", "")

SESSIONS_DIR = "sessions"
MEDIA_DIR    = "media_files"
USER_SESSION = os.path.join(SESSIONS_DIR, "user")
CONFIG_FILE  = "config.json"

for _d in [SESSIONS_DIR, MEDIA_DIR]:
    os.makedirs(_d, exist_ok=True)

# ─────────────────────────────────────────────────────────────
# الإعدادات المحفوظة (كروب البصمات، إلخ)
# ─────────────────────────────────────────────────────────────
def load_cfg() -> dict:
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}

def save_cfg(data: dict):
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

cfg = load_cfg()

# تعيين كروب البصمات الافتراضي إذا لم يكن محفوظاً
if "yoot_group" not in cfg:
    cfg["yoot_group"] = "https://t.me/u33u0"
    save_cfg(cfg)

# ─────────────────────────────────────────────────────────────
# الحالات والكاش
# ─────────────────────────────────────────────────────────────
bot_states   = {}   # حالات واجهة البوت
ub_states    = {}   # حالات اليوزربوت متعددة الخطوات
file_cache   = {}   # كاش الملفات
active_chats = {}   # مكالمات الاستيج النشطة {chat_id: PyTgCalls}
yoot_pending = {}   # انتظار رد بوت البصمات {yoot_msg_id: (src_chat_id, event)}

CHUNK_SIZE  = 1 << 20  # 1MB
NUM_WORKERS = 4

# ─────────────────────────────────────────────────────────────
# العملاء
# ─────────────────────────────────────────────────────────────
bot = TelegramClient("bot_session", API_ID, API_HASH,
                     flood_sleep_threshold=60, connection_retries=5)

user_client: TelegramClient | None = None
call_manager: "PyTgCalls | None" = None


def _make_user_client() -> TelegramClient | None:
    if SESSION_STRING:
        return TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH,
                              flood_sleep_threshold=60)
    if os.path.exists(USER_SESSION + ".session"):
        return TelegramClient(USER_SESSION, API_ID, API_HASH,
                              flood_sleep_threshold=60)
    return None


# ─────────────────────────────────────────────────────────────
# مساعدات
# ─────────────────────────────────────────────────────────────
def _fmt_size(n: int) -> str:
    if n >= 1 << 20:
        return f"{n / (1 << 20):.1f} MB"
    if n >= 1 << 10:
        return f"{n / (1 << 10):.1f} KB"
    return f"{n} B"

def _fmt_eta(s: int) -> str:
    return f"{s}ث" if s < 60 else f"{s // 60}د {s % 60}ث"

def _bar(pct: float, w: int = 10) -> str:
    f = int(pct / 100 * w)
    return "█" * f + "░" * (w - f)

def _fake_bar(step: int, total: int = 20, w: int = 10) -> str:
    pct = min(step / total * 100, 100)
    return f"[{_bar(pct, w)}] {pct:.0f}%"


async def fast_download(message, status_msg, client: TelegramClient = None) -> "tuple[str,bool] | None":
    if client is None:
        client = user_client or bot
    media = (message.video or message.document or message.voice
             or message.audio or message.photo or message.sticker)
    if not media:
        return None
    is_video = bool(message.video) or (
        message.document and getattr(message.document, "mime_type", "").startswith("video/")
    )
    if message.video:
        ext = ".mp4"
    elif message.voice:
        ext = ".ogg"
    elif message.photo or (message.document and getattr(message.document, "mime_type", "").startswith("image/")):
        ext = ".jpg"
    elif message.sticker:
        ext = ".webm"
    else:
        ext = ".mp3"
    fid = getattr(media, "id", id(media))
    if fid and fid in file_cache and os.path.exists(file_cache[fid]):
        try:
            await status_msg.edit("✅ الملف موجود في الكاش، جاري التشغيل…")
        except Exception:
            pass
        return file_cache[fid], is_video

    fsize = getattr(media, "size", 0) or 0
    fpath = os.path.join(MEDIA_DIR, f"file_{fid}{ext}")
    start_t = time.time()
    downloaded = [0]
    lock = asyncio.Lock()
    last_upd = [0.0]

    async def _upd():
        now = time.time()
        async with lock:
            if now - last_upd[0] < 4:
                return
            last_upd[0] = now
        elapsed = (time.time() - start_t) or 1e-6
        dl = min(downloaded[0], fsize) if fsize > 0 else downloaded[0]
        spd = dl / elapsed
        if fsize > 0:
            pct = min(dl * 100 / fsize, 100.0)
            eta = _fmt_eta(int(max(fsize - dl, 0) / (spd or 1)))
            txt = (f"⬇️ جاري التحميل…\n[{_bar(pct)}] {pct:.1f}%\n"
                   f"{_fmt_size(dl)} / {_fmt_size(fsize)}\n"
                   f"⚡ {_fmt_size(int(spd))}/ث  ⏱ {eta}")
        else:
            txt = f"⬇️ جاري التحميل…\n{_fmt_size(dl)} | ⚡ {_fmt_size(int(spd))}/ث"
        try:
            await status_msg.edit(txt)
        except Exception:
            pass

    try:
        min_par = CHUNK_SIZE * NUM_WORKERS
        if fsize >= min_par:
            aligned = (fsize // NUM_WORKERS // CHUNK_SIZE) * CHUNK_SIZE or CHUNK_SIZE
            parts = []
            for i in range(NUM_WORKERS):
                off = i * aligned
                if off >= fsize:
                    break
                lim = aligned if i < NUM_WORKERS - 1 else (fsize - off)
                parts.append((off, lim))
            tmps = [f"{fpath}.part{i}" for i in range(len(parts))]

            async def _dl(idx, offset, limit, tmp):
                written = 0
                async with aiofiles.open(tmp, "wb") as f:
                    async for chunk in client.iter_download(
                        message.media, offset=offset, limit=limit, request_size=CHUNK_SIZE
                    ):
                        rem = limit - written
                        if rem <= 0:
                            break
                        data = chunk[:rem]
                        await f.write(data)
                        written += len(data)
                        downloaded[0] += len(data)
                        await _upd()

            await asyncio.gather(*[_dl(i, off, lim, tmp)
                                    for i, ((off, lim), tmp) in enumerate(zip(parts, tmps))])
            async with aiofiles.open(fpath, "wb") as out:
                for tmp in tmps:
                    async with aiofiles.open(tmp, "rb") as inp:
                        while True:
                            data = await inp.read(CHUNK_SIZE)
                            if not data:
                                break
                            await out.write(data)
                    os.remove(tmp)
        else:
            async with aiofiles.open(fpath, "wb") as f:
                async for chunk in client.iter_download(message.media, request_size=CHUNK_SIZE):
                    await f.write(chunk)
                    downloaded[0] += len(chunk)
                    await _upd()
    except Exception as e:
        for tmp in [f"{fpath}.part{i}" for i in range(NUM_WORKERS)]:
            if os.path.exists(tmp):
                os.remove(tmp)
        if os.path.exists(fpath):
            os.remove(fpath)
        raise e

    if fid:
        file_cache[fid] = fpath
    return fpath, is_video


async def _get_chat(client, link):
    try:
        link = link.strip()
        if link.lstrip("-").isdigit():
            return await client.get_entity(int(link))
        if "t.me/+" in link or "t.me/joinchat/" in link:
            h = link.split("/")[-1].replace("+", "")
            try:
                r = await client(functions.messages.ImportChatInviteRequest(hash=h))
                return r.chats[0]
            except UserAlreadyParticipantError:
                info = await client(functions.messages.CheckChatInviteRequest(hash=h))
                if isinstance(info, types.ChatInviteAlready):
                    return info.chat
                async for d in client.iter_dialogs():
                    if d.name == info.title:
                        return d.entity
        return await client.get_entity(link)
    except Exception as e:
        logger.error(f"خطأ في جلب المجموعة: {e}")
        return None


async def _resolve_target(event, txt: str):
    """استخراج كيان المستخدم المستهدف من الرد أو اليوزر أو الـ ID."""
    replied = await event.get_reply_message()
    if replied:
        return replied.sender_id
    parts = txt.split()
    if len(parts) >= 2:
        arg = parts[-1]
        try:
            return int(arg)
        except ValueError:
            try:
                e = await user_client.get_entity(arg)
                return e.id
            except Exception:
                pass
    return None


def _estimate_reg_date(user_id: int) -> str:
    """تقدير تاريخ التسجيل من الـ ID."""
    ranges = [
        (100000000,  "2013"),
        (200000000,  "2014"),
        (400000000,  "2015"),
        (700000000,  "2016"),
        (1000000000, "2017"),
        (1500000000, "2018"),
        (2000000000, "2019"),
        (2500000000, "2020"),
        (3000000000, "2021"),
        (4000000000, "2022"),
        (5000000000, "2023"),
        (6000000000, "2024"),
    ]
    for threshold, year in ranges:
        if user_id < threshold:
            return f"تقريباً {year}"
    return "2025 أو أحدث"


# ─────────────────────────────────────────────────────────────
# إدارة الاستيج (PyTgCalls)
# ─────────────────────────────────────────────────────────────
async def _ensure_call_manager():
    global call_manager
    if not CALLS_AVAILABLE or user_client is None:
        return None
    if call_manager is None:
        call_manager = PyTgCalls(user_client)
        await call_manager.start()
    return call_manager


async def play_in_chat(chat_id: int, file_path: str, is_video: bool = False) -> bool:
    cm = await _ensure_call_manager()
    if cm is None:
        return False
    try:
        stream = (
            MediaStream(file_path, audio_parameters=AudioQuality.STUDIO,
                        video_parameters=VideoQuality.FHD_1080p)
            if is_video else
            MediaStream(file_path, audio_parameters=AudioQuality.STUDIO)
        )
        await cm.play(chat_id, stream)
        active_chats[chat_id] = cm
        return True
    except Exception as e:
        logger.error(f"خطأ في التشغيل: {e}")
        return False


async def stop_in_chat(chat_id: int) -> bool:
    cm = active_chats.pop(chat_id, call_manager)
    if cm is None:
        return False
    try:
        await cm.leave_call(chat_id)
        return True
    except Exception as e:
        logger.error(f"خطأ في الإيقاف: {e}")
        return False


async def join_stage_only(chat_id: int) -> bool:
    cm = await _ensure_call_manager()
    if cm is None:
        return False
    try:
        silence = "http://docs.evostream.com/sample_content/assets/sintel.mp4"
        await cm.play(chat_id, MediaStream(silence, audio_parameters=AudioQuality.STUDIO))
        active_chats[chat_id] = cm
        return True
    except Exception as e:
        logger.error(f"خطأ في الصعود: {e}")
        return False


# ─────────────────────────────────────────────────────────────
# واجهة البوت
# ─────────────────────────────────────────────────────────────
def main_menu():
    return [
        [Button.inline("➕ إضافة حساب للاستيج",       b"login_acc")],
        [Button.inline("📱 تسجيل جلسة اليوزربوت",     b"gen_session")],
        [Button.inline("🎵 تشغيل في الاستيج (يدوي)",  b"manual_play")],
        [
            Button.inline("📞 صعود اتصال",  b"call_up"),
            Button.inline("📴 نزول اتصال",  b"call_down"),
        ],
        [Button.inline("🎤 تعيين كروب البصمات",        b"set_yoot")],
        [Button.inline("📋 قائمة الأوامر",             b"cmd_list")],
        [
            Button.inline("👥 عرض الحسابات", b"show_accs"),
            Button.inline("🗑 مسح حساب",     b"del_acc"),
        ],
        [Button.url("👨‍💻 المطوّر", "https://t.me/c3cccc3c")],
    ]


COMMANDS_TEXT = """📋 **أوامر اليوزربوت** (أرسلها أنت في الجروب):

**🎭 تقليد** (رداً على مستخدم أو @يوزر)
  • `تقليد` — تقليد الاسم + البايو + الصور
  • `تقليد صور` — تقليد الصور فقط
  • `تقليد بايو` — تقليد البايو فقط
  • `تقليد اسم` — تقليد الاسم فقط

**🔇 كتم / 🔊 فك كتم**
  • `كتم` (رداً) أو `كتم @يوزر` أو `كتم ايدي 12345`
  • `فك كتم` (بنفس الطريقة)

**🚫 حظر / ✅ فك حظر**
  • `حظر` (رداً) أو `حظر @يوزر`
  • `فك حظر` (بنفس الطريقة)

**ℹ️ معلوماته** (رداً أو @يوزر) — معلومات المستخدم مع صورته

**📤 تحويل** (رداً على ميديا) — تحويل الرسالة/الميديا

**🗑 مسح [عدد]** — مثال: `مسح 5` يمسح آخر 5 رسائل لك

**🐪 رفع مطي** (رداً) — ترقية وهمية مضحكة
**👟 رفع قندرة** (رداً) — ترقية وهمية أخرى

**🎵 يوت [نص]** — إرسال لبوت البصمات وتحويل الرد هنا

**▶️ شغل** (رداً على ميديا) — تشغيل في الاستيج
  • في الجروب: يشغّل مباشرة
  • في الرسائل المحفوظة: يطلب رابط الجروب

**⏹ ايقاف / إيقاف** — إيقاف الاستيج

**🎙 تسجيل** — صعود وهمي مع رسائل تمثيلية (مزحة)

**📝 تحويل البايو** (رداً على نص) — تغيير البايو

**🖼 ضف صورة** (رداً على صورة) — تغيير صورة البروفايل

**🚪 خروج من الجميع** — الخروج من جميع الجروبات والقنوات
  (ما عدا الحالي وأين أنت مشرف/مالك)
"""


def get_saved_accounts():
    accs = []
    if os.path.exists(SESSIONS_DIR):
        for f in os.listdir(SESSIONS_DIR):
            if f.endswith(".session") and not f.startswith("user"):
                accs.append(f.replace(".session", ""))
    return accs


# ─────────────────────────────────────────────────────────────
# معالجات البوت
# ─────────────────────────────────────────────────────────────
@bot.on(events.NewMessage(pattern="/start"))
async def bot_start(event):
    if event.sender_id != OWNER_ID:
        return
    ub_st = "✅ مفعّل" if user_client else "❌ غير مفعّل"
    yoot_g = cfg.get("yoot_group", "غير مُعيَّن")
    await event.respond(
        f"مرحباً 👋\nاليوزربوت: {ub_st}\nكروب البصمات: `{yoot_g}`",
        buttons=main_menu(),
    )


@bot.on(events.CallbackQuery)
async def bot_cb(event):
    if event.sender_id != OWNER_ID:
        return
    d = event.data

    if d == b"cmd_list":
        await event.edit(COMMANDS_TEXT, buttons=[[Button.inline("🔙 رجوع", b"back")]])

    elif d == b"back":
        await event.edit("ok:", buttons=main_menu())

    elif d == b"cancel":
        bot_states.pop(event.sender_id, None)
        await event.edit("تم الإلغاء.", buttons=main_menu())

    # ── تسجيل جلسة اليوزربوت ──
    elif d == b"gen_session":
        bot_states[event.sender_id] = {"step": "gs_phone"}
        await event.edit(
            "📱 **تسجيل جلسة اليوزربوت**\n\nأرسل رقم الهاتف مع رمز الدولة\n(مثال: +9647801234567):",
            buttons=[[Button.inline("❌ إلغاء", b"cancel")]],
        )

    # ── تعيين كروب البصمات ──
    elif d == b"set_yoot":
        current = cfg.get("yoot_group", "غير مُعيَّن")
        bot_states[event.sender_id] = {"step": "yoot_group"}
        await event.edit(
            f"🎤 **تعيين كروب البصمات**\n\nالحالي: `{current}`\n\n"
            "أرسل رابط الكروب أو ID أو @يوزر الكروب الذي فيه @W60yBot:",
            buttons=[[Button.inline("❌ إلغاء", b"cancel")]],
        )

    # ── إضافة حساب للاستيج ──
    elif d == b"login_acc":
        accs = get_saved_accounts()
        if len(accs) >= 1:
            await event.answer("يمكنك إضافة حساب واحد فقط. امسح الحالي أولاً.", alert=True)
            return
        bot_states[event.sender_id] = {"step": "acc_phone"}
        await event.edit(
            "أرسل رقم الهاتف للحساب:",
            buttons=[[Button.inline("❌ إلغاء", b"cancel")]],
        )

    # ── تشغيل يدوي ──
    elif d == b"manual_play":
        if not get_saved_accounts():
            await event.answer("لا يوجد حساب مضاف!", alert=True)
            return
        bot_states[event.sender_id] = {"step": "m_media"}
        await event.edit(
            "أرسل الميديا (فيديو/صوت/صوتية):",
            buttons=[[Button.inline("❌ إلغاء", b"cancel")]],
        )

    # ── عرض الحسابات ──
    elif d == b"show_accs":
        accs = get_saved_accounts()
        if not accs:
            await event.answer("لا توجد حسابات.", alert=True)
            return
        txt = "**حساباتك:**\n\n"
        for a in accs:
            txt += f"• `{a}` — {'🔊 نشط' if active_chats else '💤 غير نشط'}\n"
        await event.edit(txt, buttons=[[Button.inline("🔙 رجوع", b"back")]])

    # ── مسح حساب ──
    elif d == b"del_acc":
        accs = get_saved_accounts()
        if not accs:
            await event.answer("لا توجد حسابات.", alert=True)
            return
        btns = [[Button.inline(a, f"delacc_{a}".encode())] for a in accs]
        btns.append([Button.inline("❌ إلغاء", b"cancel")])
        await event.edit("اختر الحساب للمسح:", buttons=btns)

    elif d.startswith(b"delacc_"):
        acc = d.decode().replace("delacc_", "")
        path = os.path.join(SESSIONS_DIR, f"{acc}.session")
        if os.path.exists(path):
            os.remove(path)
            await event.answer(f"تم مسح {acc}.", alert=True)
        await event.edit("ok:", buttons=main_menu())

    # ── صعود/نزول اتصال ──
    elif d == b"call_up":
        accs = get_saved_accounts()
        if not accs:
            await event.answer("لا يوجد حساب مضاف!", alert=True)
            return
        bot_states[event.sender_id] = {"step": "join_link", "phone": accs[0]}
        await event.edit("أرسل رابط الجروب:", buttons=[[Button.inline("❌ إلغاء", b"cancel")]])

    elif d == b"call_down":
        if not active_chats:
            await event.answer("لا توجد مكالمات نشطة.", alert=True)
            return
        for cid in list(active_chats.keys()):
            await stop_in_chat(cid)
        await event.answer("تم الإيقاف.", alert=True)
        await event.edit("ok:", buttons=main_menu())


@bot.on(events.NewMessage)
async def bot_msgs(event):
    if event.sender_id != OWNER_ID:
        return
    state = bot_states.get(event.sender_id)
    if not state:
        return
    step = state.get("step", "")

    # ════ تسجيل جلسة اليوزربوت ════
    if step == "gs_phone":
        phone = (event.text or "").strip()
        if not phone:
            return
        tmp = TelegramClient(StringSession(), API_ID, API_HASH)
        await tmp.connect()
        try:
            r = await tmp.send_code_request(phone)
            bot_states[event.sender_id] = {"step": "gs_code", "phone": phone,
                                            "hash": r.phone_code_hash, "client": tmp}
            await event.respond(
                f"📨 تم إرسال الكود إلى `{phone}`\nأرسل الكود بنقاط (مثال: 1.2.3.4.5):",
                buttons=[[Button.inline("❌ إلغاء", b"cancel")]],
            )
        except Exception as e:
            await event.respond(f"خطأ: {e}", buttons=main_menu())
            bot_states.pop(event.sender_id, None)
            await tmp.disconnect()

    elif step == "gs_code":
        code = (event.text or "").replace(".", "").replace(" ", "").strip()
        tmp: TelegramClient = state["client"]
        try:
            await tmp.sign_in(state["phone"], code, phone_code_hash=state["hash"])
            await _finish_session_gen(event, tmp, state["phone"])
        except SessionPasswordNeededError:
            bot_states[event.sender_id]["step"] = "gs_2fa"
            await event.respond("🔐 أرسل كلمة المرور (2FA):",
                                 buttons=[[Button.inline("❌ إلغاء", b"cancel")]])
        except PhoneCodeInvalidError:
            await event.respond("الكود غير صحيح. أعد المحاولة:")
        except Exception as e:
            await event.respond(f"خطأ: {e}", buttons=main_menu())
            bot_states.pop(event.sender_id, None)

    elif step == "gs_2fa":
        tmp: TelegramClient = state["client"]
        try:
            await tmp.sign_in(password=(event.text or "").strip())
            await _finish_session_gen(event, tmp, state["phone"])
        except PasswordHashInvalidError:
            await event.respond("كلمة مرور خاطئة. أعد:")
        except Exception as e:
            await event.respond(f"خطأ: {e}", buttons=main_menu())
            bot_states.pop(event.sender_id, None)

    # ════ تعيين كروب البصمات ════
    elif step == "yoot_group":
        val = (event.text or "").strip()
        if val:
            cfg["yoot_group"] = val
            save_cfg(cfg)
            await event.respond(f"✅ تم تعيين كروب البصمات: `{val}`", buttons=main_menu())
        bot_states.pop(event.sender_id, None)

    # ════ إضافة حساب للاستيج ════
    elif step == "acc_phone":
        phone = (event.text or "").strip()
        sess = os.path.join(SESSIONS_DIR, phone)
        tmp = TelegramClient(sess, API_ID, API_HASH)
        await tmp.connect()
        try:
            r = await tmp.send_code_request(phone)
            bot_states[event.sender_id] = {"step": "acc_code", "phone": phone,
                                            "hash": r.phone_code_hash, "client": tmp}
            await event.respond(f"أرسل الكود لـ `{phone}`:",
                                 buttons=[[Button.inline("❌ إلغاء", b"cancel")]])
        except Exception as e:
            await event.respond(f"خطأ: {e}", buttons=main_menu())
            bot_states.pop(event.sender_id, None)
            await tmp.disconnect()

    elif step == "acc_code":
        code = (event.text or "").replace(".", "").replace(" ", "").strip()
        tmp: TelegramClient = state["client"]
        try:
            await tmp.sign_in(state["phone"], code, phone_code_hash=state["hash"])
            await event.respond(f"✅ تم حفظ `{state['phone']}`.", buttons=main_menu())
            bot_states.pop(event.sender_id, None)
            await tmp.disconnect()
        except SessionPasswordNeededError:
            bot_states[event.sender_id]["step"] = "acc_2fa"
            await event.respond("🔐 أرسل كلمة المرور:")
        except PhoneCodeInvalidError:
            await event.respond("كود خاطئ، أعد:")
        except Exception as e:
            await event.respond(f"خطأ: {e}", buttons=main_menu())
            bot_states.pop(event.sender_id, None)

    elif step == "acc_2fa":
        tmp: TelegramClient = state["client"]
        try:
            await tmp.sign_in(password=(event.text or "").strip())
            await event.respond(f"✅ تم حفظ `{state['phone']}`.", buttons=main_menu())
            bot_states.pop(event.sender_id, None)
            await tmp.disconnect()
        except PasswordHashInvalidError:
            await event.respond("كلمة مرور خاطئة:")
        except Exception as e:
            await event.respond(f"خطأ: {e}", buttons=main_menu())
            bot_states.pop(event.sender_id, None)

    # ════ تشغيل يدوي ════
    elif step == "m_media":
        has = event.message.video or event.message.document or \
              event.message.voice or event.message.audio
        if not has:
            await event.respond("أرسل فيديو أو ملف صوتي.")
            return
        sm = await event.respond("⬇️ جاري التحميل…")
        try:
            r = await fast_download(event.message, sm, client=bot)
            if not r:
                raise Exception("تعذّر الاستخراج")
            fp, iv = r
            bot_states[event.sender_id] = {"step": "m_link", "path": fp, "is_video": iv}
            await sm.delete()
            await event.respond("✅ تم! أرسل رابط الجروب:",
                                 buttons=[[Button.inline("❌ إلغاء", b"cancel")]])
        except Exception as e:
            await sm.delete()
            await event.respond(f"خطأ: {e}", buttons=main_menu())
            bot_states.pop(event.sender_id, None)

    elif step == "m_link":
        link = (event.text or "").strip()
        if not link:
            return
        accs = get_saved_accounts()
        if not accs:
            await event.respond("لا يوجد حساب!", buttons=main_menu())
            bot_states.pop(event.sender_id, None)
            return
        sm = await event.respond("⏳ جاري الصعود والتشغيل…")
        phone = accs[0]
        sess = os.path.join(SESSIONS_DIR, phone)
        try:
            mc = TelegramClient(sess, API_ID, API_HASH, receive_updates=False)
            await mc.connect()
            chat = await _get_chat(mc, link)
            if not chat:
                raise Exception("لم أجد الجروب")
            cid = utils.get_peer_id(chat)
            cm = PyTgCalls(mc)
            await cm.start()
            fp, iv = state["path"], state["is_video"]
            stream = (MediaStream(fp, audio_parameters=AudioQuality.STUDIO,
                                  video_parameters=VideoQuality.FHD_1080p)
                      if iv else MediaStream(fp, audio_parameters=AudioQuality.STUDIO))
            await cm.play(cid, stream)
            active_chats[cid] = cm
            await sm.delete()
            await event.respond("✅ يُشغَّل الآن!", buttons=main_menu())
        except Exception as e:
            await sm.delete()
            await event.respond(f"فشل: {e}", buttons=main_menu())
        bot_states.pop(event.sender_id, None)

    # ════ صعود اتصال ════
    elif step == "join_link":
        link = (event.text or "").strip()
        phone = state["phone"]
        sm = await event.respond("⏳ جاري الصعود…")
        try:
            mc = TelegramClient(os.path.join(SESSIONS_DIR, phone), API_ID, API_HASH,
                                 receive_updates=False)
            await mc.connect()
            chat = await _get_chat(mc, link)
            if not chat:
                raise Exception("لم أجد الجروب")
            cid = utils.get_peer_id(chat)
            cm = PyTgCalls(mc)
            await cm.start()
            await cm.play(
                cid,
                MediaStream("http://docs.evostream.com/sample_content/assets/sintel.mp4",
                             audio_parameters=AudioQuality.STUDIO),
            )
            active_chats[cid] = cm
            await sm.delete()
            await event.respond("✅ تم الصعود!", buttons=main_menu())
        except Exception as e:
            await sm.delete()
            await event.respond(f"فشل: {e}", buttons=main_menu())
        bot_states.pop(event.sender_id, None)


async def _finish_session_gen(event, tmp: TelegramClient, phone: str):
    global user_client, call_manager
    sess_str = tmp.session.save()
    await tmp.disconnect()
    me_test = TelegramClient(StringSession(sess_str), API_ID, API_HASH)
    await me_test.connect()
    if not await me_test.is_user_authorized():
        await event.respond("❌ فشل التحقق!", buttons=main_menu())
        bot_states.pop(event.sender_id, None)
        return
    me = await me_test.get_me()
    await me_test.disconnect()
    call_manager = None
    user_client = TelegramClient(StringSession(sess_str), API_ID, API_HASH)
    await user_client.connect()
    _register_ub_handlers()
    bot_states.pop(event.sender_id, None)
    await event.respond(
        f"✅ **تم تسجيل الجلسة بنجاح!**\n"
        f"الحساب: {me.first_name} (`{me.id}`)\n\n"
        f"📋 **نص الجلسة** — الصقه في GitHub Secret باسم `SESSION_STRING`:\n\n"
        f"`{sess_str}`\n\n"
        f"⚠️ لا تشاركه مع أحد!",
        buttons=main_menu(),
    )


# ─────────────────────────────────────────────────────────────
# معالجات اليوزربوت
# ─────────────────────────────────────────────────────────────
def _register_ub_handlers():
    if user_client is None:
        return

    @user_client.on(events.NewMessage(outgoing=True))
    async def _ub(event):
        txt = (event.text or "").strip()

        # ── شغل ──
        if txt == "شغل":
            await _cmd_play(event)

        # ── ايقاف ──
        elif txt in ["ايقاف", "إيقاف"]:
            await _cmd_stop(event)

        # ── تسجيل وهمي ──
        elif txt == "تسجيل":
            await _cmd_fake_record(event)

        # ── تحويل البايو ──
        elif txt == "تحويل البايو":
            await _cmd_change_bio(event)

        # ── ضف صورة ──
        elif txt == "ضف صورة":
            await _cmd_add_photo(event)

        # ── خروج من الجميع ──
        elif txt == "خروج من الجميع":
            await _cmd_leave_all(event)

        # ── تقليد ──
        elif txt == "تقليد" or txt.startswith("تقليد "):
            await _cmd_imitate(event, txt)

        # ── كتم ──
        elif txt == "كتم" or txt.startswith("كتم "):
            await _cmd_mute(event, txt)

        # ── فك كتم ──
        elif txt == "فك كتم" or txt.startswith("فك كتم "):
            await _cmd_unmute(event, txt)

        # ── حظر ──
        elif txt == "حظر" or txt.startswith("حظر "):
            await _cmd_ban(event, txt)

        # ── فك حظر ──
        elif txt == "فك حظر" or txt.startswith("فك حظر "):
            await _cmd_unban(event, txt)

        # ── معلوماته ──
        elif txt == "معلوماته" or txt.startswith("معلوماته "):
            await _cmd_info(event, txt)

        # ── تحويل (forward) ──
        elif txt == "تحويل":
            await _cmd_forward_media(event)

        # ── مسح [عدد] ──
        elif txt.startswith("مسح "):
            await _cmd_delete_msgs(event, txt)

        # ── رفع مطي ──
        elif txt == "رفع مطي":
            await _cmd_raise_camel(event)

        # ── رفع قندرة ──
        elif txt == "رفع قندرة":
            await _cmd_raise_shoe(event)

        # ── يوت [نص] ──
        elif txt.startswith("يوت "):
            await _cmd_yoot(event, txt)

        # ── اريد [نص] → يُحوَّل تلقائياً إلى يوت في كروب البصمات ──
        elif txt.startswith("اريد "):
            await _cmd_arid(event, txt)

        # ── انتظار رابط الجروب (شغل من الرسائل المحفوظة) ──
        elif event.is_private:
            me = await user_client.get_me()
            if event.chat_id == me.id:
                st = ub_states.get("play_pm")
                if st and st.get("waiting_link"):
                    await _cmd_play_pm_with_link(event, txt, st)

    # مستمع رد @W60yBot لأمر يوت / اريد
    @user_client.on(events.NewMessage(incoming=True))
    async def _yoot_listener(event):
        if not yoot_pending:
            return
        msg = event.message
        sender = await event.get_sender()
        if not sender:
            return
        sender_username = (getattr(sender, "username", "") or "").lower()
        if sender_username != "w60ybot":
            return
        # نتحقق أنه رد على رسالتنا
        reply_id = getattr(msg.reply_to, "reply_to_msg_id", None) if msg.reply_to else None
        if not reply_id or reply_id not in yoot_pending:
            return
        src_chat_id, status_msg = yoot_pending.pop(reply_id)
        # نحوّل البصمة/الصوت فقط — بدون نص أو روابط إضافية
        try:
            if msg.voice or msg.audio:
                await user_client.send_file(
                    src_chat_id,
                    msg.media,
                    voice_note=bool(msg.voice),
                )
            elif msg.document:
                await user_client.send_file(src_chat_id, msg.media)
        except Exception as e:
            logger.error(f"خطأ في إعادة إرسال البصمة: {e}")
        # حذف رسالة "جاري البحث"
        try:
            await status_msg.delete()
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════
# تنفيذ الأوامر
# ══════════════════════════════════════════════════════════════

async def _ub_reply(event, text: str, delay: int = 5):
    """إرسال رسالة ثم حذفها بعد delay ثانية."""
    nm = await user_client.send_message(event.chat_id, text)
    await asyncio.sleep(delay)
    try:
        await nm.delete()
    except Exception:
        pass


# ── شغل ──
async def _cmd_play(event):
    is_pm = event.is_private
    chat_id = event.chat_id
    replied = await event.get_reply_message()
    if not replied:
        await event.delete()
        return await _ub_reply(event, "⚠️ اكتب **شغل** رداً على ميديا.")
    has_media = any([replied.video, replied.document, replied.voice,
                     replied.audio, replied.photo, replied.sticker])
    if not has_media:
        await event.delete()
        return await _ub_reply(event, "⚠️ الرسالة لا تحتوي على ميديا.")
    if is_pm:
        me = await user_client.get_me()
        if chat_id == me.id:
            await event.delete()
            ask = await user_client.send_message(chat_id,
                "📍 أعطيني رابط الجروب أو ID الذي تريد التشغيل في استيجه:")
            ub_states["play_pm"] = {"waiting_link": True, "replied_msg": replied,
                                     "ask_msg_id": ask.id}
            return
    await event.delete()
    sm = await user_client.send_message(chat_id, "⬇️ جاري التحميل…")
    await _download_and_play(replied, sm, chat_id)


async def _cmd_play_pm_with_link(event, link: str, state: dict):
    chat_id = event.chat_id
    replied_msg = state["replied_msg"]
    ask_id = state.get("ask_msg_id")
    try:
        await user_client.delete_messages(chat_id, [ask_id, event.id])
    except Exception:
        pass
    ub_states.pop("play_pm", None)
    sm = await user_client.send_message(chat_id, "⬇️ جاري التحميل…")
    try:
        chat = await _get_chat(user_client, link)
        if not chat:
            return await sm.edit("❌ لم أتمكن من الوصول للجروب.")
        target_id = utils.get_peer_id(chat)
    except Exception as e:
        return await sm.edit(f"❌ خطأ: {e}")
    await _download_and_play(replied_msg, sm, target_id, progress_chat=chat_id)


async def _download_and_play(replied_msg, sm, target_chat_id: int, progress_chat: int = None):
    try:
        result = await fast_download(replied_msg, sm, client=user_client)
        if not result:
            return await sm.edit("❌ لم أتمكن من تحميل الملف.")
        fp, is_video = result
        await sm.edit("🚀 جاري الصعود للاستيج والتشغيل…")
        ok = await play_in_chat(target_chat_id, fp, is_video)
        if ok:
            await sm.edit(
                f"✅ {'الفيديو 🎬' if is_video else 'الصوت 🎵'} يُشغَّل الآن!\n"
                "اكتب **ايقاف** للإيقاف."
            )
        else:
            await sm.edit("❌ فشل التشغيل. تأكد أن الاستيج نشط وأنك عضو في الجروب.")
    except Exception as e:
        try:
            await sm.edit(f"❌ خطأ: {e}")
        except Exception:
            pass


# ── ايقاف ──
async def _cmd_stop(event):
    chat_id = event.chat_id
    await event.delete()
    target = chat_id
    if event.is_private:
        me = await user_client.get_me()
        if chat_id == me.id:
            target = next(iter(active_chats), None)
            if not target:
                return await _ub_reply(event, "⚠️ لا توجد استيجات نشطة.")
    ok = await stop_in_chat(target)
    await _ub_reply(event,
                    "✅ تم الإيقاف والنزول من الاستيج." if ok else "⚠️ لا يوجد استيج نشط.")


# ── تسجيل وهمي ──
async def _cmd_fake_record(event):
    chat_id = event.chat_id
    await event.delete()
    if event.is_private:
        return await _ub_reply(event, "⚠️ هذا الأمر يعمل في الجروبات فقط.")
    msg = await user_client.send_message(chat_id, "🎙 جار بدأ الاستماع إلى أصواتت الاستيج…")
    try:
        await join_stage_only(chat_id)
    except Exception:
        pass
    await asyncio.sleep(3)
    await msg.edit("🔴 تم بدأ تسجيل الاستيج وسيحفظ في الرسائل المحفوظة كملف صوتي 🎵")


# ── تحويل البايو ──
async def _cmd_change_bio(event):
    replied = await event.get_reply_message()
    await event.delete()
    if not replied or not replied.text:
        return await _ub_reply(event, "⚠️ رُدّ على رسالة نصية لتغيير البايو.")
    new_bio = replied.text[:70]
    try:
        await user_client(functions.account.UpdateProfileRequest(about=new_bio))
        await _ub_reply(event, f"✅ تم تغيير البايو إلى:\n{new_bio}", delay=6)
    except Exception as e:
        await _ub_reply(event, f"❌ فشل: {e}")


# ── ضف صورة ──
async def _cmd_add_photo(event):
    replied = await event.get_reply_message()
    await event.delete()
    if not replied or not replied.photo:
        return await _ub_reply(event, "⚠️ رُدّ على صورة.")
    sm = await user_client.send_message(event.chat_id, "⬇️ جاري التحميل…")
    photo_path = os.path.join(MEDIA_DIR, f"photo_{replied.id}.jpg")
    try:
        await user_client.download_media(replied.photo, photo_path)
        with open(photo_path, "rb") as f:
            await user_client(functions.photos.UploadProfilePhotoRequest(
                file=await user_client.upload_file(f)
            ))
        await sm.edit("✅ تم إضافة الصورة كصورة بروفايل!")
    except Exception as e:
        await sm.edit(f"❌ فشل: {e}")
    finally:
        if os.path.exists(photo_path):
            os.remove(photo_path)
    await asyncio.sleep(5)
    try:
        await sm.delete()
    except Exception:
        pass


# ── خروج من الجميع ──
async def _cmd_leave_all(event):
    chat_id = event.chat_id
    me = await user_client.get_me()
    my_id = me.id
    await event.delete()
    sm = await user_client.send_message(chat_id, "🔄 جاري المسح…")
    left, skipped = 0, 0
    current = None if event.is_private else chat_id
    async for dialog in user_client.iter_dialogs():
        entity = dialog.entity
        if not isinstance(entity, (types.Chat, types.Channel)):
            continue
        eid = utils.get_peer_id(entity)
        if current and eid == current:
            skipped += 1
            continue
        try:
            if isinstance(entity, types.Channel):
                part = await user_client(functions.channels.GetParticipantRequest(entity, my_id))
                p = part.participant
                if isinstance(p, (types.ChannelParticipantCreator, types.ChannelParticipantAdmin)):
                    skipped += 1
                    continue
                await user_client(functions.channels.LeaveChannelRequest(entity))
            else:
                full = await user_client(functions.messages.GetFullChatRequest(entity.id))
                is_admin = False
                for pu in full.full_chat.participants.participants:
                    if getattr(pu, "user_id", None) == my_id:
                        if isinstance(pu, (types.ChatParticipantCreator, types.ChatParticipantAdmin)):
                            is_admin = True
                        break
                if is_admin:
                    skipped += 1
                    continue
                await user_client(functions.messages.DeleteChatUserRequest(
                    chat_id=entity.id, user_id="me"))
            left += 1
            await asyncio.sleep(0.5)
        except Exception:
            skipped += 1
    await sm.edit(
        f"✅ **تم الخروج**\n\n• خرج من: **{left}**\n• تجاوز (مشرف/حالي): **{skipped}**"
    )


# ── تقليد ──
async def _cmd_imitate(event, txt: str):
    chat_id = event.chat_id
    parts = txt.split()
    mode = parts[1] if len(parts) > 1 and parts[1] in ("صور", "بايو", "اسم") else "الكل"

    target_id = await _resolve_target(event, txt)
    await event.delete()
    if not target_id:
        return await _ub_reply(event, "⚠️ رُدّ على مستخدم أو اكتب @يوزر / ايدي.")

    sm = await user_client.send_message(chat_id, f"⬇️ جاري تقليد الحساب…\n[{_bar(0)}] 0%")

    # شريط تقدم وهمي
    async def fake_progress():
        for i in range(1, 21):
            await asyncio.sleep(0.3)
            pct = i * 5
            try:
                await sm.edit(f"⬇️ جاري تقليد الحساب…\n{_fake_bar(i)}")
            except Exception:
                pass

    progress_task = asyncio.create_task(fake_progress())

    try:
        target = await user_client.get_entity(target_id)
        me = await user_client.get_me()

        first = getattr(target, "first_name", "") or ""
        last  = getattr(target, "last_name", "") or ""
        about = ""
        photo_path = None

        if mode in ("الكل", "بايو", "اسم"):
            full = await user_client(functions.users.GetFullUserRequest(target))
            about = full.full_user.about or ""

        if mode in ("الكل", "صور"):
            photos = await user_client.get_profile_photos(target)
            if photos:
                photo_path = os.path.join(MEDIA_DIR, f"imitate_{target_id}.jpg")
                await user_client.download_profile_photo(target, photo_path)

        await progress_task

        # تطبيق التقليد
        update_kwargs = {}
        if mode in ("الكل", "اسم"):
            update_kwargs["first_name"] = first
            update_kwargs["last_name"] = last
        if mode in ("الكل", "بايو"):
            update_kwargs["about"] = about[:70] if about else ""

        if update_kwargs:
            await user_client(functions.account.UpdateProfileRequest(**update_kwargs))

        if photo_path and os.path.exists(photo_path) and mode in ("الكل", "صور"):
            with open(photo_path, "rb") as f:
                await user_client(functions.photos.UploadProfilePhotoRequest(
                    file=await user_client.upload_file(f)
                ))
            os.remove(photo_path)

        what = {"الكل": "الاسم والبايو والصورة", "صور": "الصور",
                "بايو": "البايو", "اسم": "الاسم"}[mode]
        await sm.edit(f"✅ تم تقليد {what} بنجاح!")
        await asyncio.sleep(5)
        await sm.delete()

    except Exception as e:
        progress_task.cancel()
        await sm.edit(f"❌ فشل التقليد: {e}")
        await asyncio.sleep(5)
        try:
            await sm.delete()
        except Exception:
            pass


# ── كتم ──
async def _cmd_mute(event, txt: str):
    chat_id = event.chat_id
    target_id = await _resolve_target(event, txt)
    await event.delete()
    if not target_id:
        return await _ub_reply(event, "⚠️ حدد المستخدم (رداً أو @يوزر أو ايدي).")
    try:
        await user_client(functions.channels.EditBannedRequest(
            channel=chat_id,
            participant=target_id,
            banned_rights=types.ChatBannedRights(
                until_date=None,
                send_messages=True,
                send_media=True,
                send_stickers=True,
                send_gifs=True,
                send_games=True,
                send_inline=True,
                embed_links=True,
            )
        ))
        await _ub_reply(event, "🔇 تم كتمه بنجاح.", delay=4)
    except ChatAdminRequiredError:
        await _ub_reply(event, "❌ أنت لست مشرفاً.")
    except Exception as e:
        await _ub_reply(event, f"❌ فشل: {e}")


# ── فك كتم ──
async def _cmd_unmute(event, txt: str):
    chat_id = event.chat_id
    target_id = await _resolve_target(event, txt)
    await event.delete()
    if not target_id:
        return await _ub_reply(event, "⚠️ حدد المستخدم.")
    try:
        await user_client(functions.channels.EditBannedRequest(
            channel=chat_id,
            participant=target_id,
            banned_rights=types.ChatBannedRights(until_date=None),
        ))
        await _ub_reply(event, "🔊 تم فك الكتم.", delay=4)
    except ChatAdminRequiredError:
        await _ub_reply(event, "❌ أنت لست مشرفاً.")
    except Exception as e:
        await _ub_reply(event, f"❌ فشل: {e}")


# ── حظر ──
async def _cmd_ban(event, txt: str):
    chat_id = event.chat_id
    target_id = await _resolve_target(event, txt)
    await event.delete()
    if not target_id:
        return await _ub_reply(event, "⚠️ حدد المستخدم.")
    try:
        await user_client(functions.channels.EditBannedRequest(
            channel=chat_id,
            participant=target_id,
            banned_rights=types.ChatBannedRights(
                until_date=None, view_messages=True,
            )
        ))
        await _ub_reply(event, "🚫 تم حظره.", delay=4)
    except ChatAdminRequiredError:
        await _ub_reply(event, "❌ أنت لست مشرفاً.")
    except Exception as e:
        await _ub_reply(event, f"❌ فشل: {e}")


# ── فك حظر ──
async def _cmd_unban(event, txt: str):
    chat_id = event.chat_id
    target_id = await _resolve_target(event, txt)
    await event.delete()
    if not target_id:
        return await _ub_reply(event, "⚠️ حدد المستخدم.")
    try:
        await user_client(functions.channels.EditBannedRequest(
            channel=chat_id,
            participant=target_id,
            banned_rights=types.ChatBannedRights(until_date=None),
        ))
        await _ub_reply(event, "✅ تم فك الحظر.", delay=4)
    except ChatAdminRequiredError:
        await _ub_reply(event, "❌ أنت لست مشرفاً.")
    except Exception as e:
        await _ub_reply(event, f"❌ فشل: {e}")


# ── معلوماته ──
async def _cmd_info(event, txt: str):
    chat_id = event.chat_id
    target_id = await _resolve_target(event, txt)
    await event.delete()
    if not target_id:
        return await _ub_reply(event, "⚠️ رُدّ على مستخدم أو اكتب @يوزر.")

    sm = await user_client.send_message(chat_id, "⏳ جاري جلب المعلومات…")
    try:
        target = await user_client.get_entity(target_id)
        full = await user_client(functions.users.GetFullUserRequest(target))
        fu = full.full_user

        name = (f"{target.first_name or ''} {target.last_name or ''}").strip()
        username = f"@{target.username}" if getattr(target, "username", None) else "لا يوجد"
        about = fu.about or "لا يوجد"
        reg_est = _estimate_reg_date(target.id)
        phone = getattr(target, "phone", None) or "مخفي"

        info_txt = (
            f"👤 **معلومات الحساب**\n\n"
            f"• **الاسم:** {name}\n"
            f"• **اليوزر:** {username}\n"
            f"• **الـ ID:** `{target.id}`\n"
            f"• **رقم الهاتف:** `{phone}`\n"
            f"• **البايو:** {about}\n"
            f"• **تاريخ التسجيل (تقريبي):** {reg_est}\n"
        )

        # محاولة جلب الصورة
        photo_path = None
        try:
            photos = await user_client.get_profile_photos(target, limit=1)
            if photos:
                photo_path = os.path.join(MEDIA_DIR, f"info_{target.id}.jpg")
                await user_client.download_media(photos[0], photo_path)
        except Exception:
            info_txt += "\n_لا أستطيع جلب الصورة (الصور مخفية أو مقيّدة)_"

        await sm.delete()
        if photo_path and os.path.exists(photo_path):
            await user_client.send_file(chat_id, photo_path, caption=info_txt)
            os.remove(photo_path)
        else:
            await user_client.send_message(chat_id, info_txt)

    except Exception as e:
        await sm.edit(f"❌ فشل: {e}")


# ── تحويل (forward) ──
async def _cmd_forward_media(event):
    replied = await event.get_reply_message()
    await event.delete()
    if not replied:
        return await _ub_reply(event, "⚠️ رُدّ على رسالة/ميديا لتحويلها.")
    try:
        await user_client.forward_messages(event.chat_id, replied)
    except Exception as e:
        await _ub_reply(event, f"❌ فشل التحويل: {e}")


# ── مسح [عدد] ──
async def _cmd_delete_msgs(event, txt: str):
    chat_id = event.chat_id
    parts = txt.split()
    try:
        n = int(parts[1])
    except (IndexError, ValueError):
        await event.delete()
        return await _ub_reply(event, "⚠️ اكتب: مسح [عدد]  مثال: مسح 5")

    me = await user_client.get_me()
    to_delete = [event.id]  # نحذف رسالة الأمر نفسها
    count = 0
    async for msg in user_client.iter_messages(chat_id, limit=200):
        if len(to_delete) - 1 >= n:
            break
        if msg.out and msg.id != event.id:
            to_delete.append(msg.id)
            count += 1

    try:
        await user_client.delete_messages(chat_id, to_delete)
    except Exception as e:
        await _ub_reply(event, f"❌ فشل المسح: {e}")


# ── رفع مطي ──
async def _cmd_raise_camel(event):
    replied = await event.get_reply_message()
    await event.delete()
    name = ""
    if replied:
        sender = await replied.get_sender()
        name = (getattr(sender, "first_name", "") or "المستخدم") if sender else "المستخدم"
    await user_client.send_message(
        event.chat_id,
        f"🐪 تم رفع {name} إلى مطي أصيل بامتياز! 🐪😂\n"
        f"مبروك التكريم العالي يا صاحب المطي العتيد! 🏆🐫"
    )


# ── رفع قندرة ──
async def _cmd_raise_shoe(event):
    replied = await event.get_reply_message()
    await event.delete()
    name = ""
    if replied:
        sender = await replied.get_sender()
        name = (getattr(sender, "first_name", "") or "المستخدم") if sender else "المستخدم"
    await user_client.send_message(
        event.chat_id,
        f"👟 تم رفع {name} إلى رتبة قندرة عتيگة من الدرجة الأولى! 😂👟\n"
        f"مبروك هذا الشرف الرفيع يا صاحب القندرة المحترمة! 🏅"
    )


# ── يوت [نص] ──
async def _cmd_yoot(event, txt: str):
    chat_id = event.chat_id
    yoot_group = cfg.get("yoot_group")
    if not yoot_group:
        await event.delete()
        return await _ub_reply(event,
            "⚠️ لم يُعيَّن كروب البصمات.\nافتح البوت → تعيين كروب البصمات.")

    query = txt[len("يوت "):].strip()
    if not query:
        await event.delete()
        return await _ub_reply(event, "⚠️ اكتب: يوت [اسم الأغنية]")

    await event.delete()
    sm = await user_client.send_message(chat_id, f"🎵 جاري البحث عن: {query}…")

    try:
        chat = await _get_chat(user_client, yoot_group)
        if not chat:
            return await sm.edit("❌ لم أجد كروب البصمات.")
        sent = await user_client.send_message(chat, f"يوت {query}")
        yoot_pending[sent.id] = (chat_id, event)
        # انتظر 30 ثانية كحد أقصى للرد
        await asyncio.sleep(30)
        if sent.id in yoot_pending:
            yoot_pending.pop(sent.id, None)
            await sm.edit("❌ لم يرد @W60yBot في الوقت المحدد.")
        else:
            await sm.delete()
    except Exception as e:
        await sm.edit(f"❌ خطأ: {e}")


# ── اريد [نص] → يوت في كروب البصمات ──
async def _cmd_arid(event, txt: str):
    """
    عندما يكتب المستخدم "اريد [شيء]" في أي مجموعة:
    - يُرسل "يوت [شيء]" إلى كروب البصمات (u33u0)
    - ينتظر رد @W60yBot بالبصمة الصوتية
    - يُحوِّل البصمة إلى المجموعة الأصلية بدون روابط أو يوزر إضافي
    """
    src_chat_id = event.chat_id
    yoot_group = cfg.get("yoot_group", "https://t.me/u33u0")

    query = txt[len("اريد "):].strip()
    if not query:
        return  # لا نفعل شيئاً إذا كانت الرسالة "اريد" فقط

    # حذف رسالة المستخدم الأصلية
    await event.delete()

    sm = await user_client.send_message(src_chat_id, "انتظر قليلا ⏳")

    try:
        chat = await _get_chat(user_client, yoot_group)
        if not chat:
            return await sm.edit(
                f"❌ لم أجد كروب البصمات.\n"
                f"تأكد من تعيينه في البوت (الحالي: `{yoot_group}`)."
            )

        # إرسال "يوت [query]" إلى كروب البصمات
        sent = await user_client.send_message(chat, f"يوت {query}")

        # تسجيل الانتظار
        yoot_pending[sent.id] = (src_chat_id, sm)

        # انتظر حتى 40 ثانية للرد
        for _ in range(40):
            await asyncio.sleep(1)
            if sent.id not in yoot_pending:
                # تم استلام الرد وتحويله في المستمع
                try:
                    await sm.delete()
                except Exception:
                    pass
                return

        # انتهى الوقت بدون رد
        yoot_pending.pop(sent.id, None)
        await sm.edit("❌ لم يستجب @W60yBot. جرب مرة أخرى.")

    except Exception as e:
        try:
            await sm.edit(f"❌ خطأ: {e}")
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────
# التشغيل الرئيسي
# ─────────────────────────────────────────────────────────────
async def main():
    global user_client, call_manager

    await bot.start(bot_token=BOT_TOKEN)
    logger.info("✅ البوت يعمل")

    user_client = _make_user_client()
    if user_client:
        try:
            await user_client.start()
            me = await user_client.get_me()
            logger.info(f"✅ اليوزربوت: {me.first_name} ({me.id})")
            _register_ub_handlers()
            if CALLS_AVAILABLE:
                call_manager = PyTgCalls(user_client)
                await call_manager.start()
                logger.info("✅ مدير الاستيج جاهز")
        except Exception as e:
            logger.error(f"فشل تشغيل اليوزربوت: {e}")
            user_client = None
    else:
        logger.warning("⚠️ لا توجد جلسة يوزربوت — سجّل الجلسة من البوت")

    tasks = [bot.run_until_disconnected()]
    if user_client:
        tasks.append(user_client.run_until_disconnected())
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
