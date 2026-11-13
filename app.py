"""
GiftUp — сервер: вход через Telegram, база SQLite, честный расчёт буста на сервере,
приём и выдача подарков через бота, подключённого к Business-аккаунту (аккаунт-приёмник).

Запуск:  pip install fastapi uvicorn httpx
         BOT_TOKEN=... HOUSE_IDS=твой_tg_id uvicorn app:app --host 0.0.0.0 --port 8000
"""
import base64, html, os, re, json, time, hmac, hashlib, secrets, sqlite3, threading, datetime
from urllib.parse import parse_qsl
import httpx
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, HTMLResponse, Response

TOKEN = os.environ["BOT_TOKEN"]                       # токен бота (он же владелец мини-аппа)
ADMIN_KEY = os.environ.get("ADMIN_KEY", "")           # ключ для /api/admin/credit
HOUSE_IDS = {int(x) for x in os.environ.get("HOUSE_IDS", "").split(",") if x.strip()}  # чьи подарки идут в «склад»
TRANSFER_STARS = int(os.environ.get("TRANSFER_STARS", "0"))  # звёзды за передачу (если Telegram требует)
DB = os.environ.get("DB", "giftup.db")
HERE = os.path.dirname(os.path.abspath(__file__))
CAT = json.load(open(os.path.join(HERE, "catalog.json"), encoding="utf-8"))  # индекс = id типа на фронте
EDGE, MAXCH = 0.95, 75.0
STARS_PER_GRAM = float(os.environ.get("STARS_PER_GRAM", "125"))   # курс пополнения звёздами: сколько звёзд за 1 GRAM
STARS_MIN, STARS_MAX = 15, 10000
DEPOSIT_ADDRESS = os.environ.get("DEPOSIT_ADDRESS", "")        # адрес кошелька, куда игроки шлют GRAM (отдельный кошелёк!)
TONCENTER_KEY = os.environ.get("TONCENTER_KEY", "")            # необязательно: ключ toncenter.com (снимает лимит 1 запрос/сек)
MIN_DEPOSIT = float(os.environ.get("MIN_DEPOSIT", "0.1"))      # минимальное пополнение, GRAM
PUBLIC_URL = os.environ.get("PUBLIC_URL", "").rstrip("/")      # https://твой-домен (для манифеста TON Connect)
BUSINESS_OWNER_ID = os.environ.get("BUSINESS_OWNER_ID", "").strip()   # id аккаунта-склада: подключения от других аккаунтов игнорируются
WITHDRAW_REGULAR = os.environ.get("WITHDRAW_REGULAR") == "1"   # =1 — разрешить вывод обычных подарков за звёзды (за счёт звёзд бота)
WITHDRAW_FEE = float(os.environ.get("WITHDRAW_FEE", "0.4"))   # цена вывода подарка, GRAM
ADMIN_TG = [int(x) for x in os.environ.get("ADMIN_TG", "5996551936,8427357071").split(",") if x.strip()]   # кому приходят заявки на ручной вывод
MANUAL_WITHDRAW = os.environ.get("MANUAL_WITHDRAW") == "1"      # =1 — все выводы NFT только вручную (без попытки передать через бизнес-аккаунт)
REF_PCT = float(os.environ.get("REF_PCT", "0.05"))                # доля от пополнений рефералов, копится у пригласившего
APP_LINK = os.environ.get("APP_LINK", "").rstrip("/")            # необязательно: https://t.me/бот/короткое_имя (если у мини-приложения есть своё имя в BotFather)
TRUST_PROXY = os.environ.get("TRUST_PROXY") == "1"              # =1, если сервер стоит за Caddy/nginx
IP_LIMIT = int(os.environ.get("IP_LIMIT", "90"))                # запросов в минуту с одного IP

# ---------- защита от спама (лимит запросов) ----------
_hits, _lock = {}, threading.Lock()

def hit(key, n, per):
    """Скользящее окно: не больше n запросов за per секунд на ключ. True — можно, False — слишком часто."""
    now = time.time()
    with _lock:
        q = [t for t in _hits.get(key, []) if now - t < per]
        if len(q) >= n:
            _hits[key] = q
            return False
        q.append(now); _hits[key] = q
        if len(_hits) > 20000:                                   # чистим старые ключи
            for k in [k for k, v in _hits.items() if not v or now - v[-1] > 3600]: del _hits[k]
        return True

def too_fast(uid, name, n, per):
    return None if hit(f"{name}:{uid}", n, per) else {"ok": False, "error": "Слишком часто, подожди немного"}

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=[os.environ.get("ALLOW_ORIGIN", "*")],
                   allow_methods=["*"], allow_headers=["*"])

@app.middleware("http")
async def ip_limit(request: Request, call_next):
    ip = (request.headers.get("x-forwarded-for", "").split(",")[0].strip() if TRUST_PROXY else "") \
         or (request.client.host if request.client else "?")
    if request.url.path.startswith("/api/") and not hit("ip:" + ip, IP_LIMIT, 60):
        return JSONResponse({"ok": False, "error": "Слишком много запросов, подожди минуту"}, status_code=429)
    return await call_next(request)

# ---------- база ----------
def db():
    c = sqlite3.connect(DB, timeout=30, isolation_level=None)
    c.row_factory = sqlite3.Row
    c.execute("pragma journal_mode=wal")
    return c

def init_db():
    c = db()
    c.executescript("""
    create table if not exists users(id integer primary key, name text, bal real default 0,
      boosts integer default 0, best real default 0, free_day text default '', seed text, nonce integer default 0);
    create table if not exists gifts(id integer primary key autoincrement, type integer, number integer,
      ogid text unique, owner integer, status text);   -- held | house | out | gone | unknown
    create table if not exists wd(id integer primary key autoincrement, gift integer, user integer,
      status text default 'pending', err text, fee real default 0);
    create table if not exists star_pay(charge text primary key, user integer, type integer, stars integer, ok integer default 1);
    create table if not exists deposits(hash text primary key, user integer, amount real, comment text, src text, credited integer default 0);
    create table if not exists hist(id integer primary key autoincrement, user integer, ts integer, want integer, no integer, win integer, ch real, price real);
    create table if not exists meta(k text primary key, v text);
    create table if not exists custom(id integer primary key autoincrement, key text, model text, name text, price real);
    create table if not exists topup(id integer primary key autoincrement, user integer, ts integer, amount real, kind text, ref text);""")
    try: c.execute("alter table wd add column fee real default 0")   # для старых баз
    except Exception: pass
    try: c.execute("alter table custom add column psrc integer default 0")   # 1 — цена подтверждена (вручную или по ценности Telegram)
    except Exception: pass
    try: c.execute("alter table custom add column fixed integer default 0")   # цена задана вручную — автосинхронизация её не трогает
    except Exception: pass
    try: c.execute("alter table gifts add column img text default ''")   # картинка конкретного NFT
    except Exception: pass
    for ddl in ("alter table users add column ref_by integer default 0", "alter table users add column ref_earn real default 0"):
        try: c.execute(ddl)
        except Exception: pass
    c.close()

# ---------- NFT, которых нет в каталоге: заводятся автоматически (название, цена, картинка) ----------
DEFAULT_NFT_PRICE = float(os.environ.get("DEFAULT_NFT_PRICE", "100"))    # цена нового NFT, если такого названия ещё не было (потом поменять: /api/admin/price)
IMG_DIR = os.path.join(HERE, "nftimg"); os.makedirs(IMG_DIR, exist_ok=True)
_cat_lock = threading.Lock()

def load_custom():
    c = db()
    try:
        for r in c.execute("select * from custom order by id"):
            CAT.append({"id": len(CAT), "key": r["key"], "model": r["model"] or "", "name": r["name"], "price": r["price"], "custom": r["id"], "fixed": bool(r["fixed"]), "priced": bool(r["psrc"] or r["fixed"])})
    finally: c.close()

BLACK_SFX = " · Black"
BLACK_PRICE = float(os.environ.get("BLACK_PRICE", "27"))    # минимальная цена NFT с чёрным фоном, GRAM

def gift_model(g):
    """Имя модели; у NFT с чёрным фоном добавляется суффикс — это отдельный тип с повышенной ценой."""
    m = ((g.get("model") or {}).get("name") or "").strip()
    if ((g.get("backdrop") or {}).get("name") or "").strip().lower() == "black": m += BLACK_SFX
    return m

def ensure_type(base, model):
    """Находит или создаёт тип для NFT, которого нет в каталоге. Возвращает индекс в CAT."""
    base = (base or "").strip(); model = (model or "").strip()
    if not base: return -1
    with _cat_lock:
        for i, x in enumerate(CAT):
            if x.get("custom") and (x["key"] or "").lower() == base.lower() and (x.get("model") or "").lower() == model.lower(): return i
        known = next((x["price"] for x in CAT if (x.get("key") or "").lower() == base.lower() and not x.get("nw") and not (x.get("model") or "").endswith(BLACK_SFX)), None)
        price = known if known is not None else DEFAULT_NFT_PRICE
        if model.endswith(BLACK_SFX): price = max(price if known is not None else 0, BLACK_PRICE)
        c = db()
        try: cid = c.execute("insert into custom(key,model,name,price) values(?,?,?,?)", (base, model, base, price)).lastrowid
        finally: c.close()
        CAT.append({"id": len(CAT), "key": base, "model": model, "name": base, "price": price, "custom": cid, "priced": known is not None or model.endswith(BLACK_SFX)})
        print(f"[{stamp()}] ➕ новый тип NFT: {base} / {model}, цена {price} GRAM (поменять: /api/admin/price)", flush=True)
        return len(CAT) - 1

def is_priced(t): return t >= 0 and (not CAT[t].get("custom") or CAT[t].get("priced"))

def nft_slug(base, number): return re.sub(r"[^A-Za-z0-9]", "", base or "") + "-" + str(number)

def save_nft_image(slug):
    """Скачивает картинку конкретного NFT и кладёт к себе на сервер. Возвращает путь вида /nftimg/<файл> или ''."""
    fn = re.sub(r"[^A-Za-z0-9_-]", "", slug) + ".jpg"
    path = os.path.join(IMG_DIR, fn)
    if os.path.exists(path) and os.path.getsize(path) > 500: return "/nftimg/" + fn
    urls = [f"https://nft.fragment.com/gift/{slug.lower()}.large.jpg", f"https://nft.fragment.com/gift/{slug.lower()}.medium.jpg"]
    try:
        h = httpx.get(f"https://t.me/nft/{slug}", follow_redirects=True, timeout=12, headers={"User-Agent": "Mozilla/5.0"}).text
        m = re.search(r'<meta[^>]+property="og:image"[^>]+content="([^"]*)"', h) or re.search(r'<meta[^>]+content="([^"]*)"[^>]+property="og:image"', h)
        if m:
            u = html.unescape(m.group(1)); host = re.sub(r"^https?://([^/]+).*", r"\1", u)
            if u.startswith("https://") and any(host == d or host.endswith("." + d) for d in _IMG_OK): urls.append(u)
    except Exception as e: print("nft page:", e, flush=True)
    for u in urls:
        try:
            r = httpx.get(u, follow_redirects=True, timeout=15)
            if r.status_code == 200 and 500 < len(r.content) < 2_000_000 and r.headers.get("content-type", "").startswith("image"):
                open(path, "wb").write(r.content); return "/nftimg/" + fn
        except Exception as e: print("nft img:", u, e, flush=True)
    return ""

# ---------- цена NFT по «Ценности» из Telegram (вход под аккаунтом, только чтение) ----------
TG_API_ID = os.environ.get("TG_API_ID", ""); TG_API_HASH = os.environ.get("TG_API_HASH", ""); TG_SESSION = os.environ.get("TG_SESSION", "")
_ub = {"loop": None, "cl": None}
_rate = {"t": 0, "r": {}}

def ub_start():
    """Поднимает клиент Telethon в отдельном потоке. Нужны TG_API_ID, TG_API_HASH, TG_SESSION (создаются скриптом tg_login.py)."""
    if not (TG_API_ID and TG_API_HASH and TG_SESSION): return
    def run():
        try:
            import asyncio
            from telethon import TelegramClient
            from telethon.sessions import StringSession
            loop = asyncio.new_event_loop(); asyncio.set_event_loop(loop)
            cl = TelegramClient(StringSession(TG_SESSION), int(TG_API_ID), TG_API_HASH)
            loop.run_until_complete(cl.connect())
            if not loop.run_until_complete(cl.is_user_authorized()):
                print("цены по ценности: сессия не авторизована, запусти tg_login.py заново", flush=True); return
            _ub["loop"], _ub["cl"] = loop, cl
            print("цены по ценности: аккаунт подключён", flush=True)
            loop.run_forever()
        except Exception as e: print("цены по ценности: не запустилось:", e, flush=True)
    threading.Thread(target=run, daemon=True).start()

def ton_rate(cur):
    """Сколько единиц валюты cur в 1 TON."""
    cur = (cur or "").lower()
    if cur == "ton": return 1.0
    if time.time() - _rate["t"] > 600:
        r = httpx.get("https://api.coingecko.com/api/v3/simple/price", params={"ids": "the-open-network", "vs_currencies": "usd,eur,rub"}, timeout=10).json()["the-open-network"]
        _rate["t"], _rate["r"] = time.time(), r
    return float(_rate["r"][cur])

def tg_value_ton(slug):
    """«Ценность» подарка из Telegram, пересчитанная в TON (=GRAM в приложении). None, если не вышло."""
    if not _ub["cl"]: return None
    import asyncio
    from telethon.tl.functions.payments import GetUniqueStarGiftValueInfoRequest as Req
    r = asyncio.run_coroutine_threadsafe(_ub["cl"](Req(slug=slug)), _ub["loop"]).result(30)
    cur = (r.currency or "").upper(); v = float(r.value)
    ton = v / 1e9 if cur == "TON" else (v / 100) / ton_rate(cur)
    print(f"[{stamp()}] ценность {slug}: {v} {cur} → {ton:.2f} TON", flush=True)
    return round(ton, 2) if ton > 0 else None

def tg_floor_ton(slug):
    """Самая низкая цена этого NFT (той же модели) на маркете внутри Telegram, в TON. None, если лотов нет или не вышло."""
    if not _ub["cl"]: return None
    import asyncio
    from telethon.tl.functions.payments import GetUniqueStarGiftRequest, GetResaleStarGiftsRequest
    cl, loop = _ub["cl"], _ub["loop"]
    run = lambda coro: asyncio.run_coroutine_threadsafe(coro, loop).result(30)
    u = run(cl(GetUniqueStarGiftRequest(slug=slug))).gift
    def listings(**kw):
        r = run(cl(GetResaleStarGiftsRequest(gift_id=u.gift_id, offset="", limit=10, sort_by_price=True, **kw)))
        out = []
        for g in r.gifts:
            for a in (getattr(g, "resell_amount", None) or []):
                if "Ton" in type(a).__name__: out.append(a.amount / 1e9)
                else: out.append((a.amount + getattr(a, "nanos", 0) / 1e9) * 0.013 / ton_rate("usd"))   # звёзды → TON через ~0.013 $ за звезду
        return out
    prices = []
    try:
        from telethon.tl.types import StarGiftAttributeModel, StarGiftAttributeIdModel
        m = next(a for a in u.attributes if isinstance(a, StarGiftAttributeModel))
        prices = listings(attributes=[StarGiftAttributeIdModel(document_id=m.document.id)])
    except Exception as e: print("маркет по модели:", e, flush=True)
    if not prices: prices = listings()                        # нет лотов этой модели — берём всю коллекцию
    v = min(prices) if prices else None
    if v: print(f"[{stamp()}] маркет Telegram: {slug} от {v:.2f} TON ({len(prices)} лотов)", flush=True)
    return round(v, 2) if v else None

GAKEY = os.environ.get("GAKEY", "").strip()                # ключ Gift Asset API (giftasset.gifts)
_ga = {"t": 0.0, "cache": {}, "warn": 0}
_ga_lock = threading.Lock()

def ga_floor(name, model):
    bd = bool(model) and str(model).endswith(BLACK_SFX)
    v = _ga_floor_raw(name, (str(model)[:-len(BLACK_SFX)] if bd else model))
    return max(v or 0, BLACK_PRICE) if bd else v

def _ga_floor_raw(name, model):
    """Цена модели (минимум по маркетам) из Gift Asset API, TON. Нет данных -> None. Результат по коллекции кэшируется на 15 минут."""
    if not GAKEY or not name: return None
    key = name.lower()
    with _ga_lock:
        hit_ = _ga["cache"].get(key)
        if hit_ and time.time() - hit_[0] < 900: d = hit_[1]
        else:
            wait = 1.3 - (time.time() - _ga["t"])
            if wait > 0: time.sleep(wait)
            r = httpx.get("https://giftasset.gifts/api/v1/gifts/get" + chr(95) + "unique" + chr(95) + "gifts" + chr(95) + "price" + chr(95) + "list",
                          params={"collection" + chr(95) + "name": name}, headers={"X-Api-Key": GAKEY}, timeout=20)
            _ga["t"] = time.time()
            if r.status_code != 200:
                if time.time() - _ga["warn"] > 600: _ga["warn"] = time.time(); print("giftasset:", r.status_code, r.text[:160], flush=True)
                return None
            d = r.json(); _ga["cache"][key] = (time.time(), d)
    def best(o):
        v = [float(x) for k, x in (o or {}).items() if k in ("getgems", "mrkt", "portals", "tonnel") and isinstance(x, (int, float)) and x > 0]
        return min(v) if v else None
    mp = (d.get("models" + chr(95) + "prices") or {})
    col = next((v for k, v in mp.items() if k.lower() == key), None) or {}
    if model:
        m = next((v for k, v in (col.get("models") or {}).items() if k.lower() == str(model).lower()), None)
        v = best(m)
        if v: return round(v, 2)
    cf = (d.get("collection" + chr(95) + "floors") or {})
    v = best(next((x for k, x in cf.items() if k.lower() == key), None))
    return round(v, 2) if v else None

def auto_price(t, base, number):
    x = CAT[t] if 0 <= t < len(CAT) else None
    if not x or not x.get("custom") or x.get("fixed") or not (_ub["cl"] or GAKEY): return
    try:
        slug = nft_slug(base, number); v = None
        try: v = ga_floor(x["name"], x.get("model"))            # 0) Gift Asset API (цена модели на маркетах)
        except Exception as e: print("giftasset:", slug, e, flush=True)
        if v is None and _ub["cl"]:
            try: v = tg_floor_ton(slug)                         # 1) нижняя цена на маркете Telegram
            except Exception as e: print("маркет:", slug, e, flush=True)
            if v is None: v = tg_value_ton(slug)                # 2) «Ценность» из карточки подарка
        if str(x.get("model") or "").endswith(BLACK_SFX) and v is not None: v = max(v, BLACK_PRICE)
        if v is None: return
        with _cat_lock: x["price"] = v; x["vp"] = True; x["priced"] = True
        c = db()
        try: c.execute("update custom set price=?, psrc=1 where id=? and fixed=0", (v, x["custom"]))
        finally: c.close()
        print(f"[{stamp()}] 💰 цена {x['name']} / {x.get('model')} = {v} GRAM (по ценности Telegram)", flush=True)
    except Exception as e: print("auto_price:", base, number, e, flush=True)

def attach_image(ogid, base, number, t=-1):
    if t >= 0: auto_price(t, base, number)
    try:
        p = save_nft_image(nft_slug(base, number))
        if p:
            c = db()
            try: c.execute("update gifts set img=? where ogid=?", (p, ogid))
            finally: c.close()
        else: print(f"[{stamp()}] картинка для {base} #{number} не найдена", flush=True)
    except Exception as e: print("attach_image:", e, flush=True)

@app.get("/nftimg/{fn}")
def nft_img(fn: str):
    fn = re.sub(r"[^A-Za-z0-9_.-]", "", fn)
    p = os.path.join(IMG_DIR, fn)
    if not fn.endswith(".jpg") or not os.path.exists(p): raise HTTPException(404)
    return FileResponse(p, media_type="image/jpeg", headers={"Cache-Control": "public, max-age=86400"})

def meta_get(k):
    c = db(); r = c.execute("select v from meta where k=?", (k,)).fetchone(); c.close()
    return r["v"] if r else None

def meta_set(k, v):
    c = db(); c.execute("insert into meta(k,v) values(?,?) on conflict(k) do update set v=excluded.v", (k, v)); c.close()

def get_user(c, uid, name="", sp=""):
    r = c.execute("select * from users where id=?", (uid,)).fetchone()
    if not r:
        c.execute("insert into users(id,name,seed) values(?,?,?)", (uid, name, secrets.token_hex(32)))
        r = c.execute("select * from users where id=?", (uid,)).fetchone()
        ref = 0
        if sp.startswith("ref_") and sp[4:].isdigit(): ref = int(sp[4:])
        if ref and ref != uid and c.execute("select 1 from users where id=?", (ref,)).fetchone():
            c.execute("update users set ref_by=? where id=?", (ref, uid))
            who = name or str(uid)
            threading.Thread(target=notify, args=(ref, f"👥 По вашей реферальной ссылке зашёл {who}. Он добавлен в ваши рефералы."), daemon=True).start()
    return r

def ref_bonus(c, uid, amount):
    """Начисляет пригласившему долю от пополнения (копится в ref_earn). Возвращает (кому, сколько) или None."""
    r = c.execute("select ref_by from users where id=?", (uid,)).fetchone()
    if not r or not r["ref_by"]: return None
    b = round(amount * REF_PCT, 4)
    if b <= 0: return None
    c.execute("update users set ref_earn=ref_earn+? where id=?", (b, r["ref_by"]))
    return (r["ref_by"], b)

_bot = {"u": ""}
def ref_link(uid):
    if APP_LINK: return f"{APP_LINK}?startapp=ref_{uid}"
    if not _bot["u"]:
        try: _bot["u"] = tg("getMe").get("username", "")
        except Exception: pass
    return f"https://t.me/{_bot['u'] or 'GiftUpNowbot'}?startapp=ref_{uid}"

def sha(s): return hashlib.sha256(s.encode()).hexdigest()

_own = {"t": 0, "u": ""}
def owner_username():
    """@username владельца (куда ведёт кнопка «Отправить»): из OWNER_USERNAME или через getChat по HOUSE_IDS / бизнес-аккаунту."""
    env = os.environ.get("OWNER_USERNAME", "").lstrip("@")
    if env: return env
    if time.time() - _own["t"] < 3600: return _own["u"]
    _own["t"] = time.time()
    oid = next(iter(sorted(HOUSE_IDS)), None) or meta_get("bc_owner")
    if oid:
        try: _own["u"] = tg("getChat", chat_id=int(oid)).get("username", "") or ""
        except Exception as e: print("owner:", e)
    return _own["u"]

_seen = {}
def online_count():
    """Сколько человек открыли приложение за последние 45 секунд."""
    now = time.time()
    with _lock:
        for k in [k for k, t in _seen.items() if now - t > 45]: del _seen[k]
        return max(1, len(_seen))

def state(c, uid):
    with _lock: _seen[uid] = time.time()
    u = c.execute("select * from users where id=?", (uid,)).fetchone()
    inv = [{"id": r["id"], "t": r["type"], "no": r["number"], "im": r["img"] or "", "pr": 1 if is_priced(r["type"]) else 0} for r in
           c.execute("select * from gifts where owner=? and status='held' and type>=0 order by id desc", (uid,))]
    stock = {r["type"]: r["n"] for r in c.execute(
        "select type, count(*) n from gifts where status='house' and type>=0 group by type")}
    si = [{"id": r["id"], "t": r["type"], "no": r["number"], "im": r["img"] or "", "pr": 1 if is_priced(r["type"]) else 0} for r in
          c.execute("select id,type,number,img from gifts where status='house' and type>=0 order by id desc limit 300")]
    hist = [{"t": r["want"], "no": r["no"], "w": r["win"], "ch": r["ch"], "ts": r["ts"], "im": r["img"] or ""} for r in
            c.execute("select h.*, (select g.img from gifts g where g.type=h.want and g.number=h.no and h.no>0 order by g.id desc limit 1) as img "
                      "from hist h where h.user=? order by h.id desc limit 150", (uid,))]
    bg = c.execute("select want t, no, ch, price pr from hist where user=? and win=1 order by price desc, id desc limit 1", (uid,)).fetchone()
    if bg:
        bg = dict(bg); gi_ = c.execute("select img from gifts where type=? and number=? and img is not null order by id desc limit 1", (bg["t"], bg["no"])).fetchone() if bg["no"] else None
        bg["im"] = (gi_["img"] if gi_ else "") or ""; bg["m"] = ((CAT[bg["t"]].get("model") or "").replace(BLACK_SFX, "") if 0 <= bg["t"] < len(CAT) else "")
    return {"nwwd": WITHDRAW_REGULAR, "custom": [{"t": i, "n": x["name"], "v": x["price"], "m": x.get("model", "")} for i, x in enumerate(CAT) if x.get("custom")],
            "stock_items": si, "hist": hist,
            "topups": [{"ts": r["ts"], "a": r["amount"], "k": r["kind"], "r": r["ref"]} for r in c.execute("select * from topup where user=? order by id desc limit 100", (uid,))], "bg": bg or None,
            "bal": u["bal"], "boosts": u["boosts"], "best": u["best"], "inv": inv, "stock": stock,
            "prices": [g["price"] for g in CAT], "seed_hash": sha(u["seed"]), "nonce": u["nonce"], "fee": WITHDRAW_FEE, "ref": {"n": c.execute("select count(*) from users where ref_by=?", (uid,)).fetchone()[0], "e": round(u["ref_earn"] or 0, 4), "l": ref_link(uid), "pct": int(REF_PCT * 100)}, "spg": STARS_PER_GRAM, "smin": STARS_MIN, "online": online_count(), "owner": owner_username(),
            "total": c.execute("select coalesce(sum(boosts),0) from users").fetchone()[0]}

# ---------- вход через Telegram (проверка initData) ----------
def check_init(data):
    try:
        pairs = dict(parse_qsl(data, keep_blank_values=True))
        h = pairs.pop("hash")
        s = "\n".join(f"{k}={v}" for k, v in sorted(pairs.items()))
        secret = hmac.new(b"WebAppData", TOKEN.encode(), hashlib.sha256).digest()
        ok = hmac.compare_digest(hmac.new(secret, s.encode(), hashlib.sha256).hexdigest(), h)
        if not ok or time.time() - int(pairs["auth_date"]) > 86400: raise ValueError
        u = json.loads(pairs["user"]); u["_sp"] = pairs.get("start_param", ""); return u
    except Exception as e:
        print("initData отклонён:", repr(e), "длина:", len(data or ""))
        raise HTTPException(401, "bad init data")

def fail(c, msg):
    c.execute("rollback")
    return {"ok": False, "error": msg}

# ---------- API ----------
VENDOR = {"tgsdk.js": "https://telegram.org/js/telegram-web-app.js",
          "tcui.js": "https://cdn.jsdelivr.net/npm/@tonconnect/ui@2.0.9/dist/tonconnect-ui.min.js"}

@app.get("/vendor/{name}")
def vendor(name: str):
    """Кэширует внешние скрипты на диске сервера и отдаёт их с нашего домена."""
    if name not in VENDOR: raise HTTPException(404, "no")
    d = os.path.join(HERE, "vendor_cache"); os.makedirs(d, exist_ok=True)
    p = os.path.join(d, name)
    if not os.path.exists(p) or time.time() - os.path.getmtime(p) > 86400:
        try:
            r = httpx.get(VENDOR[name], timeout=20, follow_redirects=True); r.raise_for_status()
            open(p, "wb").write(r.content)
        except Exception as e:
            print("vendor:", name, e)
            if not os.path.exists(p): raise HTTPException(502, "upstream")
    return FileResponse(p, media_type="application/javascript", headers={"Cache-Control": "public, max-age=3600"})

@app.get("/")
def index(request: Request):
    """Отдаёт приложение, подставляя адрес сервера и имя бота (из PUBLIC_URL и BOT_USERNAME)."""
    html = open(os.path.join(HERE, "index.html"), encoding="utf-8").read()
    base = PUBLIC_URL or f"{request.url.scheme}://{request.headers.get('host', '')}"
    html = html.replace('var API_URL="";', 'var API_URL="%s";' % base, 1)
    html = html.replace("ТВОЙ_БОТ", os.environ.get("BOT_USERNAME", "GiftUpNowbot"))
    for name, url in VENDOR.items():                      # скрипты отдаём со своего сервера (telegram.org и CDN бывают недоступны)
        html = html.replace(url, "/vendor/" + name)
    return HTMLResponse(html, headers={"Cache-Control": "no-cache"})

@app.get("/tonconnect-manifest.json")
def manifest(request: Request):
    base = PUBLIC_URL or f"{request.url.scheme}://{request.headers.get('host', '')}"
    return JSONResponse({"url": base, "name": "GiftUp", "iconUrl": base + "/icon.png"},
                        headers={"Access-Control-Allow-Origin": "*"})

@app.get("/icon.png")
def icon(): return FileResponse(os.path.join(HERE, "icon.png"), headers={"Access-Control-Allow-Origin": "*"})

_av = {}
@app.get("/api/avatar")
def api_avatar(x_init_data: str = Header("")):
    """Фото профиля игрока (берём через Bot API, чтобы работало, даже если Telegram не прислал photo_url)."""
    usr = check_init(x_init_data); uid = usr["id"]
    if not hit(f"av:{uid}", 10, 60): raise HTTPException(429, "slow")
    c = _av.get(uid)
    if c and time.time() - c[0] < 3600: return Response(c[1], media_type="image/jpeg")
    try:
        ph = tg("getUserProfilePhotos", user_id=uid, limit=1)["photos"]
        fid = ph[0][-1 if len(ph[0]) < 2 else 1]["file_id"]
        path = tg("getFile", file_id=fid)["file_path"]
        data = httpx.get(f"https://api.telegram.org/file/bot{TOKEN}/{path}", timeout=20).content
    except Exception:
        raise HTTPException(404, "no photo")
    _av[uid] = (time.time(), data)
    return Response(data, media_type="image/jpeg")

@app.post("/api/deposit")
def api_deposit(x_init_data: str = Header("")):
    usr = check_init(x_init_data)
    if not hit(f"dep:{usr['id']}", 20, 60): return {"ok": False, "error": "Слишком часто, подожди немного"}
    if not DEPOSIT_ADDRESS: return {"ok": False, "error": "Пополнение пока не настроено"}
    c = db()
    try: get_user(c, usr["id"], usr.get("first_name", ""))
    finally: c.close()
    return {"ok": True, "address": DEPOSIT_ADDRESS, "comment": f"gu{usr['id']}", "min": MIN_DEPOSIT}

_imgs = None
def gift_image(t):
    """Картинка подарка из index.html (массив IM). Возвращает (байты, тип) или None (SVG Telegram не принимает)."""
    global _imgs
    if _imgs is None:
        try:
            html = open(os.path.join(HERE, "index.html"), encoding="utf-8").read()
            _imgs = json.loads(re.search(r"var IM=(\[.*?\]);", html, re.S).group(1))
        except Exception as e:
            print("картинки:", e); _imgs = []
    if not 0 <= t < len(_imgs): return None
    m = re.match(r"data:(image/(?:jpeg|png));base64,(.*)", _imgs[t], re.S)
    if not m: return None
    import base64
    return base64.b64decode(m.group(2)), m.group(1)

@app.get("/gift/{t}.jpg")
def gift_pic(t: int):
    r = gift_image(t)
    if not r: raise HTTPException(404, "no")
    return Response(r[0], media_type=r[1], headers={"Cache-Control": "public, max-age=86400"})

@app.post("/api/stars/buy")
def api_stars_buy(body: dict, x_init_data: str = Header("")):
    """Создаёт счёт в Telegram Stars (XTR). Оплату подтверждает сам Telegram, подарок выдаётся после successful_payment."""
    usr = check_init(x_init_data); uid = usr["id"]
    if not hit(f"stars:{uid}", 10, 60): return {"ok": False, "error": "Слишком часто, подожди немного"}
    try: t = int(body.get("t"))
    except Exception: return {"ok": False, "error": "Неверные данные"}
    if not 0 <= t < len(CAT) or not CAT[t].get("stars"): return {"ok": False, "error": "Этот подарок нельзя купить за звёзды"}
    x = CAT[t]
    if not x.get("nw") and not house_has(t): return {"ok": False, "error": "Этого подарка сейчас нет в наличии"}
    c = db()
    try: get_user(c, uid, usr.get("first_name", ""))
    finally: c.close()
    try:
        kw = {}
        if PUBLIC_URL and gift_image(t): kw["photo_url"] = f"{PUBLIC_URL}/gift/{t}.jpg"      # картинка подарка в окне оплаты
        link = tg("createInvoiceLink", title=x["name"][:32], description=("Подарок в инвентарь GiftUp: " + x["name"])[:255],
                  payload=f"gu:{uid}:{t}:{secrets.token_hex(4)}", currency="XTR",
                  prices=[{"label": x["name"][:32], "amount": int(x["stars"])}], **kw)
    except Exception as e:
        print("invoice:", e); return {"ok": False, "error": "Не удалось создать счёт, попробуй позже"}
    return {"ok": True, "link": link}

@app.post("/api/stars/topup")
def api_topup(body: dict, x_init_data: str = Header("")):
    usr = check_init(x_init_data); uid = usr["id"]
    if not hit(f"topup:{uid}", 10, 60): return {"ok": False, "error": "Слишком часто, подожди немного"}
    try: n = int(body.get("stars"))
    except Exception: return {"ok": False, "error": "Неверная сумма"}
    if not STARS_MIN <= n <= STARS_MAX: return {"ok": False, "error": f"Сумма от {STARS_MIN} до {STARS_MAX} звёзд"}
    c = db()
    try: get_user(c, uid, usr.get("first_name", ""))
    finally: c.close()
    gm = round(n / STARS_PER_GRAM, 4)
    try:
        link = tg("createInvoiceLink", title="Пополнение GiftUp", description=f"Пополнение баланса: {gm} GRAM",
                  payload=f"tp:{uid}:{n}:{secrets.token_hex(4)}", currency="XTR", prices=[{"label": "Пополнение", "amount": n}])
    except Exception as e:
        print("topup invoice:", e); return {"ok": False, "error": "Не удалось создать счёт, попробуй позже"}
    return {"ok": True, "link": link}

def _norm(x): return re.sub(r"[^a-z0-9а-яё]", "", (x or "").lower())
_IMG_OK = ("telegram.org", "fragment.com", "t.me", "telesco.pe", "cdn-telegram.org")
def fetch_nft(slug):
    """Название, номер и фото NFT по ссылке t.me/nft/<Имя>-<номер> (публичная страница подарка)."""
    r = httpx.get(f"https://t.me/nft/{slug}", follow_redirects=True, timeout=12, headers={"User-Agent": "Mozilla/5.0"})
    h = r.text
    def og(k):
        m = re.search(r'<meta[^>]+property="og:%s"[^>]+content="([^"]*)"' % k, h) or re.search(r'<meta[^>]+content="([^"]*)"[^>]+property="og:%s"' % k, h)
        return html.unescape(m.group(1)).strip() if m else ""
    title = og("title"); desc = og("description"); img = og("image")
    base, num = slug.rsplit("-", 1)
    m = re.match(r"^(.*?)\s*#(\d+)\s*$", title)
    name = m.group(1).strip() if m else re.sub(r"(?<=[a-z])(?=[A-Z])", " ", base)
    host = re.sub(r"^https?://([^/]+).*", r"\1", img)
    if not img.startswith("https://") or not any(host == d or host.endswith("." + d) for d in _IMG_OK):
        img = f"https://nft.fragment.com/gift/{slug.lower()}.medium.jpg"
    data = ""
    try:
        ir = httpx.get(img, follow_redirects=True, timeout=12)
        if ir.status_code == 200 and len(ir.content) < 600_000:
            data = "data:" + (ir.headers.get("content-type", "image/jpeg").split(";")[0]) + ";base64," + base64.b64encode(ir.content).decode()
    except Exception as e: print("nft img:", e, flush=True)
    return {"name": name, "number": int(num), "slug": slug, "desc": desc[:240], "image": data}

@app.post("/api/nft/preview")
def api_nft_preview(body: dict, x_init_data: str = Header("")):
    usr = check_init(x_init_data); uid = usr["id"]
    if not hit(f"nftp:{uid}", 10, 60): return {"ok": False, "error": "Слишком часто, подожди немного"}
    m = re.search(r"(?:(?:https?://)?(?:t|telegram)\.me/nft/)?([A-Za-z0-9_]{2,64}-\d{1,9})\b", str(body.get("link", "")).strip())
    if not m: return {"ok": False, "error": "Нужна ссылка вида t.me/nft/Название-123"}
    try: info = fetch_nft(m.group(1))
    except Exception as e:
        print("nft preview:", e, flush=True); return {"ok": False, "error": "Не удалось открыть ссылку, проверь её и попробуй ещё раз"}
    t = next((i for i, x in enumerate(CAT) if not x.get("nw") and _norm(x.get("key") or x["name"]) == _norm(info["name"])), -1)
    c = db()
    try:
        mine = t >= 0 and c.execute("select 1 from gifts where owner=? and type=? and number=? and status='held'", (uid, t, info["number"])).fetchone() is not None
    finally: c.close()
    return {"ok": True, "info": info, "type": t, "mine": mine, "owner": owner_username()}

@app.get("/api/state")
def api_state(x_init_data: str = Header("")):
    usr = check_init(x_init_data)
    if too_fast(usr["id"], "state", 40, 60): return too_fast(usr["id"], "state", 40, 60)
    c = db()
    try:
        get_user(c, usr["id"], usr.get("first_name", ""), usr.get("_sp", ""))
        return state(c, usr["id"])
    finally: c.close()

@app.post("/api/boost")
def api_boost(body: dict, x_init_data: str = Header("")):
    usr = check_init(x_init_data); uid = usr["id"]
    if not hit(f"boost:{uid}", 20, 60): return {"ok": False, "error": "Слишком часто, подожди немного"}
    try:
        ids = list({int(i) for i in body.get("ids", [])})
        extra = round(float(body.get("extra", 0)), 4); want = int(body.get("want", -1))
    except Exception:
        return {"ok": False, "error": "Неверные данные"}
    if extra < 0 or not 0 <= want < len(CAT): return {"ok": False, "error": "Неверные данные"}
    if len(ids) > 1: return {"ok": False, "error": "Для улучшения можно выбрать только один подарок"}
    c = db()
    try:
        c.execute("begin immediate")
        u = get_user(c, uid, usr.get("first_name", ""))
        gifts = [c.execute("select * from gifts where id=? and owner=? and status='held'", (i, uid)).fetchone() for i in ids]
        if any(g is None or g["type"] < 0 for g in gifts): return fail(c, "Подарок не найден в инвентаре")
        if any(not is_priced(g["type"]) for g in gifts): return fail(c, "Этот NFT ещё оценивается, попробуйте позже")
        if extra > u["bal"] + 1e-9: return fail(c, "Не хватает баланса")
        stake = extra + sum(CAT[g["type"]]["price"] for g in gifts)
        price = CAT[want]["price"]
        room = MAXCH / 100 * price / EDGE - sum(CAT[g["type"]]["price"] for g in gifts)     # выше максимального шанса ставку с баланса не принимаем
        if extra > max(0.0, room) + 1e-6: extra = round(max(0.0, room), 4)
        stake = extra + sum(CAT[g["type"]]["price"] for g in gifts)
        if stake <= 0: return fail(c, "Добавь ставку")
        if price <= stake: return fail(c, "Выбери подарок дороже своей ставки")
        if CAT[want].get("nw"): target = None            # обычные подарки выдаются без остатка (создаются при выигрыше)
        else:
            target = None
            try: wg = int(body.get("wgift") or 0)
            except Exception: wg = 0
            if wg: target = c.execute("select * from gifts where id=? and type=? and status='house'", (wg, want)).fetchone()
            if not target: target = c.execute("select * from gifts where type=? and status='house' order by random() limit 1", (want,)).fetchone()
            if not target: return fail(c, "Этого подарка сейчас нет в наличии")
        ch = min(MAXCH, stake / price * 100 * EDGE)
        # честный рандом: HMAC(server_seed, "id:nonce"); хэш seed виден игроку заранее
        hh = hmac.new(u["seed"].encode(), f'{uid}:{u["nonce"]}'.encode(), hashlib.sha256).digest()
        roll = int.from_bytes(hh[:8], "big") / 2**64 * 100
        win = abs(roll - 50) < ch / 2
        for g in gifts:
            if CAT[g["type"]].get("nw"): c.execute("update gifts set owner=null, status='gone' where id=?", (g["id"],))   # обычный подарок сгорает
            else: c.execute("update gifts set owner=null, status='house' where id=?", (g["id"],))
        c.execute("update users set bal=bal-?, boosts=boosts+1, nonce=nonce+1 where id=?", (extra, uid))
        gift = None
        if win:
            if target is None:
                cur = c.execute("insert into gifts(type,number,ogid,owner,status) values(?,?,?,?,'held')", (want, 0, "win:" + secrets.token_hex(8), uid))
                gift = {"id": cur.lastrowid, "t": want, "no": 0}
            else:
                c.execute("update gifts set owner=?, status='held' where id=?", (uid, target["id"]))
                gift = {"id": target["id"], "t": want, "no": target["number"], "im": target["img"] or ""}
            c.execute("update users set best=max(best,?) where id=?", (price, uid))
        now = int(time.time())
        if win or not gifts:    # выигрыш (или проигрыш ставки с баланса) — строка про желаемый подарок
            c.execute("insert into hist(user,ts,want,no,win,ch,price) values(?,?,?,?,?,?,?)",
                      (uid, now, want, (gift or {}).get("no", 0), 1 if win else 0, ch, price))
        else:                   # проигрыш: в истории — сгоревшие подарки-ставки
            for g in gifts:
                c.execute("insert into hist(user,ts,want,no,win,ch,price) values(?,?,?,?,?,?,?)",
                          (uid, now, g["type"], g["number"], 0, ch, CAT[g["type"]]["price"]))
        st = state(c, uid); c.execute("commit")
        return {"ok": True, "win": win, "roll": roll, "chance": ch, "gift": gift, "state": st}
    except Exception:
        try: c.execute("rollback")
        except Exception: pass
        raise
    finally: c.close()

@app.post("/api/free")
def api_free(x_init_data: str = Header("")):
    return {"ok": False, "error": "Недоступно"}   # бесплатный кейс отключён
    usr = check_init(x_init_data); uid = usr["id"]
    if not hit(f"free:{uid}", 5, 60): return {"ok": False, "error": "Слишком часто, подожди немного"}
    c = db()
    try:
        c.execute("begin immediate")
        u = get_user(c, uid, usr.get("first_name", ""))
        today = datetime.date.today().isoformat()
        if u["free_day"] == today: return fail(c, "Бесплатный кейс уже открыт сегодня")
        cheap = [i for i, g in enumerate(CAT) if g["price"] < 10 and not g.get("nw")]
        if not cheap: return fail(c, "Кейс пуст")
        q = ",".join("?" * len(cheap))
        t = c.execute(f"select * from gifts where status='house' and type in ({q}) order by random() limit 1", cheap).fetchone()
        if not t: return fail(c, "Кейс пуст")
        c.execute("update gifts set owner=?, status='held' where id=?", (uid, t["id"]))
        c.execute("update users set free_day=? where id=?", (today, uid))
        st = state(c, uid); c.execute("commit")
        return {"ok": True, "gift": {"id": t["id"], "t": t["type"], "no": t["number"]}, "state": st}
    except Exception:
        try: c.execute("rollback")
        except Exception: pass
        raise
    finally: c.close()

NW_EMOJI = {23: "💝", 24: "🧸", 25: "🎁", 26: "🌹", 27: "🎂", 28: "💐", 29: "🚀", 30: "🍾", 31: "🏆", 32: "💍", 33: "💎"}
_gl = {"t": 0, "d": []}
def tg_gift_id(t):
    """id обычного подарка в Telegram для типа t каталога (по эмодзи и цене в звёздах)."""
    if time.time() - _gl["t"] > 600 or not _gl["d"]:
        _gl["d"] = tg("getAvailableGifts").get("gifts", []); _gl["t"] = time.time()
    want, stars = NW_EMOJI.get(t), CAT[t].get("stars")
    for g in _gl["d"]:
        if g.get("star_count") == stars and (g.get("sticker") or {}).get("emoji") == want: return g["id"]
    same = [g for g in _gl["d"] if g.get("star_count") == stars]
    if len(same) == 1: return same[0]["id"]
    print("нет подарка в Telegram:", t, want, stars, [(g.get("star_count"), (g.get("sticker") or {}).get("emoji")) for g in _gl["d"]], flush=True)
    return None

def withdraw_regular(uid, g):
    """Обычный подарок: бесплатный вывод — бот сам отправляет его пользователю (sendGift, за звёзды бота)."""
    c = db()
    try:
        c.execute("begin immediate")
        cur = c.execute("update gifts set status='out' where id=? and owner=? and status='held'", (g["id"], uid))
        c.execute("commit")
    finally: c.close()
    if not cur.rowcount: return {"ok": False, "error": "Подарок не найден"}
    err = None
    try:
        gid = tg_gift_id(g["type"])
        if not gid: err = "Этот подарок сейчас недоступен в Telegram"
        else:
            try: tg("sendGift", user_id=uid, gift_id=gid, text="Подарок из Gift Up Now")
            except Exception as e1:
                bc = meta_get("bc")
                if "BALANCE" not in str(e1).upper() or not bc: raise
                # на балансе бота не хватает звёзд — автоматически докидываем со звёзд бизнес-аккаунта владельца
                need = int(CAT[g["type"]].get("stars") or 0)
                tg("transferBusinessAccountStars", business_connection_id=bc, star_count=need)
                print(f"[{stamp()}] ⭐ пополнен баланс бота со счёта аккаунта: {need}", flush=True)
                tg("sendGift", user_id=uid, gift_id=gid, text="Подарок из Gift Up Now")
    except Exception as e:
        print("sendGift:", e, flush=True)
        err = "Недостаточно звёзд для отправки, попробуйте позже" if "BALANCE" in str(e).upper() else "Не удалось отправить подарок, попробуйте позже"
    c = db()
    try:
        if err:
            c.execute("update gifts set status='held' where id=?", (g["id"],)); return {"ok": False, "error": err}
        print(f"[{stamp()}] 📤 выдан обычный подарок «{CAT[g['type']]['name']}» → {uid}", flush=True)
        c.execute("insert into hist(user,ts,want,no,win,ch,price) values(?,?,?,?,2,0,?)", (uid, int(time.time()), g["type"], g["number"], CAT[g["type"]]["price"]))
        c.execute("begin immediate"); st = state(c, uid); c.execute("commit")
        return {"ok": True, "state": st}
    finally: c.close()

@app.post("/api/sell")
def api_sell(body: dict, x_init_data: str = Header("")):
    """Продать подарки за GRAM по их цене: {"ids":[..]} или {"all":true}."""
    usr = check_init(x_init_data); uid = usr["id"]
    if not hit(f"sell:{uid}", 20, 60): return {"ok": False, "error": "Слишком часто, подожди немного"}
    c = db()
    try:
        c.execute("begin immediate")
        get_user(c, uid, usr.get("first_name", ""))
        rows = c.execute("select * from gifts where owner=? and status='held' and type>=0", (uid,)).fetchall()
        if not body.get("all"):
            ids = {int(i) for i in body.get("ids", [])}
            rows = [g for g in rows if g["id"] in ids]
        rows = [g for g in rows if is_priced(g["type"])]
        if not rows: return fail(c, "Нечего продавать: подарки ещё оцениваются или не найдены")
        total = 0.0
        for g in rows:
            total += CAT[g["type"]]["price"]
            c.execute("update gifts set owner=null, status=? where id=?", ("gone" if CAT[g["type"]].get("nw") else "house", g["id"]))
        total = round(total, 4)
        c.execute("update users set bal=bal+? where id=?", (total, uid))
        st = state(c, uid); c.execute("commit")
        print(f"[{stamp()}] 💱 продано {len(rows)} шт. за {total} GRAM → {uid}", flush=True)
        return {"ok": True, "sold": len(rows), "amount": total, "state": st}
    except Exception:
        try: c.execute("rollback")
        except Exception: pass
        raise
    finally: c.close()

@app.post("/api/buy")
def api_buy(body: dict, x_init_data: str = Header("")):
    """Выкуп NFT со склада за GRAM с баланса: {"ids":[id подарков со склада]} → подарки уходят в инвентарь игрока."""
    usr = check_init(x_init_data); uid = usr["id"]
    if not hit(f"buy:{uid}", 15, 60): return {"ok": False, "error": "Слишком часто, подожди немного"}
    try: ids = sorted({int(i) for i in (body.get("ids") or [])})
    except Exception: return {"ok": False, "error": "Неверный запрос"}
    if not ids or len(ids) > 60: return {"ok": False, "error": "Выберите от 1 до 60 подарков"}
    c = db()
    try:
        c.execute("begin immediate")
        u = get_user(c, uid, usr.get("first_name", ""))
        rows = c.execute(f"select * from gifts where status='house' and type>=0 and id in ({','.join('?' * len(ids))})", ids).fetchall()
        if len(rows) != len(ids): return fail(c, "Некоторые подарки уже недоступны. Обновите список")
        for g in rows:
            if CAT[g["type"]].get("nw"): return fail(c, "Этот подарок нельзя купить")
            if not is_priced(g["type"]): return fail(c, "Подарок ещё оценивается, попробуйте позже")
        total = round(sum(CAT[g["type"]]["price"] for g in rows), 4)
        if u["bal"] + 1e-9 < total: return fail(c, "Недостаточно средств")
        c.execute("update users set bal=bal-? where id=?", (total, uid))
        for g in rows: c.execute("update gifts set status='held', owner=? where id=?", (uid, g["id"]))
        st = state(c, uid); c.execute("commit")
        print(f"[{stamp()}] 🛒 куплено {len(rows)} шт. за {total} GRAM → {uid}", flush=True)
        return {"ok": True, "bought": len(rows), "amount": total, "state": st}
    except Exception:
        try: c.execute("rollback")
        except Exception: pass
        raise
    finally: c.close()

@app.post("/api/withdraw")
def api_withdraw(body: dict, x_init_data: str = Header("")):
    usr = check_init(x_init_data); uid = usr["id"]
    if not hit(f"wd:{uid}", 20, 3600): return {"ok": False, "error": "Лимит выводов: не больше 20 в час"}
    c = db()
    try:
        c.execute("begin immediate")
        u = get_user(c, uid, usr.get("first_name", ""))
        g = c.execute("select * from gifts where id=? and owner=? and status='held'", (int(body.get("id", 0)), uid)).fetchone()
        if not g: return fail(c, "Подарок не найден")
        if g["type"] >= 0 and CAT[g["type"]].get("nw") and not WITHDRAW_REGULAR:
            return fail(c, "Подарки за звёзды нельзя вывести: на них можно только играть в апгрейде.")
        if g["type"] >= 0 and not is_priced(g["type"]): return fail(c, "Этот NFT ещё оценивается, попробуйте позже")
        if g["type"] >= 0 and CAT[g["type"]].get("nw"):
            c.execute("rollback")
            c.close(); return withdraw_regular(uid, g)
        if u["bal"] + 1e-9 < WITHDRAW_FEE: return fail(c, f"Для вывода нужно {WITHDRAW_FEE} GRAM на балансе")
        c.execute("update users set bal=bal-? where id=?", (WITHDRAW_FEE, uid))
        c.execute("update gifts set status='out' where id=?", (g["id"],))
        c.execute("insert into hist(user,ts,want,no,win,ch,price) values(?,?,?,?,2,0,?)", (uid, int(time.time()), g["type"], g["number"], CAT[g["type"]]["price"] if g["type"] >= 0 else 0))
        c.execute("insert into wd(gift,user,fee) values(?,?,?)", (g["id"], uid, WITHDRAW_FEE))
        st = state(c, uid); c.execute("commit")
        return {"ok": True, "state": st}
    except Exception:
        try: c.execute("rollback")
        except Exception: pass
        raise
    finally: c.close()

@app.post("/api/ref/claim")
def api_ref_claim(x_init_data: str = Header("")):
    """Переводит накопленный реферальный доход на игровой баланс."""
    usr = check_init(x_init_data); uid = usr["id"]
    if not hit(f"refc:{uid}", 10, 60): return {"ok": False, "error": "Слишком часто"}
    c = db()
    try:
        c.execute("begin immediate")
        u = get_user(c, uid, usr.get("first_name", ""))
        e = round(u["ref_earn"] or 0, 4)
        if e < 0.01: return fail(c, "Пока нечего выводить")
        c.execute("update users set bal=bal+?, ref_earn=0 where id=?", (e, uid))
        c.execute("insert into topup(user,ts,amount,kind,ref) values(?,?,?,?,?)", (uid, int(time.time()), e, "ref", "Реферальный доход"))
        st = state(c, uid); c.execute("commit")
        return {"ok": True, "amount": e, "state": st}
    except Exception:
        try: c.execute("rollback")
        except Exception: pass
        raise
    finally: c.close()

@app.post("/api/seed/rotate")
def api_rotate(x_init_data: str = Header("")):
    """Раскрывает старый seed (можно проверить все прошлые броски) и выдаёт новый."""
    usr = check_init(x_init_data)
    if not hit(f"rot:{usr['id']}", 5, 3600): return {"ok": False, "error": "Слишком часто"}
    c = db()
    try:
        u = get_user(c, usr["id"], usr.get("first_name", "")); new = secrets.token_hex(32)
        c.execute("update users set seed=?, nonce=0 where id=?", (new, usr["id"]))
        return {"old_seed": u["seed"], "old_nonce": u["nonce"], "new_hash": sha(new)}
    finally: c.close()

@app.post("/api/admin/credit")
def api_credit(body: dict, x_admin_key: str = Header("")):
    if not ADMIN_KEY or not hmac.compare_digest(x_admin_key, ADMIN_KEY): raise HTTPException(403)
    c = db()
    try:
        get_user(c, int(body["user"])); c.execute("update users set bal=bal+? where id=?", (float(body["amount"]), int(body["user"])))
        c.execute("insert into topup(user,ts,amount,kind,ref) values(?,?,?,?,?)", (int(body["user"]), int(time.time()), float(body["amount"]), "admin", "Начисление"))
        return {"ok": True}
    finally: c.close()

# ---------- аккаунт-приёмник (Bot API + Business) ----------
def tg(method, **p):
    r = httpx.post(f"https://api.telegram.org/bot{TOKEN}/{method}", json=p, timeout=70).json()
    if not r.get("ok"): raise RuntimeError(r.get("description", str(r)))
    return r["result"]

def notify(uid, text):
    try: tg("sendMessage", chat_id=uid, text=text)
    except Exception as e: print("notify:", e)

def reply_in_chat(m, text):
    """Отвечает в том же чате от имени аккаунта (через бизнес-подключение) — работает, даже если человек не запускал бота; иначе личное сообщение от бота."""
    bc = m.get("business_connection_id") or meta_get("bc"); chat = (m.get("chat") or {}).get("id")
    if bc and chat:
        try: tg("sendMessage", business_connection_id=bc, chat_id=chat, text=text); return
        except Exception as e: print("ответ в чат:", e, flush=True)
    uid = (m.get("from") or {}).get("id")
    if uid: notify(uid, text)

def match(base, model):
    base, model, best = (base or "").lower(), (model or "").lower(), -1
    for i, x in enumerate(CAT):
        if not x["key"] or x["key"].lower() != base: continue
        if x.get("model") and x["model"].lower() == model: return i
        if not x.get("model") and best < 0 and not model.endswith(BLACK_SFX.lower()): best = i
    return best

def stamp(): return datetime.datetime.now().strftime("%d.%m %H:%M:%S")

def on_message(m):
    fr = m.get("from") or {}
    who = f"{fr.get('id')} @{fr.get('username', '-')} {fr.get('first_name', '')}".strip()
    cid = m.get("business_connection_id")
    if cid and BUSINESS_OWNER_ID and str(fr.get("id")) == BUSINESS_OWNER_ID and cid != meta_get("bc"):
        # сообщение отправил сам аккаунт-склад → это его подключение: запоминаем и забираем подарки
        meta_set("bc", cid); meta_set("bc_owner", BUSINESS_OWNER_ID)
        print(f"[{stamp()}] подключение склада определено по сообщению: {cid[:8]}…", flush=True)
        threading.Thread(target=lambda: import_stock(), daemon=True).start()
    ug = m.get("unique_gift")
    if m.get("gift") and not ug:
        print(f"[{stamp()}] 🎁 ПОЛУЧЕН обычный подарок от {who} (в игре не используется)", flush=True)
    if not ug:
        print(f"[{stamp()}] бизнес-сообщение от {who}: {','.join(k for k in m if k not in ('message_id','date','chat','from','business_connection_id'))}", flush=True)
        return
    sender = fr.get("id")
    if not sender or str(sender) == str(meta_get("bc_owner")):
        print(f"[{stamp()}] исходящий подарок с аккаунта (пропуск)", flush=True); return      # исходящий — пропускаем
    g = ug.get("gift", {}); ogid = ug.get("owned_gift_id")
    print(f"[{stamp()}] 🎁 ПОЛУЧЕН NFT от {who}: {g.get('base_name')} #{g.get('number')}, модель {(g.get('model') or {}).get('name')}, "
          f"owned_gift_id={ogid}, origin={ug.get('origin')}", flush=True)
    if not ogid: print("нет owned_gift_id", ug, flush=True); return
    t = match(g.get("base_name"), gift_model(g))
    if t < 0: t = ensure_type(g.get("base_name"), gift_model(g))
    house = sender in HOUSE_IDS
    status = "unknown" if t < 0 else ("house" if house else "held")
    c = db()
    try:
        get_user(c, sender, (m.get("from") or {}).get("first_name", ""))
        cur = c.execute("insert or ignore into gifts(type,number,ogid,owner,status) values(?,?,?,?,?)",
                        (t, g.get("number"), ogid, None if house else sender, status))
    finally: c.close()
    if not cur.rowcount:
        print(f"[{stamp()}] ↳ уже учтён ранее (повтор), пропуск", flush=True)
    threading.Thread(target=attach_image, args=(ogid, g.get("base_name"), g.get("number"), t), daemon=True).start()
    if cur.rowcount:
        nm = CAT[t]["name"] if t >= 0 else g.get("base_name", "подарок")
        print(f"[{stamp()}] ↳ ЗАЧИСЛЕН: {nm} #{g.get('number')} → {'в склад (хозяин)' if house else 'в инвентарь ' + str(sender)}"
              + ("" if t >= 0 else "  ⚠ НЕ РАСПОЗНАН (нет в catalog.json)"), flush=True)
        if t < 0: reply_in_chat(m, f"⚠️ Ваш NFT «{nm}» #{g.get('number')} получен, но пока не распознан. Напишите в поддержку.")
        elif not house:
            if is_priced(t): reply_in_chat(m, f"✅ Ваш NFT «{nm}» #{g.get('number')} зачислен в ваш инвентарь GiftUp. Откройте приложение, чтобы улучшить его.")
            else: reply_in_chat(m, f"✅ Ваш NFT «{nm}» #{g.get('number')} зачислен в ваш инвентарь GiftUp. Сейчас он на оценке: как только цена будет определена, его можно будет использовать в приложении.")

def on_start(m):
    fr = m.get("from") or {}
    name = (fr.get("first_name") or "друг").replace("<", "").replace(">", "")
    own = owner_username()
    send = f'отправьте подарок на <a href="https://t.me/{own}">@{own}</a>' if own else "отправьте подарок владельцу приложения"
    text = (f"Привет, {name}! 🎁\n\n<b>Gift Up Now</b> — апгрейд Telegram-подарков.\n\n"
            f"💝 Чтобы пополнить инвентарь, {send}\n\n"
            "Нажмите кнопку ниже, чтобы открыть приложение")
    kb = {"inline_keyboard": [[{"text": "Открыть GiftUp", "web_app": {"url": PUBLIC_URL}}]]} if PUBLIC_URL else None
    try:
        p = dict(chat_id=fr.get("id"), text=text, parse_mode="HTML", disable_web_page_preview=True)
        if kb: p["reply_markup"] = kb
        tg("sendMessage", **p)
    except Exception as e: print("start:", e)

def import_stock(restore=False):
    """Забирает NFT, которые уже лежат на аккаунте владельца, в «склад» (то, что пришло до подключения бота, событием не приходит)."""
    bc = meta_get("bc")
    if not bc: print("импорт склада: нет бизнес-подключения", flush=True); return 0
    n_new = n_all = 0; off = ""; seen = set()
    while True:
        r = tg("getBusinessAccountGifts", business_connection_id=bc, exclude_unique=False, limit=100, **({"offset": off} if off else {}))
        for og in r.get("gifts", []):
            if og.get("type") != "unique": continue
            g = og.get("gift", {}); ogid = og.get("owned_gift_id")
            if not ogid: continue
            n_all += 1; seen.add(ogid)
            t = match(g.get("base_name"), gift_model(g))
            if t < 0: t = ensure_type(g.get("base_name"), gift_model(g))
            c = db()
            try:
                cur = c.execute("insert or ignore into gifts(type,number,ogid,owner,status) values(?,?,?,?,?)",
                                (t, g.get("number"), ogid, None, "house" if t >= 0 else "unknown"))
                row = c.execute("select * from gifts where ogid=?", (ogid,)).fetchone()
                if row and row["type"] < 0 and t >= 0 and row["status"] == "unknown":     # раньше не распознавался — чиним
                    c.execute("update gifts set type=?, status='house', owner=null where id=?", (t, row["id"]))
                if restore and row and row["status"] == "gone" and t >= 0:      # раньше «очищен», но подарок всё ещё в профиле — возвращаем на склад
                    c.execute("update gifts set status='house', owner=null where id=?", (row["id"],)); n_new += 1
                need_img = bool(row) and not row["img"]
            finally: c.close()
            need_pr = t >= 0 and CAT[t].get("custom") and not CAT[t].get("fixed") and not CAT[t].get("vp") and _ub["cl"] is not None
            if need_img or need_pr: threading.Thread(target=attach_image, args=(ogid, g.get("base_name"), g.get("number"), t), daemon=True).start()
            if cur.rowcount:
                n_new += 1
                print(f"[{stamp()}] 📦 импорт на склад: {g.get('base_name')} #{g.get('number')} модель {(g.get('model') or {}).get('name')}"
                      + ("" if t >= 0 else "  ⚠ НЕ РАСПОЗНАН (нет в catalog.json)"), flush=True)
        off = r.get("next_offset") or ""
        if not off: break
    if restore:     # убираем со склада NFT, которых уже нет на аккаунте-складе (остатки от чужого аккаунта и т.п.)
        c = db(); gone = 0
        try:
            for r in c.execute("select id,type,ogid from gifts where status='house'").fetchall():
                if 0 <= r["type"] < len(CAT) and CAT[r["type"]].get("custom") and r["ogid"] not in seen:
                    c.execute("update gifts set status='gone' where id=?", (r["id"],)); gone += 1
        finally: c.close()
        print(f"[{stamp()}] со склада убрано NFT, которых нет на аккаунте: {gone}", flush=True)
    print(f"[{stamp()}] импорт склада: на аккаунте {n_all} NFT, добавлено {n_new}", flush=True)
    return n_new

@app.post("/api/admin/import")
def api_import(body: dict = None, x_admin_key: str = Header("")):
    if not ADMIN_KEY or not hmac.compare_digest(x_admin_key, ADMIN_KEY): raise HTTPException(403)
    try:
        n = import_stock(restore=bool((body or {}).get("restore")))
        return {"ok": True, "added": n, "custom_types": [{"id": i, "name": x["name"], "model": x.get("model"), "price": x["price"]} for i, x in enumerate(CAT) if x.get("custom")]}
    except Exception as e: return {"ok": False, "error": str(e)}

@app.post("/api/admin/unfix")
def api_unfix(body: dict = None, x_admin_key: str = Header("")):
    """Снять ручную цену: {"names":["Myth Mask"]} — у всех моделей этого названия; цена дальше определяется автоматически (запусти /api/admin/reprice)."""
    if not ADMIN_KEY or not hmac.compare_digest(x_admin_key, ADMIN_KEY): raise HTTPException(403)
    names = {_norm(n) for n in ((body or {}).get("names") or [])}
    if not names: return {"ok": False, "error": "укажи names"}
    n = 0
    with _cat_lock:
        for x in CAT:
            if x.get("custom") and x.get("fixed") and _norm(x["key"]) in names:
                x["fixed"] = False; x["priced"] = False; n += 1
                c = db()
                try: c.execute("update custom set fixed=0, psrc=0 where id=?", (x["custom"],))
                finally: c.close()
    return {"ok": True, "unfixed": n}

@app.post("/api/admin/reprice")
def api_reprice(x_admin_key: str = Header("")):
    """Пересчитать цены всех NFT-типов каталога по Gift Asset (фоном). Заблокированные (fixed) не трогает."""
    if not ADMIN_KEY or not hmac.compare_digest(x_admin_key, ADMIN_KEY): raise HTTPException(403)
    if not GAKEY: return {"ok": False, "error": "GAKEY не задан"}
    def job():
        n = 0
        for x in list(CAT):
            if not x.get("custom") or x.get("fixed"): continue
            try: v = ga_floor(x["name"], x.get("model"))
            except Exception as e: print("reprice:", x.get("name"), e, flush=True); continue
            if v is None: continue
            with _cat_lock: x["price"] = v; x["vp"] = True; x["priced"] = True
            c = db()
            try: c.execute("update custom set price=?, psrc=1 where id=? and fixed=0", (v, x["custom"]))
            finally: c.close()
            n += 1
        print(f"[{stamp()}] reprice: обновлено цен {n}", flush=True)
    threading.Thread(target=job, daemon=True).start()
    return {"ok": True, "started": True}

@app.post("/api/admin/clear")
def api_clear(body: dict = None, x_admin_key: str = Header("")):
    """Убирает «старые» NFT со склада приложения (на аккаунте они остаются). {"keep":["Myth Mask"]} — эти оставить. {} — убрать все."""
    if not ADMIN_KEY or not hmac.compare_digest(x_admin_key, ADMIN_KEY): raise HTTPException(403)
    keep = {_norm(k) for k in ((body or {}).get("keep") or [])}
    c = db(); n = 0
    try:
        for r in c.execute("select id,type from gifts where status in ('house','unknown')").fetchall():
            t = r["type"]
            if keep and 0 <= t < len(CAT) and _norm(CAT[t].get("key") or CAT[t]["name"]) in keep: continue
            c.execute("update gifts set status='gone' where id=?", (r["id"],)); n += 1
    finally: c.close()
    print(f"[{stamp()}] склад очищен: убрано {n}", flush=True)
    return {"ok": True, "removed": n}

@app.get("/api/admin/types")
def api_types(x_admin_key: str = Header("")):
    """Список автоматически добавленных NFT: цена и подтверждена ли она."""
    if not ADMIN_KEY or not hmac.compare_digest(x_admin_key, ADMIN_KEY): raise HTTPException(403)
    return [{"id": i, "name": x["name"], "model": x.get("model"), "price": x["price"], "priced": bool(x.get("priced"))} for i, x in enumerate(CAT) if x.get("custom")]

@app.post("/api/admin/price")
def api_price(body: dict, x_admin_key: str = Header("")):
    """Цена автоматически добавленному NFT. Один: {"name":"Plush Pepe","price":1500} или {"id":43,"price":900}.
    Сразу несколько: {"prices":{"Myth Mask":80,"Homemade Cake":20}}. Название — для всех моделей."""
    if not ADMIN_KEY or not hmac.compare_digest(x_admin_key, ADMIN_KEY): raise HTTPException(403)
    items = []
    if isinstance(body.get("prices"), dict): items = [("name", k, v) for k, v in body["prices"].items()]
    else: items = [("id", body["id"], body.get("price"))] if "id" in body else [("name", body.get("name"), body.get("price"))]
    done = []
    for kind, ref, pr in items:
        try: price = float(pr)
        except Exception: return {"ok": False, "error": f"нужна цена для {ref}"}
        if price <= 0: return {"ok": False, "error": f"цена для {ref} должна быть больше 0"}
        with _cat_lock:
            for i, x in enumerate(CAT):
                if not x.get("custom"): continue
                if (kind == "id" and int(ref) == i) or (kind == "name" and ref and _norm(x["key"]) == _norm(ref)):
                    x["price"] = price; x["fixed"] = True; x["priced"] = True; done.append(i)
                    c = db()
                    try: c.execute("update custom set price=?, fixed=1, psrc=1 where id=?", (price, x["custom"]))
                    finally: c.close()
    return {"ok": bool(done), "updated": done}

@app.get("/api/admin/check")
def api_check(x_admin_key: str = Header("")):
    """Диагностика: подключён ли бот к аккаунту и какие права есть."""
    if not ADMIN_KEY or not hmac.compare_digest(x_admin_key, ADMIN_KEY): raise HTTPException(403)
    out = {}
    try: me = tg("getMe"); out["bot"] = "@" + me.get("username", "?"); out["can_connect_to_business"] = me.get("can_connect_to_business")
    except Exception as e: out["bot_error"] = str(e)
    bc = meta_get("bc"); out["business_connection_id"] = bc or None; out["owner_id"] = meta_get("bc_owner")
    if not bc:
        out["verdict"] = "Бот НЕ подключён к аккаунту: Настройки Telegram → Бизнес → Чат-боты → выберите бота. Затем напишите любое сообщение на аккаунт."
        return out
    try:
        c = tg("getBusinessConnection", business_connection_id=bc)
        out["enabled"] = c.get("is_enabled"); out["rights"] = c.get("rights")
    except Exception as e: out["connection_error"] = str(e)
    for name, call in (("stars_balance", lambda: tg("getBusinessAccountStarBalance", business_connection_id=bc)),
                       ("gifts", lambda: tg("getBusinessAccountGifts", business_connection_id=bc, limit=5))):
        try:
            r = call()
            out[name] = r if name == "stars_balance" else {"total": r.get("total_count"), "sample": [((g.get("gift") or {}).get("base_name") or (g.get("gift") or {}).get("name"), (g.get("gift") or {}).get("number")) for g in r.get("gifts", [])]}
        except Exception as e: out[name + "_error"] = str(e)
    out["verdict"] = "OK: бот управляет аккаунтом" if not any(k.endswith("_error") for k in out) else "Есть ошибки (см. поля *_error): чаще всего не выданы права на подарки/звёзды или Telegram ограничил их для ботов"
    return out

def poll_loop():
    offset = 0
    while True:
        try:
            for u in tg("getUpdates", offset=offset, timeout=50, allowed_updates=["business_connection", "business_message", "pre_checkout_query", "message", "callback_query"]):
                offset = u["update_id"] + 1
                bc = u.get("business_connection")
                if bc:
                    print(f"[{stamp()}] бизнес-подключение: user={bc['user']['id']} включено={bc.get('is_enabled')} права={bc.get('rights')}", flush=True)
                if bc and BUSINESS_OWNER_ID and str(bc["user"]["id"]) != BUSINESS_OWNER_ID:
                    print(f"[{stamp()}] ⚠ подключение от чужого аккаунта {bc['user']['id']} проигнорировано (склад — {BUSINESS_OWNER_ID})", flush=True); bc = None
                if bc and bc.get("is_enabled"):
                    meta_set("bc", bc["id"]); meta_set("bc_owner", str(bc["user"]["id"]))
                    try: import_stock()
                    except Exception as e: print("импорт склада:", e, flush=True)
                if u.get("callback_query"): on_wd_callback(u["callback_query"])
                if u.get("business_message"): on_message(u["business_message"])
                if u.get("pre_checkout_query"): on_pre_checkout(u["pre_checkout_query"])
                if (u.get("message") or {}).get("successful_payment"): on_stars_paid(u["message"])
                mm = u.get("message") or {}
                if (mm.get("text") or "").startswith("/wd") and (mm.get("from") or {}).get("id") in ADMIN_TG and (mm.get("chat") or {}).get("type") == "private": resend_manual(mm["chat"]["id"])
                if (mm.get("text") or "").startswith("/start") and (mm.get("chat") or {}).get("type") == "private": on_start(mm)
        except Exception as e:
            print("poll:", e); time.sleep(5)

def _manual_request(r, nm, number, to=None):
    """Ручной вывод: заявка уходит админу в бота с кнопками «Выдал» / «Отклонить». Плата за вывод остаётся списанной, подарок — в статусе «out»."""
    uid = r["user"]; uname = ""
    try: uname = (tg("getChat", chat_id=uid).get("username") or "")
    except Exception: pass
    slug = nft_slug(CAT[r["type"]]["key"], number) if r["type"] >= 0 and CAT[r["type"]].get("key") else ""
    who = (f"@{uname} " if uname else "") + f"(id {uid})"
    text = (f"📤 Заявка на вывод #{r['wid']}\nИгрок: {who}\nNFT: {nm} #{number}" + (f"\nhttps://t.me/nft/{slug}" if slug else "")
            + f"\nОплата вывода: {r['fee']} GRAM\nПрофиль: tg://user?id={uid}\n\nПередай подарок игроку вручную и нажми «Выдал».")
    kb = {"inline_keyboard": [[{"text": "✅ Выдал", "callback_data": f"wdok:{r['wid']}"}, {"text": "↩️ Отклонить (вернуть)", "callback_data": f"wdno:{r['wid']}"}]]}
    for a in (to or ADMIN_TG):
        try: tg("sendMessage", chat_id=a, text=text, reply_markup=kb)
        except Exception as e: print(f"[{stamp()}] заявка админу {a} НЕ доставлена: {e}  (открой бота с этого аккаунта и нажми Старт)", flush=True)
    if not to: notify(uid, f"📤 Заявка на вывод «{nm}» принята. Подарок будет отправлен вручную в ближайшее время.")

def resend_manual(chat):
    """Команда /wd: присылает все невыданные заявки на ручной вывод."""
    c = db()
    try: rows = c.execute("select w.id wid, w.user, w.fee, g.type, g.number from wd w join gifts g on g.id=w.gift where w.status='manual' order by w.id").fetchall()
    finally: c.close()
    if not rows:
        try: tg("sendMessage", chat_id=chat, text="Невыданных заявок нет ✅")
        except Exception as e: print("wd:", e)
        return
    for r in rows: _manual_request(r, CAT[r["type"]]["name"] if r["type"] >= 0 else "подарок", r["number"], to=[chat])

def on_wd_callback(cq):
    a = (cq.get("from") or {}).get("id")
    if a not in ADMIN_TG: return
    data = cq.get("data") or ""
    if ":" not in data: return
    act, wid = data.split(":", 1)
    if act not in ("wdok", "wdno") or not wid.isdigit(): return
    c = db(); msg = ""
    try:
        c.execute("begin immediate")
        r = c.execute("select w.id wid, w.user, w.fee, w.status, g.id gid, g.type, g.number from wd w join gifts g on g.id=w.gift where w.id=?", (int(wid),)).fetchone()
        if not r: msg = "Заявка не найдена"
        elif r["status"] != "manual": msg = "Заявка уже обработана"
        else:
            nm = CAT[r["type"]]["name"] if r["type"] >= 0 else "подарок"
            if act == "wdok":
                c.execute("update wd set status='done' where id=?", (r["wid"],)); c.execute("update gifts set status='gone' where id=?", (r["gid"],))
                msg = "✅ Отмечено как выданное"; note = f"📤 «{nm}» отправлен вам в Telegram"
            else:
                c.execute("update wd set status='rejected' where id=?", (r["wid"],)); c.execute("update gifts set status='held' where id=?", (r["gid"],))
                c.execute("update users set bal=bal+? where id=?", (r["fee"] or 0, r["user"]))
                msg = "↩️ Отклонено: подарок возвращён в инвентарь, плата возвращена"; note = f"❌ Вывод «{nm}» отклонён. Подарок остался в инвентаре, {r['fee']} GRAM возвращены."
        c.execute("commit")
    except Exception as e:
        try: c.execute("rollback")
        except Exception: pass
        print("callback:", e, flush=True); msg = "Ошибка, смотри журнал"
    finally: c.close()
    try: tg("answerCallbackQuery", callback_query_id=cq["id"], text=msg[:190])
    except Exception: pass
    if msg.startswith(("✅", "↩️")):
        try:
            m = cq.get("message") or {}
            tg("editMessageText", chat_id=m["chat"]["id"], message_id=m["message_id"], text=(m.get("text") or "") + "\n\n" + msg)
        except Exception: pass
        notify(r["user"], note)

def process_withdrawals():
    bc = meta_get("bc")
    c = db()
    try:
        rows = c.execute("select w.id wid, w.user, w.fee, g.ogid, g.id gid, g.type, g.number from wd w join gifts g on g.id=w.gift where w.status='pending'").fetchall()
        for r in rows:
            nm = CAT[r["type"]]["name"] if r["type"] >= 0 else "подарок"
            manual = MANUAL_WITHDRAW or not bc
            if not manual:
                try:
                    p = {"business_connection_id": bc, "owned_gift_id": r["ogid"], "new_owner_chat_id": r["user"]}
                    if TRANSFER_STARS: p["star_count"] = TRANSFER_STARS
                    tg("transferGift", **p)
                    c.execute("update wd set status='done' where id=?", (r["wid"],))
                    c.execute("update gifts set status='gone' where id=?", (r["gid"],))
                    notify(r["user"], f"📤 «{nm}» отправлен вам в Telegram")
                    continue
                except Exception as e:
                    if "FORBIDDEN" in str(e).upper() or "RIGHT" in str(e).upper(): manual = True      # у бота нет прав — переходим на ручной вывод
                    else:
                        c.execute("update wd set status='error', err=? where id=?", (str(e)[:300], r["wid"]))
                        c.execute("update gifts set status='held' where id=?", (r["gid"],))
                        c.execute("update users set bal=bal+? where id=?", (r["fee"] or 0, r["user"]))   # возвращаем плату за вывод
                        print(f"[{stamp()}] вывод не удался: {nm} для {r['user']}: {e}", flush=True)
                        notify(r["user"], f"❌ Не удалось отправить «{nm}»: {e}. Подарок остался в инвентаре, {r['fee']} GRAM возвращены.")
                        continue
            c.execute("update wd set status='manual' where id=?", (r["wid"],))
            print(f"[{stamp()}] ✋ ручной вывод #{r['wid']}: {nm} #{r['number']} → {r['user']}", flush=True)
            _manual_request(r, nm, r["number"])
    finally: c.close()

def withdraw_loop():
    while True:
        time.sleep(5)
        try: process_withdrawals()
        except Exception as e: print("withdraw:", e)

def house_has(t):
    c = db()
    try: return c.execute("select 1 from gifts where status='house' and type=? limit 1", (t,)).fetchone() is not None
    finally: c.close()

def parse_payload(p):
    m = re.fullmatch(r"gu:(\d+):(\d+):[0-9a-f]+", p or "")
    return (int(m.group(1)), int(m.group(2))) if m else (None, None)

def parse_topup(p):
    m = re.fullmatch(r"tp:(\d+):(\d+):[0-9a-f]+", p or "")
    return (int(m.group(1)), int(m.group(2))) if m else (None, None)

def on_pre_checkout(q):
    tu, tn = parse_topup(q.get("invoice_payload"))
    if tu is not None:
        ok = q.get("currency") == "XTR" and tu == (q.get("from") or {}).get("id") and q.get("total_amount") == tn and STARS_MIN <= tn <= STARS_MAX
        try: tg("answerPreCheckoutQuery", pre_checkout_query_id=q["id"], ok=bool(ok), **({} if ok else {"error_message": "Неверный платёж"}))
        except Exception as e: print("pre_checkout:", e)
        return
    uid, t = parse_payload(q.get("invoice_payload"))
    err = None
    if q.get("currency") != "XTR" or uid is None or uid != (q.get("from") or {}).get("id") or not 0 <= t < len(CAT):
        err = "Неверный платёж"
    elif q.get("total_amount") != CAT[t].get("stars"): err = "Цена изменилась, открой магазин заново"
    elif not CAT[t].get("nw") and not house_has(t): err = "Этого подарка уже нет в наличии"
    try:
        if err: tg("answerPreCheckoutQuery", pre_checkout_query_id=q["id"], ok=False, error_message=err)
        else: tg("answerPreCheckoutQuery", pre_checkout_query_id=q["id"], ok=True)
    except Exception as e: print("pre_checkout:", e)

def on_stars_paid(m):
    sp = m["successful_payment"]; uid, t = parse_payload(sp.get("invoice_payload"))
    charge = sp.get("telegram_payment_charge_id")
    tu, tn = parse_topup(sp.get("invoice_payload"))
    if tu is not None:
        if tu != (m.get("from") or {}).get("id") or not charge or sp.get("total_amount") != tn: print("странный платёж", sp); return
        gm = round(tn / STARS_PER_GRAM, 4); c = db()
        try:
            c.execute("begin immediate")
            if c.execute("select 1 from star_pay where charge=?", (charge,)).fetchone(): c.execute("rollback"); return
            get_user(c, tu, (m.get("from") or {}).get("first_name", ""))
            c.execute("update users set bal=bal+? where id=?", (gm, tu))
            c.execute("insert into star_pay(charge,user,type,stars,ok) values(?,?,?,?,1)", (charge, tu, -1, tn))
            c.execute("insert into topup(user,ts,amount,kind,ref) values(?,?,?,?,?)", (tu, int(time.time()), gm, "stars", f"{tn} Stars"))
            rb = ref_bonus(c, tu, gm)
            c.execute("commit")
            if rb: notify(rb[0], f"💰 Реферальный доход: +{rb[1]:g} GRAM")
        except Exception:
            c.execute("rollback"); raise
        finally: c.close()
        print(f"[{stamp()}] ⭐ пополнение: {tu} +{gm} GRAM за {tn} звёзд", flush=True)
        notify(tu, f"✅ Баланс пополнен на {gm} GRAM ({tn} ⭐)")
        return
    if uid is None or uid != (m.get("from") or {}).get("id") or not 0 <= t < len(CAT) or not charge: print("странный платёж", sp); return
    x = CAT[t]; refund = False; c = db()
    try:
        c.execute("begin immediate")
        try:
            if c.execute("select 1 from star_pay where charge=?", (charge,)).fetchone(): c.execute("rollback"); return
            get_user(c, uid, (m.get("from") or {}).get("first_name", ""))
            if x.get("nw"):
                c.execute("insert into gifts(type,number,ogid,owner,status) values(?,?,?,?,'held')", (t, 0, "star:" + charge, uid))
            else:
                g = c.execute("select id from gifts where status='house' and type=? order by random() limit 1", (t,)).fetchone()
                if g: c.execute("update gifts set owner=?, status='held' where id=?", (uid, g["id"]))
                else: refund = True
            c.execute("insert into star_pay(charge,user,type,stars,ok) values(?,?,?,?,?)", (charge, uid, t, sp.get("total_amount") or 0, 0 if refund else 1))
            c.execute("commit")
        except Exception:
            c.execute("rollback"); raise
    finally: c.close()
    if refund:
        try:
            tg("refundStarPayment", user_id=uid, telegram_payment_charge_id=charge)
            notify(uid, f"❌ «{x['name']}» закончился. Звёзды ({sp.get('total_amount')}) возвращены.")
        except Exception as e: print("refund:", e); notify(uid, "❌ Подарок закончился, возврат звёзд оформит поддержка.")
    else: notify(uid, f"✅ «{x['name']}» куплен и добавлен в ваш инвентарь")

def process_deposits(txs):
    """Зачисляет входящие переводы GRAM на баланс по комментарию вида gu<id>. Один перевод зачисляется только один раз."""
    c = db()
    try:
        for tx in reversed(txs):                                  # от старых к новым
            m = tx.get("in_msg") or {}
            if not m.get("source"): continue                      # внешние сообщения не считаем
            try: value = int(m.get("value") or 0)
            except ValueError: continue
            h = (tx.get("transaction_id") or {}).get("hash")
            if value <= 0 or not h: continue
            amount, comment = value / 1e9, (m.get("message") or "").strip()
            mt = re.fullmatch(r"gu(\d+)", comment)
            uid = int(mt.group(1)) if mt else None
            c.execute("begin immediate")
            try:
                if c.execute("select 1 from deposits where hash=?", (h,)).fetchone():
                    c.execute("rollback"); continue
                ok = bool(uid is not None and amount >= MIN_DEPOSIT and c.execute("select 1 from users where id=?", (uid,)).fetchone())
                c.execute("insert into deposits(hash,user,amount,comment,src,credited) values(?,?,?,?,?,?)",
                          (h, uid, amount, comment[:100], m.get("source"), 1 if ok else 0))
                if ok:
                    c.execute("update users set bal=bal+? where id=?", (amount, uid))
                    src = m.get("source") or ""
                    c.execute("insert into topup(user,ts,amount,kind,ref) values(?,?,?,?,?)", (uid, int(time.time()), amount, "ton", (src[:6] + "..." + src[-5:]) if len(src) > 12 else src))
                    rb = ref_bonus(c, uid, amount)
                c.execute("commit")
            except Exception:
                c.execute("rollback"); raise
            if ok: notify(uid, f"✅ Баланс пополнен на {amount:g} GRAM")
            if ok and rb: notify(rb[0], f"💰 Реферальный доход: +{rb[1]:g} GRAM")
    finally: c.close()

def deposit_loop():
    if not DEPOSIT_ADDRESS: print("DEPOSIT_ADDRESS не задан — пополнение отключено"); return
    while True:
        try:
            r = httpx.get("https://toncenter.com/api/v2/getTransactions", params={"address": DEPOSIT_ADDRESS, "limit": 50},
                          headers={"X-API-Key": TONCENTER_KEY} if TONCENTER_KEY else {}, timeout=20).json()
            if r.get("ok"): process_deposits(r["result"])
        except Exception as e: print("deposits:", e)
        time.sleep(10)

def price_loop():   # необязательно: PRICE_SYNC=1 и pip install TelegramGifts
    from TelegramGifts import TelegramGifts
    api = TelegramGifts()
    while True:
        for x in CAT:
            if not x["key"] or x.get("fixed") or (x.get("custom") and _ub["cl"]): continue
            try:
                info = api.get_gift(x["key"]) or {}
                v = [p for k, p in (info.get("prices") or {}).items() if k.endswith("_price_ton") and isinstance(p, (int, float)) and p > 0]
                if v:
                    x["price"] = round(sum(v) / len(v), 2)
                    if x.get("custom"):
                        x["priced"] = True
                        c = db()
                        try: c.execute("update custom set price=?, psrc=1 where id=? and fixed=0", (x["price"], x["custom"]))
                        finally: c.close()
            except Exception as e: print("price:", x["key"], e)
        time.sleep(300)

def ga_loop():
    """Каждые 5 минут пробует оценить NFT-типы, у которых ещё «На оценке» (Gift Asset)."""
    time.sleep(20)
    while True:
        try:
            for x in list(CAT):
                if not x.get("custom") or x.get("fixed") or x.get("priced"): continue
                v = ga_floor(x["name"], x.get("model"))
                if v is None: continue
                with _cat_lock: x["price"] = v; x["vp"] = True; x["priced"] = True
                c = db()
                try: c.execute("update custom set price=?, psrc=1 where id=? and fixed=0", (v, x["custom"]))
                finally: c.close()
                print(f"[{stamp()}] 💰 цена {x['name']} / {x.get('model')} = {v} GRAM (Gift Asset)", flush=True)
        except Exception as e: print("ga_loop:", e, flush=True)
        time.sleep(300)

@app.on_event("startup")
def startup():
    init_db(); load_custom(); ub_start()
    threading.Thread(target=poll_loop, daemon=True).start()
    threading.Thread(target=withdraw_loop, daemon=True).start()
    threading.Thread(target=deposit_loop, daemon=True).start()
    if os.environ.get("PRICE_SYNC") == "1": threading.Thread(target=price_loop, daemon=True).start()
    if GAKEY: threading.Thread(target=ga_loop, daemon=True).start()
