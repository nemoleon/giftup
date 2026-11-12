"""
GiftUp — сервер: вход через Telegram, база SQLite, честный расчёт буста на сервере,
приём и выдача подарков через бота, подключённого к Business-аккаунту (аккаунт-приёмник).

Запуск:  pip install fastapi uvicorn httpx
         BOT_TOKEN=... HOUSE_IDS=твой_tg_id uvicorn app:app --host 0.0.0.0 --port 8000
"""
import os, re, json, time, hmac, hashlib, secrets, sqlite3, threading, datetime
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
EDGE, MAXCH = 0.95, 95.0
DEPOSIT_ADDRESS = os.environ.get("DEPOSIT_ADDRESS", "")        # адрес кошелька, куда игроки шлют GRAM (отдельный кошелёк!)
TONCENTER_KEY = os.environ.get("TONCENTER_KEY", "")            # необязательно: ключ toncenter.com (снимает лимит 1 запрос/сек)
MIN_DEPOSIT = float(os.environ.get("MIN_DEPOSIT", "0.1"))      # минимальное пополнение, GRAM
PUBLIC_URL = os.environ.get("PUBLIC_URL", "").rstrip("/")      # https://твой-домен (для манифеста TON Connect)
WITHDRAW_FEE = float(os.environ.get("WITHDRAW_FEE", "0.4"))   # цена вывода подарка, GRAM
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
    create table if not exists meta(k text primary key, v text);""")
    try: c.execute("alter table wd add column fee real default 0")   # для старых баз
    except Exception: pass
    c.close()

def meta_get(k):
    c = db(); r = c.execute("select v from meta where k=?", (k,)).fetchone(); c.close()
    return r["v"] if r else None

def meta_set(k, v):
    c = db(); c.execute("insert into meta(k,v) values(?,?) on conflict(k) do update set v=excluded.v", (k, v)); c.close()

def get_user(c, uid, name=""):
    r = c.execute("select * from users where id=?", (uid,)).fetchone()
    if not r:
        c.execute("insert into users(id,name,seed) values(?,?,?)", (uid, name, secrets.token_hex(32)))
        r = c.execute("select * from users where id=?", (uid,)).fetchone()
    return r

def sha(s): return hashlib.sha256(s.encode()).hexdigest()

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
    inv = [{"id": r["id"], "t": r["type"], "no": r["number"]} for r in
           c.execute("select * from gifts where owner=? and status='held' and type>=0 order by id desc", (uid,))]
    stock = {r["type"]: r["n"] for r in c.execute(
        "select type, count(*) n from gifts where status='house' and type>=0 group by type")}
    return {"bal": u["bal"], "boosts": u["boosts"], "best": u["best"], "inv": inv, "stock": stock,
            "prices": [g["price"] for g in CAT], "seed_hash": sha(u["seed"]), "nonce": u["nonce"], "fee": WITHDRAW_FEE, "online": online_count(),
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
        return json.loads(pairs["user"])
    except Exception as e:
        print("initData отклонён:", repr(e), "длина:", len(data or ""))
        raise HTTPException(401, "bad init data")

def fail(c, msg):
    c.execute("rollback")
    return {"ok": False, "error": msg}

# ---------- API ----------
@app.get("/")
def index(request: Request):
    """Отдаёт приложение, подставляя адрес сервера и имя бота (из PUBLIC_URL и BOT_USERNAME)."""
    html = open(os.path.join(HERE, "index.html"), encoding="utf-8").read()
    base = PUBLIC_URL or f"{request.url.scheme}://{request.headers.get('host', '')}"
    html = html.replace('var API_URL="";', 'var API_URL="%s";' % base, 1)
    html = html.replace("ТВОЙ_БОТ", os.environ.get("BOT_USERNAME", "GiftUpNowbot"))
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
        ph = tg("getUserProfilePhotos", user_id=uid, limit=1)["result"]["photos"]
        fid = ph[0][-1 if len(ph[0]) < 2 else 1]["file_id"]
        path = tg("getFile", file_id=fid)["result"]["file_path"]
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
        link = tg("createInvoiceLink", title=x["name"][:32], description=("Подарок в инвентарь GiftUp: " + x["name"])[:255],
                  payload=f"gu:{uid}:{t}:{secrets.token_hex(4)}", currency="XTR",
                  prices=[{"label": x["name"][:32], "amount": int(x["stars"])}])
    except Exception as e:
        print("invoice:", e); return {"ok": False, "error": "Не удалось создать счёт, попробуй позже"}
    return {"ok": True, "link": link}

@app.get("/api/state")
def api_state(x_init_data: str = Header("")):
    usr = check_init(x_init_data)
    if too_fast(usr["id"], "state", 40, 60): return too_fast(usr["id"], "state", 40, 60)
    c = db()
    try:
        get_user(c, usr["id"], usr.get("first_name", ""))
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
    if extra < 0 or not 0 <= want < len(CAT) or CAT[want].get("nw"): return {"ok": False, "error": "Неверные данные"}
    if len(ids) > 1: return {"ok": False, "error": "Для улучшения можно выбрать только один подарок"}
    c = db()
    try:
        c.execute("begin immediate")
        u = get_user(c, uid, usr.get("first_name", ""))
        gifts = [c.execute("select * from gifts where id=? and owner=? and status='held'", (i, uid)).fetchone() for i in ids]
        if any(g is None or g["type"] < 0 for g in gifts): return fail(c, "Подарок не найден в инвентаре")
        if extra > u["bal"] + 1e-9: return fail(c, "Не хватает баланса")
        stake = extra + sum(CAT[g["type"]]["price"] for g in gifts)
        price = CAT[want]["price"]
        if stake <= 0: return fail(c, "Добавь ставку")
        if price <= stake: return fail(c, "Выбери подарок дороже своей ставки")
        target = c.execute("select * from gifts where type=? and status='house' order by random() limit 1", (want,)).fetchone()
        if not target: return fail(c, "Этого подарка сейчас нет в наличии")
        ch = min(MAXCH, stake / price * 100 * EDGE)
        # честный рандом: HMAC(server_seed, "id:nonce"); хэш seed виден игроку заранее
        hh = hmac.new(u["seed"].encode(), f'{uid}:{u["nonce"]}'.encode(), hashlib.sha256).digest()
        roll = int.from_bytes(hh[:8], "big") / 2**64 * 100
        win = abs(roll - 50) < ch / 2
        for g in gifts: c.execute("update gifts set owner=null, status='house' where id=?", (g["id"],))
        c.execute("update users set bal=bal-?, boosts=boosts+1, nonce=nonce+1 where id=?", (extra, uid))
        gift = None
        if win:
            c.execute("update gifts set owner=?, status='held' where id=?", (uid, target["id"]))
            c.execute("update users set best=max(best,?) where id=?", (price, uid))
            gift = {"id": target["id"], "t": want, "no": target["number"]}
        st = state(c, uid); c.execute("commit")
        return {"ok": True, "win": win, "roll": roll, "chance": ch, "gift": gift, "state": st}
    except Exception:
        try: c.execute("rollback")
        except Exception: pass
        raise
    finally: c.close()

@app.post("/api/free")
def api_free(x_init_data: str = Header("")):
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

@app.post("/api/withdraw")
def api_withdraw(body: dict, x_init_data: str = Header("")):
    usr = check_init(x_init_data); uid = usr["id"]
    if not hit(f"wd:{uid}", 5, 3600): return {"ok": False, "error": "Лимит выводов: не больше 5 в час"}
    c = db()
    try:
        c.execute("begin immediate")
        u = get_user(c, uid, usr.get("first_name", ""))
        g = c.execute("select * from gifts where id=? and owner=? and status='held'", (int(body.get("id", 0)), uid)).fetchone()
        if not g: return fail(c, "Подарок не найден")
        if g["type"] >= 0 and CAT[g["type"]].get("nw"): return fail(c, "Обычные подарки вывести нельзя")
        if u["bal"] + 1e-9 < WITHDRAW_FEE: return fail(c, f"Для вывода нужно {WITHDRAW_FEE} GRAM на балансе")
        c.execute("update users set bal=bal-? where id=?", (WITHDRAW_FEE, uid))
        c.execute("update gifts set status='out' where id=?", (g["id"],))
        c.execute("insert into wd(gift,user,fee) values(?,?,?)", (g["id"], uid, WITHDRAW_FEE))
        st = state(c, uid); c.execute("commit")
        return {"ok": True, "state": st}
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

def match(base, model):
    base, model, best = (base or "").lower(), (model or "").lower(), -1
    for i, x in enumerate(CAT):
        if not x["key"] or x["key"].lower() != base: continue
        if x.get("model") and x["model"].lower() == model: return i
        if not x.get("model") and best < 0: best = i
    return best

def stamp(): return datetime.datetime.now().strftime("%d.%m %H:%M:%S")

def on_message(m):
    fr = m.get("from") or {}
    who = f"{fr.get('id')} @{fr.get('username', '-')} {fr.get('first_name', '')}".strip()
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
    t = match(g.get("base_name"), (g.get("model") or {}).get("name"))
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
    if cur.rowcount:
        nm = CAT[t]["name"] if t >= 0 else g.get("base_name", "подарок")
        print(f"[{stamp()}] ↳ ЗАЧИСЛЕН: {nm} #{g.get('number')} → {'в склад (хозяин)' if house else 'в инвентарь ' + str(sender)}"
              + ("" if t >= 0 else "  ⚠ НЕ РАСПОЗНАН (нет в catalog.json)"), flush=True)
        if t < 0: notify(sender, f"⚠️ Ваш NFT «{nm}» #{g.get('number')} получен, но пока не распознан. Напиши в поддержку.")
        elif not house: notify(sender, f"✅ Ваш NFT «{nm}» #{g.get('number')} был зачислен в ваш инвентарь")

def poll_loop():
    offset = 0
    while True:
        try:
            for u in tg("getUpdates", offset=offset, timeout=50, allowed_updates=["business_connection", "business_message", "pre_checkout_query", "message"]):
                offset = u["update_id"] + 1
                bc = u.get("business_connection")
                if bc:
                    print(f"[{stamp()}] бизнес-подключение: user={bc['user']['id']} включено={bc.get('is_enabled')} права={bc.get('rights')}", flush=True)
                if bc and bc.get("is_enabled"):
                    meta_set("bc", bc["id"]); meta_set("bc_owner", str(bc["user"]["id"]))
                if u.get("business_message"): on_message(u["business_message"])
                if u.get("pre_checkout_query"): on_pre_checkout(u["pre_checkout_query"])
                if (u.get("message") or {}).get("successful_payment"): on_stars_paid(u["message"])
        except Exception as e:
            print("poll:", e); time.sleep(5)

def process_withdrawals():
    bc = meta_get("bc")
    if not bc: return
    c = db()
    try:
        rows = c.execute("select w.id wid, w.user, w.fee, g.ogid, g.id gid, g.type from wd w join gifts g on g.id=w.gift where w.status='pending'").fetchall()
        for r in rows:
            nm = CAT[r["type"]]["name"] if r["type"] >= 0 else "подарок"
            try:
                p = {"business_connection_id": bc, "owned_gift_id": r["ogid"], "new_owner_chat_id": r["user"]}
                if TRANSFER_STARS: p["star_count"] = TRANSFER_STARS
                tg("transferGift", **p)
                c.execute("update wd set status='done' where id=?", (r["wid"],))
                c.execute("update gifts set status='gone' where id=?", (r["gid"],))
                notify(r["user"], f"📤 «{nm}» отправлен вам в Telegram")
            except Exception as e:
                c.execute("update wd set status='error', err=? where id=?", (str(e)[:300], r["wid"]))
                c.execute("update gifts set status='held' where id=?", (r["gid"],))
                c.execute("update users set bal=bal+? where id=?", (r["fee"] or 0, r["user"]))   # возвращаем плату за вывод
                notify(r["user"], f"❌ Не удалось отправить «{nm}»: {e}. Подарок остался в инвентаре, {r['fee']} GRAM возвращены.")
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

def on_pre_checkout(q):
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
                if ok: c.execute("update users set bal=bal+? where id=?", (amount, uid))
                c.execute("commit")
            except Exception:
                c.execute("rollback"); raise
            if ok: notify(uid, f"✅ Баланс пополнен на {amount:g} GRAM")
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
            if not x["key"]: continue
            try:
                info = api.get_gift(x["key"])
                v = [p for k, p in (info.get("prices") or {}).items() if k.endswith("_price_ton") and isinstance(p, (int, float)) and p > 0]
                if v: x["price"] = round(sum(v) / len(v), 2)
            except Exception as e: print("price:", x["key"], e)
        time.sleep(300)

@app.on_event("startup")
def startup():
    init_db()
    threading.Thread(target=poll_loop, daemon=True).start()
    threading.Thread(target=withdraw_loop, daemon=True).start()
    threading.Thread(target=deposit_loop, daemon=True).start()
    if os.environ.get("PRICE_SYNC") == "1": threading.Thread(target=price_loop, daemon=True).start()
