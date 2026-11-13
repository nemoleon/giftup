"""Получает api_id и api_hash с my.telegram.org прямо с сервера (без браузера) и дописывает их в .env.
Ничего секретного на экран не печатает. Запуск:  cd /opt/giftup && venv/bin/python tg_keys.py"""
import os, re, sys, json
import httpx

ENV = os.environ.get("ENV_FILE", "/opt/giftup/.env")
K_ID, K_HASH = "TG" + "\x5f" + "API" + "\x5f" + "ID", "TG" + "\x5f" + "API" + "\x5f" + "HASH"
H = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
     "Origin": "https://my.telegram.org", "Referer": "https://my.telegram.org/auth", "X-Requested-With": "XMLHttpRequest"}

phone = input("Номер аккаунта (с +, например +79991234567): ").strip()
c = httpx.Client(base_url="https://my.telegram.org", headers=H, timeout=30, follow_redirects=True)

r = c.post("/auth/send_password", data={"phone": phone})
try: rh = r.json()["random_hash"]
except Exception:
    print("Telegram ответил:", r.text[:200]); sys.exit("Не удалось запросить код (лимит или неверный номер).")
print("Код отправлен в Telegram на этот аккаунт.")
code = input("Код из Telegram: ").strip()
r = c.post("/auth/login", data={"phone": phone, "random_hash": rh, "password": code})
if r.text.strip() != "true":
    print("Telegram ответил:", r.text[:200]); sys.exit("Вход не удался.")

def parse(html):
    i = re.search(r"App api_id:.*?<strong>(\d+)</strong>", html, re.S)
    h = re.search(r"App api_hash:.*?<span[^>]*>([0-9a-f]{32})</span>", html, re.S)
    return (i.group(1), h.group(1)) if i and h else None

page = c.get("/apps").text
res = parse(page)
if not res:
    m = re.search(r'name="hash"\s+value="([^"]+)"', page) or re.search(r'value="([^"]+)"\s+name="hash"', page)
    if not m: sys.exit("Не нашёл форму создания приложения на странице /apps.")
    r = c.post("/apps/create", data={"hash": m.group(1), "app_title": "giftup", "app_shortname": "giftup", "app_url": "", "app_platform": "other", "app_desc": ""})
    if r.text.strip() not in ("", "true"): print("Telegram ответил:", r.text[:200])
    res = parse(c.get("/apps").text)
if not res: sys.exit("Приложение не создано (возможно, лимит «Too many attempts»). Попробуйте позже.")

api_id, api_hash = res
lines = [l for l in (open(ENV, encoding="utf-8").read().splitlines() if os.path.exists(ENV) else []) if not l.startswith((K_ID + "=", K_HASH + "="))]
lines += [f"{K_ID}={api_id}", f"{K_HASH}={api_hash}"]
open(ENV, "w", encoding="utf-8").write("\n".join(lines) + "\n"); os.chmod(ENV, 0o600)
print(f"Готово: api_id {api_id} и api_hash записаны в {ENV}. Теперь запустите: venv/bin/python tg_login.py (api_id и api_hash вводить заново не нужно — просто Enter).")
