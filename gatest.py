# Проверка ключа Gift Asset: python3 gatest.py   (ключ берётся из .env, переменная GAKEY; сам ключ не печатается)
import os, sys, json
try:
    import httpx
except ImportError:
    print("нет httpx: запускай через venv/bin/python gatest.py"); sys.exit(1)
key = ""
for p in (".env", "/opt/giftup/.env"):
    if os.path.exists(p):
        for ln in open(p, encoding="utf-8"):
            if ln.startswith("GAKEY="): key = ln.split("=", 1)[1].strip().strip('"').strip("'")
        if key: break
if not key: print("GAKEY не найден в .env"); sys.exit(1)
print("ключ найден, длина", len(key), "начало", key[:3])
name = sys.argv[1] if len(sys.argv) > 1 else "EasterEgg-1"
path = "/api/v1/gifts/get" + chr(95) + "gift" + chr(95) + "by" + chr(95) + "name"
hosts = ["https://giftasset.pro", "https://giftasset.gifts"]
hdrs = [("X-Api-Key", key), ("Authorization", "Bearer " + key), ("Authorization", key), ("api" + chr(45) + "key", key), ("X-API-KEY", key), ("apikey", key)]
ok = None
for h in hosts:
    for hn, hv in hdrs + [(None, None)]:
        try:
            if hn: r = httpx.get(h + path, params={"name": name}, headers={hn: hv}, timeout=15, follow_redirects=True)
            else: r = httpx.get(h + path, params={"name": name, "api" + chr(95) + "key": key}, timeout=15, follow_redirects=True)
        except Exception as e:
            print(h, hn or "query", "ошибка:", type(e).__name__); break
        ct = r.headers.get("content-type", "")
        print(h, hn or "query api" + chr(95) + "key", "->", r.status_code, ct[:20])
        if r.status_code == 200 and "json" in ct: ok = (h, hn, r); break
    if ok: break
if not ok: print("Ни одна комбинация не подошла."); sys.exit(2)
h, hn, r = ok
print("\nРАБОТАЕТ: хост", h, "заголовок", hn or "query")
d = r.json()
print("поля:", ", ".join(list(d.keys())[:25]) if isinstance(d, dict) else type(d))
for k in ("market" + chr(95) + "floor", "telegram" + chr(95) + "gift" + chr(95) + "name"):
    if isinstance(d, dict) and k in d: print(k, ":", json.dumps(d[k], ensure_ascii=False)[:400])
