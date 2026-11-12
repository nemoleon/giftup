"""Один раз запускается на сервере: входит в Telegram под аккаунтом и сам дописывает TG_API_ID / TG_API_HASH / TG_SESSION в .env.
Сессия нигде не печатается. Запуск:  cd /opt/giftup && venv/bin/python tg_login.py"""
import os, re, sys, asyncio
from telethon import TelegramClient
from telethon.sessions import StringSession

ENV = os.environ.get("ENV_FILE", "/opt/giftup/.env")
api_id = input("api_id (число с my.telegram.org): ").strip()
api_hash = input("api_hash: ").strip()

async def main():
    cl = TelegramClient(StringSession(), int(api_id), api_hash)
    await cl.start()                                   # спросит телефон, код из Telegram и пароль 2FA, если он включён
    me = await cl.get_me()
    sess = cl.session.save()
    await cl.disconnect()
    keys = ("TG" + "\x5f" + "API" + "\x5f" + "ID", "TG" + "\x5f" + "API" + "\x5f" + "HASH", "TG" + "\x5f" + "SESSION")
    lines = [l for l in (open(ENV, encoding="utf-8").read().splitlines() if os.path.exists(ENV) else []) if not l.startswith(keys)]
    lines += [f"{keys[0]}={api_id}", f"{keys[1]}={api_hash}", f"{keys[2]}={sess}"]
    open(ENV, "w", encoding="utf-8").write("\n".join(lines) + "\n")
    os.chmod(ENV, 0o600)
    print(f"Готово: вошли как {me.first_name} (id {me.id}). Данные записаны в {ENV}. Теперь: systemctl restart giftup")
asyncio.run(main())
