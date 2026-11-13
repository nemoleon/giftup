# Вернуть все подарки, числящиеся у пользователей (held), на склад: venv/bin/python tostock.py
import sqlite3, os
db = os.environ.get("DB", "giftup.db")
c = sqlite3.connect(db)
before = c.execute("select status, count(*) from gifts group by status").fetchall()
n = c.execute("update gifts set status='house', owner=NULL where status='held'").rowcount
c.commit()
after = c.execute("select status, count(*) from gifts group by status").fetchall()
print("было:", before)
print("возвращено на склад:", n)
print("стало:", after)
