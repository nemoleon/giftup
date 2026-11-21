import sqlite3, sys
# запуск: python return_to_shop.py НОМЕР_NFT        — только показать, что с ним в базе
#         python return_to_shop.py НОМЕР_NFT go     — вернуть NFT на склад (в магазин); ожидающий вывод отклоняется, плата возвращается игроку
num = int(sys.argv[1]); go = len(sys.argv) > 2 and sys.argv[2] == "go"
c = sqlite3.connect("giftup.db"); c.row_factory = sqlite3.Row
rows = c.execute("select id,type,number,status,owner from gifts where number=?", (num,)).fetchall()
if len(rows) != 1: print("найдено записей:", len(rows), [tuple(r) for r in rows]); sys.exit(1)
g = rows[0]; print("в базе:", tuple(g))
wds = c.execute("select id,user,fee,status from wd where gift=?", (g["id"],)).fetchall()
for w in wds: print("заявка на вывод:", tuple(w))
if not go: print("ничего не менял. Чтобы вернуть на склад: python return_to_shop.py", num, "go"); sys.exit(0)
for w in wds:
    if w["status"] in ("manual", "pending", "wait", "new"):
        c.execute("update wd set status='rejected' where id=?", (w["id"],))
        if w["fee"]: c.execute("update users set bal=bal+? where id=?", (w["fee"], w["user"])); print("возвращено игроку", w["user"], w["fee"], "GRAM")
c.execute("update gifts set status='house', owner=0 where id=?", (g["id"],))
c.commit(); print("готово: NFT", num, "снова на складе (было:", g["status"] + ")")
