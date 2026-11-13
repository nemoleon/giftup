import sqlite3, sys, datetime
# запуск: python giveback.py НОМЕР_NFT            — сам найдёт, кто его поставил и проиграл
#         python giveback.py НОМЕР_NFT ID_ИГРОКА — вернуть конкретному игроку
num = int(sys.argv[1])
c = sqlite3.connect("giftup.db")
r = c.execute("select id,status,owner,type from gifts where number=?", (num,)).fetchall()
if len(r) != 1:
    print("найдено записей:", len(r), r); sys.exit(1)
gid, status, owner, typ = r[0]
rows = c.execute("select user,ts,win from hist where no=? and want=? order by id desc", (num, typ)).fetchall()
names = {0: "проиграл ставку", 1: "выиграл", 2: "вывел"}
print("история NFT", num, ":")
for u, ts, w in rows:
    print("  ", datetime.datetime.fromtimestamp(ts).strftime("%d.%m %H:%M"), "игрок", u, names.get(w, w))
if len(sys.argv) > 2: uid = int(sys.argv[2])
else:
    lost = [u for u, ts, w in rows if w == 0]
    if not lost: print("не нашёл, кто проиграл этот NFT — укажи ID игрока вторым аргументом"); sys.exit(1)
    uid = lost[0]
c.execute("update gifts set owner=?, status='held' where id=?", (uid, gid))
c.commit()
print("готово: NFT", num, "теперь в инвентаре игрока", uid, "(было:", status + ")")
