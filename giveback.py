import sqlite3, sys
# запуск: python giveback.py НОМЕР_NFT ID_ИГРОКА   (например: python giveback.py 81067 5996551936)
num, uid = int(sys.argv[1]), int(sys.argv[2])
c = sqlite3.connect("giftup.db")
r = c.execute("select id,status from gifts where number=?", (num,)).fetchall()
if len(r) != 1:
    print("найдено записей:", len(r), r); sys.exit(1)
c.execute("update gifts set owner=?, status='held' where id=?", (uid, r[0][0]))
c.commit()
print("готово: NFT", num, "теперь в инвентаре игрока", uid, "(было:", r[0][1] + ")")
