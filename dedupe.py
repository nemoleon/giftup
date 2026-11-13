import sqlite3
# убирает дубликаты NFT на складе/в инвентарях: одинаковый тип и номер, лишние записи помечаются «ушёл»
c = sqlite3.connect("giftup.db")
rows = c.execute("select id,type,number,ogid,owner,status from gifts where status in ('house','held') and number>0 order by type,number,id").fetchall()
groups = {}
for r in rows: groups.setdefault((r[1], r[2]), []).append(r)
n = 0
for k, g in groups.items():
    if len(g) < 2: continue
    keep = next((r for r in g if not str(r[3]).startswith("win:")), g[0])      # оставляем запись с настоящим id подарка
    print("дубликат тип", k[0], "номер", k[1], "- оставляю запись", keep[0], "(" + keep[5] + ")")
    for r in g:
        if r[0] != keep[0]:
            c.execute("update gifts set status='gone', owner=null where id=?", (r[0],)); n += 1
            print("   убираю запись", r[0], "был:", r[5], "владелец:", r[4])
c.commit()
print("готово, убрано записей:", n)
