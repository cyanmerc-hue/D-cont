import sqlite3

conn = sqlite3.connect('users.db')
c = conn.cursor()
print("username\taadhaar_doc\tpan_doc\tpassport_doc")
for row in c.execute("SELECT username, aadhaar_doc, pan_doc, passport_doc FROM users;"):
    print(f"{row[0]}\t{row[1]}\t{row[2]}\t{row[3]}")
conn.close()
