import sqlite3

con = sqlite3.connect('database.db')
csr = con.cursor()

# csr.execute("SELECT name FROM sqlite_master WHERE type='table';")
csr.execute("SELECT * FROM user;")
print(csr.fetchall())
