import sqlite3

DB_PATH = 'users.db'
USER_ID = 'one'

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()


query = """
SELECT doc_type, file_path, status, uploaded_at 
FROM documents 
WHERE username=? 
ORDER BY uploaded_at DESC;
"""

cursor.execute(query, (USER_ID,))
documents = cursor.fetchall()

if not documents:
    print(f"No documents found for username '{USER_ID}'.")
else:
    print(f"Documents for username '{USER_ID}':\n")
    for doc in documents:
        print(f"Type: {doc[0]} | Path: {doc[1]} | Status: {doc[2]} | Uploaded: {doc[3]}")

if not documents:
    print(f"No documents found for user_id '{USER_ID}'.")
else:
    print(f"Documents for user_id '{USER_ID}':\n")
    for doc in documents:
        print(f"Type: {doc[0]} | Path: {doc[1]} | URL: {doc[2]} | Status: {doc[3]} | Created: {doc[4]}")

conn.close()
