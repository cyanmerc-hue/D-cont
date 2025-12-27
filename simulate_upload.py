import sqlite3
import os

# Simulate a test document upload for user 'test' (change username as needed)
username = 'test'
doc_folder = 'uploads'
os.makedirs(doc_folder, exist_ok=True)

def fake_upload(doc_type, filename):
    # Create a dummy file
    path = os.path.join(doc_folder, filename)
    with open(path, 'w') as f:
        f.write(f"Fake {doc_type} document for {username}")
    return filename

conn = sqlite3.connect('users.db')
c = conn.cursor()

aadhaar_file = fake_upload('aadhaar', f'{username}_aadhaar_testfile.pdf')
pan_file = fake_upload('pan', f'{username}_pan_testfile.pdf')
passport_file = fake_upload('passport', f'{username}_passport_testfile.pdf')

c.execute("UPDATE users SET aadhaar_doc=?, pan_doc=?, passport_doc=? WHERE username=?", (aadhaar_file, pan_file, passport_file, username))
conn.commit()
conn.close()
print(f"Test documents uploaded for user: {username}")
