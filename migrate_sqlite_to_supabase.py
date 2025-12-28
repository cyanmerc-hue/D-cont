import os
import sqlite3
import secrets
import string
import requests
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")  # service_role secret
SQLITE_PATH = os.getenv("DCONT_DATABASE_PATH", "users.db")

if not SUPABASE_URL or not SERVICE_ROLE_KEY:
    raise SystemExit("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in .env")

def gen_temp_password(length: int = 14) -> str:
    alphabet = string.ascii_letters + string.digits + "!@#$%_-"
    return "".join(secrets.choice(alphabet) for _ in range(length))

def supabase_admin_create_user(email: str, temp_password: str):
    """
    Creates a Supabase Auth user via Admin API.
    Returns (user_id, error_text)
    """
    url = f"{SUPABASE_URL}/auth/v1/admin/users"
    headers = {
        "apikey": SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SERVICE_ROLE_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "email": email,
        "password": temp_password,
        "email_confirm": True,  # mark confirmed to avoid email confirmation blocking
    }
    r = requests.post(url, headers=headers, json=payload, timeout=30)
    if r.status_code in (200, 201):
        data = r.json()
        return data.get("id"), None

    # If user already exists, return None and error
    return None, f"{r.status_code}: {r.text}"

def supabase_upsert_profile(user_id: str, email: str, profile: dict):
    url = f"{SUPABASE_URL}/rest/v1/profiles"
    headers = {
        "apikey": SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SERVICE_ROLE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=representation",
    }
    payload = {
        "id": user_id,
        "email": email,
        "full_name": profile.get("full_name") or profile.get("name") or "",
        "mobile": profile.get("mobile") or "",
        "legacy_user_id": str(profile.get("id") or ""),
    }
    r = requests.post(url, headers=headers, json=payload, timeout=30)
    if not r.ok:
        raise RuntimeError(f"Profile upsert failed {r.status_code}: {r.text}")

def fetch_sqlite_users():
    """
    Adjust the SELECT fields if your SQLite table uses different column names.
    """
    conn = sqlite3.connect(SQLITE_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Use actual columns from users table.
    cur.execute("""
        SELECT
            id,
            email,
            full_name,
            mobile,
            city_state
        FROM users
        WHERE email IS NOT NULL AND TRIM(email) <> ''
    """)
    rows = cur.fetchall()
    conn.close()
    return rows

def main():
    rows = fetch_sqlite_users()
    print(f"Found {len(rows)} users in SQLite: {SQLITE_PATH}")

    created = 0
    skipped = 0
    failed = 0

    # Save temp passwords so you can share/reset (store securely!)
    out_path = "migration_temp_passwords.csv"
    with open(out_path, "w", encoding="utf-8") as out:
        out.write("email,temp_password,status,notes\n")

        for row in rows:
            email = (row["email"] or "").strip().lower()
            if "@" not in email:
                skipped += 1
                out.write(f"{email},,skipped,invalid email\n")
                continue

            temp_pw = gen_temp_password()
            user_id, err = supabase_admin_create_user(email, temp_pw)

            if user_id is None:
                # Often means "user already exists" → you can still upsert profile if you fetch the user id,
                # but simplest: mark as failed and handle manually.
                failed += 1
                out.write(f"{email},{temp_pw},failed,{err.replace(',', ' ')}\n")
                print(f"[FAILED] {email} -> {err}")
                continue

            # Upsert profile
            try:
                supabase_upsert_profile(user_id, email, dict(row))
            except Exception as e:
                failed += 1
                out.write(f"{email},{temp_pw},failed,profile upsert: {str(e).replace(',', ' ')}\n")
                print(f"[FAILED] profile {email} -> {e}")
                continue

            created += 1
            out.write(f"{email},{temp_pw},created,ok\n")
            print(f"[CREATED] {email} -> {user_id}")

    print("\nDone.")
    print(f"Created: {created} | Skipped: {skipped} | Failed: {failed}")
    print(f"Temp passwords saved to: {out_path}")
    print("Next: Ask users to log in with temp password and change it, OR implement a reset-password flow.")

if __name__ == "__main__":
    main()
