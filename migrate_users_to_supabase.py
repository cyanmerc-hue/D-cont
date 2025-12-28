import os
import sqlite3
import secrets
import string
import requests
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
SQLITE_PATH = os.getenv("DCONT_DATABASE_PATH", "users.db")

if not SUPABASE_URL or not SERVICE_ROLE_KEY:
    raise SystemExit("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in .env")

def gen_temp_password(length: int = 14) -> str:
    alphabet = string.ascii_letters + string.digits + "!@#$%_-"
    return "".join(secrets.choice(alphabet) for _ in range(length))

def headers_admin():
    return {
        "apikey": SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SERVICE_ROLE_KEY}",
        "Content-Type": "application/json",
    }

def create_auth_user(email: str, temp_password: str):
    url = f"{SUPABASE_URL}/auth/v1/admin/users"
    payload = {"email": email, "password": temp_password, "email_confirm": True}
    r = requests.post(url, headers=headers_admin(), json=payload, timeout=30)
    if r.status_code in (200, 201):
        return r.json()["id"], None
    return None, (r.status_code, r.text)

def find_auth_user_by_email(email: str):
    """
    Reliable lookup: page through admin user list and match email.
    Works even when ?email= filter isn't supported on your project version.
    """
    url = f"{SUPABASE_URL}/auth/v1/admin/users"
    page = 1
    per_page = 200

    while page <= 20:  # up to 4000 users max scan (enough for now)
        r = requests.get(
            url,
            headers=headers_admin(),
            params={"page": page, "per_page": per_page},
            timeout=30,
        )
        if not r.ok:
            return None

        data = r.json()
        # Many setups return {"users":[...]} instead of [...]
        users = data.get("users") if isinstance(data, dict) else data
        if not users:
            return None

        for u in users:
            if (u.get("email") or "").strip().lower() == email.strip().lower():
                return u.get("id")

        # If fewer than per_page, we reached the end
        if isinstance(users, list) and len(users) < per_page:
            return None

        page += 1

    return None

def upsert_profile(user_id: str, row: dict):
    url = f"{SUPABASE_URL}/rest/v1/profiles"
    headers = {
        "apikey": SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SERVICE_ROLE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=representation",
    }

    payload = {
        "id": user_id,
        "email": (row.get("email") or "").strip().lower(),
        "full_name": row.get("full_name") or "",
        "mobile": row.get("mobile") or "",
        "city_state": row.get("city_state") or "",
        "role": row.get("role") or "user",
        "is_admin": True if (row.get("role") or "").lower() == "admin" else False,
        "legacy_user_id": str(row.get("id") or ""),
    }

    r = requests.post(url, headers=headers, json=payload, timeout=30)
    if not r.ok:
        raise RuntimeError(f"Profile upsert failed {r.status_code}: {r.text}")

def fetch_sqlite_users():
    conn = sqlite3.connect(SQLITE_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("""
        SELECT id, email, full_name, mobile, city_state, role
        FROM users
        WHERE email IS NOT NULL AND TRIM(email) <> ''
    """)
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows

def main():
    rows = fetch_sqlite_users()
    print(f"Found {len(rows)} users in SQLite: {SQLITE_PATH}")

    created_auth = 0
    linked_existing = 0
    profiles_ok = 0
    failed = 0

    out_path = "migration_temp_passwords.csv"
    with open(out_path, "w", encoding="utf-8") as out:
        out.write("email,temp_password,auth_status,profile_status,notes\n")

        for row in rows:
            email = (row.get("email") or "").strip().lower()
            if "@" not in email:
                failed += 1
                out.write(f"{email},,failed,failed,invalid email\n")
                continue

            temp_pw = gen_temp_password()
            user_id, err = create_auth_user(email, temp_pw)

            if user_id:
                created_auth += 1
                auth_status = "created"
            else:
                user_id = find_auth_user_by_email(email)
                if user_id:
                    linked_existing += 1
                    auth_status = "exists"
                else:
                    failed += 1
                    out.write(f"{email},{temp_pw},failed,failed,auth error: {err}\n")
                    print(f"[FAILED AUTH] {email} -> {err}")
                    continue

            try:
                upsert_profile(user_id, row)
                profiles_ok += 1
                out.write(f"{email},{temp_pw},{auth_status},ok,\n")
                print(f"[OK] {email}")
            except Exception as e:
                failed += 1
                out.write(f"{email},{temp_pw},{auth_status},failed,{str(e)}\n")

    print("\nDone.")
    print(f"Auth created: {created_auth}")
    print(f"Auth existing linked: {linked_existing}")
    print(f"Profiles upserted: {profiles_ok}")
    print(f"Failed: {failed}")
    print(f"Temp passwords saved to: {out_path}")

if __name__ == "__main__":
    main()
