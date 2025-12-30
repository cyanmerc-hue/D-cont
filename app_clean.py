import os
import requests
from flask import Flask, render_template, request, redirect, url_for, session, flash

app = Flask(__name__)
app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "dev-only-change-me")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

def map_identifier_to_email(identifier: str) -> str:
    identifier = (identifier or "").strip()
    if identifier.isdigit() and 8 <= len(identifier) <= 15:
        return f"{identifier}@migrated.local"
    return identifier

def supabase_login(email: str, password: str):
    url = f"{SUPABASE_URL}/auth/v1/token?grant_type=password"
    headers = {"apikey": SUPABASE_ANON_KEY, "Content-Type": "application/json"}
    return requests.post(url, headers=headers, json={"email": email, "password": password}, timeout=30)

def supabase_is_admin(user_id: str) -> bool:
    url = f"{SUPABASE_URL}/rest/v1/profiles"
    headers = {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
    }
    params = {"id": f"eq.{user_id}", "select": "is_admin"}
    r = requests.get(url, headers=headers, params=params, timeout=30)
    print("[ADMIN CHECK]", r.status_code, r.text)
    if not r.ok:
        return False
    rows = r.json()
    return bool(rows and rows[0].get("is_admin") is True)

# Minimal translation helper so {{ t('...') }} in templates doesn't crash
@app.context_processor
def inject_t():
    def t(key, default=None):
        return default or key
    return {"t": t}

@app.route("/")
def home():
    return redirect(url_for("login"))

@app.route("/login", methods=["GET", "POST"])
def login():
    # Safety: ensure env vars exist
    missing = [k for k in ["SUPABASE_URL", "SUPABASE_ANON_KEY", "SUPABASE_SERVICE_ROLE_KEY"] if not os.getenv(k)]
    if missing:
        return f"Missing env vars: {', '.join(missing)}", 500

    if request.method == "GET":
        return render_template("login.html")

    identifier = (
        request.form.get("username")
        or request.form.get("identifier")
        or request.form.get("email")
        or request.form.get("phone")
        or ""
    ).strip()

    password = (request.form.get("password") or request.form.get("mpin") or "").strip()

    if not identifier or not password:
        flash("Please enter email/phone and password.")
        return redirect(url_for("login"))

    email = identifier if "@" in identifier else map_identifier_to_email(identifier)
    resp = supabase_login(email, password)

    if resp.ok:
        data = resp.json()
        user_id = data.get("user", {}).get("id")
        session.clear()
        session["user_id"] = user_id
        session["email"] = data.get("user", {}).get("email")
        session["username"] = identifier

        is_admin = supabase_is_admin(user_id)
        session["role"] = "admin" if is_admin else "customer"
        return redirect(url_for("admin_home" if is_admin else "app_home"))

    # show readable error
    try:
        d = resp.json()
        err = d.get("error_description") or d.get("msg") or d.get("message") or d.get("error") or "Invalid credentials"
    except Exception:
        err = resp.text or "Invalid credentials"
    flash(f"Login failed: {err}")
    return redirect(url_for("login"))

@app.route("/app")
def app_home():
    # placeholder customer landing
    if not session.get("user_id"):
        return redirect(url_for("login"))
    return "Logged in (customer)."



@app.route("/admin")
def admin_home():
    return redirect(url_for("owner_dashboard"))

@app.route("/owner/dashboard")
def owner_dashboard():
    if session.get("role") != "admin":
        return redirect(url_for("login"))
    return render_template("owner_dashboard.html")

@app.route("/owner/users")
def owner_users():
    if session.get("role") != "admin":
        return redirect(url_for("login"))
    return render_template("owner_users.html")

@app.route("/owner/groups")
def owner_groups():
    if session.get("role") != "admin":
        return redirect(url_for("login"))
    return render_template("owner_groups.html")

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

# Debug endpoints
@app.route("/debug/whoami")
def debug_whoami():
    return {
        "user_id": session.get("user_id"),
        "email": session.get("email"),
        "role": session.get("role"),
    }

@app.route("/debug/admincheck")
def debug_admincheck():
    uid = session.get("user_id")
    if not uid:
        return {"error": "not logged in"}, 401
    return {"user_id": uid, "is_admin": supabase_is_admin(uid)}
