@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

import os
import requests
from flask import Flask, render_template, request, redirect, url_for, session, flash

app = Flask(__name__)
app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "dev-only-change-me")


SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

# --- TRANSLATION DICTIONARY ---
TRANSLATIONS = {
    "en": {
        "login_title": "Login",
        "trust_notice_title": "Trust & Safety Notice",
        "trust_notice_line1": "Never share your MPIN or password with anyone.",
        "trust_notice_line2": "If someone asks, report it immediately.",
        "login_language": "Language",
        "login_mpin_title": "Login with MPIN",
        "login_mpin_help": "Enter your MPIN to continue.",
        "login_mpin_btn": "Login with MPIN",
        "login_or": "OR",
        "login_mobile_title": "Login with Mobile / Email",
        "login_mobile_label": "Mobile / Email",
        "login_password_label": "Password",
        "login_btn": "Login",
        "login_fingerprint_btn": "Login with Fingerprint",
        "login_fingerprint_help": "Use your device fingerprint if set up.",
        "login_admin_title": "Admin Login",
        "login_admin_user": "Admin Email",
        "login_admin_pw": "Admin Password",
        "login_admin_btn": "Admin Login",
        "login_no_account": "Don't have an account?",
        "login_register": "Register",
        "terms": "Terms & Conditions",
    }
}

@app.context_processor
def inject_t():
    def t(key, default=None):
        lang = (session.get("lang") or "en").lower()
        return TRANSLATIONS.get(lang, {}).get(key, default or key)
    return {"t": t}

# --- ADMIN OWNER ROUTES (risk, payments, settings, transactions, referrals) ---
def _admin_required():
    return session.get("role") == "admin"

@app.route("/owner/risk")
def owner_risk():
    if not _admin_required():
        return redirect(url_for("login"))
    try:
        return render_template("owner_risk.html")
    except Exception:
        return redirect(url_for("owner_dashboard"))

@app.route("/owner/payments")
def owner_payments():
    if not _admin_required():
        return redirect(url_for("login"))
    try:
        return render_template("owner_payments.html")
    except Exception:
        return redirect(url_for("owner_dashboard"))

@app.route("/owner/settings")
def owner_settings():
    if not _admin_required():
        return redirect(url_for("login"))
    try:
        return render_template("owner_settings.html")
    except Exception:
        return redirect(url_for("owner_dashboard"))

@app.route("/owner/transactions")
def owner_transactions():
    if not _admin_required():
        return redirect(url_for("login"))
    try:
        return render_template("owner_transactions.html")
    except Exception:
        return redirect(url_for("owner_dashboard"))

@app.route("/owner/referrals")
def owner_referrals():
    if not _admin_required():
        return redirect(url_for("login"))
    try:
        return render_template("owner_referrals.html")
    except Exception:
        return redirect(url_for("owner_dashboard"))

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

# Minimal translation helper using TRANSLATIONS dict
@app.context_processor
def inject_t():
    def t(key, default=None):
        lang = (session.get("lang") or "en").lower()
        return TRANSLATIONS.get(lang, {}).get(key, default or key)
    return {"t": t}

# Optional: Language switch route
@app.route("/set-lang/<lang>")
def set_lang(lang):
    session["lang"] = (lang or "en").lower()
    return redirect(request.referrer or url_for("login"))

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

