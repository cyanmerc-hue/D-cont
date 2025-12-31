from functools import wraps
from flask import redirect, url_for, session

def customer_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not session.get("user_id"):
            return redirect(url_for("login"))
        # if admin logged in, block customer pages (optional)
        if session.get("role") == "admin":
            return redirect(url_for("admin_home"))
        return fn(*args, **kwargs)
    return wrapper
# Helper: ensure_logged_in (not a decorator)
def ensure_logged_in():
    if not session.get("user_id"):
        return redirect(url_for("login"))
    return None

import os
import requests
from flask import Flask, render_template, request, redirect, url_for, session, flash
from datetime import datetime, timezone
from werkzeug.security import generate_password_hash, check_password_hash


app = Flask(__name__)
app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "dev-only-change-me")

from functools import wraps

def require_login(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not session.get("user_id"):
            return redirect(url_for("login"))
        return fn(*args, **kwargs)
    return wrapper

def require_admin(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not session.get("user_id"):
            return redirect(url_for("login"))
        if session.get("role") != "admin":
            return redirect(url_for("login"))
        return fn(*args, **kwargs)
    return wrapper

def require_customer(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not session.get("user_id"):
            return redirect(url_for("login"))
        if session.get("role") == "admin":
            return redirect(url_for("admin_home"))
        return fn(*args, **kwargs)
    return wrapper

# --- PUBLIC PAGES ---
@app.route("/welcome")
def welcome():
    return render_template("welcome.html")

@app.route("/terms")
def terms():
    return render_template("terms.html")

@app.route("/how-it-works")
def how_it_works():
    return render_template("how_it_works.html")

@app.route("/setup")
def setup():
    return render_template("setup.html")

# --- CUSTOMER ROUTES (minimal, working) ---
@app.route("/home")
@app.route("/app")
@require_login
def app_home():
    return render_template("home_tab.html", tab="home")

@app.route("/profile")
def profile():
    guard = login_required()
    if guard: return guard
    return render_template("profile_tab.html", tab="profile")

@app.route("/groups")
def groups():
    guard = login_required()
    if guard: return guard
    return render_template("groups_tab.html", tab="groups")

@app.route("/payments")
def payments():
    guard = login_required()
    if guard: return guard
    return render_template("payments_tab.html", tab="payments")

@app.route("/rewards")
def rewards():
    guard = login_required()
    if guard: return guard
    return render_template("rewards_tab.html", tab="rewards")

@app.route("/chat")
def chat():
    guard = login_required()
    if guard: return guard
    return render_template("chat.html", tab="support")

@app.route("/transactions")
def transactions():
    guard = login_required()
    if guard: return guard
    return render_template("transactions.html")

@app.route("/support/whatsapp")
def support_whatsapp():
    guard = login_required()
    if guard: return guard
    return redirect("https://wa.me/")  # TODO: put your number here


# --- ADMIN GATE HELPER ---
def admin_required():
    if not session.get("user_id") or session.get("role") != "admin":
        return redirect(url_for("login"))
    return None


# --- REGISTER ROUTE ---
@app.route("/register", methods=["GET", "POST"])
def register():
    if request.method == "POST":
        email = (request.form.get("email") or "").strip().lower()
        password = (request.form.get("password") or "").strip()
        full_name = (request.form.get("full_name") or "").strip()
        phone = (request.form.get("phone") or "").strip()

        if not email or not password:
            flash("Email and password are required.")
            return redirect(url_for("register"))

        # 1) Create user in Supabase Auth
        r = supabase_signup(email, password)
        if not r.ok:
            try:
                msg = r.json()
            except Exception:
                msg = r.text
            flash(f"Registration failed: {msg}")
            return redirect(url_for("register"))

        data = r.json()
        user_id = (data.get("user") or {}).get("id")
        if not user_id:
            flash("Registration created but no user id returned. Check Supabase email confirmation setting.")
            return redirect(url_for("login"))

        # 2) Create profile row
        p = supabase_upsert_profile(user_id, email, full_name, phone, role="customer")
        if not p.ok:
            print("[PROFILE UPSERT ERROR]", p.status_code, p.text)

        flash("Account created. Please log in.")
        return redirect(url_for("login"))

    return render_template("register.html")

# --- MPIN SETUP ROUTE ---
@app.route("/mpin/setup", methods=["GET", "POST"])
@require_login
@customer_required
def mpin_setup():
    guard = login_required()
    if guard: return guard

    if request.method == "POST":
        mpin = request.form.get("mpin", "").strip()
        if not mpin:
            flash("Please enter an MPIN.")
            return redirect(url_for("mpin_setup"))
        import hashlib, datetime
        mpin_hash = hashlib.sha256(mpin.encode()).hexdigest()
        now = datetime.datetime.utcnow().isoformat()
        url = f"{SUPABASE_URL}/rest/v1/profiles?id=eq.{session['user_id']}"
        headers = {
            "apikey": SUPABASE_SERVICE_ROLE_KEY,
            "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
            "Content-Type": "application/json"
        }
        data = {"mpin_hash": mpin_hash, "mpin_set_at": now}
        requests.patch(url, headers=headers, json=data, timeout=30)
        flash("MPIN set successfully.")
        return redirect(url_for("home"))
    # TEMP user object to satisfy template
    user = {"upi_id": ""}
    return render_template("add_upi.html", user=user)


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

# --- SUPABASE HELPER FUNCTIONS ---
def supabase_signup(email: str, password: str):
    url = f"{SUPABASE_URL}/auth/v1/signup"
    headers = {"apikey": SUPABASE_ANON_KEY, "Content-Type": "application/json"}
    return requests.post(url, headers=headers, json={"email": email, "password": password}, timeout=30)

def supabase_upsert_profile(user_id: str, email: str, full_name: str = "", phone: str = "", role: str = "customer"):
    url = f"{SUPABASE_URL}/rest/v1/profiles"
    headers = {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }
    payload = {
        "id": user_id,
        "email": email,
        "full_name": full_name or None,
        "phone": phone or None,
        "role": role,
    }
    return requests.post(url, headers=headers, json=payload, timeout=30)

def supabase_get_profile(user_id: str):
    url = f"{SUPABASE_URL}/rest/v1/profiles"
    headers = {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
    }
    params = {"id": f"eq.{user_id}", "select": "id,email,full_name,phone,role,is_admin,mpin_hash,mpin_set_at"}
    return requests.get(url, headers=headers, params=params, timeout=30)

def supabase_set_mpin(user_id: str, mpin_hash: str):
    url = f"{SUPABASE_URL}/rest/v1/profiles?id=eq.{user_id}"
    headers = {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "mpin_hash": mpin_hash,
        "mpin_set_at": datetime.now(timezone.utc).isoformat()
    }
    return requests.patch(url, headers=headers, json=payload, timeout=30)

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
def root():
    if session.get("user_id"):
        if session.get("role") == "admin":
            return redirect(url_for("owner_dashboard"))
        return redirect(url_for("app_home"))
    return redirect(url_for("login"))

@app.route("/login", methods=["GET", "POST"])
def login():
    from flask import abort
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
        if is_admin:
            session["role"] = "admin"
            return redirect("/owner/dashboard")

        # customer
        session["role"] = "customer"
        # Check if MPIN is set in profiles
        prof = supabase_get_profile(session["user_id"])
        mpin_set = False
        if prof.ok:
            rows = prof.json()
            if rows and rows[0].get("mpin_hash"):
                mpin_set = True
        if not mpin_set:
            return redirect("/mpin/setup")
        return redirect("/home")
# --- LOGOUT ROUTE (ensure present) ---
@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))





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

