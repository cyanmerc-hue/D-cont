from flask import Flask, render_template, request, redirect, url_for, session, flash, abort, send_from_directory
import sqlite3
import os
import random
import uuid
from werkzeug.utils import secure_filename
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import date
from functools import wraps
from urllib.parse import quote
import smtplib
from email.message import EmailMessage
import re

app = Flask(__name__)
# In production (Render/Heroku/etc.), set SECRET_KEY as an environment variable.
app.secret_key = os.environ.get('SECRET_KEY', 'your_secret_key')
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Render's filesystem is ephemeral unless you attach a persistent disk.
# You can override these paths via env vars to point at a persistent mount.
DATABASE = os.environ.get('DCONT_DATABASE_PATH', os.path.join(BASE_DIR, 'users.db'))
# Store uploads outside /static by default so sensitive documents aren't publicly accessible.
# Override with DCONT_UPLOAD_FOLDER if you want a mounted disk path (recommended in production).
UPLOAD_FOLDER = os.environ.get('DCONT_UPLOAD_FOLDER', os.path.join(BASE_DIR, 'uploads'))
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# Demo admin identity (change these for your deployment)
ADMIN_USERNAME = os.environ.get('DCONT_ADMIN_USERNAME', 'cyanmerc')
ADMIN_PASSWORD = os.environ.get('DCONT_ADMIN_PASSWORD', 'Bond1010#')
ADMIN_MOBILE = os.environ.get('DCONT_ADMIN_MOBILE', '9999999999')

WHATSAPP_SUPPORT_NUMBER = '917506680031'  # +91 7506680031

ALLOWED_IMAGE_EXTENSIONS = {"png", "jpg", "jpeg", "webp", "gif"}
ALLOWED_DOCUMENT_EXTENSIONS = {"png", "jpg", "jpeg", "webp", "pdf"}


def _allowed_extension(filename: str, allowed: set[str]) -> bool:
    if not filename or '.' not in filename:
        return False
    ext = filename.rsplit('.', 1)[1].lower().strip()
    return ext in allowed


def _save_user_document(*, username: str, doc_type: str, file_storage) -> str:
    original = secure_filename(file_storage.filename or '')
    if not _allowed_extension(original, ALLOWED_DOCUMENT_EXTENSIONS):
        raise ValueError('Unsupported file type. Please upload PDF, JPG, PNG, or WEBP.')
    ext = original.rsplit('.', 1)[1].lower().strip()
    out_name = f"{username}_{doc_type}_{uuid.uuid4().hex}.{ext}"
    out_path = os.path.join(app.config['UPLOAD_FOLDER'], out_name)
    file_storage.save(out_path)
    return out_name

BOT_QUICK_REPLIES = [
    "What is D-CONT?",
    "How do groups work?",
    "How do I join a group?",
    "When do I pay and to whom?",
    "How is the receiver selected?",
    "What if someone doesn’t pay?",
    "I paid but it shows pending",
    "How to update my UPI ID",
    "How to upload UTR / proof",
    "How to leave a group",
    "Safety tips / avoid scams",
    "Contact support",
]


ADVANCED_HANDOFF_KEYWORDS = [
    "fraud",
    "scam",
    "cheated",
    "legal",
    "police",
    "receiver not confirming",
    "utr mismatch",
    "paid wrong",
    "paid wrong person",
    "otp not coming",
    "number changed",
    "lost access",
    "remove member",
    "refund",
    "replace member",
    "dispute",
    "complaint",
]


FAQ_INTENTS = [
    {
        'key': 'what_is',
        'triggers': ["what is d-cont", "what is d cont", "about", "what is this", "d-cont"],
        'answer': "D-CONT is a non-custodial savings/group contribution helper. It helps groups coordinate contributions, but payments happen directly between members via UPI.",
    },
    {
        'key': 'non_custodial',
        'triggers': ["hold my money", "custodial", "does d-cont hold", "does d cont hold"],
        'answer': "No. D-CONT never holds money. You pay directly to the selected member via UPI.",
    },
    {
        'key': 'how_groups_work',
        'triggers': ["how do groups work", "group work", "rosca", "how it works"],
        'answer': "You join a group with a fixed monthly amount. Each cycle, members contribute and one member receives, based on the group’s rules. D-CONT only helps track and coordinate—payments are member-to-member.",
    },
    {
        'key': 'join_group',
        'triggers': ["join group", "how do i join", "join"],
        'answer': "To join: open Home → choose ₹500 or ₹1000 → preview a group → add your UPI ID (required) → request to join. Your request will show as pending until approved.",
        'link': '/home',
    },
    {
        'key': 'pay_when_who',
        'triggers': ["when do i pay", "who do i pay", "pay to whom", "payment"],
        'answer': "On the due date, the group shares the receiver details (name + UPI). You pay directly to that member using UPI. D-CONT does not take payments.",
    },
    {
        'key': 'receiver_selected',
        'triggers': ["receiver selected", "who gets", "how is the receiver", "selection"],
        'answer': "Receiver selection depends on your group’s rules. If you’re unsure for your group, message support on WhatsApp and include the group name/amount.",
        'handoff': True,
    },
    {
        'key': 'missed_payment',
        'triggers': ["doesnt pay", "doesnt pay", "missed payment", "not paid"],
        'answer': "If someone misses a payment, avoid arguments in the group chat. For help handling missed contributions safely, contact support on WhatsApp.",
        'handoff': True,
    },
    {
        'key': 'paid_pending',
        'triggers': ["paid but", "shows pending", "paid pending", "pending"],
        'answer': "If you paid but it still shows pending: double-check you paid the correct receiver UPI and keep your UTR/reference ready. If it still doesn’t resolve, contact support on WhatsApp.",
        'handoff': True,
    },
    {
        'key': 'update_upi',
        'triggers': ["update upi", "change upi", "upi update", "upi id"],
        'answer': "You can update your UPI ID from the Add UPI screen.",
        'link': '/add-upi',
    },
    {
        'key': 'utr_proof',
        'triggers': ["utr", "proof", "screenshot", "reference number"],
        'answer': "UTR/proof upload isn’t available in the app yet. Please contact support on WhatsApp with your group name, amount, and UTR/reference.",
        'handoff': True,
    },
    {
        'key': 'leave_group',
        'triggers': ["leave group", "exit group", "remove me"],
        'answer': "Leaving a group may affect the cycle and other members. Please contact support on WhatsApp and we’ll guide you.",
        'handoff': True,
    },
    {
        'key': 'safety',
        'triggers': ["safety", "avoid scams", "tips", "safe"],
        'answer': "Safety tips: never share OTP/UPI PIN, verify receiver UPI ID before paying, and keep your UTR/reference. If anything feels suspicious, contact support on WhatsApp.",
    },
    {
        'key': 'contact',
        'triggers': ["contact", "support", "help", "talk to support", "whatsapp"],
        'answer': "This looks like something our team should handle. Tap below to chat on WhatsApp.",
        'handoff': True,
    },
]


def build_whatsapp_link(prefill_text: str) -> str:
    return f"https://wa.me/{WHATSAPP_SUPPORT_NUMBER}?text={quote(prefill_text)}"


def _smtp_configured() -> bool:
    return bool(
        os.environ.get('SMTP_HOST')
        and os.environ.get('SMTP_PORT')
        and os.environ.get('SMTP_USERNAME')
        and os.environ.get('SMTP_PASSWORD')
        and os.environ.get('SMTP_FROM')
    )


def send_login_otp_email(*, to_email: str, otp_code: str, mobile: str) -> None:
    """Send a login OTP to the user's registered email.

    Uses standard SMTP configuration from environment variables:
    SMTP_HOST, SMTP_PORT, SMTP_USERNAME, SMTP_PASSWORD, SMTP_FROM.
    Optional: SMTP_USE_TLS (default: true).
    """
    smtp_host = os.environ['SMTP_HOST']
    smtp_port = int(os.environ['SMTP_PORT'])
    smtp_username = os.environ['SMTP_USERNAME']
    smtp_password = os.environ['SMTP_PASSWORD']
    smtp_from = os.environ['SMTP_FROM']
    use_tls = (os.environ.get('SMTP_USE_TLS', 'true') or '').strip().lower() not in {'0', 'false', 'no'}

    message = EmailMessage()
    message['From'] = smtp_from
    message['To'] = to_email
    message['Subject'] = 'Your D-CONT login OTP'
    message.set_content(
        "Your D-CONT OTP is: {otp}\n\n"
        "Mobile: {mobile}\n\n"
        "If you did not request this OTP, you can ignore this email.\n".format(
            otp=otp_code,
            mobile=mobile,
        )
    )

    with smtplib.SMTP(smtp_host, smtp_port, timeout=20) as server:
        if use_tls:
            server.starttls()
        server.login(smtp_username, smtp_password)
        server.send_message(message)


def status_label(status: str) -> str:
    s = (status or '').strip().lower()
    if s == 'pending':
        return 'Pending approval'
    if s == 'joined':
        return 'Approved'
    if s == 'rejected':
        return 'Rejected'
    return '—'


def status_hint(status: str) -> str:
    s = (status or '').strip().lower()
    if s == 'pending':
        return 'Admin will review your request soon.'
    if s == 'joined':
        return 'You’re in. Watch for the group’s payment instructions.'
    if s == 'rejected':
        return 'If you think this is a mistake, contact support.'
    return ''


@app.template_filter('status_label')
def jinja_status_label_filter(value):
    return status_label(value)


@app.template_filter('status_hint')
def jinja_status_hint_filter(value):
    return status_hint(value)


@app.context_processor
def inject_support_links():
    return {
        'support_whatsapp_url': build_whatsapp_link('Hi D-CONT Support, I need help with: '),
    }


def message_needs_handoff(message: str) -> bool:
    msg = (message or "").lower()
    return any(k in msg for k in ADVANCED_HANDOFF_KEYWORDS)


def match_intent(message: str):
    msg = (message or "").strip().lower()
    if not msg:
        return None
    for intent in FAQ_INTENTS:
        for trig in intent.get('triggers', []):
            if trig in msg:
                return intent
    return None

USER_COLUMNS = {
    "username": "TEXT",
    "password": "TEXT",
    "mobile": "TEXT",
    "full_name": "TEXT",
    "language": "TEXT",
    "city_state": "TEXT",
    "photo": "TEXT",
    "email": "TEXT",
    "role": "TEXT",
    "upi_id": "TEXT",
    "onboarding_completed": "INTEGER",
    "app_fee_paid": "INTEGER",
    "trust_score": "INTEGER",
    "join_blocked": "INTEGER",
    "is_active": "INTEGER",
    # Customer KYC docs (stored as filenames in UPLOAD_FOLDER)
    "aadhaar_doc": "TEXT",
    "pan_doc": "TEXT",
    "passport_doc": "TEXT",
}


def is_user_active(username: str) -> bool:
    if not username:
        return False
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute('SELECT is_active FROM users WHERE username=?', (username,))
        row = c.fetchone()
    except sqlite3.OperationalError:
        conn.close()
        return True
    conn.close()
    if not row:
        return False
    return int(row[0] if row[0] is not None else 1) == 1


def enforce_active_session():
    username = session.get('username')
    if not username:
        return None
    if is_user_active(username):
        return None
    session.pop('username', None)
    session.pop('role', None)
    flash('Your account is blocked. Please contact support.')
    return redirect(url_for('login'))


def login_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if 'username' not in session:
            return redirect(url_for('login'))
        blocked_redirect = enforce_active_session()
        if blocked_redirect is not None:
            return blocked_redirect
        return fn(*args, **kwargs)

    return wrapper


def admin_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if 'username' not in session:
            return redirect(url_for('login'))
        blocked_redirect = enforce_active_session()
        if blocked_redirect is not None:
            return blocked_redirect
        if session.get('role') != 'admin':
            abort(403)
        return fn(*args, **kwargs)

    return wrapper


def get_user_row(username):
    conn = get_db()
    c = conn.cursor()
    c.execute(
        'SELECT username, full_name, mobile, language, city_state, email, role, upi_id, onboarding_completed, app_fee_paid, trust_score, join_blocked, is_active FROM users WHERE username=?',
        (username,),
    )
    row = c.fetchone()
    conn.close()
    if not row:
        return None
    return {
        'username': row[0],
        'full_name': row[1],
        'mobile': row[2],
        'language': row[3],
        'city_state': row[4],
        'email': row[5],
        'role': row[6] or 'customer',
        'upi_id': row[7] or '',
        'onboarding_completed': int(row[8] or 0),
        'app_fee_paid': int(row[9] if row[9] is not None else 0),
        'trust_score': int(row[10] if row[10] is not None else 50),
        'join_blocked': int(row[11] if row[11] is not None else 0),
        'is_active': int(row[12] if row[12] is not None else 1),
    }


def require_customer(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if 'username' not in session:
            return redirect(url_for('login'))
        blocked_redirect = enforce_active_session()
        if blocked_redirect is not None:
            return blocked_redirect
        if session.get('role') == 'admin':
            return redirect(url_for('dashboard'))
        return fn(*args, **kwargs)

    return wrapper


def join_group_with_status(group_id, username, status="joined"):
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT id, status FROM group_members WHERE group_id=? AND username=?', (group_id, username))
    existing = c.fetchone()
    if existing:
        conn.close()
        return existing[1] or 'joined'

    c.execute('INSERT INTO group_members (group_id, username, status) VALUES (?, ?, ?)', (group_id, username, status))
    conn.commit()
    conn.close()
    return status


def _mobile_candidates(raw: str):
    raw = (raw or '').strip()
    candidates = []
    if raw:
        candidates.append(raw)

    digits = re.sub(r'\D+', '', raw)
    if digits:
        candidates.append(digits)

        if digits.startswith('91') and len(digits) == 12:
            candidates.append(digits[2:])
            candidates.append('+91' + digits[2:])

        if digits.startswith('0') and len(digits) == 11:
            candidates.append(digits[1:])

        if len(digits) == 10:
            candidates.append('91' + digits)
            candidates.append('+91' + digits)

    # Deduplicate, preserve order
    deduped = []
    seen = set()
    for value in candidates:
        if value and value not in seen:
            seen.add(value)
            deduped.append(value)
    return deduped


def _normalize_mobile_digits(raw: str) -> str:
    raw = (raw or '').strip()
    digits = re.sub(r'\D+', '', raw)
    if not digits:
        return ''
    # Normalize common India formats to 10-digit mobile when possible
    if digits.startswith('91') and len(digits) == 12:
        return digits[2:]
    if digits.startswith('0') and len(digits) == 11:
        return digits[1:]
    if len(digits) == 10:
        return digits
    return digits
@app.route('/profile', methods=['GET', 'POST'])
@require_customer
def profile():
    username = session['username']
    conn = get_db()
    c = conn.cursor()
    c.execute(
        'SELECT full_name, mobile, upi_id, app_fee_paid, aadhaar_doc, pan_doc, passport_doc FROM users WHERE username=?',
        (username,),
    )
    row = c.fetchone()
    full_name = mobile = upi_id = None
    aadhaar_doc = pan_doc = passport_doc = None
    app_fee_paid = 0
    if row:
        full_name, mobile, upi_id, app_fee_paid, aadhaar_doc, pan_doc, passport_doc = row
    if request.method == 'POST':
        full_name = (request.form.get('full_name') or '').strip()
        upi_id = (request.form.get('upi_id') or '').strip()
        # Update profile fields
        c.execute('UPDATE users SET full_name=?, upi_id=? WHERE username=?', (full_name, upi_id, username))

        # Optional: handle document uploads
        doc_fields = [
            ('aadhaar', 'aadhaar_doc', 'aadhaar_file'),
            ('pan', 'pan_doc', 'pan_file'),
            ('passport', 'passport_doc', 'passport_file'),
        ]
        for doc_type, column, form_key in doc_fields:
            file_obj = request.files.get(form_key)
            if not file_obj or not (file_obj.filename or '').strip():
                continue
            try:
                saved_name = _save_user_document(username=username, doc_type=doc_type, file_storage=file_obj)
            except ValueError as e:
                conn.close()
                flash(str(e))
                return redirect(url_for('profile'))
            c.execute(f'UPDATE users SET {column}=? WHERE username=?', (saved_name, username))
            if column == 'aadhaar_doc':
                aadhaar_doc = saved_name
            elif column == 'pan_doc':
                pan_doc = saved_name
            elif column == 'passport_doc':
                passport_doc = saved_name
        conn.commit()
        flash('Profile updated!')
    conn.close()
    return render_template(
        'profile_tab.html',
        full_name=full_name or '',
        mobile=mobile or '',
        upi_id=upi_id or '',
        app_fee_paid=int(app_fee_paid or 0),
        aadhaar_doc=aadhaar_doc or '',
        pan_doc=pan_doc or '',
        passport_doc=passport_doc or '',
        active_tab='profile',
    )


@app.route('/profile/doc/<doc_type>')
@require_customer
def profile_doc(doc_type: str):
    doc_type = (doc_type or '').strip().lower()
    column_by_type = {
        'aadhaar': 'aadhaar_doc',
        'pan': 'pan_doc',
        'passport': 'passport_doc',
    }
    if doc_type not in column_by_type:
        abort(404)
    username = session['username']
    conn = get_db()
    c = conn.cursor()
    col = column_by_type[doc_type]
    try:
        c.execute(f'SELECT {col} FROM users WHERE username=?', (username,))
        row = c.fetchone()
    except sqlite3.OperationalError:
        row = None
    conn.close()
    if not row or not row[0]:
        abort(404)
    filename = str(row[0])
    return send_from_directory(app.config['UPLOAD_FOLDER'], filename, as_attachment=True)

def get_db():
    # If using a mounted disk path like /var/data/users.db on Render,
    # ensure the directory exists.
    try:
        db_dir = os.path.dirname(DATABASE)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
    except OSError:
        pass
    conn = sqlite3.connect(DATABASE)
    return conn

def init_db():
    conn = get_db()
    c = conn.cursor()

    # Create tables if they don't exist
    c.execute('''CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS groups (id INTEGER PRIMARY KEY, name TEXT, description TEXT, monthly_amount INTEGER)''')
    c.execute('''CREATE TABLE IF NOT EXISTS group_members (id INTEGER PRIMARY KEY, group_id INTEGER, username TEXT, status TEXT)''')

    # Ensure groups table has monthly_amount column (auto-migration)
    c.execute("PRAGMA table_info(groups)")
    existing_group_cols = {row[1] for row in c.fetchall()}
    if "monthly_amount" not in existing_group_cols:
        c.execute("ALTER TABLE groups ADD COLUMN monthly_amount INTEGER")

    # Extra group fields used by the 4-tab UI
    if "max_members" not in existing_group_cols:
        c.execute("ALTER TABLE groups ADD COLUMN max_members INTEGER")
    if "receiver_name" not in existing_group_cols:
        c.execute("ALTER TABLE groups ADD COLUMN receiver_name TEXT")
    if "receiver_upi" not in existing_group_cols:
        c.execute("ALTER TABLE groups ADD COLUMN receiver_upi TEXT")
    if "status" not in existing_group_cols:
        c.execute("ALTER TABLE groups ADD COLUMN status TEXT")
    if "is_paused" not in existing_group_cols:
        c.execute("ALTER TABLE groups ADD COLUMN is_paused INTEGER")

    # Ensure users table has required columns (auto-migration)
    c.execute("PRAGMA table_info(users)")
    existing_cols = {row[1] for row in c.fetchall()}  # row[1] = column name
    for col_name, col_type in USER_COLUMNS.items():
        if col_name not in existing_cols:
            c.execute(f"ALTER TABLE users ADD COLUMN {col_name} {col_type}")

    # Settings table for owner controls
    c.execute('''CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT
    )''')

    # Ensure group_members has status column (auto-migration)
    c.execute("PRAGMA table_info(group_members)")
    existing_member_cols = {row[1] for row in c.fetchall()}
    if "status" not in existing_member_cols:
        c.execute("ALTER TABLE group_members ADD COLUMN status TEXT")

    # Backfill onboarding + membership status
    # The 4-tab UI doesn't require onboarding; default existing users to completed.
    c.execute("UPDATE users SET onboarding_completed=1 WHERE onboarding_completed IS NULL")
    c.execute("UPDATE users SET app_fee_paid=0 WHERE app_fee_paid IS NULL")
    c.execute("UPDATE users SET trust_score=50 WHERE trust_score IS NULL")
    c.execute("UPDATE users SET join_blocked=0 WHERE join_blocked IS NULL")
    c.execute("UPDATE users SET is_active=1 WHERE is_active IS NULL")
    c.execute("UPDATE group_members SET status='joined' WHERE status IS NULL OR status='' ")
    c.execute("UPDATE groups SET is_paused=0 WHERE is_paused IS NULL")

    # Backfill roles for existing users
    c.execute("UPDATE users SET role='customer' WHERE role IS NULL OR role='' ")

    # Ensure username/mobile uniqueness (best-effort; may fail if duplicates already exist)
    try:
        c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_users_username ON users(username)")
    except sqlite3.OperationalError:
        pass
    try:
        c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_users_mobile ON users(mobile)")
    except sqlite3.OperationalError:
        pass

    # Ensure a demo admin exists (username/password)
    try:
        admin_password_hash = generate_password_hash(ADMIN_PASSWORD)

        # Prefer an exact username match
        c.execute('SELECT id, mobile FROM users WHERE username=?', (ADMIN_USERNAME,))
        row = c.fetchone()
        if row:
            c.execute(
                'UPDATE users SET role=\'admin\', is_active=1, password=?, mobile=COALESCE(NULLIF(mobile, \'\'), ?) WHERE username=?',
                (admin_password_hash, ADMIN_MOBILE, ADMIN_USERNAME),
            )
        else:
            # Fallback: if a user exists with the admin mobile, upgrade it and set username (best-effort)
            c.execute('SELECT id, username FROM users WHERE mobile=?', (ADMIN_MOBILE,))
            mobile_row = c.fetchone()
            if mobile_row:
                user_id, existing_username = mobile_row
                if existing_username != ADMIN_USERNAME:
                    try:
                        c.execute('UPDATE users SET username=? WHERE id=?', (ADMIN_USERNAME, user_id))
                    except sqlite3.IntegrityError:
                        pass
                c.execute(
                    'UPDATE users SET role=\'admin\', is_active=1, password=? WHERE id=?',
                    (admin_password_hash, user_id),
                )
            else:
                c.execute(
                    'INSERT INTO users (username, password, mobile, full_name, language, city_state, email, role, is_active) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
                    (ADMIN_USERNAME, admin_password_hash, ADMIN_MOBILE, 'Owner', 'English', '', '', 'admin', 1),
                )
    except sqlite3.OperationalError:
        pass

    # Seed / update default groups
    c.execute('SELECT COUNT(1) FROM groups')
    group_count = (c.fetchone() or [0])[0]
    if group_count == 0:
        c.execute(
            'INSERT INTO groups (name, description, monthly_amount, status, is_paused) VALUES (?, ?, ?, ?, ?)',
            ("Pilot Group 2026", "Monthly savings group", 500, 'formation', 0),
        )
        c.execute(
            'INSERT INTO groups (name, description, monthly_amount, status, is_paused) VALUES (?, ?, ?, ?, ?)',
            ("Pilot Group 2 2026", "Monthly savings group", 1000, 'formation', 0),
        )
    else:
        # Best-effort updates for existing seeded groups
        c.execute(
            'UPDATE groups SET name=?, monthly_amount=? WHERE name=?',
            ("Pilot Group 2026", 500, "ROSCA Group 1"),
        )
        c.execute(
            'UPDATE groups SET name=?, monthly_amount=? WHERE name=?',
            ("Pilot Group 2 2026", 1000, "ROSCA Group 2"),
        )
        # Fallback: update first two rows if they have no amount set
        c.execute('UPDATE groups SET name=?, monthly_amount=? WHERE id=1 AND (monthly_amount IS NULL OR monthly_amount=0)', ("Pilot Group 2026", 500))
        c.execute('UPDATE groups SET name=?, monthly_amount=? WHERE id=2 AND (monthly_amount IS NULL OR monthly_amount=0)', ("Pilot Group 2 2026", 1000))

    conn.commit()
    conn.close()


def get_setting(key: str, default: str = "") -> str:
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute('SELECT value FROM settings WHERE key=?', (key,))
        row = c.fetchone()
    except sqlite3.OperationalError:
        row = None
    conn.close()
    if not row or row[0] is None:
        return default
    return str(row[0])


def set_setting(key: str, value: str) -> None:
    conn = get_db()
    c = conn.cursor()
    c.execute('INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value', (key, str(value)))
    conn.commit()
    conn.close()


def is_join_blocked(username: str) -> bool:
    if not username:
        return False
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute('SELECT join_blocked FROM users WHERE username=?', (username,))
        row = c.fetchone()
    except sqlite3.OperationalError:
        row = None
    conn.close()
    if not row:
        return False
    return int(row[0] if row[0] is not None else 0) == 1


# Ensure DB is ready when imported by WSGI servers (e.g., Gunicorn)
try:
    init_db()
except Exception:
    # Best-effort: the app will surface DB errors on requests if init fails.
    pass

@app.route('/terms')
def terms():
    # Using a fixed, explicit format for clarity.
    last_updated = date.today().strftime('%d/%m/%Y')
    return render_template('terms.html', last_updated=last_updated)

@app.route('/')
def home():
    if 'username' in session:
        if session.get('role') == 'admin':
            return redirect(url_for('dashboard'))
        user = get_user_row(session['username'])
        if not user:
            return redirect(url_for('logout'))
        return redirect(url_for('home_tab'))
    return redirect(url_for('login'))
@app.route('/dashboard')
@login_required
def dashboard():
    # Legacy route: owner/admin UI lives under /owner/*
    if session.get('role') != 'admin':
        return redirect(url_for('home'))
    return redirect(url_for('owner_dashboard'))


@app.route('/owner/dashboard')
@admin_required
def owner_dashboard():
    conn = get_db()
    c = conn.cursor()

    c.execute("SELECT COUNT(*) FROM users WHERE COALESCE(NULLIF(role,''), 'customer') != 'admin'")
    total_users = int((c.fetchone() or [0])[0] or 0)

    c.execute("SELECT COUNT(*) FROM users WHERE COALESCE(NULLIF(role,''), 'customer') != 'admin' AND COALESCE(app_fee_paid,0)=1")
    app_fee_paid_count = int((c.fetchone() or [0])[0] or 0)

    app_fee_amount_raw = (get_setting('app_fee_amount', '0') or '0').strip()
    try:
        app_fee_amount = int(app_fee_amount_raw)
    except ValueError:
        app_fee_amount = 0
    app_fee_collected = app_fee_paid_count * max(app_fee_amount, 0)

    c.execute(
        """
        SELECT g.id,
               COALESCE(g.max_members, 10) as max_members,
               COALESCE(g.status, '') as status,
               COALESCE(g.is_paused, 0) as is_paused,
               COUNT(gm.id) as joined_members
        FROM groups g
        LEFT JOIN group_members gm ON gm.group_id = g.id AND gm.status='joined'
        GROUP BY g.id, g.max_members, g.status, g.is_paused
        """
    )
    group_rows = c.fetchall()

    active_groups = 0
    formation_groups = 0
    completed_groups = 0
    for _gid, max_members, status_raw, is_paused, joined_members in group_rows:
        if int(is_paused or 0) == 1:
            continue
        status = (status_raw or '').strip().lower()
        if status == 'completed':
            completed_groups += 1
        elif status == 'active':
            active_groups += 1
        elif status == 'formation':
            formation_groups += 1
        else:
            if int(joined_members or 0) >= int(max_members or 10):
                active_groups += 1
            else:
                formation_groups += 1

    defaults_this_month = 0  # Placeholder until contribution tracking exists

    conn.close()
    return render_template(
        'owner_dashboard.html',
        active_owner_tab='dashboard',
        total_users=total_users,
        active_groups=active_groups,
        formation_groups=formation_groups,
        completed_groups=completed_groups,
        defaults_this_month=defaults_this_month,
        app_fee_amount=app_fee_amount,
        app_fee_paid_count=app_fee_paid_count,
        app_fee_collected=app_fee_collected,
    )


@app.route('/owner/users')
@admin_required
def owner_users():
    conn = get_db()
    c = conn.cursor()

    c.execute(
        """
        SELECT username, full_name, mobile, COALESCE(NULLIF(role,''),'customer') as role,
               COALESCE(is_active,1) as is_active,
               COALESCE(join_blocked,0) as join_blocked,
               COALESCE(trust_score,50) as trust_score,
               COALESCE(app_fee_paid,0) as app_fee_paid
        FROM users
        ORDER BY id DESC
        """
    )
    users_rows = c.fetchall()

    c.execute(
        """
        SELECT g.id,
               COALESCE(g.max_members,10) as max_members,
               COALESCE(g.status,'') as status,
               COALESCE(g.is_paused,0) as is_paused,
               COUNT(gm.id) as joined_members
        FROM groups g
        LEFT JOIN group_members gm ON gm.group_id=g.id AND gm.status='joined'
        GROUP BY g.id, g.max_members, g.status, g.is_paused
        """
    )
    group_meta = {}
    for gid, max_members, status_raw, is_paused, joined_members in c.fetchall():
        status = (status_raw or '').strip().lower()
        if int(is_paused or 0) == 1:
            label = 'paused'
        elif status in {'active', 'formation', 'completed'}:
            label = status
        else:
            label = 'active' if int(joined_members or 0) >= int(max_members or 10) else 'formation'
        group_meta[int(gid)] = label

    c.execute("SELECT username, group_id FROM group_members WHERE status='joined'")
    memberships = c.fetchall()
    per_user_group_ids = {}
    for uname, gid in memberships:
        if not uname:
            continue
        per_user_group_ids.setdefault(uname, []).append(int(gid))

    users = []
    for username, full_name, mobile, role, is_active, join_blocked, trust_score, app_fee_paid in users_rows:
        group_ids = per_user_group_ids.get(username, [])
        active_group_count = sum(1 for gid in group_ids if group_meta.get(gid) == 'active')
        flags = []
        if int(join_blocked or 0) == 1:
            flags.append('Future frozen')
        users.append(
            {
                'username': username,
                'full_name': full_name or '',
                'mobile': mobile or '',
                'role': role or 'customer',
                'is_active': int(is_active or 1),
                'join_blocked': int(join_blocked or 0),
                'trust_score': int(trust_score if trust_score is not None else 50),
                'active_groups': int(active_group_count),
                'flags': ', '.join(flags) if flags else '—',
                'app_fee_paid': int(app_fee_paid or 0),
            }
        )

    conn.close()
    return render_template('owner_users.html', active_owner_tab='users', users=users)


@app.route('/owner/users/<path:username>')
@admin_required
def owner_user_profile(username):
    username = (username or '').strip()
    conn = get_db()
    c = conn.cursor()
    c.execute(
        """
        SELECT username, full_name, mobile, email,
               COALESCE(NULLIF(role,''),'customer') as role,
               COALESCE(is_active,1) as is_active,
               COALESCE(join_blocked,0) as join_blocked,
               COALESCE(trust_score,50) as trust_score,
               COALESCE(app_fee_paid,0) as app_fee_paid,
               COALESCE(upi_id,'') as upi_id
        FROM users WHERE username=?
        """,
        (username,),
    )
    row = c.fetchone()
    if not row:
        conn.close()
        flash('User not found.')
        return redirect(url_for('owner_users'))

    c.execute(
        """
        SELECT g.id, g.name, g.monthly_amount, COALESCE(g.status,'') as status, COALESCE(g.is_paused,0) as is_paused
        FROM group_members gm
        JOIN groups g ON g.id = gm.group_id
        WHERE gm.username=? AND gm.status='joined'
        ORDER BY g.monthly_amount, g.id
        """,
        (username,),
    )
    groups = [
        {
            'id': r[0],
            'name': r[1],
            'monthly_amount': r[2],
            'status': ('Paused' if int(r[4] or 0) == 1 else ((r[3] or '').strip().capitalize() if (r[3] or '').strip() else '—')),
        }
        for r in c.fetchall()
    ]
    conn.close()

    user = {
        'username': row[0],
        'full_name': row[1] or '',
        'mobile': row[2] or '',
        'email': row[3] or '',
        'role': row[4] or 'customer',
        'is_active': int(row[5] or 1),
        'join_blocked': int(row[6] or 0),
        'trust_score': int(row[7] if row[7] is not None else 50),
        'app_fee_paid': int(row[8] or 0),
        'upi_id': row[9] or '',
    }
    return render_template('owner_user_profile.html', active_owner_tab='users', user=user, groups=groups)


@app.route('/owner/users/toggle_join_block', methods=['POST'])
@admin_required
def owner_toggle_join_block():
    target_username = (request.form.get('username') or '').strip()
    if not target_username:
        flash('Missing username.')
        return redirect(url_for('owner_users'))
    if target_username == session.get('username'):
        flash('You cannot change your own access.')
        return redirect(url_for('owner_users'))

    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT COALESCE(NULLIF(role,\'\'),\'customer\') FROM users WHERE username=?', (target_username,))
    row = c.fetchone()
    if not row:
        conn.close()
        flash('User not found.')
        return redirect(url_for('owner_users'))
    if (row[0] or '').strip().lower() == 'admin':
        conn.close()
        flash('Admin users cannot be modified here.')
        return redirect(url_for('owner_users'))

    c.execute('UPDATE users SET join_blocked = CASE WHEN COALESCE(join_blocked,0)=1 THEN 0 ELSE 1 END WHERE username=?', (target_username,))
    conn.commit()
    conn.close()
    flash('User updated.')
    return redirect(url_for('owner_users'))


@app.route('/owner/users/update_trust', methods=['POST'])
@admin_required
def owner_update_trust():
    target_username = (request.form.get('username') or '').strip()
    trust_raw = (request.form.get('trust_score') or '').strip()
    try:
        trust = int(trust_raw)
    except ValueError:
        trust = 50
    trust = max(0, min(100, trust))

    if not target_username:
        flash('Missing username.')
        return redirect(url_for('owner_users'))
    if target_username == session.get('username'):
        flash('You cannot change your own trust score.')
        return redirect(url_for('owner_users'))

    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT COALESCE(NULLIF(role,\'\'),\'customer\') FROM users WHERE username=?', (target_username,))
    row = c.fetchone()
    if not row:
        conn.close()
        flash('User not found.')
        return redirect(url_for('owner_users'))
    if (row[0] or '').strip().lower() == 'admin':
        conn.close()
        flash('Admin users cannot be modified here.')
        return redirect(url_for('owner_users'))

    c.execute('UPDATE users SET trust_score=? WHERE username=?', (trust, target_username))
    conn.commit()
    conn.close()
    flash('Trust score updated.')
    return redirect(url_for('owner_users'))


@app.route('/owner/groups')
@admin_required
def owner_groups():
    conn = get_db()
    c = conn.cursor()
    c.execute(
        """
        SELECT g.id, g.name, g.description, g.monthly_amount,
               COALESCE(g.max_members,10) as max_members,
               COALESCE(g.receiver_name,'') as receiver_name,
               COALESCE(g.receiver_upi,'') as receiver_upi,
               COALESCE(g.status,'') as status,
               COALESCE(g.is_paused,0) as is_paused,
               COUNT(gm.id) as joined_members
        FROM groups g
        LEFT JOIN group_members gm ON gm.group_id=g.id AND gm.status='joined'
        GROUP BY g.id, g.name, g.description, g.monthly_amount, g.max_members, g.receiver_name, g.receiver_upi, g.status, g.is_paused
        ORDER BY g.monthly_amount, g.id
        """
    )
    groups = []
    for r in c.fetchall():
        groups.append(
            {
                'id': r[0],
                'name': r[1],
                'description': r[2] or '',
                'monthly_amount': int(r[3] or 0),
                'max_members': int(r[4] or 10),
                'receiver_name': r[5] or '',
                'receiver_upi': r[6] or '',
                'status': (r[7] or '').strip().lower(),
                'is_paused': int(r[8] or 0),
                'joined_members': int(r[9] or 0),
            }
        )

    c.execute(
        """
        SELECT gm.id, gm.group_id, gm.username, gm.status,
               g.name, g.monthly_amount,
               u.full_name, u.mobile, u.upi_id
        FROM group_members gm
        JOIN groups g ON g.id = gm.group_id
        LEFT JOIN users u ON u.username = gm.username
        WHERE gm.status='pending'
        ORDER BY gm.id DESC
        """
    )
    join_requests = [
        {
            'membership_id': row[0],
            'group_id': row[1],
            'username': row[2],
            'status': row[3],
            'group_name': row[4],
            'monthly_amount': row[5],
            'full_name': row[6] or '',
            'mobile': row[7] or '',
            'upi_id': row[8] or '',
        }
        for row in c.fetchall()
    ]
    conn.close()
    return render_template('owner_groups.html', active_owner_tab='groups', groups=groups, join_requests=join_requests)


@app.route('/owner/groups/add', methods=['POST'])
@admin_required
def owner_add_group():
    name = (request.form.get('name') or '').strip()
    description = (request.form.get('description') or '').strip()
    monthly_amount_raw = (request.form.get('monthly_amount') or '').strip()
    max_members_raw = (request.form.get('max_members') or '').strip()
    receiver_name = (request.form.get('receiver_name') or '').strip()
    receiver_upi = (request.form.get('receiver_upi') or '').strip()
    try:
        monthly_amount = int(monthly_amount_raw)
    except ValueError:
        monthly_amount = 0
    try:
        max_members = int(max_members_raw)
    except ValueError:
        max_members = 10

    if not name:
        flash('Group name is required.')
        return redirect(url_for('owner_groups'))
    if monthly_amount <= 0:
        flash('Monthly amount must be greater than 0.')
        return redirect(url_for('owner_groups'))
    if max_members <= 0:
        max_members = 10

    conn = get_db()
    c = conn.cursor()
    c.execute(
        'INSERT INTO groups (name, description, monthly_amount, max_members, receiver_name, receiver_upi, status, is_paused) VALUES (?,?,?,?,?,?,?,?)',
        (name, description, monthly_amount, max_members, receiver_name, receiver_upi, 'formation', 0),
    )
    conn.commit()
    conn.close()
    flash('Group added.')
    return redirect(url_for('owner_groups'))


@app.route('/owner/groups/update', methods=['POST'])
@admin_required
def owner_update_group():
    group_id = (request.form.get('group_id') or '').strip()
    name = (request.form.get('name') or '').strip()
    description = (request.form.get('description') or '').strip()
    receiver_name = (request.form.get('receiver_name') or '').strip()
    receiver_upi = (request.form.get('receiver_upi') or '').strip()
    status = (request.form.get('status') or '').strip().lower()
    monthly_amount_raw = (request.form.get('monthly_amount') or '').strip()
    max_members_raw = (request.form.get('max_members') or '').strip()
    try:
        monthly_amount = int(monthly_amount_raw)
    except ValueError:
        monthly_amount = 0
    try:
        max_members = int(max_members_raw)
    except ValueError:
        max_members = 10

    if status not in {'formation', 'active', 'completed', ''}:
        status = ''
    if max_members <= 0:
        max_members = 10
    if monthly_amount < 0:
        monthly_amount = 0

    conn = get_db()
    c = conn.cursor()
    c.execute(
        'UPDATE groups SET name=?, description=?, monthly_amount=?, max_members=?, receiver_name=?, receiver_upi=?, status=? WHERE id=?',
        (name, description, monthly_amount, max_members, receiver_name, receiver_upi, status, group_id),
    )
    conn.commit()
    conn.close()
    flash('Group updated.')
    return redirect(url_for('owner_groups'))


@app.route('/owner/groups/toggle_pause', methods=['POST'])
@admin_required
def owner_toggle_group_pause():
    group_id = (request.form.get('group_id') or '').strip()
    if not group_id:
        flash('Missing group id.')
        return redirect(url_for('owner_groups'))
    conn = get_db()
    c = conn.cursor()
    c.execute('UPDATE groups SET is_paused = CASE WHEN COALESCE(is_paused,0)=1 THEN 0 ELSE 1 END WHERE id=?', (group_id,))
    conn.commit()
    conn.close()
    flash('Group updated.')
    return redirect(url_for('owner_groups'))


@app.route('/owner/payments')
@admin_required
def owner_payments():
    fee_raw = (get_setting('app_fee_amount', '0') or '0').strip()
    try:
        fee_amount = int(fee_raw)
    except ValueError:
        fee_amount = 0
    fee_amount = max(0, fee_amount)

    conn = get_db()
    c = conn.cursor()
    c.execute(
        """
        SELECT username, full_name, mobile
        FROM users
        WHERE COALESCE(NULLIF(role,''), 'customer') != 'admin'
          AND COALESCE(app_fee_paid,0)=1
        ORDER BY id DESC
        """
    )
    app_fee_payments = [
        {
            'username': r[0],
            'full_name': r[1] or '',
            'mobile': r[2] or '',
            'amount': fee_amount,
        }
        for r in c.fetchall()
    ]
    conn.close()
    return render_template('owner_payments.html', active_owner_tab='payments', app_fee_amount=fee_amount, app_fee_payments=app_fee_payments)


@app.route('/owner/risk')
@admin_required
def owner_risk():
    conn = get_db()
    c = conn.cursor()
    c.execute(
        """
        SELECT username, full_name, mobile,
               COALESCE(is_active,1) as is_active,
               COALESCE(join_blocked,0) as join_blocked,
               COALESCE(trust_score,50) as trust_score
        FROM users
        WHERE COALESCE(NULLIF(role,''), 'customer') != 'admin'
        ORDER BY id DESC
        """
    )
    rows = c.fetchall()
    conn.close()

    blocked = []
    frozen = []
    low_trust = []
    for r in rows:
        username, full_name, mobile, is_active, join_blocked, trust_score = r
        item = {'username': username, 'full_name': full_name or '', 'mobile': mobile or '', 'trust_score': int(trust_score if trust_score is not None else 50)}
        if int(is_active or 1) == 0:
            blocked.append(item)
        if int(join_blocked or 0) == 1:
            frozen.append(item)
        if int(trust_score if trust_score is not None else 50) < 40:
            low_trust.append(item)

    return render_template('owner_risk.html', active_owner_tab='risk', blocked=blocked, frozen=frozen, low_trust=low_trust)


@app.route('/owner/settings', methods=['GET', 'POST'])
@admin_required
def owner_settings():
    if request.method == 'POST':
        set_setting('app_fee_amount', (request.form.get('app_fee_amount') or '').strip())
        set_setting('group_size_limit', (request.form.get('group_size_limit') or '').strip())
        set_setting('max_monthly_contribution', (request.form.get('max_monthly_contribution') or '').strip())
        set_setting('company_upi_id', (request.form.get('company_upi_id') or '').strip())
        set_setting('legal_text', (request.form.get('legal_text') or '').strip())
        flash('Settings saved.')
        return redirect(url_for('owner_settings'))

    settings = {
        'app_fee_amount': get_setting('app_fee_amount', '0'),
        'group_size_limit': get_setting('group_size_limit', ''),
        'max_monthly_contribution': get_setting('max_monthly_contribution', ''),
        'company_upi_id': get_setting('company_upi_id', ''),
        'legal_text': get_setting('legal_text', ''),
    }
    return render_template('owner_settings.html', active_owner_tab='settings', settings=settings)


@app.route('/welcome')
@require_customer
def welcome():
    return redirect(url_for('home_tab'))


@app.route('/how-it-works')
@require_customer
def how_it_works():
    return redirect(url_for('home_tab'))


@app.route('/setup', methods=['GET', 'POST'])
@require_customer
def setup():
    return redirect(url_for('home_tab'))


def _fetch_my_groups(username: str):
    conn = get_db()
    c = conn.cursor()
    c.execute(
        '''
        SELECT g.id,
               g.name,
               g.description,
               g.monthly_amount,
               COALESCE(g.max_members, 10) as max_members,
               COALESCE(g.receiver_name, '') as receiver_name,
               COALESCE(g.receiver_upi, '') as receiver_upi,
               COALESCE(g.status, '') as group_status,
               COALESCE(g.is_paused, 0) as is_paused,
               COUNT(gm2.id) as joined_members
        FROM group_members gm
        JOIN groups g ON g.id = gm.group_id
        LEFT JOIN group_members gm2 ON gm2.group_id = g.id AND gm2.status='joined'
        WHERE gm.username=? AND gm.status='joined'
        GROUP BY g.id, g.name, g.description, g.monthly_amount, g.max_members, g.receiver_name, g.receiver_upi, g.status, g.is_paused
        ORDER BY g.monthly_amount, g.id
        ''',
        (username,),
    )
    rows = c.fetchall()
    conn.close()

    groups = []
    for row in rows:
        group_id, name, description, monthly_amount, max_members, receiver_name, receiver_upi, group_status, is_paused, joined_members = row
        computed_status = 'Active' if int(joined_members or 0) >= int(max_members or 10) else 'Formation'
        status = (group_status or '').strip().lower()
        if int(is_paused or 0) == 1:
            status_label = 'Paused'
        elif status in {'active', 'formation', 'completed'}:
            status_label = status.capitalize()
        else:
            status_label = computed_status
        groups.append(
            {
                'id': group_id,
                'name': name,
                'description': description,
                'monthly_amount': monthly_amount,
                'max_members': int(max_members or 10),
                'joined_members': int(joined_members or 0),
                'status': status_label,
                'receiver_name': receiver_name or '',
                'receiver_upi': receiver_upi or '',
            }
        )
    return groups


def _fetch_available_groups(username: str):
    conn = get_db()
    c = conn.cursor()
    c.execute(
        '''
        SELECT g.id,
               g.name,
               g.description,
               g.monthly_amount,
               COALESCE(g.max_members, 10) as max_members,
               COUNT(gm2.id) as joined_members
        FROM groups g
        LEFT JOIN group_members gm2 ON gm2.group_id = g.id AND gm2.status='joined'
        WHERE g.id NOT IN (
            SELECT group_id FROM group_members WHERE username=? AND status='joined'
        )
          AND COALESCE(g.is_paused, 0) = 0
        GROUP BY g.id, g.name, g.description, g.monthly_amount, g.max_members
        ORDER BY g.monthly_amount, g.id
        ''',
        (username,),
    )
    rows = c.fetchall()
    conn.close()
    return [
        {
            'id': r[0],
            'name': r[1],
            'description': r[2],
            'monthly_amount': r[3],
            'max_members': int(r[4] or 10),
            'joined_members': int(r[5] or 0),
        }
        for r in rows
    ]


@app.route('/home')
@require_customer
def home_tab():
    username = session['username']
    user = get_user_row(username)
    if not user:
        return redirect(url_for('logout'))

    my_groups = _fetch_my_groups(username)
    active_count = sum(1 for g in my_groups if g.get('status') == 'Active')
    formation_count = sum(1 for g in my_groups if g.get('status') == 'Formation')

    return render_template(
        'home_tab.html',
        full_name=(user.get('full_name') or user.get('username') or '').strip(),
        has_groups=len(my_groups) > 0,
        active_count=active_count,
        formation_count=formation_count,
        active_tab='home',
    )


@app.route('/groups')
@require_customer
def groups_tab():
    username = session['username']
    my_groups = _fetch_my_groups(username)
    available_groups = _fetch_available_groups(username)
    user = get_user_row(username) or {}

    return render_template(
        'groups_tab.html',
        my_groups=my_groups,
        available_groups=available_groups,
        upi_id=(user.get('upi_id') or '').strip(),
        active_tab='groups',
    )


@app.route('/groups/create', methods=['POST'])
@require_customer
def create_group_customer():
    flash('Customers cannot create groups. Please join an existing group created by the owner.')
    return redirect(url_for('groups_tab'))


@app.route('/groups/join', methods=['POST'])
@require_customer
def join_group_customer():
    try:
        group_id = int(request.form.get('group_id') or 0)
    except ValueError:
        group_id = 0
    if group_id <= 0:
        flash('Invalid group.')
        return redirect(url_for('groups_tab'))

    username = session['username']
    if is_join_blocked(username):
        flash('Your access is restricted for future groups. Please contact support.')
        return redirect(url_for('groups_tab'))

    conn = get_db()
    c = conn.cursor()
    try:
        c.execute('SELECT COALESCE(is_paused, 0) FROM groups WHERE id=?', (group_id,))
        row = c.fetchone()
    except sqlite3.OperationalError:
        row = None
    conn.close()
    if row and int(row[0] if row[0] is not None else 0) == 1:
        flash('This group is currently paused.')
        return redirect(url_for('groups_tab'))

    join_group_with_status(group_id, username, status='joined')
    flash('You joined the group.')
    return redirect(url_for('groups_tab'))


@app.route('/payments')
@require_customer
def payments_tab():
    username = session['username']
    user = get_user_row(username) or {}
    upi_id = (user.get('upi_id') or '').strip()
    my_groups = _fetch_my_groups(username)

    pay_to = []
    for g in my_groups:
        receiver_upi = (g.get('receiver_upi') or '').strip()
        receiver_name = (g.get('receiver_name') or '').strip() or 'Receiver'
        amount = g.get('monthly_amount')
        note = f"D-CONT - {g.get('name') or 'Group'}"
        pay_url = ''
        if receiver_upi:
            pay_url = f"upi://pay?pa={quote(receiver_upi)}&pn={quote(receiver_name)}&am={quote(str(amount))}&tn={quote(note)}"
        pay_to.append(
            {
                'group': g,
                'receiver_upi': receiver_upi,
                'receiver_name': receiver_name,
                'amount': amount,
                'pay_url': pay_url,
            }
        )

    return render_template(
        'payments_tab.html',
        upi_id=upi_id,
        pay_to=pay_to,
        active_tab='payments',
    )


@app.route('/groups/<int:amount>')
@require_customer
def groups_by_amount(amount):
    return redirect(url_for('groups_tab'))


@app.route('/group/<int:group_id>')
@require_customer
def group_preview(group_id):
    return redirect(url_for('groups_tab'))


@app.route('/add-upi', methods=['GET', 'POST'])
@require_customer
def add_upi():
    return redirect(url_for('profile'))


@app.route('/group/<int:group_id>/join', methods=['POST'])
@require_customer
def request_join_group(group_id):
    username = session['username']
    if is_join_blocked(username):
        flash('Your access is restricted for future groups. Please contact support.')
        return redirect(url_for('groups_tab'))

    conn = get_db()
    c = conn.cursor()
    try:
        c.execute('SELECT COALESCE(is_paused, 0) FROM groups WHERE id=?', (group_id,))
        row = c.fetchone()
    except sqlite3.OperationalError:
        row = None
    conn.close()
    if row and int(row[0] if row[0] is not None else 0) == 1:
        flash('This group is currently paused.')
        return redirect(url_for('groups_tab'))

    join_group_with_status(group_id, username, status='joined')
    flash('You joined the group.')
    return redirect(url_for('groups_tab'))


@app.route('/join_group', methods=['POST'])
@login_required
def join_group():
    group_id = request.form['group_id']
    username = session['username']
    if session.get('role') == 'admin':
        join_group_with_status(group_id, username, status='joined')
        flash('You have joined the group!')
        return redirect(url_for('dashboard'))

    join_group_with_status(group_id, username, status='joined')
    flash('You joined the group.')
    return redirect(url_for('groups_tab'))

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        # Admin login (username/password)
        admin_username = (request.form.get('admin_username') or '').strip()
        admin_password = request.form.get('admin_password') or ''
        if admin_username or admin_password:
            if not admin_username or not admin_password:
                flash('Enter admin user id and password.')
                return render_template('login.html')

            conn = get_db()
            c = conn.cursor()
            try:
                c.execute('SELECT username, password, role, is_active FROM users WHERE username=?', (admin_username,))
                row = c.fetchone()
            except sqlite3.OperationalError:
                row = None
            conn.close()

            if not row or (row[2] or '').strip().lower() != 'admin':
                flash('Invalid admin credentials.')
                return render_template('login.html')
            if int(row[3] if row[3] is not None else 1) != 1:
                flash('Your account is blocked. Please contact support.')
                return render_template('login.html')

            stored_pw = row[1] or ''
            password_ok = False
            try:
                password_ok = check_password_hash(stored_pw, admin_password)
            except (ValueError, TypeError):
                password_ok = False
            if not password_ok:
                # Back-compat: if password was stored in plain text
                password_ok = stored_pw == admin_password

            if not password_ok:
                flash('Invalid admin credentials.')
                return render_template('login.html')

            session['username'] = row[0]
            session['role'] = 'admin'
            return redirect(url_for('dashboard'))

        # Customer login (username OR mobile + password)
        identifier = (request.form.get('username') or '').strip()
        password = request.form.get('password') or ''

        if not identifier or not password:
            flash('Enter your username/mobile and password.')
            return render_template('login.html')

        conn = get_db()
        c = conn.cursor()
        row = None
        try:
            # First try username
            c.execute('SELECT username, password, role, is_active, mobile FROM users WHERE username=?', (identifier,))
            row = c.fetchone()
            # Fallback: allow login via mobile for legacy accounts
            if not row:
                for mobile_value in _mobile_candidates(identifier):
                    c.execute('SELECT username, password, role, is_active, mobile FROM users WHERE mobile=?', (mobile_value,))
                    row = c.fetchone()
                    if row:
                        break

            # Last-resort fallback: legacy DB rows may store mobile with spaces/dashes/+91.
            # Compare digit-normalized mobile values.
            if not row:
                identifier_digits = _normalize_mobile_digits(identifier)
                if identifier_digits:
                    c.execute('SELECT username, password, role, is_active, mobile FROM users')
                    for cand in c.fetchall() or []:
                        cand_mobile = cand[4] if len(cand) > 4 else ''
                        if _normalize_mobile_digits(cand_mobile) == identifier_digits:
                            row = cand
                            break
        except sqlite3.OperationalError:
            row = None

        if not row:
            conn.close()
            flash('Invalid credentials.')
            return render_template('login.html')

        db_username, stored_pw, role_raw, is_active_raw, db_mobile = row
        # Auto-repair: some legacy rows may have a blank username
        if not (db_username or '').strip() and (db_mobile or '').strip():
            candidate = (db_mobile or '').strip()
            try:
                c.execute('SELECT 1 FROM users WHERE username=?', (candidate,))
                exists = c.fetchone()
                if not exists:
                    c.execute('UPDATE users SET username=? WHERE mobile=?', (candidate, candidate))
                    conn.commit()
                    db_username = candidate
            except sqlite3.OperationalError:
                pass

        conn.close()

        if not (db_username or '').strip():
            flash('Your account needs an update. Please contact support.')
            return render_template('login.html')

        role = (role_raw or 'customer').strip().lower()
        if role == 'admin':
            flash('Use the Admin Login section to sign in as admin.')
            return render_template('login.html')

        if int(is_active_raw if is_active_raw is not None else 1) != 1:
            flash('Your account is blocked. Please contact support.')
            return render_template('login.html')

        stored_pw = stored_pw or ''
        password_ok = False
        try:
            password_ok = check_password_hash(stored_pw, password)
        except (ValueError, TypeError):
            password_ok = False
        if not password_ok:
            # Back-compat: if password was stored in plain text
            password_ok = stored_pw == password

        if not password_ok:
            flash('Invalid credentials.')
            return render_template('login.html')

        session['username'] = db_username
        session['role'] = role
        return redirect(url_for('home'))

    return render_template('login.html')

@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        agree_terms = request.form.get('agree_terms')
        if agree_terms != 'yes':
            flash('You must agree to the Terms & Conditions to register.')
            return render_template('register.html')
        username = request.form.get('username')
        password = request.form.get('password')
        mobile_raw = request.form.get('mobile')
        # Store mobile in a normalized digits format when possible so
        # users can log in even if they type +91/spaces/dashes later.
        mobile = _normalize_mobile_digits(mobile_raw) or (mobile_raw or '').strip()
        full_name = request.form.get('full_name')
        language = request.form.get('language')
        city_state = request.form.get('city_state')
        email = request.form.get('email')
        try:
            password_hash = generate_password_hash(password)
            conn = get_db()
            c = conn.cursor()
            c.execute('INSERT INTO users (username, password, mobile, full_name, language, city_state, email, role, upi_id, onboarding_completed, is_active) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                      (username, password_hash, mobile, full_name, language, city_state, email, 'customer', '', 0, 1))
            conn.commit()
            conn.close()
            flash('Registration successful! Please log in.')
            return redirect(url_for('login'))
        except sqlite3.IntegrityError:
            flash('Username or mobile already exists!')
    return render_template('register.html')

@app.route('/logout')
def logout():
    session.pop('username', None)
    session.pop('role', None)
    return redirect(url_for('login'))


@app.route('/admin')
@admin_required
def admin_panel():
    # Legacy route: owner/admin UI lives under /owner/*
    return redirect(url_for('owner_dashboard'))


@app.route('/admin/update_group', methods=['POST'])
@admin_required
def admin_update_group():
    group_id = request.form.get('group_id')
    name = request.form.get('name')
    description = request.form.get('description')
    monthly_amount = request.form.get('monthly_amount')
    try:
        monthly_amount_int = int(monthly_amount) if monthly_amount is not None else 0
    except ValueError:
        monthly_amount_int = 0

    conn = get_db()
    c = conn.cursor()
    c.execute('UPDATE groups SET name=?, description=?, monthly_amount=? WHERE id=?', (name, description, monthly_amount_int, group_id))
    conn.commit()
    conn.close()
    flash('Group updated.')
    return redirect(url_for('owner_groups'))


@app.route('/admin/add_group', methods=['POST'])
@admin_required
def admin_add_group():
    name = (request.form.get('name') or '').strip()
    description = (request.form.get('description') or '').strip()
    monthly_amount = request.form.get('monthly_amount')
    try:
        monthly_amount_int = int(monthly_amount) if monthly_amount is not None else 0
    except ValueError:
        monthly_amount_int = 0

    if not name:
        flash('Group name is required.')
        return redirect(url_for('owner_groups'))
    if monthly_amount_int <= 0:
        flash('Monthly amount must be greater than 0.')
        return redirect(url_for('owner_groups'))

    conn = get_db()
    c = conn.cursor()
    c.execute(
        'INSERT INTO groups (name, description, monthly_amount, status, is_paused) VALUES (?, ?, ?, ?, ?)',
        (name, description, monthly_amount_int, 'formation', 0),
    )
    conn.commit()
    conn.close()
    flash('Group added.')
    return redirect(url_for('owner_groups'))


@app.route('/admin/delete_group', methods=['POST'])
@admin_required
def admin_delete_group():
    group_id = request.form.get('group_id')
    if not group_id:
        flash('Missing group id.')
        return redirect(url_for('owner_groups'))

    conn = get_db()
    c = conn.cursor()
    # Best-effort cleanup: remove memberships first.
    c.execute('DELETE FROM group_members WHERE group_id=?', (group_id,))
    c.execute('DELETE FROM groups WHERE id=?', (group_id,))
    conn.commit()
    conn.close()
    flash('Group deleted.')
    return redirect(url_for('owner_groups'))


@app.route('/admin/update_membership_status', methods=['POST'])
@admin_required
def admin_update_membership_status():
    membership_id = request.form.get('membership_id')
    new_status = (request.form.get('status') or '').strip().lower()
    if new_status not in {'joined', 'rejected'}:
        flash('Invalid status.')
        return redirect(url_for('owner_groups'))

    conn = get_db()
    c = conn.cursor()
    c.execute('UPDATE group_members SET status=? WHERE id=?', (new_status, membership_id))
    conn.commit()
    conn.close()
    flash(f'Updated request: {new_status}.')
    return redirect(url_for('owner_groups'))


@app.route('/admin/toggle_user_active', methods=['POST'])
@admin_required
def admin_toggle_user_active():
    target_username = (request.form.get('username') or '').strip()
    action = (request.form.get('action') or '').strip().lower()

    if not target_username:
        flash('Missing username.')
        return redirect(url_for('owner_users'))
    if target_username == session.get('username'):
        flash('You cannot block your own account.')
        return redirect(url_for('owner_users'))
    if action not in {'block', 'unblock'}:
        flash('Invalid action.')
        return redirect(url_for('owner_users'))

    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT role FROM users WHERE username=?', (target_username,))
    row = c.fetchone()
    if not row:
        conn.close()
        flash('User not found.')
        return redirect(url_for('owner_users'))
    if (row[0] or '').strip().lower() == 'admin':
        conn.close()
        flash('Admin accounts cannot be blocked from this panel.')
        return redirect(url_for('owner_users'))

    is_active_value = 1 if action == 'unblock' else 0
    c.execute('UPDATE users SET is_active=? WHERE username=?', (is_active_value, target_username))
    conn.commit()
    conn.close()
    flash('User updated.')
    return redirect(url_for('owner_users'))

if __name__ == '__main__':
    host = os.environ.get('DCONT_HOST', '127.0.0.1')
    port = int(os.environ.get('DCONT_PORT', '5000'))
    app.run(host=host, port=port, debug=True)
