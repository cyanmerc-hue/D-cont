from flask import Flask, render_template, request, redirect, url_for, session, flash, abort
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

app = Flask(__name__)
# In production (Render/Heroku/etc.), set SECRET_KEY as an environment variable.
app.secret_key = os.environ.get('SECRET_KEY', 'your_secret_key')
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Render's filesystem is ephemeral unless you attach a persistent disk.
# You can override these paths via env vars to point at a persistent mount.
DATABASE = os.environ.get('DCONT_DATABASE_PATH', os.path.join(BASE_DIR, 'users.db'))
UPLOAD_FOLDER = os.environ.get('DCONT_UPLOAD_FOLDER', os.path.join(BASE_DIR, 'static', 'uploads'))
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# Demo admin identity (change these for your deployment)
ADMIN_USERNAME = os.environ.get('DCONT_ADMIN_USERNAME', 'cyanmerc')
ADMIN_PASSWORD = os.environ.get('DCONT_ADMIN_PASSWORD', 'Bond1010#')
ADMIN_MOBILE = os.environ.get('DCONT_ADMIN_MOBILE', '9999999999')

WHATSAPP_SUPPORT_NUMBER = '917506680031'  # +91 7506680031

ALLOWED_IMAGE_EXTENSIONS = {"png", "jpg", "jpeg", "webp", "gif"}

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
    "is_active": "INTEGER",
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
        'SELECT username, full_name, mobile, language, city_state, email, role, upi_id, onboarding_completed, is_active FROM users WHERE username=?',
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
        'is_active': int(row[9] if row[9] is not None else 1),
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


def join_group_with_status(group_id, username, status="pending"):
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
def save_profile_photo(file, username):
    if not file or not file.filename:
        return None

    original = secure_filename(file.filename)
    if '.' not in original:
        return None

    ext = original.rsplit('.', 1)[-1].lower()
    if ext not in ALLOWED_IMAGE_EXTENSIONS:
        return None

    filename = f"{secure_filename(username)}_{uuid.uuid4().hex}.{ext}"
    filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    file.save(filepath)

    # Store relative path from /static so it can be served by Flask's static handler
    return f"uploads/{filename}"
@app.route('/profile', methods=['GET', 'POST'])
@login_required
def profile():
    username = session['username']
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT full_name, email, mobile, language, city_state, photo FROM users WHERE username=?', (username,))
    row = c.fetchone()
    full_name = email = mobile = language = city_state = photo = None
    if row:
        full_name, email, mobile, language, city_state, photo = row
    if request.method == 'POST':
        full_name = request.form.get('full_name')
        email = request.form.get('email')
        mobile = request.form.get('mobile')
        language = request.form.get('language')
        city_state = request.form.get('city_state')
        photo_file = request.files.get('photo')
        photo_url = photo
        if photo_file and photo_file.filename:
            photo_url = save_profile_photo(photo_file, username)
        c.execute('''UPDATE users SET full_name=?, email=?, mobile=?, language=?, city_state=?, photo=? WHERE username=?''',
                  (full_name, email, mobile, language, city_state, photo_url, username))
        conn.commit()
        flash('Profile updated!')
        photo = photo_url
    conn.close()
    photo_url = f"/static/{photo}" if photo else "/static/default-profile.svg"
    return render_template('profile.html', full_name=full_name, email=email, mobile=mobile, language=language, city_state=city_state, photo_url=photo_url)

def get_db():
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

    # Ensure users table has required columns (auto-migration)
    c.execute("PRAGMA table_info(users)")
    existing_cols = {row[1] for row in c.fetchall()}  # row[1] = column name
    for col_name, col_type in USER_COLUMNS.items():
        if col_name not in existing_cols:
            c.execute(f"ALTER TABLE users ADD COLUMN {col_name} {col_type}")

    # Ensure group_members has status column (auto-migration)
    c.execute("PRAGMA table_info(group_members)")
    existing_member_cols = {row[1] for row in c.fetchall()}
    if "status" not in existing_member_cols:
        c.execute("ALTER TABLE group_members ADD COLUMN status TEXT")

    # Backfill onboarding + membership status
    c.execute("UPDATE users SET onboarding_completed=0 WHERE onboarding_completed IS NULL")
    c.execute("UPDATE users SET is_active=1 WHERE is_active IS NULL")
    c.execute("UPDATE group_members SET status='joined' WHERE status IS NULL OR status='' ")

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
            'INSERT INTO groups (name, description, monthly_amount) VALUES (?, ?, ?)',
            ("Pilot Group 2026", "Monthly savings group", 500),
        )
        c.execute(
            'INSERT INTO groups (name, description, monthly_amount) VALUES (?, ?, ?)',
            ("Pilot Group 2 2026", "Monthly savings group", 1000),
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
        if user.get('onboarding_completed'):
            return redirect(url_for('customer_home'))
        return redirect(url_for('welcome'))
    return redirect(url_for('login'))
@app.route('/dashboard')
@login_required
def dashboard():
    if session.get('role') != 'admin':
        return redirect(url_for('home'))
    username = session['username']
    display_name = username
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT full_name FROM users WHERE username=?', (username,))
    row = c.fetchone()
    if row and row[0]:
        display_name = row[0]
    c.execute('SELECT id, name, description, monthly_amount FROM groups')
    groups = c.fetchall()
    group_list = []
    for group in groups:
        group_id, name, description, monthly_amount = group
        c.execute('SELECT * FROM group_members WHERE group_id=? AND username=?', (group_id, username))
        joined = c.fetchone() is not None
        group_list.append({'id': group_id, 'name': name, 'description': description, 'monthly_amount': monthly_amount, 'joined': joined})
    conn.close()
    return render_template('dashboard.html', username=display_name, groups=group_list, is_admin=(session.get('role') == 'admin'))


@app.route('/welcome')
@require_customer
def welcome():
    user = get_user_row(session['username'])
    if user and user.get('onboarding_completed'):
        return redirect(url_for('customer_home'))
    return render_template('welcome.html')


@app.route('/how-it-works')
@require_customer
def how_it_works():
    user = get_user_row(session['username'])
    if user and user.get('onboarding_completed'):
        return redirect(url_for('customer_home'))
    return render_template('how_it_works.html')


@app.route('/setup', methods=['GET', 'POST'])
@require_customer
def setup():
    username = session['username']
    user = get_user_row(username)
    if user and user.get('onboarding_completed'):
        return redirect(url_for('customer_home'))

    if request.method == 'POST':
        full_name = (request.form.get('full_name') or '').strip()
        language = (request.form.get('language') or '').strip()
        city_state = (request.form.get('city_state') or '').strip()
        email = (request.form.get('email') or '').strip()

        if not full_name or not language:
            flash('Please fill the required fields.')
            return render_template('setup.html', user=user)

        conn = get_db()
        c = conn.cursor()
        c.execute(
            'UPDATE users SET full_name=?, language=?, city_state=?, email=?, onboarding_completed=1 WHERE username=?',
            (full_name, language, city_state, email, username),
        )
        conn.commit()
        conn.close()
        flash('Setup complete!')
        return redirect(url_for('customer_home'))

    return render_template('setup.html', user=user)


@app.route('/home')
@require_customer
def customer_home():
    username = session['username']
    user = get_user_row(username)
    if not user:
        return redirect(url_for('logout'))
    if not user.get('onboarding_completed'):
        return redirect(url_for('welcome'))

    conn = get_db()
    c = conn.cursor()
    c.execute(
        '''
        SELECT g.id, g.name, g.description, g.monthly_amount, gm.status
        FROM group_members gm
        JOIN groups g ON g.id = gm.group_id
        WHERE gm.username=?
        ORDER BY g.monthly_amount, g.id
        ''',
        (username,),
    )
    my_groups = [
        {
            'id': row[0],
            'name': row[1],
            'description': row[2],
            'monthly_amount': row[3],
            'status': row[4] or 'joined',
        }
        for row in c.fetchall()
    ]
    conn.close()
    return render_template('customer_home.html', user=user, my_groups=my_groups)


@app.route('/groups/<int:amount>')
@require_customer
def groups_by_amount(amount):
    username = session['username']
    user = get_user_row(username)
    if not user or not user.get('onboarding_completed'):
        return redirect(url_for('welcome'))

    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT id, name, description, monthly_amount FROM groups WHERE monthly_amount=? ORDER BY id', (amount,))
    rows = c.fetchall()
    groups = []
    for row in rows:
        group_id, name, description, monthly_amount = row
        c.execute('SELECT status FROM group_members WHERE group_id=? AND username=?', (group_id, username))
        member = c.fetchone()
        groups.append(
            {
                'id': group_id,
                'name': name,
                'description': description,
                'monthly_amount': monthly_amount,
                'member_status': (member[0] if member else ''),
            }
        )
    conn.close()
    return render_template('groups_by_amount.html', amount=amount, groups=groups)


@app.route('/group/<int:group_id>')
@require_customer
def group_preview(group_id):
    username = session['username']
    user = get_user_row(username)
    if not user or not user.get('onboarding_completed'):
        return redirect(url_for('welcome'))

    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT id, name, description, monthly_amount FROM groups WHERE id=?', (group_id,))
    row = c.fetchone()
    if not row:
        conn.close()
        abort(404)
    group = {'id': row[0], 'name': row[1], 'description': row[2], 'monthly_amount': row[3]}
    c.execute('SELECT status FROM group_members WHERE group_id=? AND username=?', (group_id, username))
    member = c.fetchone()
    member_status = (member[0] if member else '')
    conn.close()
    return render_template('group_preview.html', user=user, group=group, member_status=member_status)


@app.route('/add-upi', methods=['GET', 'POST'])
@require_customer
def add_upi():
    username = session['username']
    user = get_user_row(username)
    if not user or not user.get('onboarding_completed'):
        return redirect(url_for('welcome'))

    next_url = request.args.get('next') or url_for('customer_home')

    if request.method == 'POST':
        upi_id = (request.form.get('upi_id') or '').strip()
        if not upi_id:
            flash('Please enter your UPI ID.')
            return render_template('add_upi.html', user=user, next_url=next_url)
        conn = get_db()
        c = conn.cursor()
        c.execute('UPDATE users SET upi_id=? WHERE username=?', (upi_id, username))
        conn.commit()
        conn.close()
        flash('UPI saved!')
        return redirect(next_url)

    return render_template('add_upi.html', user=user, next_url=next_url)


@app.route('/group/<int:group_id>/join', methods=['POST'])
@require_customer
def request_join_group(group_id):
    username = session['username']
    user = get_user_row(username)
    if not user or not user.get('onboarding_completed'):
        return redirect(url_for('welcome'))
    if not (user.get('upi_id') or '').strip():
        flash('Add your UPI ID before joining a group.')
        return redirect(url_for('add_upi', next=url_for('group_preview', group_id=group_id)))

    join_group_with_status(group_id, username, status='pending')
    return redirect(url_for('join_success', group_id=group_id))


@app.route('/join-success/<int:group_id>')
@require_customer
def join_success(group_id):
    username = session['username']
    user = get_user_row(username)
    if not user or not user.get('onboarding_completed'):
        return redirect(url_for('welcome'))

    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT id, name, description, monthly_amount FROM groups WHERE id=?', (group_id,))
    row = c.fetchone()
    if not row:
        conn.close()
        abort(404)
    group = {'id': row[0], 'name': row[1], 'description': row[2], 'monthly_amount': row[3]}
    c.execute('SELECT status FROM group_members WHERE group_id=? AND username=?', (group_id, username))
    member = c.fetchone()
    member_status = (member[0] if member else '')
    conn.close()
    return render_template('join_success.html', group=group, member_status=member_status)


@app.route('/chat', methods=['GET', 'POST'])
@login_required
def chat():
    # Session-backed lightweight chat history (MVP)
    history = session.get('chat_history') or []
    unknown_attempts = int(session.get('chat_unknown_attempts') or 0)

    if not history:
        history = [
            {
                'from': 'bot',
                'text': 'Hi! I’m the D-CONT Bot. Ask a quick question, or tap a FAQ button.',
            }
        ]

    whatsapp_url = None
    if request.method == 'POST':
        user_text = (request.form.get('message') or '').strip()
        if user_text:
            history.append({'from': 'user', 'text': user_text})

            # Rule-based handoff for disputes/complex issues
            if message_needs_handoff(user_text):
                bot_text = 'This looks like something our team should handle. Tap below to chat on WhatsApp.'
                whatsapp_url = build_whatsapp_link(
                    f"Hi D-CONT Support, I need help with: {user_text} (user={session.get('username','')})"
                )
                history.append({'from': 'bot', 'text': bot_text})
            else:
                intent = match_intent(user_text)
                if intent:
                    bot_text = intent.get('answer')
                    history.append({'from': 'bot', 'text': bot_text})
                    if intent.get('handoff'):
                        whatsapp_url = build_whatsapp_link(
                            f"Hi D-CONT Support, I need help with: {user_text} (user={session.get('username','')})"
                        )
                    unknown_attempts = 0
                else:
                    unknown_attempts += 1
                    if unknown_attempts >= 2:
                        bot_text = 'I may have misunderstood. This looks like something our team should handle. Tap below to chat on WhatsApp.'
                        whatsapp_url = build_whatsapp_link(
                            f"Hi D-CONT Support, I need help with: {user_text} (user={session.get('username','')})"
                        )
                        history.append({'from': 'bot', 'text': bot_text})
                        unknown_attempts = 0
                    else:
                        history.append({'from': 'bot', 'text': 'Could you tell me which topic this is about? (Join group / Payment help / UPI update / Missed payment / Support)'})

        session['chat_history'] = history
        session['chat_unknown_attempts'] = unknown_attempts

    # If an intent includes a direct in-app link, we show it inline as a suggestion
    last_intent = None
    if history:
        last_user = next((m for m in reversed(history) if m.get('from') == 'user'), None)
        if last_user:
            last_intent = match_intent(last_user.get('text'))

    intent_link = (last_intent or {}).get('link') if last_intent else None
    intent_link_label = None
    if intent_link == '/home':
        intent_link_label = 'Open Home'
    elif intent_link == '/add-upi':
        intent_link_label = 'Add / Update UPI'

    return render_template(
        'chat.html',
        history=history,
        quick_replies=BOT_QUICK_REPLIES,
        whatsapp_url=whatsapp_url,
        intent_link=intent_link,
        intent_link_label=intent_link_label,
    )

@app.route('/join_group', methods=['POST'])
@login_required
def join_group():
    group_id = request.form['group_id']
    username = session['username']
    if session.get('role') != 'admin':
        user = get_user_row(username)
        if not user or not (user.get('upi_id') or '').strip():
            flash('Add your UPI ID before joining a group.')
            return redirect(url_for('add_upi', next=url_for('group_preview', group_id=group_id)))
        join_group_with_status(group_id, username, status='pending')
        return redirect(url_for('join_success', group_id=group_id))

    join_group_with_status(group_id, username, status='joined')
    flash('You have joined the group!')
    return redirect(url_for('dashboard'))

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        # Admin login (username/password)
        admin_username = (request.form.get('admin_username') or '').strip()
        admin_password = request.form.get('admin_password') or ''
        if admin_username or admin_password:
            if not admin_username or not admin_password:
                flash('Enter admin user id and password.')
                return render_template('login.html', demo_otp=session.get('otp'), otp_mobile=session.get('otp_mobile'))

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
                return render_template('login.html', demo_otp=session.get('otp'), otp_mobile=session.get('otp_mobile'))
            if int(row[3] if row[3] is not None else 1) != 1:
                flash('Your account is blocked. Please contact support.')
                return render_template('login.html', demo_otp=session.get('otp'), otp_mobile=session.get('otp_mobile'))

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
                return render_template('login.html', demo_otp=session.get('otp'), otp_mobile=session.get('otp_mobile'))

            session['username'] = row[0]
            session['role'] = 'admin'
            return redirect(url_for('dashboard'))

        # Customer login (OTP)
        mobile = (request.form.get('mobile') or '').strip()
        otp = request.form.get('otp')
        if not mobile:
            flash('Please enter your mobile number')
            return render_template('login.html', demo_otp=session.get('otp'), otp_mobile=session.get('otp_mobile'))
        if otp:
            # Demo OTP check
            if otp == session.get('otp') and mobile == session.get('otp_mobile'):
                conn = get_db()
                c = conn.cursor()
                c.execute('SELECT username, role, is_active FROM users WHERE mobile=?', (mobile,))
                user = c.fetchone()
                conn.close()
                if user:
                    if int(user[2] if user[2] is not None else 1) != 1:
                        flash('Your account is blocked. Please contact support.')
                        return render_template('login.html', demo_otp=session.get('otp'), otp_mobile=session.get('otp_mobile'))
                    session['username'] = user[0]
                    session['role'] = user[1] or 'customer'
                    return redirect(url_for('home'))
                else:
                    flash('No user found for this mobile. Please register.')
            else:
                flash('Invalid OTP')
        else:
            # Generate and send OTP.
            conn = get_db()
            c = conn.cursor()
            try:
                c.execute('SELECT email, is_active FROM users WHERE mobile=?', (mobile,))
                row = c.fetchone()
            except sqlite3.OperationalError:
                row = None
            conn.close()

            if not row:
                flash('No user found for this mobile. Please register.')
                return render_template('login.html', demo_otp=session.get('otp'), otp_mobile=session.get('otp_mobile'))

            email = (row[0] or '').strip()
            if int(row[1] if row[1] is not None else 1) != 1:
                flash('Your account is blocked. Please contact support.')
                return render_template('login.html', demo_otp=session.get('otp'), otp_mobile=session.get('otp_mobile'))

            otp_code = str(random.randint(100000, 999999))
            session['otp'] = otp_code
            session['otp_mobile'] = mobile

            # Prefer email delivery if SMTP is configured and the user has a registered email.
            if email and _smtp_configured():
                try:
                    send_login_otp_email(to_email=email, otp_code=otp_code, mobile=mobile)
                    flash('OTP sent to your registered email.')
                except Exception:
                    # Fallback to demo OTP display so the user can still log in.
                    flash(f'Unable to email OTP right now. Your OTP is: {otp_code} (demo only)')
            else:
                flash(f'Your OTP is: {otp_code} (demo only)')
    return render_template('login.html', demo_otp=session.get('otp'), otp_mobile=session.get('otp_mobile'))

@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        agree_terms = request.form.get('agree_terms')
        if agree_terms != 'yes':
            flash('You must agree to the Terms & Conditions to register.')
            return render_template('register.html')
        username = request.form.get('username')
        password = request.form.get('password')
        mobile = request.form.get('mobile')
        full_name = request.form.get('full_name')
        language = request.form.get('language')
        city_state = request.form.get('city_state')
        email = request.form.get('email')
        try:
            conn = get_db()
            c = conn.cursor()
            c.execute('INSERT INTO users (username, password, mobile, full_name, language, city_state, email, role, upi_id, onboarding_completed, is_active) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                      (username, password, mobile, full_name, language, city_state, email, 'customer', '', 0, 1))
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
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT id, name, description, monthly_amount FROM groups ORDER BY id')
    groups = [
        {
            'id': row[0],
            'name': row[1],
            'description': row[2],
            'monthly_amount': row[3],
        }
        for row in c.fetchall()
    ]
    c.execute('SELECT username, full_name, mobile, role, is_active FROM users ORDER BY id')
    users = [
        {
            'username': row[0],
            'full_name': row[1],
            'mobile': row[2],
            'role': row[3],
            'is_active': int(row[4] if row[4] is not None else 1),
        }
        for row in c.fetchall()
    ]

    c.execute(
        '''
        SELECT gm.id, gm.group_id, gm.username, gm.status,
               g.name, g.monthly_amount,
               u.full_name, u.mobile, u.upi_id
        FROM group_members gm
        JOIN groups g ON g.id = gm.group_id
        LEFT JOIN users u ON u.username = gm.username
        WHERE gm.status='pending'
        ORDER BY gm.id DESC
        '''
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
    return render_template('admin.html', groups=groups, users=users, join_requests=join_requests)


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
    return redirect(url_for('admin_panel'))


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
        return redirect(url_for('admin_panel'))
    if monthly_amount_int <= 0:
        flash('Monthly amount must be greater than 0.')
        return redirect(url_for('admin_panel'))

    conn = get_db()
    c = conn.cursor()
    c.execute(
        'INSERT INTO groups (name, description, monthly_amount) VALUES (?, ?, ?)',
        (name, description, monthly_amount_int),
    )
    conn.commit()
    conn.close()
    flash('Group added.')
    return redirect(url_for('admin_panel'))


@app.route('/admin/delete_group', methods=['POST'])
@admin_required
def admin_delete_group():
    group_id = request.form.get('group_id')
    if not group_id:
        flash('Missing group id.')
        return redirect(url_for('admin_panel'))

    conn = get_db()
    c = conn.cursor()
    # Best-effort cleanup: remove memberships first.
    c.execute('DELETE FROM group_members WHERE group_id=?', (group_id,))
    c.execute('DELETE FROM groups WHERE id=?', (group_id,))
    conn.commit()
    conn.close()
    flash('Group deleted.')
    return redirect(url_for('admin_panel'))


@app.route('/admin/update_membership_status', methods=['POST'])
@admin_required
def admin_update_membership_status():
    membership_id = request.form.get('membership_id')
    new_status = (request.form.get('status') or '').strip().lower()
    if new_status not in {'joined', 'rejected'}:
        flash('Invalid status.')
        return redirect(url_for('admin_panel'))

    conn = get_db()
    c = conn.cursor()
    c.execute('UPDATE group_members SET status=? WHERE id=?', (new_status, membership_id))
    conn.commit()
    conn.close()
    flash(f'Updated request: {new_status}.')
    return redirect(url_for('admin_panel'))


@app.route('/admin/toggle_user_active', methods=['POST'])
@admin_required
def admin_toggle_user_active():
    target_username = (request.form.get('username') or '').strip()
    action = (request.form.get('action') or '').strip().lower()

    if not target_username:
        flash('Missing username.')
        return redirect(url_for('admin_panel'))
    if target_username == session.get('username'):
        flash('You cannot block your own account.')
        return redirect(url_for('admin_panel'))
    if action not in {'block', 'unblock'}:
        flash('Invalid action.')
        return redirect(url_for('admin_panel'))

    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT role FROM users WHERE username=?', (target_username,))
    row = c.fetchone()
    if not row:
        conn.close()
        flash('User not found.')
        return redirect(url_for('admin_panel'))
    if (row[0] or '').strip().lower() == 'admin':
        conn.close()
        flash('Admin accounts cannot be blocked from this panel.')
        return redirect(url_for('admin_panel'))

    is_active_value = 1 if action == 'unblock' else 0
    c.execute('UPDATE users SET is_active=? WHERE username=?', (is_active_value, target_username))
    conn.commit()
    conn.close()
    flash('User updated.')
    return redirect(url_for('admin_panel'))

if __name__ == '__main__':
    host = os.environ.get('DCONT_HOST', '127.0.0.1')
    port = int(os.environ.get('DCONT_PORT', '5000'))
    app.run(host=host, port=port, debug=True)
