from flask import Flask, render_template, request, redirect, url_for, session, flash, abort, send_from_directory, g
import sqlite3
import os
import random
import uuid
from werkzeug.utils import secure_filename
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import date, datetime, timedelta
from functools import wraps
from urllib.parse import quote
import smtplib
from email.message import EmailMessage
import re
import time

from flask import jsonify

try:
    from webauthn import (
        generate_registration_options,
        verify_registration_response,
        generate_authentication_options,
        verify_authentication_response,
        options_to_json,
    )
    from webauthn.helpers.structs import (
        AuthenticatorSelectionCriteria,
        PublicKeyCredentialDescriptor,
        UserVerificationRequirement,
    )
    from webauthn.helpers import base64url_to_bytes, bytes_to_base64url
except Exception:
    generate_registration_options = None
    verify_registration_response = None
    generate_authentication_options = None
    verify_authentication_response = None
    options_to_json = None
    AuthenticatorSelectionCriteria = None
    PublicKeyCredentialDescriptor = None
    UserVerificationRequirement = None
    base64url_to_bytes = None
    bytes_to_base64url = None

app = Flask(__name__)
# In production (Render/Heroku/etc.), set SECRET_KEY as an environment variable.
app.secret_key = os.environ.get('SECRET_KEY', 'your_secret_key')
BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def _compute_asset_version() -> str:
    # Prefer build/deploy identifiers when available (Render)
    commit = (
        os.environ.get('RENDER_GIT_COMMIT')
        or os.environ.get('RENDER_COMMIT')
        or os.environ.get('GIT_COMMIT')
        or os.environ.get('COMMIT_SHA')
    )
    if commit:
        return str(commit)[:12]
    try:
        css_path = os.path.join(BASE_DIR, 'static', 'app.css')
        return str(int(os.path.getmtime(css_path)))
    except Exception:
        return str(int(time.time()))


ASSET_VERSION = _compute_asset_version()

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

# Referral system
REFERRAL_REWARD_AMOUNT = 10  # ₹10 one-time
# Referral rewards are app-fee credits (not withdrawable cash).
APP_FEE_CREDIT_EXPIRY_DAYS = 183  # ~6 months
APP_FEE_CREDIT_MAX_APPLY_PER_MONTH = 30


def _current_month_key() -> str:
    # YYYY-MM
    return date.today().strftime('%Y-%m')


def _get_app_fee_amount_int(conn: sqlite3.Connection | None = None) -> int:
    # Uses owner setting `app_fee_amount` (defaults to 0 if missing/invalid)
    raw = (get_setting('app_fee_amount', '0') or '0').strip()
    try:
        v = int(raw)
    except ValueError:
        v = 0
    return max(0, v)


def _available_app_fee_credit(conn: sqlite3.Connection, username: str) -> int:
    uname = (username or '').strip()
    if not uname:
        return 0
    now = datetime.now().isoformat(timespec='seconds')
    c = conn.cursor()
    try:
        c.execute(
            """
            SELECT COALESCE(SUM(COALESCE(credit_amount,0)),0)
            FROM referrals
            WHERE referrer_username=?
              AND UPPER(COALESCE(status,''))='CREDITED'
              AND COALESCE(credit_used,0)=0
              AND COALESCE(credit_expires_at,'') > ?
            """,
            (uname, now),
        )
        return int((c.fetchone() or [0])[0] or 0)
    except sqlite3.OperationalError:
        return 0


def _preview_app_fee_credit_apply(conn: sqlite3.Connection, username: str, gross_fee: int) -> int:
    # Preview how much credit can be applied this month (does not consume credits).
    available = _available_app_fee_credit(conn, username)
    cap = max(0, int(APP_FEE_CREDIT_MAX_APPLY_PER_MONTH))
    gross = max(0, int(gross_fee or 0))
    possible = min(available, cap, gross)
    # Credits are minted in ₹10 chunks, so apply in ₹10 increments.
    if REFERRAL_REWARD_AMOUNT > 0:
        possible = (possible // int(REFERRAL_REWARD_AMOUNT)) * int(REFERRAL_REWARD_AMOUNT)
    return int(possible)


def _apply_app_fee_credit_for_month(conn: sqlite3.Connection, username: str, month_key: str, gross_fee: int) -> int:
    uname = (username or '').strip()
    mkey = (month_key or '').strip()
    if not uname or not mkey:
        return 0

    gross = max(0, int(gross_fee or 0))
    if gross <= 0:
        return 0

    target = min(gross, int(APP_FEE_CREDIT_MAX_APPLY_PER_MONTH))
    if REFERRAL_REWARD_AMOUNT > 0:
        target = (target // int(REFERRAL_REWARD_AMOUNT)) * int(REFERRAL_REWARD_AMOUNT)
    if target <= 0:
        return 0

    now = datetime.now().isoformat(timespec='seconds')
    c = conn.cursor()
    try:
        c.execute(
            """
            SELECT id, COALESCE(credit_amount,0)
            FROM referrals
            WHERE referrer_username=?
              AND UPPER(COALESCE(status,''))='CREDITED'
              AND COALESCE(credit_used,0)=0
              AND COALESCE(credit_expires_at,'') > ?
            ORDER BY COALESCE(credited_at,''), id ASC
            """,
            (uname, now),
        )
        rows = c.fetchall() or []
    except sqlite3.OperationalError:
        rows = []

    applied = 0
    for rid, amt in rows:
        try:
            amount = int(amt or 0)
        except (TypeError, ValueError):
            amount = 0
        if amount <= 0:
            continue
        if applied + amount > target:
            continue
        try:
            c.execute(
                """
                UPDATE referrals
                SET credit_used=1, credit_used_at=?, credit_used_month=?
                WHERE id=? AND COALESCE(credit_used,0)=0
                """,
                (now, mkey, int(rid or 0)),
            )
            if c.rowcount and int(c.rowcount) > 0:
                applied += amount
        except sqlite3.OperationalError:
            continue
        if applied >= target:
            break
    return int(applied)


def _ensure_app_fee_current_month(conn: sqlite3.Connection, username: str) -> None:
    # If a previous month was marked paid, reset app_fee_paid for the new month.
    uname = (username or '').strip()
    if not uname:
        return
    c = conn.cursor()
    month_key = _current_month_key()
    try:
        c.execute(
            "SELECT COALESCE(app_fee_paid,0), COALESCE(app_fee_paid_month,'') FROM users WHERE username=?",
            (uname,),
        )
        row = c.fetchone()
    except sqlite3.OperationalError:
        row = None
    if not row:
        return
    paid_flag = int(row[0] or 0)
    paid_month = (row[1] or '').strip()
    if paid_flag == 1 and paid_month and paid_month != month_key:
        try:
            c.execute("UPDATE users SET app_fee_paid=0 WHERE username=?", (uname,))
        except sqlite3.OperationalError:
            return


def _verify_app_fee_payment(conn: sqlite3.Connection, username: str) -> tuple[int, int, int, str]:
    """Marks the monthly app fee as verified for `username`.

    Returns: (gross_amount, credit_applied, net_amount, month_key)
    """
    uname = (username or '').strip()
    if not uname:
        return (0, 0, 0, _current_month_key())

    month_key = _current_month_key()
    gross = _get_app_fee_amount_int(conn)
    now = datetime.now().isoformat(timespec='seconds')
    c = conn.cursor()

    # Ensure ledger table exists (init_db should create it; this is a safety net)
    try:
        c.execute(
            '''CREATE TABLE IF NOT EXISTS app_fee_payments (
                id INTEGER PRIMARY KEY,
                username TEXT,
                month TEXT,
                gross_amount INTEGER,
                credit_applied INTEGER,
                net_amount INTEGER,
                verified_at TEXT,
                UNIQUE(username, month)
            )'''
        )
    except sqlite3.OperationalError:
        pass

    # If already verified for this month, don't consume credits again.
    try:
        c.execute(
            "SELECT gross_amount, credit_applied, net_amount FROM app_fee_payments WHERE username=? AND month=?",
            (uname, month_key),
        )
        existing = c.fetchone()
    except sqlite3.OperationalError:
        existing = None
    if existing:
        try:
            eg, ec, en = existing
        except Exception:
            eg, ec, en = gross, 0, gross
        c.execute(
            "UPDATE users SET app_fee_paid=1, app_fee_paid_month=?, first_app_fee_verified=1 WHERE username=?",
            (month_key, uname),
        )
        return (int(eg or 0), int(ec or 0), int(en or 0), month_key)

    credit_applied = _apply_app_fee_credit_for_month(conn, uname, month_key, gross)
    net = max(0, int(gross) - int(credit_applied))
    try:
        c.execute(
            "INSERT INTO app_fee_payments (username, month, gross_amount, credit_applied, net_amount, verified_at) VALUES (?,?,?,?,?,?)",
            (uname, month_key, int(gross), int(credit_applied), int(net), now),
        )
    except sqlite3.OperationalError:
        # Best-effort: keep going
        pass

    c.execute(
        "UPDATE users SET app_fee_paid=1, app_fee_paid_month=?, first_app_fee_verified=1 WHERE username=?",
        (month_key, uname),
    )
    return (int(gross), int(credit_applied), int(net), month_key)


def _normalize_referral_code(raw: str) -> str:
    code = (raw or '').strip().upper()
    return re.sub(r'[^A-Z0-9]', '', code)


def _make_referral_code_from_user_id(user_id: int) -> str:
    try:
        uid = int(user_id)
    except (TypeError, ValueError):
        uid = 0
    uid = max(0, uid)
    return f"DC{uid:06d}"


def _user_has_joined_any_group(conn: sqlite3.Connection, username: str) -> bool:
    uname = (username or '').strip()
    if not uname:
        return False
    c = conn.cursor()
    try:
        c.execute(
            "SELECT 1 FROM group_members WHERE username=? AND status='joined' LIMIT 1",
            (uname,),
        )
        return c.fetchone() is not None
    except sqlite3.OperationalError:
        return False


def _maybe_mark_referral_eligible(conn: sqlite3.Connection, new_username: str) -> None:
    uname = (new_username or '').strip()
    if not uname:
        return
    c = conn.cursor()
    try:
        c.execute(
            "SELECT id, COALESCE(status,'') FROM referrals WHERE new_username=?",
            (uname,),
        )
        row = c.fetchone()
    except sqlite3.OperationalError:
        row = None
    if not row:
        return

    referral_id = int(row[0] or 0)
    status = (row[1] or '').strip().upper()
    if referral_id <= 0 or status in {'ELIGIBLE', 'CREDITED'}:
        return

    # Eligible only when the new user has joined a group AND first app fee is verified.
    try:
        c.execute("SELECT COALESCE(first_app_fee_verified,0) FROM users WHERE username=?", (uname,))
        u = c.fetchone()
    except sqlite3.OperationalError:
        u = None
    first_verified = int((u[0] if u else 0) or 0)
    if first_verified != 1:
        return
    if not _user_has_joined_any_group(conn, uname):
        return

    now = datetime.now().isoformat(timespec='seconds')
    try:
        c.execute("UPDATE referrals SET status=?, eligible_at=? WHERE id=?", ('ELIGIBLE', now, referral_id))
    except sqlite3.OperationalError:
        return

WHATSAPP_SUPPORT_NUMBER = '917506680031'  # +91 7506680031

# ---- Language (EN/HI) ----
SUPPORTED_LANGS = {
    'en': 'English (EN)',
    'hi': 'हिंदी',
}


def _normalize_lang(value: str) -> str:
    v = (value or '').strip().lower()
    if v in {'hi', 'hindi', 'हिंदी'}:
        return 'hi'
    if v in {'en', 'english', 'eng', 'en-us', 'en-in'}:
        return 'en'
    # Back-compat values stored in DB (English/Hindi/Hinglish)
    if v == 'hinglish':
        return 'en'
    return 'en'


def _get_user_language(username: str) -> str:
    username = (username or '').strip()
    if not username:
        return 'en'
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute('SELECT COALESCE(language,\'\') FROM users WHERE username=?', (username,))
        row = c.fetchone()
    except sqlite3.OperationalError:
        row = None
    conn.close()
    return _normalize_lang(row[0] if row else '')


@app.before_request
def _set_request_language():
    # Owner/admin UI stays English.
    if session.get('role') == 'admin':
        g.lang = 'en'
        return

    lang = session.get('lang')
    if not lang and session.get('username'):
        lang = _get_user_language(session.get('username'))
        session['lang'] = lang
    g.lang = _normalize_lang(lang)


TRANSLATIONS = {
    'en': {
    'benefit_one_liner': 'D-CONT never holds your money. Payments happen directly between members via UPI.',
    'benefit_money_never_to_us_title': 'Your money never comes to us',
    'benefit_money_never_to_us_body': 'You pay directly to other group members. D-CONT never touches or holds your money.',
    'benefit_no_interest_title': 'No interest, no hidden cuts',
    'benefit_no_interest_body': 'You get the same money you put in. No interest, no deductions, no hidden charges.',
    'benefit_simple_saving_title': 'Simple monthly saving with a group',
    'benefit_simple_saving_body': 'Save a fixed amount every month with a small group. One person receives the full amount each month.',
    'benefit_transparent_title': 'Everything is transparent',
    'benefit_transparent_body': 'You can see the group status, members, and receiver details. Nothing is hidden.',
    'benefit_trust_grows_title': 'Your trust grows when you pay on time',
    'benefit_trust_grows_body': 'Paying on time improves your Trust Score. Higher trust unlocks more access.',
    'benefit_restrictions_title': 'Late payers are restricted',
    'benefit_restrictions_body': 'Missed payments reduce trust and future access. Honest members are protected.',
    'benefit_upi_only_title': 'No bank visits, no paperwork',
    'benefit_upi_only_body': 'Everything works using your UPI. No office visits.',
    'benefit_small_amounts_title': 'Works even for small amounts',
    'benefit_small_amounts_body': 'Start with just ₹500 per month. Increase later if you want.',
    'benefit_control_title': 'You stay in control',
    'benefit_control_body': 'You choose your groups and monthly amount. You can leave after completion as per group rules.',
    'benefit_deposit_title': 'Security deposit protects the group',
    'benefit_deposit_body': 'Early payout may require a refundable security deposit. This helps protect the group if someone stops paying.',
    'benefit_deposit_return_body': 'If you keep paying as agreed, the deposit can be refunded as per verification and group rules.',
    'info_payout_label': 'Payout/receiver is announced on the due date.',
    'info_deposit_label': 'Deposit is only for eligible early payout requests.',
        'brand_tagline': 'Smart Savings Circle',
        'nav_home': 'Home',
        'nav_groups': 'Groups',
        'nav_payments': 'Payments',
        'nav_profile': 'Profile',
        'nav_rewards': 'Reward',
        'nav_support': 'Support',
        'login_title': 'Login',
        'login_identifier_label': 'Username / Mobile',
        'login_identifier_ph': 'Enter username or mobile',
        'login_password_label': 'Password',
        'login_password_ph': 'Enter password',
        'login_btn': 'Login',
        'login_mpin_title': 'Enter your MPIN',
        'login_mpin_help': 'Use MPIN for faster login on this device.',
        'login_mpin_btn': 'Login with MPIN',
        'login_or': 'OR',
        'login_mobile_title': 'Login with your mobile number',
        'login_mobile_label': 'Mobile Number',
        'login_mobile_ph': 'Enter mobile number',
        'login_fingerprint_btn': 'Login using fingerprint',
        'login_fingerprint_help': 'Uses your device passkey/biometric if enabled.',
        'login_enable_fingerprint_hint': 'First login with password and enable fingerprint in Profile.',
        'login_rate_limited': 'Too many login attempts. Please try again after 15 minutes.',
        'login_language': 'Language',
        'login_admin_title': 'Admin Login',
        'login_admin_user': 'User ID',
        'login_admin_user_ph': 'Enter admin user id',
        'login_admin_pw': 'Password',
        'login_admin_btn': 'Login as Admin',
        'login_no_account': "Don’t have an account?",
        'login_register': 'Register',
        'terms': 'Terms & Conditions',
        'trust_notice_title': 'Trust:',
        'trust_notice_line1': 'D-cont never holds money. Payments happen directly between members.',
        'trust_notice_line2': 'We will never ask for your password or UPI PIN.',
        'home_greeting': 'Hi, {name}!',
        'non_custodial': 'Non-custodial:',
        'non_custodial_line': 'D-cont does not hold money. You contribute directly via your UPI.',
        'need_help': 'Need help?',
        'chat_with_bot': 'Chat with BOT',
        'join_group': 'Join Group',
        'go_to_groups': 'Go to Groups',
        'go_to_payments': 'Go to Payments',
        'groups_title': 'Groups',
        'my_groups': 'My Groups',
        'join_a_group': 'Join a Group',
        'view_details': 'View details',
        'members': 'Members',
        'status': 'Status',
        'not_set': 'Not set',
        'profile_title': 'Profile',
        'profile_quick_login_title': 'Quick Login (MPIN / Fingerprint)',
        'profile_mpin_title': 'Set or change MPIN',
        'profile_mpin_current_password': 'Current Password',
        'profile_mpin_new': 'New MPIN (4 digits)',
        'profile_mpin_confirm': 'Confirm MPIN',
        'profile_mpin_save': 'Save MPIN',
        'profile_mpin_disable': 'Disable MPIN',
        'profile_fingerprint_title': 'Fingerprint login (Passkey)',
        'profile_fingerprint_enable': 'Enable Fingerprint Login',
        'profile_fingerprint_enabled': 'Enabled on this account.',
        'profile_fingerprint_note': 'This uses your phone biometric via passkeys (WebAuthn).',
        'profile_fingerprint_disable': 'Disable Fingerprint Login',
        'save': 'Save',
        'logout': 'Logout',
        'payments_title': 'Payments',
        'who_to_pay': 'Who to Pay',
        'pay_via_upi': 'Pay via UPI',
        'payment_link_unavailable': 'Payment link unavailable (receiver UPI not set).',

        # --- Customer UI polish (labels/messages) ---
        'label_per_month': '/ month',
        'label_receiver': 'Receiver:',
        'label_upi_short': 'UPI:',
        'label_receiver_name': 'Receiver name:',
        'label_due_today': 'Due today:',
        'label_pay_before': 'Please pay before',
        'payments_pay_directly': 'Pay directly to the receiver shown above.',
        'payments_join_to_see': 'Join a group to see payment instructions.',
        'label_your_upi': 'Your UPI:',
        'btn_add_upi_in_profile': 'Add UPI in Profile',

        'label_full_name': 'Full Name',
        'label_mobile_read_only': 'Mobile (read-only)',
        'label_upi_id': 'UPI ID',
        'documents_title': 'Important documents',
        'documents_help': 'Upload PDF/JPG/PNG/WEBP. Your documents are stored privately and can only be downloaded when you are logged in.',
        'download': 'Download',
        'not_uploaded': 'Not uploaded',

        'support_title': 'Support',
        'support_body': 'Have a question? Chat with the D-CONT Bot.',
        'open_chat': 'Open Chat',

        'placeholder_reason_optional': 'Reason (optional)',
        'btn_request_early_payout': 'Request Early Payout',
        'placeholder_utr_reference': 'Enter UTR/reference',

        'home_what_do_now_title': 'What should I do right now?',
        'home_what_do_now_body': 'Join an existing group created by the owner.',
        'home_groups_status_title': 'Your groups status',
        'home_kpi_active_groups': 'Active Groups',
        'home_kpi_formation_groups': 'Formation Groups',
        'home_want_early_payout_title': 'Want Early Payout?',
        'home_want_early_payout_body': 'If eligible, request early payout from your Profile.',
        'home_open_early_payout': 'Open Early Payout',
        'home_chat_bot_help': 'Chat with the D-CONT Bot for steps, safety tips, and support.',

        # --- Referral (customer UI) ---
        'referral_card_title': 'Invite & Earn ₹10',
        'referral_card_body': 'Earn ₹10 app fee credit when your friend joins a group and pays their first app fee. Rewards are credited after payment verification.',
        'referral_credit_disclaimer': 'Referral rewards are given as app fee credits. Credits reduce your monthly platform fee and are not withdrawable as cash.',
        'referral_your_code': 'Your referral code:',
        'referral_share': 'Share',
        'referral_total_rewards': 'Total Rewards:',
        'referral_app_fee_credit_balance': 'Current Credit Balance:',
        'referral_list_title': 'Your referrals',
        'referral_status_pending': 'Pending',
        'referral_status_eligible': 'Eligible',
        'referral_status_paid': 'Paid',
        'referral_status_credited': 'Credited',
        'referral_stage_joined_group': 'Joined group',
        'referral_stage_not_joined': 'Not joined yet',
        'referral_stage_fee_paid': 'Paid app fee',
        'referral_stage_fee_pending': 'Pending payment',
        'referral_none_yet': 'No referrals yet. Share your code to invite friends.',
        'home_referral_small_title': 'Invite friends. Earn ₹10.',
        'home_referral_small_cta': 'Open in Profile',

        'app_fee_card_title': 'Monthly App Fee',
        'app_fee_monthly_fee': 'Monthly App Fee:',
        'app_fee_credit_applied': 'Credit Applied:',
        'app_fee_you_pay': 'You Pay:',
        'app_fee_paid_this_month': 'Paid (this month)',

        'profile_app_fee_paid': 'App Fee Paid',
        'yes': 'Yes',
        'no': 'No',
        'trust_score_label': 'Trust Score',

        'early_payout_title': 'Early Payout',
        'early_payout_intro': 'Early payout is a schedule/priority request (not a loan). If eligible, you can request to receive earlier than your current turn. A security deposit may be required and is verified by the owner.',
        'monthly_amount_label': 'Monthly amount:',
        'deposit_label': 'Deposit:',
        'eligible_for_early_payout': 'Eligible for early payout.',
        'not_eligible_prefix': 'Not eligible:',
        'join_group_for_early_payout': 'Join a group to request early payout.',
        'your_requests_title': 'Your requests',
        'created_label': 'Created:',
        'status_label_short': 'Status:',
        'deposit_status_label': 'Deposit:',
        'deposit_amount_label': 'Deposit amount:',
        'deposit_pay_instruction': 'Pay the security deposit to {upi}, then submit the UTR/reference.',
        'deposit_upi_not_set': 'Security deposit payment UPI is not set. Please contact support.',
        'submit_deposit_utr': 'Submit Deposit UTR',
        'deposit_verified_under_review': 'Deposit verified. Your request is under review.',
        'no_early_payout_requests': 'No early payout requests yet.',

        'members_list_unavailable': 'Members list unavailable.',
        'btn_join': 'Join',
        'no_groups_available_to_join': 'No groups available to join right now.',
        'no_joined_groups_yet': "You haven’t joined any group yet.",

        'back_to_groups': 'Back to Groups',
        'label_status': 'Status:',
        'label_members': 'Members:',
        'label_receiver_upi': 'Receiver UPI:',
    },
    'hi': {
        'benefit_one_liner': 'D-CONT आपका पैसा कभी नहीं रखता। भुगतान UPI से सीधे सदस्यों के बीच होता है।',
        'benefit_money_never_to_us_title': 'आपका पैसा हमारे पास नहीं आता',
        'benefit_money_never_to_us_body': 'आप सीधे अन्य समूह सदस्यों को भुगतान करते हैं। D-CONT आपका पैसा कभी नहीं छूता/रखता।',
        'benefit_no_interest_title': 'ना ब्याज, ना छुपे कट',
        'benefit_no_interest_body': 'आपको उतना ही पैसा मिलता है जितना आप देते हैं। कोई ब्याज, कटौती या छुपा चार्ज नहीं।',
        'benefit_simple_saving_title': 'समूह के साथ आसान मासिक बचत',
        'benefit_simple_saving_body': 'छोटे समूह में हर महीने तय राशि बचत करें। हर महीने एक सदस्य को पूरी राशि मिलती है।',
        'benefit_transparent_title': 'सब कुछ पारदर्शी है',
        'benefit_transparent_body': 'आप समूह की स्थिति, सदस्य और रिसीवर विवरण देख सकते हैं। कुछ भी छुपा नहीं।',
        'benefit_trust_grows_title': 'समय पर भुगतान से ट्रस्ट बढ़ता है',
        'benefit_trust_grows_body': 'समय पर भुगतान करने से आपका Trust Score बेहतर होता है। अधिक ट्रस्ट से अधिक अवसर मिलते हैं।',
        'benefit_restrictions_title': 'लेट पेयर्स पर रोक',
        'benefit_restrictions_body': 'भुगतान मिस होने पर ट्रस्ट और आगे की एक्सेस कम हो सकती है। ईमानदार सदस्य सुरक्षित रहते हैं।',
        'benefit_upi_only_title': 'ना बैंक के चक्कर, ना पेपरवर्क',
        'benefit_upi_only_body': 'सब कुछ आपके UPI से होता है। ऑफिस विज़िट नहीं।',
        'benefit_small_amounts_title': 'छोटी राशि से भी शुरू करें',
        'benefit_small_amounts_body': '₹500/माह से शुरू करें। चाहें तो बाद में बढ़ा सकते हैं।',
        'benefit_control_title': 'कंट्रोल आपके पास',
        'benefit_control_body': 'आप अपना समूह और राशि चुनते हैं। नियमों के अनुसार पूरा होने के बाद छोड़ सकते हैं।',
        'benefit_deposit_title': 'सिक्योरिटी डिपॉज़िट से सुरक्षा',
        'benefit_deposit_body': 'अर्ली पेआउट के लिए रिफंडेबल सिक्योरिटी डिपॉज़िट लग सकता है। यह समूह को सुरक्षा देता है।',
        'benefit_deposit_return_body': 'नियम अनुसार भुगतान जारी रखने पर डिपॉज़िट वेरिफिकेशन के बाद रिफंड हो सकता है।',
        'info_payout_label': 'ड्यू डेट पर रिसीवर/पेयआउट की जानकारी दिखती है।',
        'info_deposit_label': 'डिपॉज़िट केवल योग्य अर्ली पेआउट रिक्वेस्ट के लिए होता है।',
        'brand_tagline': 'स्मार्ट बचत समूह',
        'nav_home': 'होम',
        'nav_groups': 'समूह',
        'nav_payments': 'भुगतान',
        'nav_profile': 'प्रोफ़ाइल',
        'nav_rewards': 'रिवॉर्ड',
        'nav_support': 'सपोर्ट',
        'login_title': 'लॉगिन',
        'login_identifier_label': 'यूज़रनेम / मोबाइल',
        'login_identifier_ph': 'यूज़रनेम या मोबाइल दर्ज करें',
        'login_password_label': 'पासवर्ड',
        'login_password_ph': 'पासवर्ड दर्ज करें',
        'login_btn': 'लॉगिन करें',
        'login_mpin_title': 'अपना MPIN डालें',
        'login_mpin_help': 'इस डिवाइस पर तेज़ लॉगिन के लिए MPIN उपयोग करें।',
        'login_mpin_btn': 'MPIN से लॉगिन',
        'login_or': 'या',
        'login_mobile_title': 'मोबाइल नंबर से लॉगिन',
        'login_mobile_label': 'मोबाइल नंबर',
        'login_mobile_ph': 'मोबाइल नंबर दर्ज करें',
        'login_fingerprint_btn': 'फिंगरप्रिंट से लॉगिन',
        'login_fingerprint_help': 'यदि सक्षम है तो डिवाइस पासकी/बायोमेट्रिक उपयोग होगा।',
        'login_enable_fingerprint_hint': 'पहले पासवर्ड से लॉगिन करें, फिर प्रोफ़ाइल में फिंगरप्रिंट सक्षम करें।',
        'login_rate_limited': 'बहुत ज्यादा लॉगिन प्रयास। कृपया 15 मिनट बाद फिर प्रयास करें।',
        'login_language': 'भाषा',
        'login_admin_title': 'एडमिन लॉगिन',
        'login_admin_user': 'यूज़र आईडी',
        'login_admin_user_ph': 'एडमिन यूज़र आईडी दर्ज करें',
        'login_admin_pw': 'पासवर्ड',
        'login_admin_btn': 'एडमिन के रूप में लॉगिन',
        'login_no_account': 'अकाउंट नहीं है?',
        'login_register': 'रजिस्टर करें',
        'terms': 'नियम और शर्तें',
        'trust_notice_title': 'विश्वास:',
        'trust_notice_line1': 'D-cont कभी पैसे नहीं रखता। भुगतान सीधे सदस्यों के बीच होता है।',
        'trust_notice_line2': 'हम कभी आपका पासवर्ड या UPI PIN नहीं पूछेंगे।',
        'home_greeting': 'नमस्ते, {name}!',
        'non_custodial': 'नॉन-कस्टोडियल:',
        'non_custodial_line': 'D-cont पैसे नहीं रखता। आप अपने UPI से सीधे योगदान करते हैं।',
        'need_help': 'मदद चाहिए?',
        'chat_with_bot': 'बॉट से चैट करें',
        'join_group': 'समूह जॉइन करें',
        'go_to_groups': 'समूह देखें',
        'go_to_payments': 'भुगतान देखें',
        'groups_title': 'समूह',
        'my_groups': 'मेरे समूह',
        'join_a_group': 'समूह जॉइन करें',
        'view_details': 'विवरण देखें',
        'members': 'सदस्य',
        'status': 'स्थिति',
        'not_set': 'सेट नहीं है',
        'profile_title': 'प्रोफ़ाइल',
        'profile_quick_login_title': 'क्विक लॉगिन (MPIN / फिंगरप्रिंट)',
        'profile_mpin_title': 'MPIN सेट/बदलें',
        'profile_mpin_current_password': 'वर्तमान पासवर्ड',
        'profile_mpin_new': 'नया MPIN (4 अंक)',
        'profile_mpin_confirm': 'MPIN पुष्टि',
        'profile_mpin_save': 'MPIN सेव करें',
        'profile_mpin_disable': 'MPIN बंद करें',
        'profile_fingerprint_title': 'फिंगरप्रिंट लॉगिन (पासकी)',
        'profile_fingerprint_enable': 'फिंगरप्रिंट लॉगिन सक्षम करें',
        'profile_fingerprint_enabled': 'इस अकाउंट पर सक्षम है।',
        'profile_fingerprint_note': 'यह पासकी (WebAuthn) से फोन बायोमेट्रिक उपयोग करता है।',
        'profile_fingerprint_disable': 'फिंगरप्रिंट लॉगिन बंद करें',
        'save': 'सेव करें',
        'logout': 'लॉगआउट',
        'payments_title': 'भुगतान',
        'who_to_pay': 'किसको भुगतान करना है',
        'pay_via_upi': 'UPI से भुगतान करें',
        'payment_link_unavailable': 'पेमेंट लिंक उपलब्ध नहीं (रिसीवर UPI सेट नहीं है)।',

        # --- Customer UI polish (labels/messages) ---
        'label_per_month': '/ माह',
        'label_receiver': 'रिसीवर:',
        'label_upi_short': 'UPI:',
        'label_receiver_name': 'रिसीवर नाम:',
        'label_due_today': 'आज ड्यू:',
        'label_pay_before': 'कृपया भुगतान करें (समय सीमा)',
        'payments_pay_directly': 'ऊपर दिखाए गए रिसीवर को सीधे भुगतान करें।',
        'payments_join_to_see': 'पेमेंट निर्देश देखने के लिए किसी समूह में जॉइन करें।',
        'label_your_upi': 'आपका UPI:',
        'btn_add_upi_in_profile': 'प्रोफ़ाइल में UPI जोड़ें',

        'label_full_name': 'पूरा नाम',
        'label_mobile_read_only': 'मोबाइल (केवल पढ़ने हेतु)',
        'label_upi_id': 'UPI ID',
        'documents_title': 'महत्वपूर्ण दस्तावेज़',
        'documents_help': 'PDF/JPG/PNG/WEBP अपलोड करें। आपके दस्तावेज़ निजी रूप से सुरक्षित हैं और केवल लॉगिन के बाद डाउनलोड हो सकते हैं।',
        'download': 'डाउनलोड',
        'not_uploaded': 'अपलोड नहीं हुआ',

        'support_title': 'सपोर्ट',
        'support_body': 'कोई सवाल है? D-CONT बॉट से चैट करें।',
        'open_chat': 'चैट खोलें',

        'placeholder_reason_optional': 'कारण (वैकल्पिक)',
        'btn_request_early_payout': 'अर्ली पेआउट रिक्वेस्ट करें',
        'placeholder_utr_reference': 'UTR/रेफरेंस दर्ज करें',

        'home_what_do_now_title': 'अभी क्या करें?',
        'home_what_do_now_body': 'मालिक द्वारा बनाए गए समूह को जॉइन करें।',
        'home_groups_status_title': 'आपके समूहों की स्थिति',
        'home_kpi_active_groups': 'सक्रिय समूह',
        'home_kpi_formation_groups': 'फॉर्मेशन समूह',
        'home_want_early_payout_title': 'अर्ली पेआउट चाहिए?',
        'home_want_early_payout_body': 'यदि आप योग्य हैं, तो प्रोफ़ाइल से अर्ली पेआउट रिक्वेस्ट करें।',
        'home_open_early_payout': 'अर्ली पेआउट खोलें',
        'home_chat_bot_help': 'स्टेप्स, सुरक्षा टिप्स और सपोर्ट के लिए D-CONT बॉट से चैट करें।',

        # --- Referral (customer UI) ---
        'referral_card_title': 'इनवाइट करें और ₹10 कमाएँ',
        'referral_card_body': 'जब आपका दोस्त समूह जॉइन करे और पहली ऐप फ़ीस का भुगतान करे, तब आपको ₹10 ऐप फ़ीस क्रेडिट मिलता है। रिवॉर्ड पेमेंट वेरिफिकेशन के बाद क्रेडिट होता है।',
        'referral_credit_disclaimer': 'Referral rewards are given as app fee credits. Credits reduce your monthly platform fee and are not withdrawable as cash.',
        'referral_your_code': 'आपका रेफरल कोड:',
        'referral_share': 'शेयर करें',
        'referral_total_rewards': 'कुल रिवॉर्ड:',
        'referral_app_fee_credit_balance': 'Current Credit Balance:',
        'referral_list_title': 'आपके रेफरल',
        'referral_status_pending': 'पेंडिंग',
        'referral_status_eligible': 'योग्य',
        'referral_status_paid': 'पेड',
        'referral_status_credited': 'क्रेडिटेड',
        'referral_stage_joined_group': 'समूह जॉइन',
        'referral_stage_not_joined': 'अभी जॉइन नहीं किया',
        'referral_stage_fee_paid': 'ऐप फ़ीस भुगतान',
        'referral_stage_fee_pending': 'भुगतान पेंडिंग',
        'referral_none_yet': 'अभी कोई रेफरल नहीं है। दोस्तों को इनवाइट करने के लिए अपना कोड शेयर करें।',
        'home_referral_small_title': 'दोस्तों को इनवाइट करें। ₹10 कमाएँ।',
        'home_referral_small_cta': 'प्रोफ़ाइल में खोलें',

        'app_fee_card_title': 'Monthly App Fee',
        'app_fee_monthly_fee': 'Monthly App Fee:',
        'app_fee_credit_applied': 'Credit Applied:',
        'app_fee_you_pay': 'You Pay:',
        'app_fee_paid_this_month': 'Paid (this month)',

        'profile_app_fee_paid': 'ऐप फ़ीस भुगतान',
        'yes': 'हाँ',
        'no': 'नहीं',
        'trust_score_label': 'Trust Score',

        'early_payout_title': 'अर्ली पेआउट',
        'early_payout_intro': 'अर्ली पेआउट एक शेड्यूल/प्रायोरिटी रिक्वेस्ट है (लोन नहीं)। यदि आप योग्य हैं, तो अपने टर्न से पहले पाने के लिए रिक्वेस्ट कर सकते हैं। सिक्योरिटी डिपॉज़िट लग सकता है और इसे मालिक वेरिफाई करता है।',
        'monthly_amount_label': 'मासिक राशि:',
        'deposit_label': 'डिपॉज़िट:',
        'eligible_for_early_payout': 'अर्ली पेआउट के लिए योग्य।',
        'not_eligible_prefix': 'योग्य नहीं:',
        'join_group_for_early_payout': 'अर्ली पेआउट रिक्वेस्ट के लिए किसी समूह में जॉइन करें।',
        'your_requests_title': 'आपकी रिक्वेस्ट',
        'created_label': 'बनाया गया:',
        'status_label_short': 'स्थिति:',
        'deposit_status_label': 'डिपॉज़िट:',
        'deposit_amount_label': 'डिपॉज़िट राशि:',
        'deposit_pay_instruction': '{upi} पर सिक्योरिटी डिपॉज़िट का भुगतान करें, फिर UTR/रेफरेंस सबमिट करें।',
        'deposit_upi_not_set': 'सिक्योरिटी डिपॉज़िट के लिए UPI सेट नहीं है। कृपया सपोर्ट से संपर्क करें।',
        'submit_deposit_utr': 'डिपॉज़िट UTR सबमिट करें',
        'deposit_verified_under_review': 'डिपॉज़िट वेरिफाइड। आपकी रिक्वेस्ट रिव्यू में है।',
        'no_early_payout_requests': 'अभी कोई अर्ली पेआउट रिक्वेस्ट नहीं है।',

        'members_list_unavailable': 'सदस्यों की सूची उपलब्ध नहीं है।',
        'btn_join': 'जॉइन',
        'no_groups_available_to_join': 'अभी जॉइन करने के लिए कोई समूह उपलब्ध नहीं है।',
        'no_joined_groups_yet': 'आपने अभी तक कोई समूह जॉइन नहीं किया है।',

        'back_to_groups': 'समूहों पर वापस',
        'label_status': 'स्थिति:',
        'label_members': 'सदस्य:',
        'label_receiver_upi': 'रिसीवर UPI:',
    },
}


def t(key: str, **kwargs) -> str:
    lang = _normalize_lang(getattr(g, 'lang', None) or session.get('lang') or 'en')
    base = TRANSLATIONS.get('en', {})
    table = TRANSLATIONS.get(lang, {})
    text = table.get(key) or base.get(key) or key
    try:
        return text.format(**kwargs)
    except Exception:
        return text


@app.context_processor
def _inject_i18n():
    return {
        't': t,
        'current_lang': _normalize_lang(getattr(g, 'lang', None) or session.get('lang') or 'en'),
        'supported_langs': SUPPORTED_LANGS,
    }

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
    {
        'key': 'install_app',
        'triggers': [
            "download app",
            "get app",
            "android app",
            "iphone app",
            "ios app",
            "app on phone",
            "install app",
            "add to home screen",
            "home screen",
            "how to install",
            "how to get d-cont app",
            "how to get d cont app",
        ],
        'answer': (
            "You can use D-CONT like an app without Play Store: "
            "Android (Chrome): open https://d-cont-web.onrender.com → tap ⋮ → Add to Home screen → Add (or tap ‘Install app’ if shown). "
            "iPhone (Safari): open the link in Safari → Share → Add to Home Screen → Add."
        ),
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


def trust_band(score: int) -> str:
    try:
        s = int(score)
    except (TypeError, ValueError):
        s = 50
    if s >= 80:
        return 'Excellent'
    if s >= 60:
        return 'Good'
    if s >= 40:
        return 'Risky'
    return 'High risk'


def trust_badge_class(score: int) -> str:
    band = trust_band(score)
    if band == 'Excellent':
        return 'badge badgeSuccess'
    if band == 'Risky':
        return 'badge badgeWarn'
    if band == 'High risk':
        return 'badge badgeDanger'
    return 'badge'


@app.template_filter('trust_band')
def jinja_trust_band_filter(value):
    return trust_band(value)


@app.template_filter('trust_badge_class')
def jinja_trust_badge_class_filter(value):
    return trust_badge_class(value)


def _parse_iso_date(value: str):
    v = (value or '').strip()
    if not v:
        return None
    try:
        return datetime.strptime(v, '%Y-%m-%d').date()
    except ValueError:
        return None


def _today_iso() -> str:
    return date.today().isoformat()


def _get_trust_grace_days() -> int:
    raw = (get_setting('trust_grace_days', '2') or '2').strip()
    try:
        g = int(raw)
    except ValueError:
        g = 2
    return max(0, min(14, g))


def calculate_trust_from_history(username: str) -> dict:
    """History-based (Option A) Trust Score.

    Event types supported:
    - contribution_verified: uses due_date + verified_at to classify on-time/late/missed
    - contribution_rejected
    - payment_missed
    - default_after_payout
    - deposit_verified
    - group_completed
    """
    username = (username or '').strip()
    if not username:
        return {'score': 50, 'breakdown': {}, 'events': []}

    grace_days = _get_trust_grace_days()

    conn = get_db()
    c = conn.cursor()
    try:
        c.execute(
            """
            SELECT id, event_type, group_id, due_date, verified_at, created_at, note
            FROM trust_events
            WHERE username=?
            ORDER BY id DESC
            """,
            (username,),
        )
        rows = c.fetchall()
    except sqlite3.OperationalError:
        rows = []
    conn.close()

    on_time = 0
    late = 0
    missed = 0
    rejected = 0
    completed_groups = 0
    deposit_verified = 0
    default_after_payout = 0

    events = []
    for r in rows:
        event_id, event_type, group_id, due_date, verified_at, created_at, note = r
        event_type = (event_type or '').strip().lower()
        due = _parse_iso_date(due_date)
        verified = _parse_iso_date(verified_at)

        if event_type == 'contribution_verified':
            if due and verified:
                if verified <= due:
                    on_time += 1
                else:
                    # After due date is late; after grace is missed too.
                    late += 1
                    if (verified - due).days > grace_days:
                        missed += 1
            else:
                # If dates are missing, treat as late (minimal positive, avoids abuse)
                late += 1
        elif event_type == 'contribution_rejected':
            rejected += 1
        elif event_type == 'payment_missed':
            missed += 1
        elif event_type == 'default_after_payout':
            default_after_payout += 1
        elif event_type == 'deposit_verified':
            deposit_verified += 1
        elif event_type == 'group_completed':
            completed_groups += 1

        events.append(
            {
                'id': int(event_id),
                'type': event_type,
                'group_id': group_id,
                'due_date': (due_date or ''),
                'verified_at': (verified_at or ''),
                'created_at': (created_at or ''),
                'note': (note or ''),
            }
        )

    score = 50
    score += 3 * on_time
    score += 1 * late
    score += 5 * completed_groups
    score += 2 * deposit_verified
    score -= 8 * missed
    score -= 15 * default_after_payout
    score -= 3 * rejected
    score = max(0, min(100, int(score)))

    breakdown = {
        'on_time_verified': on_time,
        'late_verified': late,
        'missed': missed,
        'rejected': rejected,
        'completed_groups': completed_groups,
        'deposit_verified': deposit_verified,
        'default_after_payout': default_after_payout,
        'grace_days': grace_days,
    }
    return {'score': score, 'breakdown': breakdown, 'events': events}


def recalculate_and_store_trust(username: str) -> dict:
    result = calculate_trust_from_history(username)
    score = int(result.get('score', 50))
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute('UPDATE users SET trust_score=? WHERE username=?', (score, username))
        conn.commit()
        conn.close()
    except sqlite3.OperationalError:
        pass
    return result


def _early_payout_deposit_amount(monthly_amount: int, trust_score: int) -> int:
    try:
        amt = int(monthly_amount or 0)
    except (TypeError, ValueError):
        amt = 0
    amt = max(0, amt)
    ts = int(trust_score if trust_score is not None else 50)
    # Higher-trust users can have a lower deposit.
    if ts >= 85:
        return max(0, (amt + 1) // 2)
    return amt


def _user_has_any_kyc(aadhaar_doc: str, pan_doc: str, passport_doc: str) -> bool:
    return bool((aadhaar_doc or '').strip() or (pan_doc or '').strip() or (passport_doc or '').strip())


def _early_payout_eligibility(
    username: str,
    trust_score: int,
    upi_id: str,
    aadhaar_doc: str,
    pan_doc: str,
    passport_doc: str,
) -> tuple[bool, list[str]]:
    reasons: list[str] = []

    if int(trust_score if trust_score is not None else 50) < 75:
        reasons.append('Trust Score must be 75+ for early payout.')

    if not (upi_id or '').strip():
        reasons.append('Set your UPI ID in Profile.')

    if not _user_has_any_kyc(aadhaar_doc, pan_doc, passport_doc):
        reasons.append('Upload at least one KYC document (Aadhaar/PAN/Passport).')

    # Behavior gate: at least 2 verified contributions, and no missed/default events.
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute(
            """
            SELECT event_type
            FROM trust_events
            WHERE username=?
            ORDER BY id DESC
            LIMIT 100
            """,
            (username,),
        )
        rows = c.fetchall()
        conn.close()
    except sqlite3.OperationalError:
        rows = []

    verified_count = 0
    bad_count = 0
    for r in rows:
        et = (r[0] or '').strip().lower()
        if et == 'contribution_verified':
            verified_count += 1
        if et in ('payment_missed', 'default_after_payout'):
            bad_count += 1

    if verified_count < 2:
        reasons.append('Complete at least 2 verified contributions before requesting early payout.')
    if bad_count > 0:
        reasons.append('Early payout is not available if you have missed payments or past defaults.')

    return (len(reasons) == 0), reasons


@app.template_filter('status_label')
def jinja_status_label_filter(value):
    return status_label(value)


@app.template_filter('status_hint')
def jinja_status_hint_filter(value):
    return status_hint(value)


@app.context_processor
def inject_support_links():
    nav_user_pill = None
    username = session.get('username')
    if username:
        try:
            user = get_user_row(username) or {}
        except Exception:
            user = {}

        role = (session.get('role') or user.get('role') or 'customer').strip().lower()
        display_name = (user.get('full_name') or username).strip()
        mobile = (user.get('mobile') or '').strip()

        if role == 'admin':
            nav_user_pill = f"Owner • {username}"
        else:
            nav_user_pill = f"{display_name} • {mobile}" if mobile else display_name

    pay_badge = (session.get('nav_pay_badge') or '').strip()
    # Keep it tiny; only show if it's a short number.
    if len(pay_badge) > 4:
        pay_badge = ''

    return {
        'support_whatsapp_url': build_whatsapp_link('Hi D-CONT Support, I need help with: '),
        'nav_user_pill': nav_user_pill,
        'nav_pay_badge': pay_badge,
        'asset_version': ASSET_VERSION,
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
    "app_fee_paid_month": "TEXT",
    "first_app_fee_verified": "INTEGER",
    "trust_score": "INTEGER",
    "join_blocked": "INTEGER",
    "is_active": "INTEGER",
    # Customer KYC docs (stored as filenames in UPLOAD_FOLDER)
    "aadhaar_doc": "TEXT",
    "pan_doc": "TEXT",
    "passport_doc": "TEXT",

    # Referral system
    "referral_code": "TEXT",
    "referred_by": "TEXT",
    # Legacy field (deprecated). Referral rewards are not cash.
    "wallet_credit": "INTEGER",

    # Customer quick login
    "mpin_hash": "TEXT",
    "mpin_set_at": "TEXT",
    "webauthn_credential_id": "TEXT",
    "webauthn_public_key": "TEXT",
    "webauthn_sign_count": "INTEGER",
    "webauthn_added_at": "TEXT",
}


def _is_valid_mpin(raw: str) -> bool:
    pin = (raw or '').strip()
    return bool(re.fullmatch(r"\d{4}", pin))


def _webauthn_rp_id() -> str:
    # RP ID must be the effective domain without port.
    # When behind a reverse proxy (e.g., Render), rely on forwarded headers.
    xf_host = (request.headers.get('X-Forwarded-Host') or '').split(',', 1)[0].strip()
    host = xf_host or (request.host or '').strip()
    if not host:
        return ''
    return host.split(':', 1)[0]


def _webauthn_origin() -> str:
    # e.g., https://example.com (must match browser location.origin)
    xf_proto = (request.headers.get('X-Forwarded-Proto') or '').split(',', 1)[0].strip().lower()
    xf_host = (request.headers.get('X-Forwarded-Host') or '').split(',', 1)[0].strip()
    proto = xf_proto or (request.scheme or 'http')
    host = xf_host or (request.host or '')
    host = (host or '').strip()
    if not host:
        return ''
    return f"{proto}://{host}".rstrip('/')


def _lookup_customer_candidates_by_mobile(conn: sqlite3.Connection, mobile_identifier: str):
    identifier = (mobile_identifier or '').strip()
    if not identifier:
        return []
    c = conn.cursor()
    candidate_rows = []
    try:
        for mobile_value in _mobile_candidates(identifier):
            c.execute(
                'SELECT username, role, is_active, mobile, mpin_hash, COALESCE(webauthn_credential_id,\'\'), COALESCE(webauthn_public_key,\'\'), COALESCE(webauthn_sign_count,0) FROM users WHERE mobile=?',
                (mobile_value,),
            )
            candidate_rows.extend(c.fetchall() or [])

        identifier_digits = _normalize_mobile_digits(identifier)
        if identifier_digits:
            c.execute(
                'SELECT username, role, is_active, mobile, mpin_hash, COALESCE(webauthn_credential_id,\'\'), COALESCE(webauthn_public_key,\'\'), COALESCE(webauthn_sign_count,0) FROM users'
            )
            for cand in c.fetchall() or []:
                cand_mobile = cand[3] if len(cand) > 3 else ''
                if _normalize_mobile_digits(cand_mobile) == identifier_digits:
                    candidate_rows.append(cand)
    except sqlite3.OperationalError:
        candidate_rows = []

    # Dedupe by username
    seen = set()
    deduped = []
    for row in candidate_rows:
        uname = (row[0] or '').strip()
        if not uname or uname in seen:
            continue
        seen.add(uname)
        deduped.append(row)
    return deduped


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
        'SELECT username, full_name, mobile, language, city_state, email, role, upi_id, onboarding_completed, app_fee_paid, app_fee_paid_month, first_app_fee_verified, trust_score, join_blocked, is_active FROM users WHERE username=?',
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
        'app_fee_paid_month': (row[10] or '').strip(),
        'first_app_fee_verified': int(row[11] if row[11] is not None else 0),
        'trust_score': int(row[12] if row[12] is not None else 50),
        'join_blocked': int(row[13] if row[13] is not None else 0),
        'is_active': int(row[14] if row[14] is not None else 1),
    }


def require_customer(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        wants_json = False
        try:
            p = (request.path or '').lower()
            if p.startswith('/auth/webauthn/'):
                wants_json = True
            accept = (request.headers.get('Accept') or '').lower()
            if 'application/json' in accept:
                wants_json = True
            if request.is_json:
                wants_json = True
        except Exception:
            wants_json = False

        if 'username' not in session:
            if wants_json:
                return jsonify({'error': 'Login required.'}), 401
            return redirect(url_for('login'))

        blocked_redirect = enforce_active_session()
        if blocked_redirect is not None:
            if wants_json:
                return jsonify({'error': 'Account not active.'}), 403
            return blocked_redirect

        if session.get('role') == 'admin':
            if wants_json:
                return jsonify({'error': 'Not allowed.'}), 403
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

    # If this join completes the group, auto-activate it and schedule the first due date.
    if (status or '').strip().lower() == 'joined':
        _maybe_activate_group(conn, group_id)

    conn.commit()
    conn.close()
    return status


DEFAULT_PAY_CUTOFF_TIME = '15:00'  # 3 PM


def _maybe_activate_group(conn, group_id) -> bool:
    """Auto-activate the group once it reaches max members.

    Rule:
    - When max_members (default 10) have status='joined', group becomes active.
    - First payment due date is exactly 30 days after the join that completes the group.
    """
    try:
        gid = int(group_id)
    except (TypeError, ValueError):
        return False
    if gid <= 0:
        return False

    c = conn.cursor()
    try:
        c.execute(
            """
            SELECT COALESCE(g.max_members,10),
                   COALESCE(g.status,''),
                   COALESCE(g.is_paused,0),
                   COALESCE(g.activated_at,''),
                   COALESCE(g.next_due_date,''),
                   COALESCE(g.pay_cutoff_time,'')
            FROM groups g
            WHERE g.id=?
            """,
            (gid,),
        )
        row = c.fetchone()
    except sqlite3.OperationalError:
        return False

    if not row:
        return False

    max_members, status, is_paused, activated_at, next_due_date, pay_cutoff_time = row
    try:
        max_members_int = int(max_members or 10)
    except (TypeError, ValueError):
        max_members_int = 10
    max_members_int = max(1, max_members_int)

    if int(is_paused or 0) == 1:
        return False

    status_code = (status or '').strip().lower()
    if status_code == 'completed':
        return False

    # If already scheduled, nothing to do.
    if (activated_at or '').strip() and (next_due_date or '').strip():
        return False

    try:
        c.execute(
            "SELECT COUNT(1) FROM group_members WHERE group_id=? AND status='joined'",
            (gid,),
        )
        joined_count = int((c.fetchone() or [0])[0] or 0)
    except sqlite3.OperationalError:
        joined_count = 0

    if joined_count < max_members_int:
        return False

    today = date.today()
    activated = today.isoformat()
    due = (today + timedelta(days=30)).isoformat()
    cutoff = (pay_cutoff_time or '').strip() or DEFAULT_PAY_CUTOFF_TIME

    try:
        c.execute(
            """
            UPDATE groups
            SET status=?,
                activated_at=COALESCE(NULLIF(activated_at,''), ?),
                next_due_date=COALESCE(NULLIF(next_due_date,''), ?),
                pay_cutoff_time=COALESCE(NULLIF(pay_cutoff_time,''), ?),
                payout_receiver_username=NULL,
                payout_receiver_name=NULL,
                payout_receiver_upi=NULL,
                receiver_selected_at=NULL
            WHERE id=?
            """,
            ('active' if status_code in {'', 'formation', 'active'} else status_code, activated, due, cutoff, gid),
        )
    except sqlite3.OperationalError:
        return False

    return True


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


def _password_matches(stored_pw: str, provided_pw: str) -> bool:
    stored_pw = stored_pw or ''
    provided_pw = provided_pw or ''
    if not stored_pw or not provided_pw:
        return False
    try:
        if check_password_hash(stored_pw, provided_pw):
            return True
    except (ValueError, TypeError):
        pass
    # Back-compat: if password was stored in plain text
    return stored_pw == provided_pw


AUTH_RATE_LIMIT_WINDOW_SECONDS = 15 * 60
AUTH_RATE_LIMIT_MAX_PASSWORD = 10
AUTH_RATE_LIMIT_MAX_MPIN = 8


def _client_ip() -> str:
    try:
        xfwd = (request.headers.get('X-Forwarded-For') or '').strip()
        if xfwd:
            return xfwd.split(',')[0].strip()
    except Exception:
        pass
    return (request.remote_addr or '').strip()


def _auth_normalize_identifier(method: str, identifier: str) -> str:
    identifier = (identifier or '').strip()
    if not identifier:
        return ''
    if method == 'mpin':
        return _normalize_mobile_digits(identifier)
    digits = _normalize_mobile_digits(identifier)
    if digits and len(digits) >= 10:
        return digits
    return identifier.lower()


def _auth_is_rate_limited(method: str, identifier: str, ip: str) -> bool:
    method = (method or '').strip()
    ident = _auth_normalize_identifier(method, identifier)
    ip = (ip or '').strip()
    if not method or (not ident and not ip):
        return False

    cutoff = (datetime.now() - timedelta(seconds=AUTH_RATE_LIMIT_WINDOW_SECONDS)).isoformat(timespec='seconds')
    max_attempts = AUTH_RATE_LIMIT_MAX_MPIN if method == 'mpin' else AUTH_RATE_LIMIT_MAX_PASSWORD

    conn = get_db()
    c = conn.cursor()
    try:
        ip_count = 0
        ident_count = 0

        if ip:
            c.execute(
                "SELECT COUNT(1) FROM auth_attempts WHERE method=? AND success=0 AND ip=? AND created_at>=?",
                (method, ip, cutoff),
            )
            row = c.fetchone()
            ip_count = int(row[0] or 0) if row else 0

        if ident:
            c.execute(
                "SELECT COUNT(1) FROM auth_attempts WHERE method=? AND success=0 AND identifier=? AND created_at>=?",
                (method, ident, cutoff),
            )
            row = c.fetchone()
            ident_count = int(row[0] or 0) if row else 0

        conn.close()
        return max(ip_count, ident_count) >= max_attempts
    except sqlite3.OperationalError:
        conn.close()
        return False


def _auth_record_attempt(method: str, identifier: str, ip: str, success: bool) -> None:
    method = (method or '').strip()
    if not method:
        return

    ident = _auth_normalize_identifier(method, identifier)
    ip = (ip or '').strip()
    now = datetime.now().isoformat(timespec='seconds')

    conn = get_db()
    c = conn.cursor()
    try:
        c.execute(
            "INSERT INTO auth_attempts (method, identifier, ip, success, created_at) VALUES (?,?,?,?,?)",
            (method, ident, ip, 1 if success else 0, now),
        )

        cleanup_cutoff = (datetime.now() - timedelta(days=7)).isoformat(timespec='seconds')
        c.execute("DELETE FROM auth_attempts WHERE created_at < ?", (cleanup_cutoff,))
        conn.commit()
    except sqlite3.OperationalError:
        pass
    conn.close()


def _repair_blank_username(conn, username: str, mobile: str) -> str:
    """Best-effort fix for legacy rows where username is blank.

    Returns the repaired username if updated; otherwise returns the original value.
    """
    if (username or '').strip():
        return username
    mobile = (mobile or '').strip()
    if not mobile:
        return username
    candidate = mobile
    try:
        c = conn.cursor()
        c.execute('SELECT 1 FROM users WHERE username=?', (candidate,))
        exists = c.fetchone()
        if exists:
            return username
        c.execute('UPDATE users SET username=? WHERE mobile=?', (candidate, candidate))
        conn.commit()
        return candidate
    except sqlite3.OperationalError:
        return username


def _dedupe_user_rows(rows):
    deduped = []
    seen = set()
    for row in rows or []:
        if not row:
            continue
        key = (row[0] or '', _normalize_mobile_digits(row[4] if len(row) > 4 else ''))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped


@app.route('/chat', methods=['GET', 'POST'])
@require_customer
def chat():
    history = session.get('bot_history') or []
    if not isinstance(history, list):
        history = []

    intent_link = None
    intent_link_label = None
    whatsapp_url = None

    if request.method == 'POST':
        message = (request.form.get('message') or '').strip()
        if message:
            history.append({'from': 'user', 'text': message})

            intent = match_intent(message)
            needs_handoff = message_needs_handoff(message)
            bot_text = None

            if intent:
                bot_text = (intent.get('answer') or '').strip()
                intent_link = intent.get('link')
                if intent_link:
                    if intent_link == '/home':
                        intent_link_label = 'Open Home'
                    elif intent_link == '/add-upi':
                        intent_link_label = 'Open Add UPI'
                    else:
                        intent_link_label = 'Open'
                if intent.get('handoff'):
                    needs_handoff = True
            else:
                # If we can't confidently answer, direct the customer to WhatsApp support.
                needs_handoff = True
                bot_text = (
                    "I may not have the right answer for this. "
                    "Tap below to chat with our support team on WhatsApp."
                )

            if needs_handoff:
                uname = (session.get('username') or '').strip()
                prefill = f"Hi D-CONT Support, I need help. User: {uname}. Message: {message}"
                whatsapp_url = build_whatsapp_link(prefill)
                session['whatsapp_handoff_url'] = whatsapp_url
                session['whatsapp_handoff_message'] = message

            if bot_text:
                history.append({'from': 'bot', 'text': bot_text})

            # Keep cookie-sized session data small
            history = history[-30:]
            session['bot_history'] = history

    return render_template(
        'chat.html',
        history=history,
        quick_replies=BOT_QUICK_REPLIES,
        intent_link=intent_link,
        intent_link_label=intent_link_label,
        whatsapp_url=whatsapp_url,
        active_tab='support',
    )


@app.route('/support/whatsapp', methods=['GET'])
@require_customer
def support_whatsapp_handoff():
    url = (session.get('whatsapp_handoff_url') or '').strip()
    message = (session.get('whatsapp_handoff_message') or '').strip()

    if not url:
        return redirect(url_for('chat'))

    # Basic allowlist: only redirect to WhatsApp domains
    if not (url.startswith('https://wa.me/') or url.startswith('https://api.whatsapp.com/') or url.startswith('https://web.whatsapp.com/')):
        session.pop('whatsapp_handoff_url', None)
        session.pop('whatsapp_handoff_message', None)
        return redirect(url_for('chat'))

    username = session.get('username')
    ip = _client_ip()
    now = datetime.now().isoformat(timespec='seconds')
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute(
            'INSERT INTO support_handoffs (username, channel, message, ip, created_at) VALUES (?,?,?,?,?)',
            (username, 'whatsapp', message, ip, now),
        )
        conn.commit()
    except sqlite3.OperationalError:
        pass
    conn.close()

    session.pop('whatsapp_handoff_url', None)
    session.pop('whatsapp_handoff_message', None)
    return redirect(url)


@app.route('/rewards', methods=['GET'])
@require_customer
def rewards():
    username = session['username']
    conn = get_db()
    c = conn.cursor()

    # Keep monthly app-fee flag aligned with current month.
    _ensure_app_fee_current_month(conn, username)
    conn.commit()

    c.execute(
        'SELECT COALESCE(referral_code,\'\') FROM users WHERE username=?',
        (username,),
    )
    row = c.fetchone()
    referral_code = (row[0] if row else '') or ''

    referrals = []
    total_rewards_earned = 0
    app_fee_credit_balance = 0
    try:
        app_fee_credit_balance = _available_app_fee_credit(conn, username)
        c.execute(
            """
            SELECT r.id,
                   r.new_username,
                   COALESCE(u.full_name,''),
                   COALESCE(u.app_fee_paid,0) as app_fee_paid,
                   EXISTS(
                     SELECT 1 FROM group_members gm
                     WHERE gm.username = r.new_username AND gm.status='joined'
                   ) as joined_group,
                   COALESCE(r.status,''),
                   COALESCE(r.created_at,''),
                   COALESCE(r.eligible_at,''),
                   COALESCE(r.paid_at,''),
                   COALESCE(r.credited_at,''),
                   COALESCE(r.credit_expires_at,''),
                   COALESCE(r.credit_amount,0),
                   COALESCE(r.credit_used,0),
                   COALESCE(r.credit_used_month,'')
            FROM referrals r
            LEFT JOIN users u ON u.username = r.new_username
            WHERE r.referrer_username=?
            ORDER BY r.id DESC
            LIMIT 200
            """,
            (username,),
        )
        for (
            rid,
            new_username,
            new_full_name,
            fee_paid,
            joined_group,
            status,
            created_at,
            eligible_at,
            paid_at,
            credited_at,
            credit_expires_at,
            credit_amount,
            credit_used,
            credit_used_month,
        ) in c.fetchall() or []:
            normalized_status = (status or '').strip().upper() or 'PENDING'
            if normalized_status == 'CREDITED':
                try:
                    total_rewards_earned += int(credit_amount or REFERRAL_REWARD_AMOUNT)
                except (TypeError, ValueError):
                    total_rewards_earned += int(REFERRAL_REWARD_AMOUNT)
            referrals.append(
                {
                    'id': int(rid or 0),
                    'new_username': (new_username or '').strip(),
                    'new_full_name': (new_full_name or '').strip(),
                    'app_fee_paid': int(fee_paid or 0),
                    'joined_group': bool(joined_group),
                    'status': normalized_status,
                    'created_at': created_at or '',
                    'eligible_at': eligible_at or '',
                    'paid_at': paid_at or '',
                    'credited_at': credited_at or '',
                    'credit_expires_at': credit_expires_at or '',
                    'credit_amount': int(credit_amount or 0),
                    'credit_used': int(credit_used or 0),
                    'credit_used_month': (credit_used_month or '').strip(),
                }
            )
    except sqlite3.OperationalError:
        referrals = []
        total_rewards_earned = 0
        app_fee_credit_balance = 0

    conn.close()

    return render_template(
        'rewards_tab.html',
        referral_code=_normalize_referral_code(referral_code or ''),
        app_fee_credit_balance=int(app_fee_credit_balance or 0),
        referrals=referrals,
        referral_reward_amount=int(REFERRAL_REWARD_AMOUNT),
        total_rewards_earned=int(total_rewards_earned or 0),
        active_tab='rewards',
    )


def _fetch_user_early_payout_requests(username: str):
    username = (username or '').strip()
    if not username:
        return []
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute(
            """
            SELECT r.id, r.group_id, COALESCE(g.name,''), COALESCE(r.monthly_amount,0),
                   COALESCE(r.deposit_amount,0), COALESCE(r.status,''), COALESCE(r.deposit_status,''),
                   COALESCE(r.utr,''), COALESCE(r.reason,''), COALESCE(r.created_at,'')
            FROM early_payout_requests r
            LEFT JOIN groups g ON g.id = r.group_id
            WHERE r.username=?
            ORDER BY r.id DESC
            LIMIT 10
            """,
            (username,),
        )
        rows = c.fetchall()
        conn.close()
    except sqlite3.OperationalError:
        rows = []

    out = []
    for r in rows:
        out.append(
            {
                'id': int(r[0]),
                'group_id': int(r[1] or 0),
                'group_name': r[2] or '',
                'monthly_amount': int(r[3] or 0),
                'deposit_amount': int(r[4] or 0),
                'status': (r[5] or ''),
                'deposit_status': (r[6] or ''),
                'utr': (r[7] or ''),
                'reason': (r[8] or ''),
                'created_at': (r[9] or ''),
            }
        )
    return out


@app.route('/early-payout/request', methods=['POST'])
@require_customer
def early_payout_request():
    username = session['username']
    try:
        group_id = int(request.form.get('group_id') or 0)
    except ValueError:
        group_id = 0
    reason = (request.form.get('reason') or '').strip()

    if group_id <= 0:
        flash('Select a group for early payout.')
        return redirect(url_for('profile'))

    conn = get_db()
    c = conn.cursor()

    try:
        c.execute(
            'SELECT COALESCE(aadhaar_doc,\'\'), COALESCE(pan_doc,\'\'), COALESCE(passport_doc,\'\'), COALESCE(trust_score,50), COALESCE(upi_id,\'\') FROM users WHERE username=?',
            (username,),
        )
        u = c.fetchone()
    except sqlite3.OperationalError:
        u = None
    if not u:
        conn.close()
        flash('User not found.')
        return redirect(url_for('profile'))
    aadhaar_doc, pan_doc, passport_doc, trust_score, upi_id = u
    trust_score = int(trust_score if trust_score is not None else 50)
    upi_id = (upi_id or '').strip()

    # Validate membership + get monthly amount
    try:
        c.execute('SELECT COALESCE(monthly_amount,0) FROM groups WHERE id=?', (group_id,))
        g = c.fetchone()
    except sqlite3.OperationalError:
        g = None
    monthly_amount = int(g[0] if g and g[0] is not None else 0)

    try:
        c.execute(
            """
            SELECT 1
            FROM group_members
            WHERE group_id=? AND username=? AND COALESCE(NULLIF(status,''),'joined')='joined'
            """,
            (group_id, username),
        )
        is_member = c.fetchone() is not None
    except sqlite3.OperationalError:
        is_member = False

    if not is_member:
        conn.close()
        flash('You must be a joined member of the group to request early payout.')
        return redirect(url_for('profile'))

    eligible, reasons = _early_payout_eligibility(username, trust_score, upi_id, aadhaar_doc, pan_doc, passport_doc)
    if not eligible:
        conn.close()
        flash('Not eligible for early payout: ' + ' '.join(reasons))
        return redirect(url_for('profile'))

    deposit_amount = _early_payout_deposit_amount(monthly_amount, trust_score)
    now = datetime.now().isoformat(timespec='seconds')
    try:
        c.execute(
            'INSERT INTO early_payout_requests (username, group_id, monthly_amount, trust_score, deposit_amount, status, deposit_status, utr, reason, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)',
            (username, group_id, monthly_amount, trust_score, deposit_amount, 'pending_deposit', 'not_paid', '', reason, now, now),
        )
        conn.commit()
    except sqlite3.OperationalError:
        conn.close()
        flash('Unable to create early payout request right now.')
        return redirect(url_for('profile'))

    conn.close()
    flash('Early payout request created. Pay the security deposit and submit the UTR/reference.')
    return redirect(url_for('profile'))


@app.route('/early-payout/deposit', methods=['POST'])
@require_customer
def early_payout_submit_deposit():
    username = session['username']
    try:
        request_id = int(request.form.get('request_id') or 0)
    except ValueError:
        request_id = 0
    utr = (request.form.get('utr') or '').strip()
    if request_id <= 0 or not utr:
        flash('Enter the UTR/reference number.')
        return redirect(url_for('profile'))

    now = datetime.now().isoformat(timespec='seconds')
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute('SELECT id FROM early_payout_requests WHERE id=? AND username=?', (request_id, username))
        row = c.fetchone()
    except sqlite3.OperationalError:
        row = None
    if not row:
        conn.close()
        flash('Request not found.')
        return redirect(url_for('profile'))

    try:
        c.execute(
            'UPDATE early_payout_requests SET utr=?, deposit_status=?, updated_at=? WHERE id=? AND username=?',
            (utr, 'submitted', now, request_id, username),
        )
        conn.commit()
    except sqlite3.OperationalError:
        conn.close()
        flash('Unable to submit deposit right now.')
        return redirect(url_for('profile'))
    conn.close()
    flash('Deposit submitted. Support will verify and update your request.')
    return redirect(url_for('profile'))
@app.route('/profile', methods=['GET', 'POST'])
@require_customer
def profile():
    username = session['username']
    recalculate_and_store_trust(username)
    conn = get_db()
    c = conn.cursor()

    # Keep monthly app-fee flag aligned with current month.
    _ensure_app_fee_current_month(conn, username)
    conn.commit()

    c.execute(
        'SELECT full_name, mobile, upi_id, app_fee_paid, COALESCE(app_fee_paid_month,\'\'), aadhaar_doc, pan_doc, passport_doc, COALESCE(trust_score,50), COALESCE(referral_code,\'\'), COALESCE(mpin_hash,\'\'), COALESCE(webauthn_credential_id,\'\') FROM users WHERE username=?',
        (username,),
    )
    row = c.fetchone()
    full_name = mobile = upi_id = None
    aadhaar_doc = pan_doc = passport_doc = None
    app_fee_paid = 0
    app_fee_paid_month = ''
    trust_score = 50
    referral_code = ''
    mpin_hash = ''
    webauthn_credential_id = ''
    if row:
        (
            full_name,
            mobile,
            upi_id,
            app_fee_paid,
            app_fee_paid_month,
            aadhaar_doc,
            pan_doc,
            passport_doc,
            trust_score,
            referral_code,
            mpin_hash,
            webauthn_credential_id,
        ) = row
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
    # Referral list for this user as referrer
    referrals = []
    total_rewards_earned = 0
    app_fee_credit_balance = 0
    try:
        app_fee_credit_balance = _available_app_fee_credit(conn, username)
        c.execute(
            """
            SELECT r.id,
                   r.new_username,
                   COALESCE(u.full_name,''),
                   COALESCE(u.app_fee_paid,0) as app_fee_paid,
                   EXISTS(
                     SELECT 1 FROM group_members gm
                     WHERE gm.username = r.new_username AND gm.status='joined'
                   ) as joined_group,
                   COALESCE(r.status,''),
                   COALESCE(r.created_at,''),
                   COALESCE(r.eligible_at,''),
                   COALESCE(r.paid_at,''),
                   COALESCE(r.credited_at,''),
                   COALESCE(r.credit_expires_at,''),
                   COALESCE(r.credit_amount,0),
                   COALESCE(r.credit_used,0),
                   COALESCE(r.credit_used_month,'')
            FROM referrals r
            LEFT JOIN users u ON u.username = r.new_username
            WHERE r.referrer_username=?
            ORDER BY r.id DESC
            LIMIT 200
            """,
            (username,),
        )
        for rid, new_username, new_full_name, fee_paid, joined_group, status, created_at, eligible_at, paid_at, credited_at, credit_expires_at, credit_amount, credit_used, credit_used_month in c.fetchall() or []:
            normalized_status = (status or '').strip().upper() or 'PENDING'
            if normalized_status == 'CREDITED':
                try:
                    total_rewards_earned += int(credit_amount or REFERRAL_REWARD_AMOUNT)
                except (TypeError, ValueError):
                    total_rewards_earned += int(REFERRAL_REWARD_AMOUNT)
            referrals.append(
                {
                    'id': int(rid or 0),
                    'new_username': (new_username or '').strip(),
                    'new_full_name': (new_full_name or '').strip(),
                    'app_fee_paid': int(fee_paid or 0),
                    'joined_group': bool(joined_group),
                    'status': normalized_status,
                    'created_at': created_at or '',
                    'eligible_at': eligible_at or '',
                    'paid_at': paid_at or '',
                    'credited_at': credited_at or '',
                    'credit_expires_at': credit_expires_at or '',
                    'credit_amount': int(credit_amount or 0),
                    'credit_used': int(credit_used or 0),
                    'credit_used_month': (credit_used_month or '').strip(),
                }
            )
    except sqlite3.OperationalError:
        referrals = []
        total_rewards_earned = 0
        app_fee_credit_balance = 0

    conn.close()

    my_groups = _fetch_my_groups(username)
    company_upi_id = (get_setting('company_upi_id', '') or '').strip()

    early_payout_groups = []
    for g in my_groups or []:
        try:
            gid = int(g.get('id') or 0)
        except (TypeError, ValueError):
            gid = 0
        monthly_amount = int(g.get('monthly_amount') or 0)
        deposit_amount = _early_payout_deposit_amount(monthly_amount, int(trust_score if trust_score is not None else 50))
        eligible, reasons = _early_payout_eligibility(
            username=username,
            trust_score=int(trust_score if trust_score is not None else 50),
            upi_id=(upi_id or ''),
            aadhaar_doc=(aadhaar_doc or ''),
            pan_doc=(pan_doc or ''),
            passport_doc=(passport_doc or ''),
        )
        early_payout_groups.append(
            {
                'id': gid,
                'name': g.get('name') or '',
                'monthly_amount': monthly_amount,
                'deposit_amount': deposit_amount,
                'eligible': bool(eligible),
                'reasons': reasons,
            }
        )

    early_payout_requests = _fetch_user_early_payout_requests(username)
    return render_template(
        'profile_tab.html',
        full_name=full_name or '',
        mobile=mobile or '',
        upi_id=upi_id or '',
        app_fee_paid=int(app_fee_paid or 0),
        app_fee_paid_month=(app_fee_paid_month or '').strip(),
        aadhaar_doc=aadhaar_doc or '',
        pan_doc=pan_doc or '',
        passport_doc=passport_doc or '',
        trust_score=int(trust_score if trust_score is not None else 50),
        early_payout_groups=early_payout_groups,
        early_payout_requests=early_payout_requests,
        company_upi_id=company_upi_id,
        referral_code=_normalize_referral_code(referral_code or ''),
        app_fee_credit_balance=int(app_fee_credit_balance or 0),
        referrals=referrals,
        referral_reward_amount=int(REFERRAL_REWARD_AMOUNT),
        total_rewards_earned=int(total_rewards_earned or 0),
        mpin_enabled=bool((mpin_hash or '').strip()),
        fingerprint_enabled=bool((webauthn_credential_id or '').strip()),
        active_tab='profile',
    )


@app.route('/profile/mpin', methods=['POST'])
@require_customer
def profile_set_mpin():
    username = session['username']
    current_password = request.form.get('current_password') or ''
    new_mpin = (request.form.get('new_mpin') or '').strip()
    confirm_mpin = (request.form.get('confirm_mpin') or '').strip()

    if not current_password:
        flash('Enter your password to set MPIN.')
        return redirect(url_for('profile'))
    if not _is_valid_mpin(new_mpin):
        flash('MPIN must be exactly 4 digits.')
        return redirect(url_for('profile'))
    if new_mpin != confirm_mpin:
        flash('MPIN confirmation does not match.')
        return redirect(url_for('profile'))

    conn = get_db()
    c = conn.cursor()
    try:
        c.execute('SELECT password FROM users WHERE username=?', (username,))
        row = c.fetchone()
    except sqlite3.OperationalError:
        row = None
    if not row or not _password_matches(row[0] or '', current_password):
        conn.close()
        flash('Invalid password.')
        return redirect(url_for('profile'))

    now = datetime.now().isoformat(timespec='seconds')
    try:
        c.execute(
            'UPDATE users SET mpin_hash=?, mpin_set_at=? WHERE username=?',
            (generate_password_hash(new_mpin), now, username),
        )
        conn.commit()
    except sqlite3.OperationalError:
        conn.close()
        flash('Unable to set MPIN right now.')
        return redirect(url_for('profile'))
    conn.close()
    flash('MPIN updated successfully.')
    return redirect(url_for('profile'))


@app.route('/profile/mpin/disable', methods=['POST'])
@require_customer
def profile_disable_mpin():
    username = session['username']
    current_password = request.form.get('current_password') or ''
    if not current_password:
        flash('Enter your password to disable MPIN.')
        return redirect(url_for('profile'))

    conn = get_db()
    c = conn.cursor()
    try:
        c.execute('SELECT password FROM users WHERE username=?', (username,))
        row = c.fetchone()
    except sqlite3.OperationalError:
        row = None
    if not row or not _password_matches(row[0] or '', current_password):
        conn.close()
        flash('Invalid password.')
        return redirect(url_for('profile'))

    try:
        c.execute('UPDATE users SET mpin_hash=?, mpin_set_at=NULL WHERE username=?', ('', username))
        conn.commit()
    except sqlite3.OperationalError:
        conn.close()
        flash('Unable to disable MPIN right now.')
        return redirect(url_for('profile'))
    conn.close()
    flash('MPIN disabled.')
    return redirect(url_for('profile'))


@app.route('/profile/fingerprint/disable', methods=['POST'])
@require_customer
def profile_disable_fingerprint():
    username = session['username']
    current_password = request.form.get('current_password') or ''
    if not current_password:
        flash('Enter your password to disable fingerprint login.')
        return redirect(url_for('profile'))

    conn = get_db()
    c = conn.cursor()
    try:
        c.execute('SELECT password FROM users WHERE username=?', (username,))
        row = c.fetchone()
    except sqlite3.OperationalError:
        row = None
    if not row or not _password_matches(row[0] or '', current_password):
        conn.close()
        flash('Invalid password.')
        return redirect(url_for('profile'))

    try:
        c.execute(
            'UPDATE users SET webauthn_credential_id=?, webauthn_public_key=?, webauthn_sign_count=?, webauthn_added_at=NULL WHERE username=?',
            ('', '', 0, username),
        )
        conn.commit()
    except sqlite3.OperationalError:
        conn.close()
        flash('Unable to disable fingerprint login right now.')
        return redirect(url_for('profile'))
    conn.close()
    session.pop('webauthn_reg_challenge', None)
    session.pop('webauthn_auth_challenge', None)
    session.pop('webauthn_auth_username', None)
    flash('Fingerprint login disabled.')
    return redirect(url_for('profile'))


@app.route('/auth/webauthn/register/options', methods=['GET'])
@require_customer
def webauthn_register_options():
    if generate_registration_options is None:
        return jsonify({'error': 'Fingerprint login is not available on this server.'}), 501

    username = session['username']
    rp_id = _webauthn_rp_id()
    origin = _webauthn_origin()
    if not rp_id or not origin:
        return jsonify({'error': 'Unable to determine RP settings.'}), 400

    # Use a stable user_id
    user_id = f"dcont:{username}".encode('utf-8')
    challenge = os.urandom(32)
    session['webauthn_reg_challenge'] = bytes_to_base64url(challenge) if bytes_to_base64url else ''

    # Exclude existing credential if present
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute('SELECT COALESCE(webauthn_credential_id,\'\') FROM users WHERE username=?', (username,))
        row = c.fetchone()
    except sqlite3.OperationalError:
        row = None
    conn.close()
    exclude = []
    if row and (row[0] or '').strip() and PublicKeyCredentialDescriptor is not None and base64url_to_bytes is not None:
        try:
            exclude = [PublicKeyCredentialDescriptor(id=base64url_to_bytes(row[0]))]
        except Exception:
            exclude = []

    authenticator_selection = None
    if AuthenticatorSelectionCriteria is not None:
        try:
            authenticator_selection = AuthenticatorSelectionCriteria(
                resident_key='preferred',
                user_verification='preferred',
            )
        except Exception:
            authenticator_selection = None

    options = generate_registration_options(
        rp_id=rp_id,
        rp_name='D-CONT',
        user_name=username,
        user_id=user_id,
        user_display_name=username,
        challenge=challenge,
        timeout=60000,
        authenticator_selection=authenticator_selection,
        exclude_credentials=exclude or None,
    )
    return app.response_class(options_to_json(options), mimetype='application/json')


@app.route('/auth/webauthn/register/verify', methods=['POST'])
@require_customer
def webauthn_register_verify():
    if verify_registration_response is None:
        return jsonify({'error': 'Fingerprint login is not available on this server.'}), 501

    username = session['username']
    rp_id = _webauthn_rp_id()
    origin = _webauthn_origin()
    challenge_b64 = (session.get('webauthn_reg_challenge') or '').strip()
    if not challenge_b64 or base64url_to_bytes is None:
        return jsonify({'error': 'Registration challenge expired. Please try again.'}), 400
    expected_challenge = base64url_to_bytes(challenge_b64)

    credential = request.get_json(silent=True) or {}
    try:
        verified = verify_registration_response(
            credential=credential,
            expected_challenge=expected_challenge,
            expected_rp_id=rp_id,
            expected_origin=origin,
            require_user_verification=False,
        )
    except Exception:
        return jsonify({'error': 'Unable to verify fingerprint setup.'}), 400

    # Persist credential
    cred_id = bytes_to_base64url(verified.credential_id) if bytes_to_base64url else ''
    public_key = bytes_to_base64url(verified.credential_public_key) if bytes_to_base64url else ''
    sign_count = int(getattr(verified, 'sign_count', 0) or 0)
    now = datetime.now().isoformat(timespec='seconds')

    conn = get_db()
    c = conn.cursor()
    try:
        c.execute(
            'UPDATE users SET webauthn_credential_id=?, webauthn_public_key=?, webauthn_sign_count=?, webauthn_added_at=? WHERE username=?',
            (cred_id, public_key, sign_count, now, username),
        )
        conn.commit()
    except sqlite3.OperationalError:
        conn.close()
        return jsonify({'error': 'Unable to save fingerprint setup.'}), 500
    conn.close()
    session.pop('webauthn_reg_challenge', None)
    return jsonify({'ok': True})


@app.route('/auth/webauthn/authenticate/options', methods=['GET'])
def webauthn_auth_options():
    if generate_authentication_options is None:
        return jsonify({'error': 'Fingerprint login is not available on this server.'}), 501

    mobile_identifier = (request.args.get('mobile') or '').strip()
    if not mobile_identifier:
        return jsonify({'error': 'Mobile number is required.'}), 400

    rp_id = _webauthn_rp_id()
    origin = _webauthn_origin()
    if not rp_id or not origin:
        return jsonify({'error': 'Unable to determine RP settings.'}), 400

    conn = get_db()
    candidates = _lookup_customer_candidates_by_mobile(conn, mobile_identifier)
    selected = None
    for row in candidates:
        uname, role_raw, is_active_raw, db_mobile, _mpin_hash, cred_id, pub_key, sign_count = row
        role = (role_raw or 'customer').strip().lower()
        if role == 'admin':
            continue
        if int(is_active_raw if is_active_raw is not None else 1) != 1:
            continue
        if (cred_id or '').strip() and (pub_key or '').strip():
            selected = (uname, cred_id, pub_key, int(sign_count or 0))
            break
    conn.close()

    if not selected:
        return jsonify({'error': 'Fingerprint not enabled for this mobile number.'}), 404

    uname, cred_id, _pub_key, _sign_count = selected
    if PublicKeyCredentialDescriptor is None or base64url_to_bytes is None:
        return jsonify({'error': 'Fingerprint login not available.'}), 501
    try:
        allow = [PublicKeyCredentialDescriptor(id=base64url_to_bytes(cred_id))]
    except Exception:
        return jsonify({'error': 'Fingerprint login not available.'}), 501

    challenge = os.urandom(32)
    session['webauthn_auth_challenge'] = bytes_to_base64url(challenge) if bytes_to_base64url else ''
    session['webauthn_auth_username'] = uname

    options = generate_authentication_options(
        rp_id=rp_id,
        challenge=challenge,
        timeout=60000,
        allow_credentials=allow,
        user_verification=UserVerificationRequirement.PREFERRED if UserVerificationRequirement else 'preferred',
    )
    return app.response_class(options_to_json(options), mimetype='application/json')


@app.route('/auth/webauthn/authenticate/verify', methods=['POST'])
def webauthn_auth_verify():
    if verify_authentication_response is None:
        return jsonify({'error': 'Fingerprint login is not available on this server.'}), 501

    rp_id = _webauthn_rp_id()
    origin = _webauthn_origin()
    uname = (session.get('webauthn_auth_username') or '').strip()
    challenge_b64 = (session.get('webauthn_auth_challenge') or '').strip()
    if not uname or not challenge_b64 or base64url_to_bytes is None:
        return jsonify({'error': 'Fingerprint challenge expired. Please try again.'}), 400
    expected_challenge = base64url_to_bytes(challenge_b64)

    conn = get_db()
    c = conn.cursor()
    try:
        c.execute(
            'SELECT username, role, is_active, COALESCE(webauthn_public_key,\'\'), COALESCE(webauthn_sign_count,0) FROM users WHERE username=?',
            (uname,),
        )
        row = c.fetchone()
    except sqlite3.OperationalError:
        row = None
    if not row:
        conn.close()
        return jsonify({'error': 'User not found.'}), 404
    role = (row[1] or 'customer').strip().lower()
    if role == 'admin':
        conn.close()
        return jsonify({'error': 'Invalid account.'}), 400
    if int(row[2] if row[2] is not None else 1) != 1:
        conn.close()
        return jsonify({'error': 'Your account is blocked. Please contact support.'}), 403

    pub_key_b64 = (row[3] or '').strip()
    try:
        sign_count = int(row[4] or 0)
    except Exception:
        sign_count = 0
    if not pub_key_b64:
        conn.close()
        return jsonify({'error': 'Fingerprint not enabled.'}), 404

    credential = request.get_json(silent=True) or {}
    try:
        verified = verify_authentication_response(
            credential=credential,
            expected_challenge=expected_challenge,
            expected_rp_id=rp_id,
            expected_origin=origin,
            credential_public_key=base64url_to_bytes(pub_key_b64),
            credential_current_sign_count=sign_count,
            require_user_verification=False,
        )
    except Exception:
        conn.close()
        return jsonify({'error': 'Fingerprint verification failed.'}), 400

    new_sign_count = int(getattr(verified, 'new_sign_count', sign_count) or sign_count)
    try:
        c.execute('UPDATE users SET webauthn_sign_count=? WHERE username=?', (new_sign_count, uname))
        conn.commit()
    except sqlite3.OperationalError:
        pass
    conn.close()

    session.pop('webauthn_auth_challenge', None)
    session.pop('webauthn_auth_username', None)

    session['username'] = uname
    session['role'] = 'customer'
    session['lang'] = _get_user_language(uname)
    return jsonify({'ok': True})


def _fetch_group_members_with_trust(group_id: int):
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute(
            """
            SELECT u.username, COALESCE(u.full_name,''), COALESCE(u.trust_score,50)
            FROM group_members gm
            JOIN users u ON u.username = gm.username
            WHERE gm.group_id=? AND gm.status='joined'
            ORDER BY u.id ASC
            """,
            (group_id,),
        )
        rows = c.fetchall()
    except sqlite3.OperationalError:
        rows = []
    conn.close()

    members = []
    for username, full_name, trust_score in rows:
        # Keep scores fresh (group sizes are small)
        details = recalculate_and_store_trust(username)
        score = int(details.get('score', trust_score if trust_score is not None else 50))
        members.append({'username': username, 'full_name': full_name or '', 'trust_score': score})
    return members


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

    # Trust score history (Option A: recompute from events)
    c.execute(
        '''CREATE TABLE IF NOT EXISTS trust_events (
            id INTEGER PRIMARY KEY,
            username TEXT,
            event_type TEXT,
            group_id INTEGER,
            due_date TEXT,
            verified_at TEXT,
            created_at TEXT,
            note TEXT
        )'''
    )

    # Early payout requests (schedule/priority request + security deposit tracking)
    c.execute(
        '''CREATE TABLE IF NOT EXISTS early_payout_requests (
            id INTEGER PRIMARY KEY,
            username TEXT,
            group_id INTEGER,
            monthly_amount INTEGER,
            trust_score INTEGER,
            deposit_amount INTEGER,
            status TEXT,
            deposit_status TEXT,
            utr TEXT,
            reason TEXT,
            created_at TEXT,
            updated_at TEXT
        )'''
    )

    # Referral records
    c.execute(
        '''CREATE TABLE IF NOT EXISTS referrals (
            id INTEGER PRIMARY KEY,
            referrer_username TEXT,
            new_username TEXT,
            status TEXT,
            created_at TEXT,
            eligible_at TEXT,
            paid_at TEXT
        )'''
    )

    # App-fee payments ledger (for monthly fee + credits applied)
    c.execute(
        '''CREATE TABLE IF NOT EXISTS app_fee_payments (
            id INTEGER PRIMARY KEY,
            username TEXT,
            month TEXT,
            gross_amount INTEGER,
            credit_applied INTEGER,
            net_amount INTEGER,
            verified_at TEXT,
            UNIQUE(username, month)
        )'''
    )

    # Login rate-limiting (failed attempt counters)
    c.execute(
        '''CREATE TABLE IF NOT EXISTS auth_attempts (
            id INTEGER PRIMARY KEY,
            method TEXT,
            identifier TEXT,
            ip TEXT,
            success INTEGER,
            created_at TEXT
        )'''
    )

    # Support handoff logging
    c.execute(
        '''CREATE TABLE IF NOT EXISTS support_handoffs (
            id INTEGER PRIMARY KEY,
            username TEXT,
            channel TEXT,
            message TEXT,
            ip TEXT,
            created_at TEXT
        )'''
    )

    # Ensure referrals table has credit columns (auto-migration)
    c.execute("PRAGMA table_info(referrals)")
    existing_referral_cols = {row[1] for row in c.fetchall()}
    if "credited_at" not in existing_referral_cols:
        c.execute("ALTER TABLE referrals ADD COLUMN credited_at TEXT")
    if "credit_expires_at" not in existing_referral_cols:
        c.execute("ALTER TABLE referrals ADD COLUMN credit_expires_at TEXT")
    if "credit_amount" not in existing_referral_cols:
        c.execute("ALTER TABLE referrals ADD COLUMN credit_amount INTEGER")
    if "credit_used" not in existing_referral_cols:
        c.execute("ALTER TABLE referrals ADD COLUMN credit_used INTEGER")
    if "credit_used_at" not in existing_referral_cols:
        c.execute("ALTER TABLE referrals ADD COLUMN credit_used_at TEXT")
    if "credit_used_month" not in existing_referral_cols:
        c.execute("ALTER TABLE referrals ADD COLUMN credit_used_month TEXT")

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

    # Group cycle automation fields
    if "activated_at" not in existing_group_cols:
        c.execute("ALTER TABLE groups ADD COLUMN activated_at TEXT")
    if "next_due_date" not in existing_group_cols:
        c.execute("ALTER TABLE groups ADD COLUMN next_due_date TEXT")
    if "pay_cutoff_time" not in existing_group_cols:
        c.execute("ALTER TABLE groups ADD COLUMN pay_cutoff_time TEXT")
    if "payout_receiver_username" not in existing_group_cols:
        c.execute("ALTER TABLE groups ADD COLUMN payout_receiver_username TEXT")
    if "payout_receiver_name" not in existing_group_cols:
        c.execute("ALTER TABLE groups ADD COLUMN payout_receiver_name TEXT")
    if "payout_receiver_upi" not in existing_group_cols:
        c.execute("ALTER TABLE groups ADD COLUMN payout_receiver_upi TEXT")
    if "receiver_selected_at" not in existing_group_cols:
        c.execute("ALTER TABLE groups ADD COLUMN receiver_selected_at TEXT")

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
    c.execute("UPDATE users SET app_fee_paid_month='' WHERE app_fee_paid_month IS NULL")
    c.execute("UPDATE users SET first_app_fee_verified=0 WHERE first_app_fee_verified IS NULL")
    c.execute("UPDATE users SET trust_score=50 WHERE trust_score IS NULL")
    c.execute("UPDATE users SET join_blocked=0 WHERE join_blocked IS NULL")
    c.execute("UPDATE users SET is_active=1 WHERE is_active IS NULL")
    c.execute("UPDATE users SET wallet_credit=0 WHERE wallet_credit IS NULL")
    c.execute("UPDATE group_members SET status='joined' WHERE status IS NULL OR status='' ")
    c.execute("UPDATE groups SET is_paused=0 WHERE is_paused IS NULL")

    # Best-effort: if a group is already full but has no activation schedule yet, start it now.
    try:
        c.execute(
            """
            SELECT g.id, COALESCE(g.max_members,10)
            FROM groups g
            WHERE COALESCE(NULLIF(g.activated_at,''),'')='' OR COALESCE(NULLIF(g.next_due_date,''),'')=''
            """
        )
        candidates = c.fetchall()
        for gid, max_members in candidates:
            try:
                max_m = int(max_members or 10)
            except (TypeError, ValueError):
                max_m = 10
            max_m = max(1, max_m)
            c.execute(
                "SELECT COUNT(1) FROM group_members WHERE group_id=? AND status='joined'",
                (gid,),
            )
            joined_count = int((c.fetchone() or [0])[0] or 0)
            if joined_count >= max_m:
                _maybe_activate_group(conn, gid)
    except sqlite3.OperationalError:
        pass

    # Backfill roles for existing users
    c.execute("UPDATE users SET role='customer' WHERE role IS NULL OR role='' ")

    # If a user already has app_fee_paid=1 in the legacy schema, treat it as first verified.
    try:
        c.execute("UPDATE users SET first_app_fee_verified=1 WHERE COALESCE(first_app_fee_verified,0)=0 AND COALESCE(app_fee_paid,0)=1")
    except sqlite3.OperationalError:
        pass

    # Best-effort: if app_fee_paid is set but month is empty, assume current month.
    try:
        month_key = _current_month_key()
        c.execute(
            "UPDATE users SET app_fee_paid_month=? WHERE COALESCE(app_fee_paid,0)=1 AND COALESCE(app_fee_paid_month,'')=''",
            (month_key,),
        )
    except sqlite3.OperationalError:
        pass

    # Migrate any old 'PAID' referral rows to 'CREDITED' credits (non-withdrawable)
    try:
        c.execute(
            "SELECT id, COALESCE(paid_at,''), COALESCE(eligible_at,''), COALESCE(created_at,'') FROM referrals WHERE UPPER(COALESCE(status,''))='PAID'"
        )
        old_paid = c.fetchall() or []
        for rid, paid_at, eligible_at, created_at in old_paid:
            credited_at = (paid_at or '').strip() or (eligible_at or '').strip() or (created_at or '').strip()
            if not credited_at:
                credited_at = datetime.now().isoformat(timespec='seconds')
            try:
                ca = datetime.fromisoformat(credited_at)
            except ValueError:
                ca = datetime.now()
                credited_at = ca.isoformat(timespec='seconds')
            expires_at = (ca + timedelta(days=int(APP_FEE_CREDIT_EXPIRY_DAYS))).isoformat(timespec='seconds')
            c.execute(
                """
                UPDATE referrals
                SET status='CREDITED',
                    credited_at=?,
                    credit_expires_at=?,
                    credit_amount=?,
                    credit_used=0
                WHERE id=?
                """,
                (credited_at, expires_at, int(REFERRAL_REWARD_AMOUNT), int(rid or 0)),
            )
    except sqlite3.OperationalError:
        pass

    # Ensure username/mobile uniqueness (best-effort; may fail if duplicates already exist)
    try:
        c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_users_username ON users(username)")
    except sqlite3.OperationalError:
        pass
    try:
        c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_users_mobile ON users(mobile)")
    except sqlite3.OperationalError:
        pass

    # Referral indexes
    try:
        c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_users_referral_code ON users(referral_code)")
    except sqlite3.OperationalError:
        pass
    try:
        c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_referrals_new_username ON referrals(new_username)")
    except sqlite3.OperationalError:
        pass
    try:
        c.execute("CREATE INDEX IF NOT EXISTS idx_referrals_referrer ON referrals(referrer_username)")
    except sqlite3.OperationalError:
        pass

    # Backfill referral codes for existing customer users (best-effort)
    try:
        c.execute("SELECT id, username, COALESCE(NULLIF(role,''),'customer') as role, COALESCE(referral_code,'') FROM users")
        rows = c.fetchall() or []
        for uid, uname, role, rcode in rows:
            uname = (uname or '').strip()
            if not uname:
                continue
            if (role or '').strip().lower() == 'admin':
                continue
            if _normalize_referral_code(rcode):
                continue

            candidate = _make_referral_code_from_user_id(uid)
            suffix = 0
            while True:
                try:
                    c.execute("SELECT 1 FROM users WHERE referral_code=? LIMIT 1", (candidate,))
                    taken = c.fetchone() is not None
                except sqlite3.OperationalError:
                    taken = False
                if not taken:
                    break
                suffix += 1
                candidate = f"{_make_referral_code_from_user_id(uid)}{suffix}"
                if suffix > 9:
                    break
            try:
                c.execute("UPDATE users SET referral_code=? WHERE username=?", (candidate, uname))
            except sqlite3.OperationalError:
                pass
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

    month_key = _current_month_key()
    app_fee_paid_count = 0
    try:
        c.execute("SELECT COUNT(*) FROM app_fee_payments WHERE month=?", (month_key,))
        app_fee_paid_count = int((c.fetchone() or [0])[0] or 0)
    except sqlite3.OperationalError:
        c.execute(
            "SELECT COUNT(*) FROM users WHERE COALESCE(NULLIF(role,''), 'customer') != 'admin' AND COALESCE(app_fee_paid,0)=1 AND COALESCE(app_fee_paid_month,'')=?",
            (month_key,),
        )
        app_fee_paid_count = int((c.fetchone() or [0])[0] or 0)

    app_fee_amount_raw = (get_setting('app_fee_amount', '0') or '0').strip()
    try:
        app_fee_amount = int(app_fee_amount_raw)
    except ValueError:
        app_fee_amount = 0
    app_fee_collected = app_fee_paid_count * max(app_fee_amount, 0)
    try:
        c.execute("SELECT COALESCE(SUM(COALESCE(net_amount,0)),0) FROM app_fee_payments WHERE month=?", (month_key,))
        app_fee_collected = int((c.fetchone() or [0])[0] or 0)
    except sqlite3.OperationalError:
        pass

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
    trust_details = recalculate_and_store_trust(username)
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
        'trust_score': int(trust_details.get('score', row[7] if row[7] is not None else 50)),
        'app_fee_paid': int(row[8] or 0),
        'upi_id': row[9] or '',
    }
    return render_template(
        'owner_user_profile.html',
        active_owner_tab='users',
        user=user,
        groups=groups,
        trust_breakdown=trust_details.get('breakdown', {}),
        trust_events=(trust_details.get('events', [])[:25]),
    )


@app.route('/owner/users/verify_app_fee', methods=['POST'])
@admin_required
def owner_users_verify_app_fee():
    target_username = (request.form.get('username') or '').strip()
    if not target_username:
        flash('Missing username.')
        return redirect(url_for('owner_users'))

    conn = get_db()
    c = conn.cursor()
    try:
        c.execute("SELECT COALESCE(NULLIF(role,''),'customer') FROM users WHERE username=?", (target_username,))
        row = c.fetchone()
    except sqlite3.OperationalError:
        row = None
    if not row:
        conn.close()
        flash('User not found.')
        return redirect(url_for('owner_users'))
    if (row[0] or '').strip().lower() == 'admin':
        conn.close()
        flash('Admin users cannot be modified here.')
        return redirect(url_for('owner_users'))

    try:
        gross, credit_applied, net, month_key = _verify_app_fee_payment(conn, target_username)
        _maybe_mark_referral_eligible(conn, target_username)
        conn.commit()
    except sqlite3.OperationalError:
        conn.close()
        flash('Unable to verify app fee right now.')
        return redirect(url_for('owner_user_profile', username=target_username))
    conn.close()

    if credit_applied > 0:
        flash(f"App fee verified for {month_key}. Credit applied: ₹{credit_applied}. Net paid: ₹{net}.")
    else:
        flash(f"App fee verified for {month_key}.")
    return redirect(url_for('owner_user_profile', username=target_username))


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
    flash('Trust Score is automatic now (history-based). Manual overrides are disabled.')
    return redirect(url_for('owner_users'))


@app.route('/owner/users/add_trust_event', methods=['POST'])
@admin_required
def owner_add_trust_event():
    target_username = (request.form.get('username') or '').strip()
    event_type = (request.form.get('event_type') or '').strip().lower()
    due_date = (request.form.get('due_date') or '').strip()
    verified_at = (request.form.get('verified_at') or '').strip()
    note = (request.form.get('note') or '').strip()
    group_id_raw = (request.form.get('group_id') or '').strip()
    try:
        group_id = int(group_id_raw) if group_id_raw else None
    except ValueError:
        group_id = None

    allowed = {
        'contribution_verified',
        'contribution_rejected',
        'payment_missed',
        'default_after_payout',
        'deposit_verified',
        'group_completed',
    }
    if not target_username:
        flash('Missing username.')
        return redirect(url_for('owner_users'))
    if event_type not in allowed:
        flash('Invalid event type.')
        return redirect(url_for('owner_user_profile', username=target_username))
    if event_type == 'contribution_verified':
        if not _parse_iso_date(due_date):
            flash('For verified contributions, due date is required (YYYY-MM-DD).')
            return redirect(url_for('owner_user_profile', username=target_username))
        if not _parse_iso_date(verified_at):
            verified_at = _today_iso()
    else:
        # Other events can omit dates
        if verified_at and not _parse_iso_date(verified_at):
            verified_at = ''
        if due_date and not _parse_iso_date(due_date):
            due_date = ''

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

    c.execute(
        'INSERT INTO trust_events (username, event_type, group_id, due_date, verified_at, created_at, note) VALUES (?,?,?,?,?,?,?)',
        (target_username, event_type, group_id, due_date, verified_at, _today_iso(), note),
    )
    conn.commit()
    conn.close()

    recalculate_and_store_trust(target_username)
    flash('Trust event recorded. Score updated.')
    return redirect(url_for('owner_user_profile', username=target_username))


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
               COALESCE(g.activated_at,'') as activated_at,
               COALESCE(g.next_due_date,'') as next_due_date,
               COALESCE(g.pay_cutoff_time,'') as pay_cutoff_time,
               COALESCE(g.payout_receiver_username,'') as payout_receiver_username,
               COALESCE(g.payout_receiver_name,'') as payout_receiver_name,
               COALESCE(g.payout_receiver_upi,'') as payout_receiver_upi,
               COALESCE(g.status,'') as status,
               COALESCE(g.is_paused,0) as is_paused,
               COUNT(gm.id) as joined_members
        FROM groups g
        LEFT JOIN group_members gm ON gm.group_id=g.id AND gm.status='joined'
        GROUP BY g.id, g.name, g.description, g.monthly_amount, g.max_members, g.receiver_name, g.receiver_upi,
                 g.activated_at, g.next_due_date, g.pay_cutoff_time,
                 g.payout_receiver_username, g.payout_receiver_name, g.payout_receiver_upi,
                 g.status, g.is_paused
        ORDER BY g.monthly_amount, g.id
        """
    )
    groups = []
    today_iso = _today_iso()
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
                'activated_at': (r[7] or '').strip(),
                'next_due_date': (r[8] or '').strip(),
                'pay_cutoff_time': (r[9] or '').strip(),
                'payout_receiver_username': (r[10] or '').strip(),
                'payout_receiver_name': (r[11] or '').strip(),
                'payout_receiver_upi': (r[12] or '').strip(),
                'status': (r[13] or '').strip().lower(),
                'is_paused': int(r[14] or 0),
                'joined_members': int(r[15] or 0),
                'is_due_today': ((r[8] or '').strip() == today_iso),
            }
        )

    receiver_candidates = {}
    due_group_ids = [g['id'] for g in groups if g.get('is_due_today') and not (g.get('payout_receiver_username') or '').strip()]
    if due_group_ids:
        placeholders = ','.join(['?'] * len(due_group_ids))
        try:
            c.execute(
                f"""
                SELECT gm.group_id, u.username, COALESCE(u.full_name,''), COALESCE(u.upi_id,'')
                FROM group_members gm
                LEFT JOIN users u ON u.username = gm.username
                WHERE gm.status='joined' AND gm.group_id IN ({placeholders})
                ORDER BY COALESCE(u.full_name,''), u.username
                """,
                tuple(due_group_ids),
            )
            for gid, uname, full_name, upi_id in c.fetchall():
                receiver_candidates.setdefault(int(gid), []).append(
                    {
                        'username': uname or '',
                        'full_name': full_name or '',
                        'upi_id': upi_id or '',
                    }
                )
        except sqlite3.OperationalError:
            receiver_candidates = {}

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
    return render_template(
        'owner_groups.html',
        active_owner_tab='groups',
        groups=groups,
        join_requests=join_requests,
        receiver_candidates=receiver_candidates,
        today_iso=today_iso,
    )


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


@app.route('/owner/groups/select_receiver', methods=['POST'])
@admin_required
def owner_select_group_receiver():
    group_id_raw = (request.form.get('group_id') or '').strip()
    receiver_username = (request.form.get('receiver_username') or '').strip()
    try:
        group_id = int(group_id_raw)
    except ValueError:
        group_id = 0
    if group_id <= 0 or not receiver_username:
        flash('Missing group or receiver.')
        return redirect(url_for('owner_groups'))

    conn = get_db()
    c = conn.cursor()
    try:
        c.execute(
            """
            SELECT COALESCE(g.next_due_date,''), COALESCE(g.status,''), COALESCE(g.is_paused,0)
            FROM groups g
            WHERE g.id=?
            """,
            (group_id,),
        )
        row = c.fetchone()
    except sqlite3.OperationalError:
        row = None

    if not row:
        conn.close()
        flash('Group not found.')
        return redirect(url_for('owner_groups'))

    next_due_date, status, is_paused = row
    if int(is_paused or 0) == 1:
        conn.close()
        flash('This group is paused.')
        return redirect(url_for('owner_groups'))

    status_code = (status or '').strip().lower()
    if status_code != 'active':
        conn.close()
        flash('Receiver can be selected only for active groups.')
        return redirect(url_for('owner_groups'))

    due = (next_due_date or '').strip()
    today_iso = _today_iso()
    if due and today_iso < due:
        conn.close()
        flash('Receiver can be selected on the due date.')
        return redirect(url_for('owner_groups'))

    # Validate receiver is a joined member
    try:
        c.execute(
            "SELECT 1 FROM group_members WHERE group_id=? AND username=? AND status='joined'",
            (group_id, receiver_username),
        )
        is_member = c.fetchone()
    except sqlite3.OperationalError:
        is_member = None
    if not is_member:
        conn.close()
        flash('Selected receiver is not a joined member of this group.')
        return redirect(url_for('owner_groups'))

    # Pull receiver payment details
    try:
        c.execute(
            "SELECT COALESCE(full_name,''), COALESCE(upi_id,'') FROM users WHERE username=?",
            (receiver_username,),
        )
        urow = c.fetchone()
    except sqlite3.OperationalError:
        urow = None

    full_name = (urow[0] if urow else '') or ''
    upi_id = (urow[1] if urow else '') or ''
    if not upi_id.strip():
        conn.close()
        flash('Receiver must have a UPI ID set in Profile.')
        return redirect(url_for('owner_groups'))

    receiver_name = (full_name or receiver_username).strip()

    try:
        c.execute(
            """
            UPDATE groups
            SET payout_receiver_username=?,
                payout_receiver_name=?,
                payout_receiver_upi=?,
                receiver_selected_at=?
            WHERE id=?
            """,
            (receiver_username, receiver_name, upi_id.strip(), today_iso, group_id),
        )
        conn.commit()
    except sqlite3.OperationalError:
        conn.close()
        flash('Unable to set receiver right now.')
        return redirect(url_for('owner_groups'))

    conn.close()
    flash('Receiver selected for today’s payout.')
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
    month_key = _current_month_key()
    app_fee_payments = []
    try:
        c.execute(
            """
            SELECT p.username,
                   COALESCE(u.full_name,''),
                   COALESCE(u.mobile,''),
                   COALESCE(p.gross_amount,0),
                   COALESCE(p.credit_applied,0),
                   COALESCE(p.net_amount,0),
                   COALESCE(p.verified_at,'')
            FROM app_fee_payments p
            LEFT JOIN users u ON u.username = p.username
            WHERE p.month=?
            ORDER BY p.id DESC
            """,
            (month_key,),
        )
        for uname, full_name, mobile, gross, credit_applied, net, verified_at in c.fetchall() or []:
            app_fee_payments.append(
                {
                    'username': uname,
                    'full_name': full_name or '',
                    'mobile': mobile or '',
                    'gross': int(gross or 0),
                    'credit_applied': int(credit_applied or 0),
                    'amount': int(net or 0),
                    'verified_at': verified_at or '',
                }
            )
    except sqlite3.OperationalError:
        # Fallback legacy behavior
        try:
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
                    'gross': fee_amount,
                    'credit_applied': 0,
                    'amount': fee_amount,
                    'verified_at': '',
                }
                for r in c.fetchall()
            ]
        except sqlite3.OperationalError:
            app_fee_payments = []

    conn.close()
    return render_template(
        'owner_payments.html',
        active_owner_tab='payments',
        app_fee_amount=fee_amount,
        app_fee_payments=app_fee_payments,
        app_fee_month=month_key,
    )


@app.route('/owner/referrals')
@admin_required
def owner_referrals():
    conn = get_db()
    c = conn.cursor()
    rows = []
    try:
        c.execute(
            """
            SELECT r.id,
                   r.referrer_username,
                   COALESCE(ru.full_name,''),
                   r.new_username,
                   COALESCE(nu.full_name,''),
                   COALESCE(nu.mobile,''),
                   COALESCE(nu.app_fee_paid,0) as app_fee_paid,
                   EXISTS(
                     SELECT 1 FROM group_members gm
                     WHERE gm.username = r.new_username AND gm.status='joined'
                   ) as joined_group,
                   COALESCE(r.status,''),
                   COALESCE(r.created_at,''),
                   COALESCE(r.eligible_at,''),
                   COALESCE(r.credited_at,''),
                   COALESCE(r.credit_expires_at,''),
                   COALESCE(r.credit_amount,0),
                   COALESCE(r.credit_used,0),
                   COALESCE(r.credit_used_month,'')
            FROM referrals r
            LEFT JOIN users ru ON ru.username = r.referrer_username
            LEFT JOIN users nu ON nu.username = r.new_username
            ORDER BY r.id DESC
            LIMIT 300
            """
        )
        rows = c.fetchall() or []
    except sqlite3.OperationalError:
        rows = []

    referrals = []
    for r in rows:
        referrals.append(
            {
                'id': int(r[0] or 0),
                'referrer_username': (r[1] or '').strip(),
                'referrer_full_name': (r[2] or '').strip(),
                'new_username': (r[3] or '').strip(),
                'new_full_name': (r[4] or '').strip(),
                'new_mobile': (r[5] or '').strip(),
                'app_fee_paid': int(r[6] or 0),
                'joined_group': bool(r[7]),
                'status': (r[8] or '').strip().upper() or 'PENDING',
                'created_at': r[9] or '',
                'eligible_at': r[10] or '',
                'credited_at': r[11] or '',
                'credit_expires_at': r[12] or '',
                'credit_amount': int(r[13] or 0),
                'credit_used': int(r[14] or 0),
                'credit_used_month': (r[15] or '').strip(),
            }
        )

    conn.close()
    return render_template(
        'owner_referrals.html',
        active_owner_tab='referrals',
        referrals=referrals,
        referral_reward_amount=int(REFERRAL_REWARD_AMOUNT),
    )


@app.route('/owner/referrals/verify-app-fee', methods=['POST'])
@admin_required
def owner_referrals_verify_app_fee():
    new_username = (request.form.get('new_username') or '').strip()
    if not new_username:
        flash('Missing user.')
        return redirect(url_for('owner_referrals'))

    conn = get_db()
    c = conn.cursor()
    try:
        c.execute("SELECT COALESCE(NULLIF(role,''),'customer') FROM users WHERE username=?", (new_username,))
        row = c.fetchone()
    except sqlite3.OperationalError:
        row = None

    if not row:
        conn.close()
        flash('User not found.')
        return redirect(url_for('owner_referrals'))
    if (row[0] or '').strip().lower() == 'admin':
        conn.close()
        flash('Invalid user.')
        return redirect(url_for('owner_referrals'))

    try:
        gross, credit_applied, net, month_key = _verify_app_fee_payment(conn, new_username)
        _maybe_mark_referral_eligible(conn, new_username)
        conn.commit()
    except sqlite3.OperationalError:
        conn.close()
        flash('Unable to verify app fee right now.')
        return redirect(url_for('owner_referrals'))
    conn.close()

    if credit_applied > 0:
        flash(f"App fee verified for {month_key}. Credit applied: ₹{credit_applied}. Net paid: ₹{net}.")
    else:
        flash('App fee marked as verified. Referral eligibility updated.')
    return redirect(url_for('owner_referrals'))


@app.route('/owner/referrals/refresh-eligibility', methods=['POST'])
@admin_required
def owner_referrals_refresh_eligibility():
    new_username = (request.form.get('new_username') or '').strip()
    if not new_username:
        flash('Missing user.')
        return redirect(url_for('owner_referrals'))
    conn = get_db()
    try:
        _maybe_mark_referral_eligible(conn, new_username)
        conn.commit()
    except sqlite3.OperationalError:
        conn.close()
        flash('Unable to refresh eligibility right now.')
        return redirect(url_for('owner_referrals'))
    conn.close()
    flash('Eligibility refreshed.')
    return redirect(url_for('owner_referrals'))


@app.route('/owner/referrals/mark-paid', methods=['POST'])
@admin_required
def owner_referrals_mark_paid():
    try:
        referral_id = int(request.form.get('referral_id') or 0)
    except ValueError:
        referral_id = 0
    if referral_id <= 0:
        flash('Invalid referral.')
        return redirect(url_for('owner_referrals'))

    conn = get_db()
    c = conn.cursor()
    try:
        c.execute(
            "SELECT id, COALESCE(status,''), COALESCE(referrer_username,''), COALESCE(new_username,'') FROM referrals WHERE id=?",
            (referral_id,),
        )
        row = c.fetchone()
    except sqlite3.OperationalError:
        row = None

    if not row:
        conn.close()
        flash('Referral not found.')
        return redirect(url_for('owner_referrals'))

    status = (row[1] or '').strip().upper()
    referrer_username = (row[2] or '').strip()
    new_username = (row[3] or '').strip()

    if status != 'ELIGIBLE':
        conn.close()
        flash('Referral is not eligible yet.')
        return redirect(url_for('owner_referrals'))
    if not referrer_username or not new_username:
        conn.close()
        flash('Invalid referral data.')
        return redirect(url_for('owner_referrals'))

    now = datetime.now().isoformat(timespec='seconds')
    expires_at = (datetime.now() + timedelta(days=int(APP_FEE_CREDIT_EXPIRY_DAYS))).isoformat(timespec='seconds')
    try:
        c.execute(
            """
            UPDATE referrals
            SET status=?,
                credited_at=?,
                credit_expires_at=?,
                credit_amount=?,
                credit_used=0
            WHERE id=?
            """,
            ('CREDITED', now, expires_at, int(REFERRAL_REWARD_AMOUNT), referral_id),
        )
        conn.commit()
    except sqlite3.OperationalError:
        conn.close()
        flash('Unable to credit right now.')
        return redirect(url_for('owner_referrals'))
    conn.close()

    flash('Referral reward credited as app fee credit and marked as CREDITED.')
    return redirect(url_for('owner_referrals'))


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

    try:
        conn = get_db()
        c = conn.cursor()
        c.execute(
            """
            SELECT r.id, r.username, COALESCE(u.full_name,''), COALESCE(u.mobile,''),
                   r.group_id, COALESCE(g.name,''), COALESCE(r.monthly_amount,0), COALESCE(r.deposit_amount,0),
                   COALESCE(r.status,''), COALESCE(r.deposit_status,''), COALESCE(r.utr,''), COALESCE(r.created_at,'')
            FROM early_payout_requests r
            LEFT JOIN users u ON u.username = r.username
            LEFT JOIN groups g ON g.id = r.group_id
            ORDER BY r.id DESC
            LIMIT 50
            """
        )
        req_rows = c.fetchall()
        conn.close()
    except sqlite3.OperationalError:
        req_rows = []

    early_payout_requests = []
    for r in req_rows:
        early_payout_requests.append(
            {
                'id': int(r[0]),
                'username': r[1] or '',
                'full_name': r[2] or '',
                'mobile': r[3] or '',
                'group_id': int(r[4] or 0),
                'group_name': r[5] or '',
                'monthly_amount': int(r[6] or 0),
                'deposit_amount': int(r[7] or 0),
                'status': r[8] or '',
                'deposit_status': r[9] or '',
                'utr': r[10] or '',
                'created_at': r[11] or '',
            }
        )

    return render_template(
        'owner_risk.html',
        active_owner_tab='risk',
        blocked=blocked,
        frozen=frozen,
        low_trust=low_trust,
        early_payout_requests=early_payout_requests,
    )


@app.route('/owner/early-payout/<int:request_id>/verify-deposit', methods=['POST'])
@admin_required
def owner_verify_early_payout_deposit(request_id: int):
    utr = (request.form.get('utr') or '').strip()
    if request_id <= 0:
        flash('Invalid request.')
        return redirect(url_for('owner_risk'))

    conn = get_db()
    c = conn.cursor()
    try:
        c.execute(
            'SELECT id, username, COALESCE(group_id,0), COALESCE(status,\'\'), COALESCE(deposit_status,\'\'), COALESCE(utr,\'\') FROM early_payout_requests WHERE id=?',
            (request_id,),
        )
        row = c.fetchone()
    except sqlite3.OperationalError:
        row = None

    if not row:
        conn.close()
        flash('Request not found.')
        return redirect(url_for('owner_risk'))

    req_username = (row[1] or '').strip()
    group_id = int(row[2] or 0)
    status = (row[3] or '').strip()
    deposit_status = (row[4] or '').strip()
    existing_utr = (row[5] or '').strip()

    if deposit_status == 'verified':
        conn.close()
        flash('Deposit already verified.')
        return redirect(url_for('owner_risk'))

    if not utr:
        utr = existing_utr
    if not utr:
        conn.close()
        flash('UTR/reference is required to verify deposit.')
        return redirect(url_for('owner_risk'))

    now = datetime.now().isoformat(timespec='seconds')
    try:
        c.execute(
            'UPDATE early_payout_requests SET utr=?, deposit_status=?, status=?, updated_at=? WHERE id=?',
            (utr, 'verified', 'under_review', now, request_id),
        )
        c.execute(
            'INSERT INTO trust_events (username, event_type, group_id, due_date, verified_at, created_at, note) VALUES (?,?,?,?,?,?,?)',
            (req_username, 'deposit_verified', group_id or None, '', _today_iso(), _today_iso(), 'Early payout security deposit verified'),
        )
        conn.commit()
    except sqlite3.OperationalError:
        conn.close()
        flash('Unable to verify deposit right now.')
        return redirect(url_for('owner_risk'))
    conn.close()

    recalculate_and_store_trust(req_username)
    flash('Deposit verified. Request moved to review.')
    return redirect(url_for('owner_risk'))


@app.route('/owner/early-payout/<int:request_id>/decision', methods=['POST'])
@admin_required
def owner_decide_early_payout(request_id: int):
    action = (request.form.get('action') or '').strip().lower()
    reason = (request.form.get('reason') or '').strip()
    if request_id <= 0:
        flash('Invalid request.')
        return redirect(url_for('owner_risk'))
    if action not in {'approve', 'reject'}:
        flash('Invalid action.')
        return redirect(url_for('owner_risk'))

    conn = get_db()
    c = conn.cursor()
    try:
        c.execute(
            'SELECT id, COALESCE(status,\'\'), COALESCE(deposit_status,\'\') FROM early_payout_requests WHERE id=?',
            (request_id,),
        )
        row = c.fetchone()
    except sqlite3.OperationalError:
        row = None

    if not row:
        conn.close()
        flash('Request not found.')
        return redirect(url_for('owner_risk'))

    status = (row[1] or '').strip()
    deposit_status = (row[2] or '').strip()
    if status in {'approved', 'rejected'}:
        conn.close()
        flash('This request is already decided.')
        return redirect(url_for('owner_risk'))

    if action == 'approve' and deposit_status != 'verified':
        conn.close()
        flash('Verify the security deposit before approving.')
        return redirect(url_for('owner_risk'))

    now = datetime.now().isoformat(timespec='seconds')
    new_status = 'approved' if action == 'approve' else 'rejected'
    if action == 'reject' and not reason:
        reason = 'Rejected by admin.'

    try:
        c.execute(
            'UPDATE early_payout_requests SET status=?, reason=?, updated_at=? WHERE id=?',
            (new_status, reason, now, request_id),
        )
        conn.commit()
    except sqlite3.OperationalError:
        conn.close()
        flash('Unable to update request right now.')
        return redirect(url_for('owner_risk'))
    conn.close()

    flash(f'Request {new_status}.')
    return redirect(url_for('owner_risk'))


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
               COALESCE(g.payout_receiver_username, '') as payout_receiver_username,
               COALESCE(g.payout_receiver_name, '') as payout_receiver_name,
               COALESCE(g.payout_receiver_upi, '') as payout_receiver_upi,
               COALESCE(g.activated_at, '') as activated_at,
               COALESCE(g.next_due_date, '') as next_due_date,
               COALESCE(g.pay_cutoff_time, '') as pay_cutoff_time,
               COALESCE(g.status, '') as group_status,
               COALESCE(g.is_paused, 0) as is_paused,
               COUNT(gm2.id) as joined_members
        FROM group_members gm
        JOIN groups g ON g.id = gm.group_id
        LEFT JOIN group_members gm2 ON gm2.group_id = g.id AND gm2.status='joined'
        WHERE gm.username=? AND gm.status='joined'
        GROUP BY g.id, g.name, g.description, g.monthly_amount, g.max_members, g.receiver_name, g.receiver_upi,
                 g.payout_receiver_username, g.payout_receiver_name, g.payout_receiver_upi,
                 g.activated_at, g.next_due_date, g.pay_cutoff_time,
                 g.status, g.is_paused
        ORDER BY g.monthly_amount, g.id
        ''',
        (username,),
    )
    rows = c.fetchall()
    conn.close()

    groups = []
    for row in rows:
        (
            group_id,
            name,
            description,
            monthly_amount,
            max_members,
            receiver_name,
            receiver_upi,
            payout_receiver_username,
            payout_receiver_name,
            payout_receiver_upi,
            activated_at,
            next_due_date,
            pay_cutoff_time,
            group_status,
            is_paused,
            joined_members,
        ) = row
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
                'payout_receiver_username': (payout_receiver_username or '').strip(),
                'payout_receiver_name': (payout_receiver_name or '').strip(),
                'payout_receiver_upi': (payout_receiver_upi or '').strip(),
                'activated_at': (activated_at or '').strip(),
                'next_due_date': (next_due_date or '').strip(),
                'pay_cutoff_time': (pay_cutoff_time or '').strip(),
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

    # Monthly app-fee preview (credit is applied when owner verifies payment)
    conn = get_db()
    _ensure_app_fee_current_month(conn, username)
    app_fee_amount = _get_app_fee_amount_int(conn)
    credit_balance = _available_app_fee_credit(conn, username)
    credit_preview = _preview_app_fee_credit_apply(conn, username, app_fee_amount)
    net_due = max(0, int(app_fee_amount) - int(credit_preview))
    conn.commit()
    conn.close()

    try:
        trust_score = int(user.get('trust_score') if user.get('trust_score') is not None else 50)
    except Exception:
        trust_score = 50
    trust_score = max(0, min(100, trust_score))

    return render_template(
        'home_tab.html',
        full_name=(user.get('full_name') or user.get('username') or '').strip(),
        has_groups=len(my_groups) > 0,
        active_count=active_count,
        formation_count=formation_count,
        trust_score=trust_score,
        app_fee_amount=int(app_fee_amount or 0),
        app_fee_credit_balance=int(credit_balance or 0),
        app_fee_credit_preview=int(credit_preview or 0),
        app_fee_net_due=int(net_due or 0),
        active_tab='home',
    )


@app.route('/groups')
@require_customer
def groups_tab():
    username = session['username']
    my_groups = _fetch_my_groups(username)
    for g in my_groups:
        try:
            gid = int(g.get('id') or 0)
        except (TypeError, ValueError):
            gid = 0
        g['members'] = _fetch_group_members_with_trust(gid) if gid > 0 else []
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

    conn = get_db()
    c = conn.cursor()
    _ensure_app_fee_current_month(conn, username)

    month_key = _current_month_key()
    app_fee_amount = _get_app_fee_amount_int(conn)
    credit_balance = _available_app_fee_credit(conn, username)
    credit_preview = _preview_app_fee_credit_apply(conn, username, app_fee_amount)
    app_fee_paid_this_month = False
    applied_credit_actual = 0
    net_amount_actual = app_fee_amount
    try:
        c.execute(
            "SELECT gross_amount, credit_applied, net_amount FROM app_fee_payments WHERE username=? AND month=?",
            (username, month_key),
        )
        row = c.fetchone()
        if row:
            app_fee_paid_this_month = True
            applied_credit_actual = int(row[1] or 0)
            net_amount_actual = int(row[2] or 0)
    except sqlite3.OperationalError:
        row = None

    if not app_fee_paid_this_month:
        # Fallback to users flag if ledger is unavailable
        try:
            c.execute("SELECT COALESCE(app_fee_paid,0), COALESCE(app_fee_paid_month,'') FROM users WHERE username=?", (username,))
            urow = c.fetchone()
            if urow and int(urow[0] or 0) == 1 and (urow[1] or '').strip() == month_key:
                app_fee_paid_this_month = True
        except sqlite3.OperationalError:
            pass

    conn.commit()
    conn.close()

    net_to_show = (net_amount_actual if app_fee_paid_this_month else max(0, int(app_fee_amount) - int(credit_preview)))
    session['nav_pay_badge'] = str(int(net_to_show)) if (not app_fee_paid_this_month and int(net_to_show) > 0) else ''

    today_iso = _today_iso()

    pay_to = []
    for g in my_groups:
        # Show payment details only on the due date when a receiver is selected.
        due_date = (g.get('next_due_date') or '').strip()
        if not due_date or due_date != today_iso:
            continue

        # Must have reached max members (default 10)
        try:
            joined_members = int(g.get('joined_members') or 0)
            max_members = int(g.get('max_members') or 10)
        except (TypeError, ValueError):
            joined_members = 0
            max_members = 10
        if joined_members < max(1, max_members):
            continue

        receiver_username = (g.get('payout_receiver_username') or '').strip()
        if receiver_username and receiver_username == username:
            continue

        receiver_upi = (g.get('payout_receiver_upi') or '').strip() or (g.get('receiver_upi') or '').strip()
        receiver_name = (g.get('payout_receiver_name') or '').strip() or (g.get('receiver_name') or '').strip() or 'Receiver'
        amount = g.get('monthly_amount')
        note = f"D-CONT - {g.get('name') or 'Group'}"
        pay_url = ''
        if receiver_upi:
            pay_url = f"upi://pay?pa={quote(receiver_upi)}&pn={quote(receiver_name)}&am={quote(str(amount))}&tn={quote(note)}"

        cutoff = (g.get('pay_cutoff_time') or '').strip() or DEFAULT_PAY_CUTOFF_TIME
        pay_to.append(
            {
                'group': g,
                'receiver_upi': receiver_upi,
                'receiver_name': receiver_name,
                'amount': amount,
                'pay_url': pay_url,
                'due_date': due_date,
                'pay_cutoff_time': cutoff,
            }
        )

    return render_template(
        'payments_tab.html',
        upi_id=upi_id,
        pay_to=pay_to,
        app_fee_amount=int(app_fee_amount or 0),
        app_fee_credit_balance=int(credit_balance or 0),
        app_fee_credit_preview=int(credit_preview or 0),
        app_fee_paid_this_month=bool(app_fee_paid_this_month),
        app_fee_credit_applied_actual=int(applied_credit_actual or 0),
        app_fee_net_amount_actual=int(net_amount_actual or 0),
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
    # Allow pre-login language switching (updates immediately on page reload).
    if request.method == 'GET':
        requested = _normalize_lang(request.args.get('lang') or '')
        if requested in SUPPORTED_LANGS:
            session['lang'] = requested
        return render_template('login.html')

    if request.method == 'POST':
        login_type = (request.form.get('login_type') or '').strip().lower()

        requested_lang = _normalize_lang(request.form.get('lang') or '')
        if requested_lang in SUPPORTED_LANGS:
            # Persist early so error states render in the selected language.
            session['lang'] = requested_lang

        # Read both forms' fields up-front (browser autofill can populate hidden/unused fields)
        identifier = (request.form.get('username') or '').strip()
        password = request.form.get('password') or ''
        admin_username = (request.form.get('admin_username') or '').strip()
        admin_password = request.form.get('admin_password') or ''

        # Admin login (username/password)
        is_admin_attempt = login_type == 'admin'
        if not is_admin_attempt:
            # Back-compat: treat as admin attempt only if admin creds are fully provided
            # and customer creds are empty (avoids autofill breaking customer logins).
            if admin_username and admin_password and (not identifier) and (not password):
                is_admin_attempt = True

        if is_admin_attempt:
            if not admin_username or not admin_password:
                flash('Enter admin user id and password.')
                return render_template('login.html')

            client_ip = _client_ip()
            if _auth_is_rate_limited('admin_pw', admin_username, client_ip):
                flash(t('login_rate_limited'))
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
                _auth_record_attempt('admin_pw', admin_username, client_ip, success=False)
                return render_template('login.html')

            _auth_record_attempt('admin_pw', admin_username, client_ip, success=True)

            session['username'] = row[0]
            session['role'] = 'admin'
            session['lang'] = 'en'
            return redirect(url_for('dashboard'))

        mode = (request.form.get('mode') or '').strip().lower()

        # Customer MPIN login
        if mode == 'mpin':
            mobile_identifier = (request.form.get('mobile') or '').strip()
            mpin = (request.form.get('mpin') or '').strip()
            if not mobile_identifier:
                flash('Enter your mobile number.')
                return render_template('login.html')
            if not _is_valid_mpin(mpin):
                flash('Enter your 4-digit MPIN.')
                return render_template('login.html')

            client_ip = _client_ip()
            if _auth_is_rate_limited('mpin', mobile_identifier, client_ip):
                flash(t('login_rate_limited'))
                return render_template('login.html')

            conn = get_db()
            candidates = _lookup_customer_candidates_by_mobile(conn, mobile_identifier)
            matched = None
            for row in candidates:
                db_username, role_raw, is_active_raw, db_mobile, mpin_hash, _cred_id, _pub_key, _sign_count = row
                db_username = _repair_blank_username(conn, db_username, db_mobile)
                if not (db_username or '').strip():
                    continue
                role = (role_raw or 'customer').strip().lower()
                if role == 'admin':
                    continue
                if int(is_active_raw if is_active_raw is not None else 1) != 1:
                    continue
                if not (mpin_hash or '').strip():
                    continue
                try:
                    if check_password_hash(mpin_hash, mpin):
                        matched = (db_username, role)
                        break
                except (ValueError, TypeError):
                    continue
            conn.close()

            if not matched:
                _auth_record_attempt('mpin', mobile_identifier, client_ip, success=False)
                flash('Invalid MPIN or mobile number.')
                return render_template('login.html')

            _auth_record_attempt('mpin', mobile_identifier, client_ip, success=True)

            session['username'] = matched[0]
            session['role'] = matched[1]
            if requested_lang in SUPPORTED_LANGS:
                session['lang'] = requested_lang
            else:
                session['lang'] = _get_user_language(matched[0])
            return redirect(url_for('home'))

        # Customer login (username OR mobile + password)
        if login_type == 'admin':
            # Explicit admin submission already handled above.
            flash('Invalid admin credentials.')
            return render_template('login.html')

        if not identifier or not password:
            flash('Enter your username/mobile and password.')
            return render_template('login.html')

        client_ip = _client_ip()
        if _auth_is_rate_limited('password', identifier, client_ip):
            flash(t('login_rate_limited'))
            return render_template('login.html')

        conn = get_db()
        c = conn.cursor()

        candidate_rows = []
        try:
            # Username: tolerate case/spacing differences
            c.execute(
                'SELECT username, password, role, is_active, mobile FROM users WHERE lower(trim(username)) = lower(?)',
                (identifier.strip(),),
            )
            candidate_rows.extend(c.fetchall() or [])

            # Mobile: try exact candidates (raw/digits/+91 variations)
            for mobile_value in _mobile_candidates(identifier):
                c.execute(
                    'SELECT username, password, role, is_active, mobile FROM users WHERE mobile=?',
                    (mobile_value,),
                )
                candidate_rows.extend(c.fetchall() or [])

            # Last-resort fallback: digit-normalized mobile matching
            identifier_digits = _normalize_mobile_digits(identifier)
            if identifier_digits:
                c.execute('SELECT username, password, role, is_active, mobile FROM users')
                for cand in c.fetchall() or []:
                    cand_mobile = cand[4] if len(cand) > 4 else ''
                    if _normalize_mobile_digits(cand_mobile) == identifier_digits:
                        candidate_rows.append(cand)
        except sqlite3.OperationalError:
            candidate_rows = []

        candidate_rows = _dedupe_user_rows(candidate_rows)

        if not candidate_rows:
            conn.close()
            _auth_record_attempt('password', identifier, client_ip, success=False)
            flash('Invalid credentials.')
            return render_template('login.html')

        matched = None
        for row in candidate_rows:
            db_username, stored_pw, role_raw, is_active_raw, db_mobile = row
            db_username = _repair_blank_username(conn, db_username, db_mobile)
            if not (db_username or '').strip():
                continue

            role = (role_raw or 'customer').strip().lower()
            if role == 'admin':
                # Don't allow admin to login via customer section
                continue

            if int(is_active_raw if is_active_raw is not None else 1) != 1:
                # Preserve earlier behavior for blocked accounts
                continue

            if _password_matches(stored_pw, password):
                matched = (db_username, role)
                break

        conn.close()

        if not matched:
            # If any candidate is blocked, show the blocked message (more helpful than Invalid credentials)
            any_blocked = False
            for row in candidate_rows:
                try:
                    if int(row[3] if row[3] is not None else 1) != 1:
                        any_blocked = True
                        break
                except Exception:
                    continue
            if any_blocked:
                flash('Your account is blocked. Please contact support.')
            else:
                _auth_record_attempt('password', identifier, client_ip, success=False)
                flash('Invalid credentials.')
            return render_template('login.html')

        _auth_record_attempt('password', identifier, client_ip, success=True)

        session['username'] = matched[0]
        session['role'] = matched[1]

        # Store preferred language for customer UI
        if requested_lang in SUPPORTED_LANGS:
            session['lang'] = requested_lang
            try:
                conn = get_db()
                c = conn.cursor()
                c.execute('UPDATE users SET language=? WHERE username=?', (requested_lang, matched[0]))
                conn.commit()
                conn.close()
            except sqlite3.OperationalError:
                pass
        else:
            session['lang'] = _get_user_language(matched[0])

        return redirect(url_for('home'))

    return render_template('login.html')

@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        agree_terms = request.form.get('agree_terms')
        if agree_terms != 'yes':
            flash('You must agree to the Terms & Conditions to register.')
            return render_template('register.html')
        username = (request.form.get('username') or '').strip()
        password = request.form.get('password') or ''
        mobile_raw = request.form.get('mobile')
        # Store mobile in a normalized digits format when possible so
        # users can log in even if they type +91/spaces/dashes later.
        mobile = _normalize_mobile_digits(mobile_raw) or (mobile_raw or '').strip()
        full_name = request.form.get('full_name')
        language = _normalize_lang(request.form.get('language'))
        city_state = request.form.get('city_state')
        email = request.form.get('email')
        referral_code_input = _normalize_referral_code(request.form.get('referral_code') or '')

        if not username:
            flash('Username is required.')
            return render_template('register.html')
        if not password or len(password.strip()) < 6:
            flash('Password is required (min 6 characters).')
            return render_template('register.html')
        if not mobile:
            flash('Mobile number is required.')
            return render_template('register.html')
        try:
            password_hash = generate_password_hash(password)
            conn = get_db()
            c = conn.cursor()

            c.execute(
                'INSERT INTO users (username, password, mobile, full_name, language, city_state, email, role, upi_id, onboarding_completed, is_active) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                (username, password_hash, mobile, full_name, language, city_state, email, 'customer', '', 0, 1),
            )
            user_id = int(c.lastrowid or 0)

            # Assign referral code for the new user (best-effort)
            new_referral_code = _make_referral_code_from_user_id(user_id)
            try:
                c.execute(
                    'UPDATE users SET referral_code=?, wallet_credit=COALESCE(wallet_credit,0) WHERE username=?',
                    (new_referral_code, username),
                )
            except sqlite3.OperationalError:
                pass

            # If a referral code was provided, validate and create a PENDING record.
            if referral_code_input:
                try:
                    c.execute(
                        "SELECT username, COALESCE(mobile,''), COALESCE(NULLIF(role,''),'customer') FROM users WHERE upper(COALESCE(referral_code,''))=upper(?)",
                        (referral_code_input,),
                    )
                    ref = c.fetchone()
                except sqlite3.OperationalError:
                    ref = None

                if not ref or (ref[2] or '').strip().lower() == 'admin':
                    conn.rollback()
                    conn.close()
                    flash('Invalid referral code. Please remove it or enter a valid code.')
                    return render_template('register.html')

                referrer_username = (ref[0] or '').strip()
                referrer_mobile = _normalize_mobile_digits(ref[1] or '')
                new_mobile_digits = _normalize_mobile_digits(mobile)

                # Anti-fraud: self-referral blocked (username or phone)
                if referrer_username.lower() == username.lower() or (
                    referrer_mobile and new_mobile_digits and referrer_mobile == new_mobile_digits
                ):
                    conn.rollback()
                    conn.close()
                    flash('Self-referral is not allowed. Please remove the referral code.')
                    return render_template('register.html')

                now = datetime.now().isoformat(timespec='seconds')
                try:
                    # One user = one referral record (no chain commissions)
                    c.execute(
                        'INSERT INTO referrals (referrer_username, new_username, status, created_at) VALUES (?,?,?,?)',
                        (referrer_username, username, 'PENDING', now),
                    )
                    c.execute('UPDATE users SET referred_by=? WHERE username=?', (referrer_username, username))
                except sqlite3.IntegrityError:
                    pass
                except sqlite3.OperationalError:
                    pass

            conn.commit()
            conn.close()
            flash('Registration successful! Please log in.')
            return redirect(url_for('login'))
        except sqlite3.IntegrityError:
            flash('Username or mobile already exists!')
    return render_template('register.html')

@app.route('/owner/users/reset_password', methods=['POST'])
@admin_required
def owner_reset_user_password():
    target_username = (request.form.get('username') or '').strip()
    new_password = request.form.get('new_password') or ''

    if not target_username:
        flash('Missing username.')
        return redirect(url_for('owner_users'))
    if not new_password or len(new_password.strip()) < 6:
        flash('Password must be at least 6 characters.')
        return redirect(url_for('owner_user_profile', username=target_username))

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
        flash('Admin passwords are not reset here.')
        return redirect(url_for('owner_users'))

    new_hash = generate_password_hash(new_password)
    c.execute('UPDATE users SET password=? WHERE username=?', (new_hash, target_username))
    conn.commit()
    conn.close()
    flash('Password reset successfully.')
    return redirect(url_for('owner_user_profile', username=target_username))

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
    try:
        c.execute('SELECT group_id FROM group_members WHERE id=?', (membership_id,))
        row = c.fetchone()
        group_id = int(row[0] or 0) if row else 0
    except (TypeError, ValueError, sqlite3.OperationalError):
        group_id = 0

    c.execute('UPDATE group_members SET status=? WHERE id=?', (new_status, membership_id))

    if new_status == 'joined' and group_id > 0:
        _maybe_activate_group(conn, group_id)

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
