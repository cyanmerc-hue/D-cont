import os
from flask import Flask, render_template, request, redirect, url_for, session, flash

app = Flask(__name__)

app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "dev-only-change-me")

# Minimal translation helper so templates using {{ t('...') }} don't crash
@app.context_processor
def inject_t():
    def t(key, default=None):
        # If you have a real translations dict later, plug it here.
        return default or key
    return {"t": t}

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "GET":
        return render_template("login.html")
    # For now just accept the form and redirect (we’ll wire Supabase next)
    flash("Login backend not connected yet.")
    return redirect(url_for("login"))

@app.route("/")
def home():
    return redirect(url_for("login"))
