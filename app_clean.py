import os
from flask import Flask, redirect, url_for

app = Flask(__name__)
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "dev-only")

@app.route("/login", methods=["GET", "POST"])
def login():
    return "TEMP LOGIN OK"

@app.route("/")
def home():
    return redirect(url_for("login"))
