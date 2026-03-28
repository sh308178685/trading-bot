"""Flask application for the martingale dashboard."""

from __future__ import annotations

import hmac
import os
import secrets
from pathlib import Path

from flask import Flask, jsonify, redirect, render_template, request, session, url_for

from dashboard.data_provider import DashboardService
from trading.runtime_config import load_runtime_config


ROOT = Path(__file__).resolve().parent.parent
CONFIG_FILE = ROOT / "config" / "config.json"
CONFIG = load_runtime_config(CONFIG_FILE, default={})


def load_auth_settings() -> dict[str, str]:
    return {
        "username": os.getenv("MARTIN_DASHBOARD_USERNAME")
        or CONFIG.get("dashboardUsername")
        or "admin",
        "password": os.getenv("MARTIN_DASHBOARD_PASSWORD")
        or CONFIG.get("dashboardPassword")
        or "",
        "secret": os.getenv("MARTIN_DASHBOARD_SECRET")
        or CONFIG.get("dashboardSecret")
        or secrets.token_hex(32),
    }


AUTH = load_auth_settings()

app = Flask(
    __name__,
    template_folder="templates",
    static_folder="static",
)
app.secret_key = AUTH["secret"]
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
)
service = DashboardService(refresh_ttl=2)


def auth_enabled() -> bool:
    return bool(AUTH["password"])


def is_authenticated() -> bool:
    return (not auth_enabled()) or bool(session.get("dashboard_authenticated"))


def safe_next_url(value: str | None) -> str:
    if value and value.startswith("/") and not value.startswith("//"):
        return value
    return url_for("index")


@app.before_request
def require_login():
    if not auth_enabled():
        return None

    if request.endpoint in {"login", "logout", "static"}:
        return None

    if is_authenticated():
        return None

    if request.path.startswith("/api/"):
        return jsonify({"ok": False, "error": "需要先登录"}), 401

    next_url = request.full_path if request.query_string else request.path
    return redirect(url_for("login", next=next_url))


@app.context_processor
def inject_auth_state():
    return {
        "dashboard_auth_enabled": auth_enabled(),
        "dashboard_user": session.get("dashboard_user") or AUTH["username"],
    }


@app.route("/login", methods=["GET", "POST"])
def login():
    if not auth_enabled():
        return redirect(url_for("index"))

    error = None
    next_url = safe_next_url(request.args.get("next") or request.form.get("next"))

    if request.method == "POST":
        username = (request.form.get("username") or "").strip()
        password = request.form.get("password") or ""

        username_ok = hmac.compare_digest(username, AUTH["username"])
        password_ok = hmac.compare_digest(password, AUTH["password"])

        if username_ok and password_ok:
            session.clear()
            session["dashboard_authenticated"] = True
            session["dashboard_user"] = AUTH["username"]
            return redirect(next_url)

        error = "用户名或密码错误。"

    return render_template(
        "login.html",
        error=error,
        next_url=next_url,
        username_hint=AUTH["username"],
    )


@app.route("/logout")
def logout():
    session.clear()
    if auth_enabled():
        return redirect(url_for("login"))
    return redirect(url_for("index"))


@app.route("/")
def index():
    return render_template(
        "index.html",
        auth_enabled=auth_enabled(),
        auth_user=session.get("dashboard_user") or AUTH["username"],
    )


@app.route("/api/dashboard")
def dashboard_snapshot():
    force = request.args.get("force") in {"1", "true", "yes"}
    return jsonify(service.get_snapshot(force=force))


@app.route("/api/health")
def health():
    snapshot = service.get_snapshot(force=False)
    return jsonify(
        {
            "ok": True,
            "timestamp": snapshot.get("timestamp"),
            "stale": snapshot.get("server", {}).get("stale", False),
            "warnings": snapshot.get("server", {}).get("warnings", []),
            "auth_enabled": auth_enabled(),
        }
    )
