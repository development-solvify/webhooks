import os
import logging
from logging.handlers import RotatingFileHandler

from flask import Flask, request, jsonify
import requests

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
VPBX_API_KEY = os.environ.get("VPBX_API_KEY")
VPBX_BASE_URL = os.environ.get("VPBX_BASE_URL", "https://vpbx.me/api")

ALLOWED_ORIGINS = os.environ.get(
    "ALLOWED_ORIGINS",
    "https://app.solvify.es,https://portal.eliminamostudeuda.com"
).split(",")

INTERNAL_TOKEN = os.environ.get("CLICK2CALL_TOKEN")  # opcional

LOG_FILE = os.environ.get(
    "CLICK2CALL_LOG_FILE",
    "/home/isidoro/webhooks/click_to_call_service.log"
)


VPBX_API_KEY="JxrjJ7I5oE20g64ua3XQ0Hocv7YlBbMd"
VPBX_BASE_URL="https://vpbx.me/api"
ALLOWED_ORIGINS="https://app.solvify.es,https://portal.eliminamostudeuda.com"
CLICK2CALL_LOG_FILE="/home/isidoro/webhooks/logs/click_to_call_service.log"
CLICK2CALL_TOKEN="pon_aqui_un_token_largo_y_secreto"

# -----------------------------------------------------------------------------
# Flask app
# -----------------------------------------------------------------------------
app = Flask(__name__)

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

logger = logging.getLogger("click_to_call")
logger.setLevel(logging.INFO)

handler = RotatingFileHandler(LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=5)
formatter = logging.Formatter(
    "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
handler.setFormatter(formatter)
logger.addHandler(handler)

# que Flask use este logger también
app.logger.handlers = logger.handlers
app.logger.setLevel(logger.level)


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def is_origin_allowed(origin):
    if not origin:
        return False
    origin = origin.strip()
    allowed = [o.strip() for o in ALLOWED_ORIGINS if o.strip()]
    return origin in allowed


def require_internal_token():
    """Validación simple por token interno en cabecera X-Internal-Token."""
    if not INTERNAL_TOKEN:
        return True  # si no está configurado, no forzamos seguridad
    token = request.headers.get("X-Internal-Token")
    if token != INTERNAL_TOKEN:
        logger.warning("❌ Token interno inválido en click_to_call")
        return False
    return True


# -----------------------------------------------------------------------------
# CORS
# -----------------------------------------------------------------------------
@app.after_request
def add_cors_headers(response):
    origin = request.headers.get("Origin")
    if is_origin_allowed(origin):
        response.headers["Access-Control-Allow-Origin"] = origin
        response.headers["Vary"] = "Origin"
        response.headers["Access-Control-Allow-Headers"] = (
            "Content-Type,Authorization,X-Internal-Token"
        )
        response.headers["Access-Control-Allow-Methods"] = "POST,OPTIONS"
    return response


@app.route("/click_to_call", methods=["OPTIONS"])
def click_to_call_options():
    # Preflight CORS
    resp = jsonify({})
    origin = request.headers.get("Origin")
    if is_origin_allowed(origin):
        resp.headers["Access-Control-Allow-Origin"] = origin
        resp.headers["Vary"] = "Origin"
        resp.headers["Access-Control-Allow-Headers"] = (
            "Content-Type,Authorization,X-Internal-Token"
        )
        resp.headers["Access-Control-Allow-Methods"] = "POST,OPTIONS"
    return resp, 200


# -----------------------------------------------------------------------------
# Healthcheck
# -----------------------------------------------------------------------------
@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200


# -----------------------------------------------------------------------------
# Endpoint principal: click to call
# -----------------------------------------------------------------------------
@app.route("/click_to_call", methods=["POST"])
def click_to_call():
    if not require_internal_token():
        return jsonify({"error": "unauthorized"}), 401

    data = request.get_json(silent=True) or {}
    phone = str(data.get("phone") or "").strip()
    extension = str(data.get("extension") or "").strip()

    logger.info(f"📞 Petición click_to_call phone={phone} extension={extension}")

    if not phone or not extension:
        logger.warning("⚠️ phone o extension vacíos")
        return jsonify({"error": "phone and extension are required"}), 400

    if not VPBX_API_KEY:
        logger.error("❌ VPBX_API_KEY no configurada en entorno")
        return jsonify({"error": "server misconfigured"}), 500

    url = f"{VPBX_BASE_URL}/c2cexternal/{phone}/*{extension}"
    headers = {
        "X-Api-Key": VPBX_API_KEY,
        "Accept": "application/json",
    }

    logger.info(f"➡️ Llamando a VPBX {url}")

    try:
        resp = requests.get(url, headers=headers, timeout=5)
    except requests.RequestException as e:
        logger.exception("❌ Error llamando a VPBX")
        return jsonify({"error": "vpbx request failed", "details": str(e)}), 502

    try:
        body = resp.json()
    except ValueError:
        body = {"raw": resp.text}

    logger.info(f"⬅️ Respuesta VPBX status={resp.status_code} body={body}")

    success = False
    cause = None
    if isinstance(body, dict):
        success = bool(body.get("success"))
        vars_ = body.get("variables") or {}
        cause = vars_.get("cause")

    return jsonify({
        "ok": success,
        "status_code": resp.status_code,
        "vpbx_response": body,
        "cause": cause,
    }), resp.status_code


# -----------------------------------------------------------------------------
# Entry point (sin gunicorn)
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    # Para tu uso interno está bien el servidor incorporado
    logger.info("🚀 Iniciando click_to_call_service en 0.0.0.0:5010")
    app.run(host="0.0.0.0", port=5010)
