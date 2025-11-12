import os
import logging
from logging.handlers import RotatingFileHandler

from flask import Flask, request, jsonify
import requests

# -----------------------------------------------------------------------------
# Config (con defaults si no hay entorno)
# -----------------------------------------------------------------------------
VPBX_API_KEY = os.environ.get("VPBX_API_KEY") or "JxrjJ7I5oE20g64ua3XQ0Hocv7YlBbMd"
VPBX_BASE_URL = os.environ.get("VPBX_BASE_URL") or "https://vpbx.me/api"
VPBX_TIMEOUT = int(os.environ.get("VPBX_TIMEOUT", "20"))  # segundos

ALLOWED_ORIGINS_RAW = os.environ.get(
    "ALLOWED_ORIGINS",
    "https://app.solvify.es,https://portal.eliminamostudeuda.com"
)
ALLOWED_ORIGINS = [o.strip() for o in ALLOWED_ORIGINS_RAW.split(",") if o.strip()]


LOG_FILE = os.environ.get(
    "CLICK2CALL_LOG_FILE",
    "/home/isidoro/webhooks/logs/click_to_call_service.log"
)

# -----------------------------------------------------------------------------
# Flask app
# -----------------------------------------------------------------------------
app = Flask(__name__)
CORS(
    app,
    resources={
        r"/click_to_call": {
            "origins": ALLOWED_ORIGINS,
            "methods": ["POST", "OPTIONS"],
            "allow_headers": ["Content-Type", "Authorization", "X-Internal-Token"],
        }
    },
)
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

logger.info("🔧 Config inicial:")
logger.info(f"   VPBX_BASE_URL={VPBX_BASE_URL}")
logger.info(f"   VPBX_TIMEOUT={VPBX_TIMEOUT}")
logger.info(f"   ALLOWED_ORIGINS={ALLOWED_ORIGINS}")
logger.info(f"   LOG_FILE={LOG_FILE}")


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def is_origin_allowed(origin):
    if not origin:
        return False
    origin = origin.strip()
    allowed = origin in ALLOWED_ORIGINS
    logger.info(f"🔎 CORS check origin={origin} allowed={allowed}")
    return allowed

def log_request_debug():
    """Log detallado de la petición entrante."""
    try:
        raw_body = request.get_data(as_text=True)
    except Exception:
        raw_body = "<no_body>"

    safe_headers = {}
    for k, v in request.headers.items():
        if k.lower() in ("authorization", "cookie"):
            safe_headers[k] = "***redacted***"
        else:
            safe_headers[k] = v

    logger.info("📥 REQUEST DEBUG:")
    logger.info(f"   method={request.method} path={request.path}")
    logger.info(f"   remote_addr={request.remote_addr}")
    logger.info(f"   origin={request.headers.get('Origin')}")
    logger.info(f"   headers={safe_headers}")
    logger.info(f"   raw_body={raw_body}")


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
    log_request_debug()
    resp = jsonify({})
    origin = request.headers.get("Origin")
    if is_origin_allowed(origin):
        resp.headers["Access-Control-Allow-Origin"] = origin
        resp.headers["Vary"] = "Origin"
        resp.headers["Access-Control-Allow-Headers"] = (
            "Content-Type,Authorization,X-Internal-Token"
        )
        resp.headers["Access-Control-Allow-Methods"] = "POST,OPTIONS"
    logger.info("✅ Respuesta OPTIONS /click_to_call enviada")
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
    log_request_debug()

    # Intentar JSON, si no, form/query (por si acaso)
    data = request.get_json(silent=True)
    if data is None:
        data = request.form.to_dict() or request.args.to_dict() or {}

    logger.info(f"🧾 Payload parseado: {data}")

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

    logger.info(f"➡️ Llamando a VPBX url={url} headers={headers} timeout={VPBX_TIMEOUT}")

    try:
        resp = requests.get(url, headers=headers, timeout=VPBX_TIMEOUT)
    except requests.exceptions.ReadTimeout as e:
        logger.warning(f"⏱️ Timeout llamando a VPBX tras {VPBX_TIMEOUT}s", exc_info=True)
        return jsonify({
            "ok": False,
            "error": "vpbx_timeout",
            "message": f"VPBX no respondió en {VPBX_TIMEOUT} segundos",
        }), 504
    except requests.RequestException as e:
        logger.exception("❌ Error llamando a VPBX")
        return jsonify({
            "ok": False,
            "error": "vpbx_request_failed",
            "message": "Error en la llamada a VPBX",
            "details": str(e),
        }), 502

    # Log bruto de la respuesta de VPBX
    vpbx_headers = dict(resp.headers)
    logger.info(f"⬅️ Respuesta VPBX status={resp.status_code}")
    logger.info(f"   VPBX headers={vpbx_headers}")
    logger.info(f"   VPBX raw_body={resp.text}")

    try:
        body = resp.json()
    except ValueError:
        body = {"raw": resp.text}

    logger.info(f"   VPBX parsed_body={body}")

    success = False
    cause = None
    call_id = None
    if isinstance(body, dict):
        success = bool(body.get("success"))
        vars_ = body.get("variables") or {}
        cause = vars_.get("cause")
        call_id = vars_.get("callId")

    logger.info(
        f"📊 Resultado normalizado: success={success} cause={cause} call_id={call_id}"
    )

    return jsonify({
        "ok": success,
        "status_code": resp.status_code,
        "vpbx_response": body,
        "cause": cause,
        "call_id": call_id,
    }), resp.status_code


# -----------------------------------------------------------------------------
# Entry point (sin gunicorn)
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    logger.info("🚀 Iniciando click_to_call_service en 0.0.0.0:5010")
    app.run(host="0.0.0.0", port=5010)
