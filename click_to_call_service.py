#!/usr/bin/env python3
# click_to_call_service.py
import os
import logging
from logging.handlers import RotatingFileHandler
import uuid
import time
import threading
import traceback

from flask import Flask, request, jsonify
import requests

# ---------------------------------------------------------------------------
# Config (con defaults si no hay entorno)
# ---------------------------------------------------------------------------
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

# Si quieres que la llamada a VPBX sea asíncrona (devuelve 202 al cliente)
CLICK2CALL_ASYNC = os.environ.get("CLICK2CALL_ASYNC", "false").lower() in ("1", "true", "yes")

# ---------------------------------------------------------------------------
# Flask app
# ---------------------------------------------------------------------------
app = Flask(__name__)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
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
logger.info(f"   CLICK2CALL_ASYNC={CLICK2CALL_ASYNC}")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def is_origin_allowed(origin):
    if not origin:
        logger.info("🔎 CORS check: no Origin header present -> denied")
        return False
    origin = origin.strip()
    allowed = origin in ALLOWED_ORIGINS
    logger.info(f"🔎 CORS check origin={origin} allowed={allowed}")
    return allowed

def log_request_debug(request_id: str):
    """Log detallado de la petición entrante con request_id."""
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

    logger.info(f"📥 [{request_id}] REQUEST DEBUG:")
    logger.info(f"   [{request_id}] method={request.method} path={request.path}")
    logger.info(f"   [{request_id}] remote_addr={request.remote_addr}")
    logger.info(f"   [{request_id}] origin={request.headers.get('Origin')}")
    logger.info(f"   [{request_id}] headers={safe_headers}")
    logger.info(f"   [{request_id}] raw_body={raw_body}")

def call_vpbx(phone: str, extension: str, request_id: str):
    """Hace la llamada GET a VPBX y devuelve (status_code, resp_text, parsed_json_or_none)."""
    url = f"{VPBX_BASE_URL}/c2cexternal/{phone}/*{extension}"
    headers = {"X-Api-Key": VPBX_API_KEY, "Accept": "application/json"}
    logger.info(f"➡️ [{request_id}] Llamando a VPBX url={url} headers_keys={list(headers.keys())} timeout={VPBX_TIMEOUT}")

    start = time.perf_counter()
    try:
        resp = requests.get(url, headers=headers, timeout=VPBX_TIMEOUT)
        elapsed = time.perf_counter() - start
        logger.info(f"⬅️ [{request_id}] VPBX responded status={resp.status_code} elapsed={elapsed:.3f}s")
        try:
            parsed = resp.json()
            logger.info(f"   [{request_id}] VPBX parsed_json={parsed}")
        except ValueError:
            parsed = None
            logger.info(f"   [{request_id}] VPBX raw_text={resp.text}")
        return resp.status_code, resp.headers, resp.text, parsed, elapsed
    except requests.exceptions.ReadTimeout:
        elapsed = time.perf_counter() - start
        logger.warning(f"⏱️ [{request_id}] Timeout llamando a VPBX tras {VPBX_TIMEOUT}s (elapsed={elapsed:.3f}s)")
        raise
    except Exception as exc:
        elapsed = time.perf_counter() - start
        logger.exception(f"❌ [{request_id}] Exception llamando a VPBX (elapsed={elapsed:.3f}s): {exc}")
        raise

# ---------------------------------------------------------------------------
# Unified endpoint: POST + OPTIONS
# ---------------------------------------------------------------------------
@app.route("/click_to_call", methods=["POST", "OPTIONS"], endpoint="click_to_call_handler")
def click_to_call_handler():
    request_id = str(uuid.uuid4())[:8]
    try:
        # Preflight
        if request.method == "OPTIONS":
            log_request_debug(request_id)
            resp = jsonify({})
            origin = request.headers.get("Origin")
            if is_origin_allowed(origin):
                resp.headers["Access-Control-Allow-Origin"]  = origin
                resp.headers["Vary"]                         = "Origin"
                resp.headers["Access-Control-Allow-Headers"] = "Content-Type,Authorization,X-Internal-Token"
                resp.headers["Access-Control-Allow-Methods"] = "POST,OPTIONS"
                resp.headers["Access-Control-Max-Age"]       = "86400"
                logger.info(f"✅ [{request_id}] Preflight OK for origin={origin}")
                return resp, 204
            logger.warning(f"⛔ [{request_id}] Preflight denied for origin={origin}")
            return resp, 403

        # POST real
        log_request_debug(request_id)
        # Intentar JSON, si no, form/query
        data = request.get_json(silent=True) or request.form.to_dict() or request.args.to_dict() or {}
        logger.info(f"🧾 [{request_id}] Payload parseado: {data}")

        phone = str(data.get("phone") or "").strip()
        extension = str(data.get("extension") or "").strip()
        logger.info(f"📞 [{request_id}] Petición click_to_call phone='{phone}' extension='{extension}'")

        if not phone or not extension:
            logger.warning(f"⚠️ [{request_id}] phone o extension vacíos")
            return jsonify({"error": "phone and extension are required", "request_id": request_id}), 400

        if not VPBX_API_KEY:
            logger.error(f"❌ [{request_id}] VPBX_API_KEY no configurada en entorno")
            return jsonify({"error": "server misconfigured", "request_id": request_id}), 500

        # Si se ha pedido asincronía, se dispara en background y devolvemos 202
        if CLICK2CALL_ASYNC:
            logger.info(f"🚀 [{request_id}] CLICK2CALL_ASYNC activo: se dispara la llamada en background y se responde 202")
            thread = threading.Thread(
                target=_background_vpbx_call,
                args=(phone, extension, request_id),
                daemon=True,
            )
            thread.start()
            return jsonify({"ok": True, "accepted": True, "request_id": request_id}), 202

        # Síncrono: hacemos la llamada y esperamos respuesta
        try:
            status, vpbx_headers, vpbx_text, vpbx_json, elapsed = call_vpbx(phone, extension, request_id)
        except requests.exceptions.ReadTimeout:
            return jsonify({
                "ok": False,
                "error": "vpbx_timeout",
                "message": f"VPBX no respondió en {VPBX_TIMEOUT} segundos",
                "request_id": request_id,
            }), 504
        except requests.RequestException as e:
            logger.exception(f"❌ [{request_id}] Error en requests")
            return jsonify({
                "ok": False,
                "error": "vpbx_request_failed",
                "message": "Error en la llamada a VPBX",
                "details": str(e),
                "request_id": request_id,
            }), 502
        except Exception as e:
            logger.exception(f"❌ [{request_id}] Error inesperado en call_vpbx")
            return jsonify({
                "ok": False,
                "error": "internal_error",
                "message": "Error interno ejecutando la llamada",
                "details": str(e),
                "request_id": request_id,
            }), 500

        # Normalizar respuesta
        body = vpbx_json or {}
        success = bool(body.get("success")) if isinstance(body, dict) else False
        vars_ = (body or {}).get("variables") or {}
        cause = vars_.get("cause")
        call_id = vars_.get("callId")

        logger.info(f"📊 [{request_id}] Resultado normalizado: success={success} cause={cause} call_id={call_id} elapsed={elapsed:.3f}s")

        resp_payload = {
            "ok": success,
            "status_code": status,
            "vpbx_response": body if body else {"raw": vpbx_text},
            "cause": cause,
            "call_id": call_id,
            "request_id": request_id,
        }
        return jsonify(resp_payload), status

    except Exception as e:
        logger.exception(f"❌ [{request_id}] Unhandled exception en click_to_call_handler: {e}")
        return jsonify({
            "ok": False,
            "error": "internal_error",
            "message": "Unhandled exception",
            "details": str(e),
            "traceback": traceback.format_exc(),
            "request_id": request_id,
        }), 500

def _background_vpbx_call(phone: str, extension: str, request_id: str):
    """Worker background para llamadas asíncronas (solo logs, no devuelve nada al cliente)."""
    try:
        logger.info(f"🧵 [{request_id}] Background worker started for phone={phone} extension={extension}")
        status, vpbx_headers, vpbx_text, vpbx_json, elapsed = call_vpbx(phone, extension, request_id)
        logger.info(f"🧵 [{request_id}] Background VPBX finished status={status} elapsed={elapsed:.3f}s")
    except Exception as e:
        logger.exception(f"🧵 [{request_id}] Error en background_vpbx_call: {e}")

# ---------------------------------------------------------------------------
# Healthcheck
# ---------------------------------------------------------------------------
@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200

# ---------------------------------------------------------------------------
# Entry point (sin gunicorn)
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    logger.info("🚀 Iniciando click_to_call_service en 0.0.0.0:5010")
    app.run(host="0.0.0.0", port=5010)
