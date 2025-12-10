#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Servicio independiente para Webhook de Meta Leads.

- Verificación del webhook (GET /meta-leads)
- Recepción de leads (POST /meta-leads)
- Solo loguea los leads de momento

Se desplegará detrás de gunicorn + nginx (HTTPS en nginx).
"""

import os
import json
import logging
from flask import Flask, request, jsonify

# ==========================
# CONFIGURACIÓN BÁSICA
# ==========================
# Usa la misma filosofía que CloudAPI: token configurable
VERIFY_TOKEN = os.getenv("META_VERIFY_TOKEN", "SICUEL2025")  # pon aquí el que uses en Meta
LOG_LEVEL = os.getenv("META_LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("meta-leads")

app = Flask(__name__)


@app.before_request
def log_request():
    # Log simple estilo CloudAPI
    logger.debug(f"--> {request.method} {request.url}")
    try:
        if request.data and len(request.data) < 4000:
            logger.debug(f"Body: {request.data.decode('utf-8', errors='ignore')}")
    except Exception:
        pass


# ==========================================
# 1) VERIFICACIÓN WEBHOOK (GET /meta-leads)
# ==========================================
@app.route("/meta-leads", methods=["GET", "POST"])
def meta_leads():
    # -------- GET: verificación --------
    if request.method == "GET":
        mode = request.args.get("hub.mode")
        verify_token = request.args.get("hub.verify_token")
        challenge = request.args.get("hub.challenge")

        logger.info(
            f"[META-VERIFY] GET recibido - mode={mode}, token={verify_token}, challenge={challenge}"
        )

        if mode == "subscribe" and verify_token == VERIFY_TOKEN:
            logger.info("[META-VERIFY] ✅ Verificación correcta")
            # Meta exige devolver el challenge en texto plano
            return challenge or "", 200

        logger.warning("[META-VERIFY] ❌ Verificación fallida (verify_token incorrecto)")
        return "Verification token mismatch", 403

    # ==========================================
    # 2) LEADS REALES (POST /meta-leads)
    # ==========================================
    if request.method == "POST":
        payload = request.get_json(silent=True) or {}
        logger.info("========================================")
        logger.info("[META-LEAD] Payload recibido (POST):")
        logger.info(json.dumps(payload, indent=2, ensure_ascii=False))
        logger.info("========================================")

        # Intentar extraer campos típicos del lead
        try:
            entry = payload["entry"][0]
            change = entry["changes"][0]
            value = change["value"]

            leadgen_id = value.get("leadgen_id")
            form_id = value.get("form_id")
            page_id = value.get("page_id")
            created_time = value.get("created_time")

            logger.info(
                f"[META-LEAD] leadgen_id={leadgen_id}, form_id={form_id}, "
                f"page_id={page_id}, created_time={created_time}"
            )

        except Exception as e:
            logger.exception(f"[META-LEAD] Error parseando lead: {e}")

        # Respuesta que espera Meta
        return jsonify({"status": "ok"}), 200


# ==========================================
# ENTRYPOINT LOCAL (solo para pruebas)
# ==========================================
if __name__ == "__main__":
    # Solo para pruebas locales con http://localhost:5005/meta-leads
    logger.info("🚀 Arrancando Meta Leads Webhook en modo desarrollo (sin HTTPS)")
    app.run(host="0.0.0.0", port=5005, debug=True)
