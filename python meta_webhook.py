#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
INSTANCIA INDEPENDIENTE PARA WEBHOOK DE META LEADS
Autor: Isidoro
Descripción:
  - Servidor Flask independiente
  - Valida el webhook de Meta (GET)
  - Recibe leads (POST)
  - Imprime en consola todo lo recibido
"""

from flask import Flask, request, jsonify
import json
import logging

# ==========================================
# CONFIGURACIÓN
# ==========================================
VERIFY_TOKEN = "SICUEL2025"       # Token que pones en Meta
PORT = 5005                      # Puerto local
HOST = "0.0.0.0"                 # Exponer hacia fuera

# Inicializar Flask app
app = Flask(__name__)

# Logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("META-LEADS")


# ==========================================
# ROUTE: /meta-leads
# ==========================================
@app.route("/meta-leads", methods=["GET", "POST"])
def meta_leads():

    # ------------------------------------------
    # 1) VERIFICACIÓN META (GET)
    # ------------------------------------------
    if request.method == "GET":
        mode = request.args.get("hub.mode")
        verify_token = request.args.get("hub.verify_token")
        challenge = request.args.get("hub.challenge")

        logger.info(f"[META] GET recibido: mode={mode}, token={verify_token}, challenge={challenge}")

        if mode == "subscribe" and verify_token == VERIFY_TOKEN:
            logger.info("[META] VERIFICACIÓN CORRECTA ✔️")
            return challenge, 200

        logger.warning("[META] VERIFICACIÓN FALLIDA ❌")
        return "Verification token mismatch", 403

    # ------------------------------------------
    # 2) LEADS REALES (POST)
    # ------------------------------------------
    if request.method == "POST":
        payload = request.get_json(silent=True) or {}

        logger.info("========================================")
        logger.info("[META] LEAD RECIBIDO (POST):")
        logger.info(json.dumps(payload, indent=2))
        logger.info("========================================")

        # Extraer campos del lead:
        try:
            entry = payload["entry"][0]
            change = entry["changes"][0]
            value = change["value"]

            leadgen_id = value.get("leadgen_id")
            form_id = value.get("form_id")
            created_time = value.get("created_time")
            page_id = value.get("page_id")

            logger.info(f"[META] leadgen_id={leadgen_id}, form_id={form_id}, page_id={page_id}")

        except Exception as e:
            logger.error(f"[META] Error extrayendo datos del lead: {e}")

        return jsonify({"status": "ok"}), 200


# ==========================================
# EJECUTAR SERVIDOR 
# ==========================================
if __name__ == "__main__":
    logger.info("========================================")
    logger.info("  SERVICIO META LEADS INICIADO ✔️")
    logger.info(f"  Escuchando en http://{HOST}:{PORT}/meta-leads")
    logger.info("  CTRL+C para detener")
    logger.info("========================================")
    app.run(host=HOST, port=PORT)
