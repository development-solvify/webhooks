#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Servicio independiente para Webhook de Meta Leads.

- Verificación del webhook (GET /meta-leads)
- Recepción de leads (POST /meta-leads)
- Llamada a Graph API para obtener datos del lead (field_data)
- Log en consola de los datos normalizados (nombre, email, teléfono, etc.)

Se puede ejecutar:
  - En local: python3 meta_leads_service.py
  - En producción: detrás de gunicorn + nginx con HTTPS
"""

import os
import json
import logging
from typing import Dict, Any

import requests
from flask import Flask, request, jsonify

# ======================================================
# CONFIGURACIÓN POR VARIABLES DE ENTORNO
# ======================================================

# Token de verificación que configuras en Meta Developers (Webhooks)
VERIFY_TOKEN = os.getenv("META_VERIFY_TOKEN", "SICUEL2025")

# Page Access Token para llamar a Graph API y sacar los datos del lead
# (de momento puedes sacarlo a mano desde Graph API Explorer)
PAGE_ACCESS_TOKEN = os.getenv("META_PAGE_ACCESS_TOKEN", "EAAMUdsKLqqkBQD4g1R8WeBiYrshJZB3Rkw88Vxa4P3i69czmIx6ZBmME4C3sIF9mtvVpP0nBZBkZA2cE6ZASj8AXg5JYrCWKIDWrIGSthmACZC7ZAZCSwjclNZCT4S7pYAuZC0de3mRcAoOgmHJc2GmC3jkOtDvkBOTGLt4RxiReFTJp8zuL9Q55qlgthgDle8OQZDZD")

# Nivel de log
LOG_LEVEL = os.getenv("META_LOG_LEVEL", "INFO").upper()

# Puerto por defecto para modo desarrollo (solo si lanzas __main__)
DEV_PORT = int(os.getenv("META_DEV_PORT", "5005"))

# ======================================================
# LOGGING
# ======================================================

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("meta-leads")

# ======================================================
# FLASK APP
# ======================================================

app = Flask(__name__)


@app.before_request
def log_request():
    """
    Log simple de cada request entrante (similar a tu CloudAPI2).
    """
    try:
        logger.debug(f"--> {request.method} {request.url}")
        if request.data and len(request.data) < 4000:
            logger.debug(f"Body: {request.data.decode('utf-8', errors='ignore')}")
    except Exception:
        pass


# ======================================================
# FUNCIÓN AUXILIAR: LLAMAR A GRAPH API
# ======================================================

def fetch_lead_details(leadgen_id: str) -> Dict[str, Any]:
    """
    Pide a la Graph API los datos del lead (field_data).

    Devuelve el JSON completo que responde Meta, o {} en caso de error.
    """
    if not PAGE_ACCESS_TOKEN:
        logger.warning(
            "[META-LEAD] No hay META_PAGE_ACCESS_TOKEN configurado; "
            "no se puede pedir detalle del lead a Graph API."
        )
        return {}

    url = f"https://graph.facebook.com/v21.0/{leadgen_id}"
    params = {
        "access_token": PAGE_ACCESS_TOKEN
    }

    logger.info(f"[META-LEAD] Llamando a Graph API para leadgen_id={leadgen_id}")

    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        logger.info(
            f"[META-LEAD] Respuesta Graph API para lead {leadgen_id}: "
            f"{json.dumps(data, indent=2, ensure_ascii=False)}"
        )
        return data
    except Exception as e:
        logger.exception(f"[META-LEAD] Error llamando a Graph API para lead {leadgen_id}: {e}")
        return {}


def normalize_field_data(field_data: Any) -> Dict[str, Any]:
    """
    A partir de field_data de Meta, construye un diccionario simple:

    {
      "full_name": "Juan Pérez",
      "email": "juan@example.com",
      "phone_number": "+34666666666",
      ...
    }
    """
    simplified: Dict[str, Any] = {}

    if not isinstance(field_data, list):
        return simplified

    for field in field_data:
        name = field.get("name")
        values = field.get("values") or []
        if name and values:
            simplified[name] = values[0]

    return simplified


# ======================================================
# ENDPOINT PRINCIPAL: /meta-leads
# ======================================================

@app.route("/meta-leads", methods=["GET", "POST"])
def meta_leads():
    # ------------------------------------------
    # 1) VERIFICACIÓN META (GET)
    # ------------------------------------------
    if request.method == "GET":
        mode = request.args.get("hub.mode")
        verify_token = request.args.get("hub.verify_token")
        challenge = request.args.get("hub.challenge")

        logger.info(
            f"[META-VERIFY] GET recibido: mode={mode}, "
            f"verify_token={verify_token}, challenge={challenge}"
        )

        if mode == "subscribe" and verify_token == VERIFY_TOKEN:
            logger.info("[META-VERIFY] ✅ Verificación correcta")
            # Meta exige devolver el challenge en texto plano
            return challenge or "", 200

        logger.warning("[META-VERIFY] ❌ Verificación fallida (verify_token incorrecto)")
        return "Verification token mismatch", 403

    # ------------------------------------------
    # 2) LEADS REALES (POST)
    # ------------------------------------------
    if request.method == "POST":
        payload = request.get_json(silent=True) or {}

        logger.info("========================================")
        logger.info("[META-LEAD] Payload recibido (POST):")
        logger.info(json.dumps(payload, indent=2, ensure_ascii=False))
        logger.info("========================================")

        leadgen_id = None
        form_id = None
        page_id = None
        created_time = None

        # Extraer datos básicos del evento del webhook
        try:
            entry = payload["entry"][0]
            change = entry["changes"][0]
            value = change["value"]

            leadgen_id = value.get("leadgen_id")
            form_id = value.get("form_id")
            page_id = value.get("page_id")
            created_time = value.get("created_time")

            logger.info(
                f"[META-LEAD] Info básica: leadgen_id={leadgen_id}, "
                f"form_id={form_id}, page_id={page_id}, created_time={created_time}"
            )

        except Exception as e:
            logger.exception(f"[META-LEAD] Error parseando payload básico: {e}")

        # ------------------------------------------
        # 3) LLAMAR A GRAPH API PARA DETALLE DEL LEAD
        # ------------------------------------------
        if leadgen_id:
            details = fetch_lead_details(leadgen_id)
            field_data = details.get("field_data", [])
            simplified = normalize_field_data(field_data)

            logger.info("[META-LEAD] Campos del lead normalizados (field_data):")
            logger.info(json.dumps(simplified, indent=2, ensure_ascii=False))

            # Aquí podrías ya:
            # - Enviar a tu API de SICUEL
            # - Insertar en Supabase
            # - Crear lead en Pipedrive, etc.
            #
            # Por ahora, solo lo dejamos en logs.

        # Respuesta estándar para Meta
        return jsonify({"status": "ok"}), 200


# ======================================================
# MAIN PARA DESARROLLO LOCAL
# ======================================================

if __name__ == "__main__":
    logger.info("========================================")
    logger.info("  SERVICIO META LEADS - MODO DESARROLLO")
    logger.info(f"  VERIFY_TOKEN        = {VERIFY_TOKEN}")
    logger.info(f"  PAGE_ACCESS_TOKEN   = {'SET' if PAGE_ACCESS_TOKEN else 'NOT SET'}")
    logger.info(f"  Escuchando en http://0.0.0.0:{DEV_PORT}/meta-leads")
    logger.info("  IMPORTANTE: Para Meta en producción debe ir detrás de HTTPS (nginx).")
    logger.info("========================================")
    app.run(host="0.0.0.0", port=DEV_PORT, debug=True)
