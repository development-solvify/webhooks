#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import logging
import requests
import pg8000
import csv
from configparser import ConfigParser
from typing import Optional, Tuple
from flask import request
from flask import Flask, jsonify

# — Configuración de logging —
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("cleanupLostDeal")

# — Carga de configuración —
logger.info("Iniciando cleanupLostDeal Flask app")
conf_path = os.path.join(os.path.dirname(__file__), "scripts.conf")
logger.info(f"Leyendo configuración de: {conf_path}")
if not os.path.exists(conf_path):
    logger.error(f"No se encontró {conf_path}")
    exit(1)

cfg = ConfigParser()
cfg.read(conf_path)
DB_HOST = cfg.get("DB", "DB_HOST")
DB_PORT = cfg.get("DB", "DB_PORT", fallback="5432")
DB_USER = cfg.get("DB", "DB_USER")
DB_PASS = cfg.get("DB", "DB_PASS")
DB_NAME = cfg.get("DB", "DB_NAME")

SPECIAL_OWNER_ID = "1704f235-0a16-4b1d-8216-0b4fb7d79975"
TIMELINES_TOKEN = os.getenv(
    "TIMELINES_API_TOKEN",
    "c1f021f4-1bb5-430b-b7c9-b5690a0cfc57"
)
BASE_URL = "https://app.timelines.ai/integrations/api"

app = Flask(__name__)


def db_conn():
    logger.info(f"Conectando a la BD en {DB_HOST}:{DB_PORT} usuario={DB_USER}")
    conn = pg8000.connect(
        host=DB_HOST,
        port=int(DB_PORT),
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME
    )
    logger.info("Conexión a la BD establecida correctamente")
    return conn


def fetch_user_and_chat(conn, deal_id: str) -> Tuple[Optional[str], Optional[str]]:
    sql = (
        "SELECT d.user_assigned_id, l.chat_id "
        "FROM deals d "
        "JOIN leads l ON d.lead_id = l.id "
        "WHERE d.id = %s "
        "LIMIT 1"
    )
    cur = conn.cursor()
    cur.execute(sql, (deal_id,))
    row = cur.fetchone()
    cur.close()
    if not row:
        return None, None
    return row[0], row[1]


def patch_close_deal(chat_id: str) -> bool:
    url = f"{BASE_URL}/chats/{chat_id}"
    headers = {
        "Authorization": f"Bearer {TIMELINES_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {"closed": "true"}

    backoff = 5

    for attempt in range(1, 6):
        logger.info(f"[{chat_id}] PATCH intento {attempt} a {url}")
        try:
            resp = requests.patch(url, headers=headers, json=payload, timeout=10)
        except Exception as e:
            logger.error(f"[{chat_id}] Excepción en requests.patch: {e}")
            if attempt < 5:
                logger.info(f"[{chat_id}] Esperando {backoff}s antes de reintentar")
                time.sleep(backoff*5)
                backoff *= 2
                continue
            return False

        logger.info(f"[{chat_id}] HTTP {resp.status_code}")
        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After")
            wait = int(retry_after) if retry_after and retry_after.isdigit() else backoff
            logger.warning(f"[{chat_id}] 429 Too Many Requests, esperando {wait}s")
            time.sleep(wait*10)
            backoff *= 2
            continue

        if resp.ok:
            logger.info(f"[{chat_id}] Cierre exitoso")
            return True
        else:
            logger.error(f"[{chat_id}] Error HTTP {resp.status_code}: {resp.text}")
            return False

    return False


@app.route("/cleanup_all", methods=["POST"])
def cleanup_all():
    logger.info("Webhook /cleanup_all llamado: procesando todos los deals perdidos")
    try:
        conn = db_conn()
        cur = conn.cursor()
        cur.execute("SELECT id FROM deals WHERE status = 'Negocio perdido'")
        lost = [row[0] for row in cur.fetchall()]
        cur.close()
    except Exception as e:
        logger.exception(f"Error al leer deals perdidos: {e}")
        return jsonify({"error": "No pude leer los deals perdidos"}), 500

    logger.info(f"Total de deals perdidos recuperados: {len(lost)} → {lost}")

    results = []
    for idx, deal_id in enumerate(lost, start=1):
        logger.info(f"[{idx}/{len(lost)}] Iniciando procesamiento deal_id={deal_id}")

        user_assigned, chat_id = fetch_user_and_chat(conn, deal_id)
        logger.info(f"[{deal_id}] Datos: user_assigned={user_assigned}, chat_id={chat_id}")

        if chat_id:
            logger.info(f"[{deal_id}] Cerrando chat duplicado {chat_id}")
            ok = patch_close_deal(chat_id)
            results.append({"deal_id": deal_id, "chat_id": chat_id, "closed": ok})
        else:
            logger.info(f"[{deal_id}] Omitido (sin chat_id)")
            results.append({"deal_id": deal_id, "skipped": True})

    conn.close()
    logger.info("Todos los deals procesados, devolviendo resultados")
    return jsonify({"processed": results, "total": len(lost)}), 200


@app.route("/cleanup_duplicates", methods=["GET"])
def cleanup_duplicates():
    """
    Lee CSV de duplicados y cierra cada chat_id.
    """
    csv_path = os.path.join(os.path.dirname(__file__), 'duplicados.csv')
    if not os.path.exists(csv_path):
        logger.error(f"No se encontró CSV de duplicados en {csv_path}")
        return jsonify({"error": "CSV not found"}), 404

    results = []
    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            chat_id = row.get('chat_id') or row.get('Chat ID')
            if not chat_id:
                logger.warning("Fila sin chat_id, omitiendo")
                continue
            chat_id = chat_id.strip()
            logger.info(f"Cerrando chat duplicado {chat_id}")
            ok = patch_close_deal(chat_id)
            results.append({"chat_id": chat_id, "closed": ok})

    return jsonify({"duplicates_processed": results}), 200


@app.route("/cleanup", methods=["POST"])
def cleanup_deal():
    """
    Webhook para cerrar el chat de un único deal.
    Espera un JSON con al menos {"lead_id": "<uuid>"}.
    """
    payload = request.get_json(force=True)
    lead_id = payload.get("lead_id")
    if not lead_id:
        logger.error("No se recibió lead_id en el payload")
        return jsonify({"error": "Payload debe incluir lead_id"}), 400

    logger.info(f"Webhook /cleanup_deal llamado para lead_id={lead_id}")
    try:
        conn = db_conn()
        # Buscamos el deal asociado al lead
        cur = conn.cursor()
        cur.execute(
            "SELECT id FROM deals WHERE lead_id = %s LIMIT 1",
            (lead_id,)
        )
        row = cur.fetchone()
        cur.close()
        if not row:
            logger.warning(f"No se encontró deal para lead_id={lead_id}")
            return jsonify({"error": "Deal no encontrado"}), 404

        deal_id = row[0]
        logger.info(f"Deal encontrado: id={deal_id}, procediendo a fetch_user_and_chat")
        user_assigned, chat_id = fetch_user_and_chat(conn, deal_id)
        conn.close()

        if chat_id:
            logger.info(f"[deal {deal_id}] Cerrando chat {chat_id}")
            closed = patch_close_deal(chat_id)
            status = {"deal_id": deal_id, "chat_id": chat_id, "closed": closed}
        else:
            logger.info(f"[deal {deal_id}] Omitido (sin chat_id)")
            status = {"deal_id": deal_id, "skipped": True}

        return jsonify(status), 200

    except Exception as e:
        logger.exception(f"Error en cleanup_deal para lead_id={lead_id}: {e}")
        return jsonify({"error": "Error interno"}), 500
        
if __name__ == "__main__":
    # Ejecuta en 5024
    app.run(host="0.0.0.0", port=5026, debug=True)
