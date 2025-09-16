import os
import sys
import json
import pg8000
import configparser
from datetime import datetime
from flask import Flask, request, jsonify
import requests
import logging
from logging.handlers import RotatingFileHandler

# ========= LOGGING con rotación =========
def setup_logging(
    logfile="assign_leads.log",
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    max_bytes=10_000_000,
    backup_count=5,
):
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, level, logging.INFO))

    fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    file_handler = RotatingFileHandler(logfile, maxBytes=max_bytes, backupCount=backup_count)
    file_handler.setFormatter(fmt)
    file_handler.setLevel(getattr(logging, level, logging.INFO))

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(fmt)
    stream_handler.setLevel(getattr(logging, level, logging.INFO))

    # reemplaza handlers previos (evita duplicados)
    logger.handlers = [file_handler, stream_handler]

    # baja el ruido del server
    logging.getLogger("werkzeug").setLevel(
        logging.INFO if os.getenv("DEV", "0") == "1" else logging.WARNING
    )

setup_logging()

# ========= CONFIG =========
config = configparser.ConfigParser()
config.read('scripts.conf')

MJ_ID = "4b4e0707-4105-4285-a592-feb504407363"
GABRIELA_ID = "879556e9-cf8d-49a5-b1c1-09d3ac19afa5"
MARIAJOSE_PCT = 0.5  # 50%

TIMELINES_TOKEN = "c1f021f4-1bb5-430b-b7c9-b5690a0cfc57"
TIMELINES_BASE = "https://app.timelines.ai/integrations/api"

# ========= DB =========
def get_db_connection():
    db = config['DB']
    logging.info(f"Conectando a la base de datos {db['DB_HOST']}:{db['DB_PORT']} como {db['DB_USER']}...")
    conn = pg8000.connect(
        user=db['DB_USER'],
        password=db['DB_PASS'],
        host=db['DB_HOST'],
        port=int(db.get('DB_PORT', '5432')),
        database=db['DB_NAME']
    )
    logging.debug("Conexión a BD establecida.")
    return conn

# ========= HELPERS =========
def getOwnerEmail(owner_id: str):
    query = "SELECT email FROM profiles WHERE id = %s LIMIT 1"
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute(query, (owner_id,))
            result = cursor.fetchone()
        conn.close()
        if result and result[0]:
            logging.debug(f"Email encontrado para owner_id={owner_id}: {result[0]}")
            return result[0]
        logging.warning(f"No owner found with id: {owner_id}")
        return None
    except Exception as e:
        logging.exception(f"Error en getOwnerEmail({owner_id}): {e}")
        return None

def update_chat_responsible(chat_id: str, owner_id: str) -> bool:
    owner_email = getOwnerEmail(owner_id)
    if not owner_email:
        logging.error(f"No se encontró email para owner_id={owner_id}")
        return False

    url = f"{TIMELINES_BASE}/chats/{chat_id}"
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {TIMELINES_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {"responsible": owner_email}
    logging.debug(f"Timelines PATCH {url} payload={payload}")
    try:
        resp = requests.patch(url, headers=headers, json=payload, timeout=10)
        resp.raise_for_status()
        logging.info(f"Responsable actualizado en Timelines para chat_id={chat_id}, responsible={owner_email}")
        return True
    except Exception as e:
        logging.exception(f"Error actualizando responsable en Timelines chat_id={chat_id}: {e}")
        return False

def get_chat_id_from_lead(lead_id: str):
    """
    1º intenta leads.chat_id, 2º busca en tabla chats por lead_id (fallback)
    """
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT chat_id FROM leads WHERE id = %s LIMIT 1", (lead_id,))
            row = cur.fetchone()
            if row and row[0]:
                chat_id = row[0]
            else:
                chat_id = None

            if not chat_id:
                # fallback a la tabla de chats si existe
                cur.execute("""
                    SELECT c.id
                      FROM chats c
                     WHERE c.lead_id = %s
                       AND c.is_deleted = FALSE
                     ORDER BY c.created_at DESC
                     LIMIT 1
                """, (lead_id,))
                r2 = cur.fetchone()
                if r2 and r2[0]:
                    chat_id = r2[0]

        conn.close()
        if chat_id:
            logging.debug(f"chat_id para lead_id={lead_id}: {chat_id}")
            return chat_id
        logging.warning(f"No chat_id found para lead_id: {lead_id}")
        return None
    except Exception as e:
        logging.exception(f"Error obteniendo chat_id para lead_id {lead_id}: {e}")
        return None

# ========= LÓGICA DE ASIGNACIÓN =========
def get_current_distribution(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT user_assigned_id, COUNT(*) 
              FROM deals 
             WHERE is_deleted = FALSE 
               AND status != 'Negocio perdido'
               AND user_assigned_id IN (%s, %s)
             GROUP BY user_assigned_id
            """,
            (MJ_ID, GABRIELA_ID)
        )
        rows = cur.fetchall()
        logging.debug(f"Raw rows distribución: {rows}")
        counts = {MJ_ID: 0, GABRIELA_ID: 0}
        for uid, cnt in rows:
            counts[str(uid)] = cnt
        logging.info(f"Distribución actual (solo vivos): MariaJose={counts[MJ_ID]}, Gabriela={counts[GABRIELA_ID]}")
        return counts

def get_next_owner(conn):
    counts = get_current_distribution(conn)
    total = counts[MJ_ID] + counts[GABRIELA_ID]
    logging.info(f"Total deals asignados: {total}")
    if total == 0:
        logging.info("No hay deals asignados aún. El primero va para MariaJose.")
        return MJ_ID
    pct_mj = counts[MJ_ID] / total
    logging.info(f"Porcentaje actual MariaJose: {pct_mj*100:.2f}% (objetivo: {MARIAJOSE_PCT*100:.0f}%)")
    if pct_mj < MARIAJOSE_PCT:
        logging.info("Toca asignar a MariaJose.")
        return MJ_ID
    else:
        logging.info("Toca asignar a Gabriela.")
        return GABRIELA_ID

def assign_deal(lead_id: str, user_id: str, conn) -> bool:
    """
    Asignación robusta: advisory lock por lead_id + SELECT ... FOR UPDATE + idempotencia.
    """
    try:
        with conn.cursor() as cur:
            # transacción explícita
            cur.execute("BEGIN")

            # 1) lock por lead_id => evita carreras en el mismo lead
            cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (lead_id,))

            # 2) bloquea el deal objetivo
            cur.execute(
                """
                SELECT id, user_assigned_id
                  FROM deals
                 WHERE lead_id = %s
                   AND is_deleted = FALSE
                 ORDER BY created_at DESC
                 LIMIT 1
                 FOR UPDATE
                """,
                (lead_id,)
            )
            row = cur.fetchone()
            if not row:
                logging.error(f"No existe deal para lead_id={lead_id}")
                cur.execute("ROLLBACK")
                return False

            deal_id, current_user = row

            # 3) idempotencia
            if current_user and str(current_user) == str(user_id):
                logging.info(f"Deal {deal_id} ya asignado a {user_id} (idempotente).")
                cur.execute("COMMIT")
                return True

            # 4) actualizar asignación
            logging.info(f"Asignando deal {deal_id} a user {user_id}...")
            cur.execute(
                """
                UPDATE deals
                   SET user_assigned_id = %s,
                       updated_by = %s,
                       updated_at = NOW()
                 WHERE id = %s
                   AND is_deleted = FALSE
                """,
                (user_id, user_id, deal_id)
            )

            cur.execute("COMMIT")
            logging.info(f"✅ Deal {deal_id} asignado a {user_id} (lead_id={lead_id})")
            return True

    except Exception as e:
        logging.exception(f"❌ Error asignando deal (lead_id={lead_id}): {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return False

# ========= FLASK =========
app = Flask(__name__)

@app.post("/")
def root_post():
    # compat: si te llaman a "/" redirige al handler
    return webhook_assign_lead()

@app.get("/health")
def health():
    return jsonify({"status": "ok"}), 200

@app.post("/assign_lead")
def webhook_assign_lead():
    logging.info("---- LLEGÓ UNA LLAMADA A /assign_lead ----")

    # 1) lee el raw body (por si vienen dos objetos pegados)
    raw = request.data.decode("utf-8", errors="ignore")
    logging.debug(f"Raw payload: {raw!r}")

    # 2) parsea SOLO el primer objeto JSON de forma segura (sin cortar a ojo)
    try:
        decoder = json.JSONDecoder()
        data, _end = decoder.raw_decode(raw.lstrip())
    except Exception as e:
        logging.exception(f"Error parseando JSON: {e}")
        return jsonify({"status": "error", "error": "invalid JSON"}), 400

    logging.debug(f"Payload parseado: {data}")

    # 3) datos clave
    lead_id = data.get("lead_id")
    if not lead_id:
        logging.error("Falta parámetro lead_id en el JSON recibido")
        return jsonif


if __name__ == "__main__":
    logging.info("Iniciando servicio Flask en 0.0.0.0:5020 /assign_lead")
    app.run(host="0.0.0.0", port=5020, debug=False)