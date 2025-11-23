import os
import json
import logging
import configparser
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import psycopg2
from psycopg2.extras import RealDictCursor
from flask import Flask, request, jsonify
from flask_cors import CORS


# ============================================================
# CONFIGURACIÓN: scripts.conf + entorno (igual que tu estilo)
# ============================================================

DEFAULT_CONF_PATH = os.path.join(os.path.dirname(__file__), "scripts.conf")
CONF_PATH = os.environ.get("SCRIPTS_CONF_PATH", DEFAULT_CONF_PATH)

config = configparser.ConfigParser()
read_files = config.read(CONF_PATH)

if not read_files:
    raise RuntimeError(f"No se ha podido leer scripts.conf en: {CONF_PATH}")

if "DB" not in config:
    raise RuntimeError("No se ha encontrado la sección [DB] en scripts.conf")

DB_HOST = config.get("DB", "DB_HOST")
DB_PORT = config.getint("DB", "DB_PORT", fallback=5432)
DB_NAME = config.get("DB", "DB_NAME")
DB_USER = config.get("DB", "DB_USER")
DB_PASS = config.get("DB", "DB_PASS")

SECRET_KEY = os.environ.get("SECRET_KEY", "dev-secret-change-me")

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("audit-logs-humanizer")

app = Flask(__name__)
app.secret_key = SECRET_KEY

# 🔹 Habilitar CORS solo para /audit/*
CORS(
    app,
    resources={r"/audit/*": {"origins": "*"}},
    supports_credentials=True
)

TZ = ZoneInfo("Europe/Madrid")


def get_db_conn():
    logger.debug(
        "LOG: Abriendo conexión a BD %s@%s:%s/%s",
        DB_USER,
        DB_HOST,
        DB_PORT,
        DB_NAME,
    )
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        sslmode="require",
    )


# ============================================================
# DICCIONARIOS DE TRADUCCIÓN / NORMALIZACIÓN
# ============================================================

FIELD_LABELS = {
    "status": "Estado",
    "sub_status": "Subestado",
    "user_assigned_id": "Comercial asignado",
    "company_address_id": "Oficina",
    "expected_close_date": "Fecha prevista de cierre",
    "value": "Valor",
    "probability": "Probabilidad",
    "reason": "Motivo",
    "product_id": "Producto",
    "is_deleted": "Archivado",
    "name": "Nombre",
}

# Si quieres reglas específicas tipo "estado -> frase bonita"
SPECIAL_FIELDS = {"status", "user_assigned_id", "company_address_id", "is_deleted"}


# ============================================================
# LOOKUPS OPCIONALES (para no enseñar UUIDs)
# ============================================================

def fetch_profile_display(profile_id: str, conn, cache: dict):
    if not profile_id:
        return None
    if profile_id in cache:
        return cache[profile_id]

    sql = """
    SELECT id, email, first_name, last_name
    FROM profiles
    WHERE id = %s
      AND is_deleted = false
    LIMIT 1
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, (profile_id,))
        row = cur.fetchone()

    if not row:
        cache[profile_id] = profile_id
        return profile_id

    name = f"{row.get('first_name') or ''} {row.get('last_name') or ''}".strip()
    display = f"{name} <{row.get('email')}>" if name else (row.get("email") or profile_id)

    cache[profile_id] = display
    return display


def fetch_office_display(company_address_id: str, conn, cache: dict):
    if not company_address_id:
        return None
    if company_address_id in cache:
        return cache[company_address_id]

    sql = """
    SELECT id, alias, trade_name, city
    FROM company_addresses
    WHERE id = %s
      AND is_deleted = false
    LIMIT 1
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, (company_address_id,))
        row = cur.fetchone()

    if not row:
        cache[company_address_id] = company_address_id
        return company_address_id

    alias = row.get("alias") or row.get("trade_name") or "Oficina"
    city = row.get("city")
    display = f"{alias}{' - ' + city if city else ''}"

    cache[company_address_id] = display
    return display


# ============================================================
# LÓGICA CORE: diff + humanización
# ============================================================

def safe_json_load(s):
    if s is None:
        return None
    if isinstance(s, dict):
        return s
    try:
        return json.loads(s)
    except Exception:
        return None


def diff_dicts(old: dict, new: dict):
    """Devuelve lista de (field, old_val, new_val) que hayan cambiado."""
    if not old:
        old = {}
    if not new:
        new = {}

    keys = set(old.keys()) | set(new.keys())
    changes = []
    for k in keys:
        if old.get(k) != new.get(k):
            changes.append((k, old.get(k), new.get(k)))
    return changes


def to_madrid_iso(ts):
    if not ts:
        return None
    if isinstance(ts, str):
        try:
            ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except Exception:
            return ts

    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)

    return ts.astimezone(TZ).strftime("%Y-%m-%d %H:%M:%S")


def humanize_audit_row(row, conn, profile_cache, office_cache):
    op = row["operation_type"]
    changed_at = to_madrid_iso(row.get("changed_at"))

    old_vals = safe_json_load(row.get("old_values"))
    new_vals = safe_json_load(row.get("new_values"))

    deal_name = None
    if new_vals and isinstance(new_vals, dict):
        deal_name = new_vals.get("name")
    if not deal_name and old_vals and isinstance(old_vals, dict):
        deal_name = old_vals.get("name")

    # 1) INSERT
    if op == "INSERT":
        title = "Creación del Deal"
        if deal_name:
            title += f": {deal_name}"
        return {
            "type": "INSERT",
            "at": changed_at,
            "title": title,
            "detail": f"Se creó el deal.{'' if not deal_name else ' Nombre: ' + deal_name}"
        }

    # 2) DELETE (si existe)
    if op == "DELETE":
        title = "Eliminación del Deal"
        if deal_name:
            title += f": {deal_name}"
        return {
            "type": "DELETE",
            "at": changed_at,
            "title": title,
            "detail": "Se eliminó el deal."
        }

    # 3) UPDATE
    if op == "UPDATE":
        changes = diff_dicts(old_vals, new_vals)
        if not changes:
            return {
                "type": "UPDATE",
                "at": changed_at,
                "title": "Actualización sin cambios relevantes",
                "detail": "No se detectaron cambios entre old_values y new_values."
            }

        human_changes = []

        for field, old_v, new_v in changes:
            label = FIELD_LABELS.get(field, field)

            # === reglas especiales ===
            if field == "status":
                human_changes.append(
                    f"Cambio de estado: '{old_v}' → '{new_v}'"
                )
                continue

            if field == "user_assigned_id":
                old_disp = fetch_profile_display(str(old_v) if old_v else None, conn, profile_cache)
                new_disp = fetch_profile_display(str(new_v) if new_v else None, conn, profile_cache)
                human_changes.append(
                    f"{label}: {old_disp or '—'} → {new_disp or '—'}"
                )
                continue

            if field == "company_address_id":
                old_disp = fetch_office_display(str(old_v) if old_v else None, conn, office_cache)
                new_disp = fetch_office_display(str(new_v) if new_v else None, conn, office_cache)
                human_changes.append(
                    f"{label}: {old_disp or '—'} → {new_disp or '—'}"
                )
                continue

            if field == "is_deleted":
                if new_v is True:
                    human_changes.append("Deal archivado/eliminado lógicamente.")
                else:
                    human_changes.append("Deal reactivado (is_deleted=false).")
                continue

            # === normalización genérica ===
            if isinstance(old_v, str) and "T" in old_v and old_v.endswith(("Z", "+00:00")):
                old_v = to_madrid_iso(old_v)
            if isinstance(new_v, str) and "T" in new_v and new_v.endswith(("Z", "+00:00")):
                new_v = to_madrid_iso(new_v)

            human_changes.append(f"{label}: {old_v} → {new_v}")

        title = "Actualización del Deal"
        if deal_name:
            title += f": {deal_name}"

        return {
            "type": "UPDATE",
            "at": changed_at,
            "title": title,
            "detail": human_changes,
        }

    # fallback
    return {
        "type": op or "UNKNOWN",
        "at": changed_at,
        "title": f"Operación {op}",
        "detail": "Sin humanización específica."
    }


def fetch_deal_audit_logs(deal_id: str):
    sql = """
    SELECT
        id, table_name, operation_type, record_id,
        old_values, new_values, changed_by, changed_at
    FROM audit_logs
    WHERE is_deleted = false
      AND table_name = 'deals'
      AND record_id = %s::uuid
    ORDER BY changed_at ASC
    """
    conn = get_db_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            logger.debug("LOG: Ejecutando SQL audit logs para deal_id=%s", deal_id)
            cur.execute(sql, (deal_id,))
            rows = cur.fetchall()

        logger.info("LOG: encontrados %d audit logs para deal_id=%s", len(rows), deal_id)

        profile_cache = {}
        office_cache = {}

        human = [
            humanize_audit_row(r, conn, profile_cache, office_cache)
            for r in rows
        ]

        return rows, human
    finally:
        conn.close()


# ============================================================
# ENDPOINTS
# ============================================================

@app.route("/audit/deal/<deal_id>", methods=["GET"])
def audit_for_deal_get(deal_id):
    try:
        raw, human = fetch_deal_audit_logs(deal_id)
        return jsonify({
            "deal_id": deal_id,
            "count": len(human),
            "human_logs": human,
        }), 200
    except Exception as e:
        logger.exception("LOG: Error en /audit/deal/<deal_id>: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/audit/deal", methods=["POST"])
def audit_for_deal_post():
    try:
        raw_body = request.data.decode("utf-8", errors="replace")
        logger.info("LOG: Petición /audit/deal RAW body: %s", raw_body)

        data = request.get_json(silent=True) or {}
        logger.info("LOG: JSON parseado en /audit/deal: %s", data)

        deal_id = data.get("deal_id")
        if not deal_id:
            return jsonify({"error": "deal_id es requerido"}), 400

        raw, human = fetch_deal_audit_logs(deal_id)

        return jsonify({
            "deal_id": deal_id,
            "count": len(human),
            "human_logs": human,
        }), 200
    except Exception as e:
        logger.exception("LOG: Error en /audit/deal POST: %s", e)
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    HTTP_PORT = int(os.environ.get("HTTP_PORT", "5110"))
    logger.info("Iniciando audit-logs-humanizer en puerto %d", HTTP_PORT)
    app.run(host="0.0.0.0", port=HTTP_PORT, debug=False, use_reloader=False)
