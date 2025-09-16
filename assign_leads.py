import os
import sys
import json
import pg8000
import configparser
from flask import Flask, request, jsonify
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

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    fh = RotatingFileHandler(logfile, maxBytes=max_bytes, backupCount=backup_count)
    fh.setFormatter(fmt); fh.setLevel(getattr(logging, level, logging.INFO))

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt); sh.setLevel(getattr(logging, level, logging.INFO))

    logger.handlers = [fh, sh]
    logging.getLogger("werkzeug").setLevel(
        logging.INFO if os.getenv("DEV", "0") == "1" else logging.WARNING
    )

setup_logging()

# ========= CONFIG / DB =========
config = configparser.ConfigParser()
config.read("scripts.conf")

def get_db_connection():
    db = config["DB"]
    logging.info(f"[DB] Conectando a {db['DB_HOST']}:{db.get('DB_PORT','5432')} DB={db['DB_NAME']} USER={db['DB_USER']}")
    return pg8000.connect(
        user=db["DB_USER"],
        password=db["DB_PASS"],
        host=db["DB_HOST"],
        port=int(db.get("DB_PORT", "5432")),
        database=db["DB_NAME"],
    )

# ========= HELPERS =========
def safe_json_load(x):
    if isinstance(x, (dict, list)):
        return x
    try:
        return json.loads(x)
    except Exception:
        return None

def get_company_id_for_lead(lead_id: str, conn):
    """Obtiene company_id directamente desde deals usando lead_id."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT company_id
            FROM deals
            WHERE lead_id = %s
            LIMIT 1
        """, (lead_id,))
        row = cur.fetchone()
    if row and row[0]:
        cid = str(row[0])
        logging.info(f"[lead_id={lead_id}] company_id via deals => {cid}")
        return cid
    logging.warning(f"[lead_id={lead_id}] No se encontró company_id en deals")
    return None

def get_candidates_from_conf(company_id: str, category_id: str, conn):
    """Lee conf_user_assignees para company+categoría → [{user_id, weight}]"""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT user_id::text, COALESCE(weight, 100)::numeric
            FROM conf_user_assignees
            WHERE company_id = %s
              AND object_reference_type = 'categories'
              AND object_reference_id = %s
              AND object_assigned_to = 'deals'
              AND is_deleted = false
            ORDER BY user_id
        """, (company_id, category_id))
        rows = cur.fetchall()
    return [{"user_id": r[0], "weight": float(r[1])} for r in rows]

def get_live_distribution(company_id: str, category_id: str, candidate_ids, conn):
    """Distribución viva por compañía+categoría entre candidatos."""
    if not candidate_ids:
        return {}
    placeholders = ",".join(["%s"] * len(candidate_ids))
    sql = f"""
        SELECT d.user_assigned_id::text, COUNT(*) AS n
        FROM deals d
        JOIN leads l ON l.id = d.lead_id
        WHERE d.company_id = %s
          AND l.category_id = %s
          AND d.is_deleted = false
          AND d.status != 'Negocio perdido'
          AND d.user_assigned_id IN ({placeholders})
        GROUP BY d.user_assigned_id
    """
    params = [company_id, category_id] + candidate_ids
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    counts = {cid: 0 for cid in candidate_ids}
    for uid, n in rows:
        counts[str(uid)] = int(n)
    return counts

def choose_owner_by_weight_quota(candidates, counts):
    """
    Regla: normaliza pesos → objetivos por usuario; elige mayor déficit (actual - objetivo) más negativo.
    """
    # Normaliza pesos
    total_w = sum(max(c["weight"], 0.0) for c in candidates) or 1.0
    for c in candidates:
        c["pct_obj"] = (max(c["weight"], 0.0) / total_w)

    total_assigned = sum(counts.values())
    if total_assigned == 0:
        # si no hay historial, primer candidato
        return candidates[0]["user_id"], "quota_first"

    best_user, best_delta = None, None
    for c in candidates:
        uid = c["user_id"]
        pct_actual = (counts.get(uid, 0) / total_assigned) if total_assigned > 0 else 0.0
        delta = pct_actual - c["pct_obj"]  # negativo = por debajo de su cuota
        if best_delta is None or delta < best_delta:
            best_delta = delta
            best_user = uid
    return best_user, "quota_conf_user_assignees"

def assign_deal_locked(lead_id: str, target_user_id: str, conn):
    """Bloqueo por lead + idempotencia."""
    with conn.cursor() as cur:
        cur.execute("BEGIN")
        cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (lead_id,))
        cur.execute("""
            SELECT id, user_assigned_id::text
            FROM deals
            WHERE lead_id = %s AND is_deleted = false
            ORDER BY created_at DESC
            LIMIT 1
            FOR UPDATE
        """, (lead_id,))
        row = cur.fetchone()
        if not row:
            cur.execute("ROLLBACK")
            return None, "no_deal"

        deal_id, current_user = row[0], row[1]
        if current_user and str(current_user) == str(target_user_id):
            cur.execute("COMMIT")
            return str(deal_id), "idempotent"

        cur.execute("""
            UPDATE deals
            SET user_assigned_id = %s,
                updated_by = %s,
                updated_at = now()
            WHERE id = %s AND is_deleted = false
        """, (target_user_id, target_user_id, deal_id))
        cur.execute("COMMIT")
        return str(deal_id), "updated"

# ========= FLASK =========
app = Flask(__name__)

@app.get("/health")
def health():
    return jsonify({"status": "ok"}), 200

@app.post("/")
def root_post():
    return webhook_assign_lead()

@app.post("/assign_lead")
def webhook_assign_lead():
    raw = request.data.decode("utf-8", errors="ignore")
    logging.info(f"[WEBHOOK] POST /assign_lead len={len(raw)} body={raw}")

    try:
        data = json.loads(raw)
    except Exception as e:
        logging.exception("[WEBHOOK] JSON inválido")
        return jsonify({"status":"error","error":"invalid_json"}), 400

    lead_id   = (data or {}).get("lead_id")
    categoria = (data or {}).get("categoria")
    propietario_hint = (data or {}).get("propietario")

    if not lead_id:
        logging.error("[WEBHOOK] Falta lead_id")
        return jsonify({"status":"error","error":"missing_lead_id"}), 400
    if not categoria:
        logging.error(f"[lead_id={lead_id}] Falta categoria")
        return jsonify({"status":"error","error":"missing_categoria"}), 400

    conn = None
    try:
        conn = get_db_connection()

        # 1) company_id
        company_id = get_company_id_for_lead(lead_id, conn)
        if not company_id:
            logging.error(f"[lead_id={lead_id}] No se pudo resolver company_id")
            return jsonify({"status":"error","error":"company_not_found"}), 404

        logging.info(f"[lead_id={lead_id}] company_id={company_id} categoria={categoria}")

        # 2) candidatos desde conf_user_assignees
        candidates = get_candidates_from_conf(company_id, categoria, conn)
        logging.info(f"[lead_id={lead_id} company_id={company_id}] candidatos={candidates}")

        strategy = None
        owner_id = None

        if candidates:
            if len(candidates) == 1:
                owner_id = candidates[0]["user_id"]
                strategy = "single_conf_user_assignees"
            else:
                candidate_ids = [c["user_id"] for c in candidates]
                counts = get_live_distribution(company_id, categoria, candidate_ids, conn)
                logging.info(f"[company_id={company_id} categoria={categoria}] dist={counts}")
                owner_id, strategy = choose_owner_by_weight_quota(candidates, counts)
        else:
            # Fallbacks
            if propietario_hint:
                owner_id = propietario_hint
                strategy = "fallback_propietario"
                logging.warning(f"[lead_id={lead_id}] Sin conf_user_assignees. Usando propietario={owner_id}")
            else:
                # least load por compañía (sin filtrar categoría)
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT user_assigned_id::text, COUNT(*) AS n
                        FROM deals
                        WHERE company_id = %s
                          AND is_deleted = false
                          AND status != 'Negocio perdido'
                          AND user_assigned_id IS NOT NULL
                        GROUP BY user_assigned_id
                        ORDER BY n ASC NULLS FIRST
                        LIMIT 1
                    """, (company_id,))
                    r = cur.fetchone()
                if r and r[0]:
                    owner_id = r[0]
                    strategy = "fallback_least_load_company"
                    logging.warning(f"[lead_id={lead_id}] Sin conf ni propietario. least_load => {owner_id}")
                else:
                    logging.error(f"[lead_id={lead_id}] No hay candidatos")
                    return jsonify({"status":"error","error":"no_candidates"}), 409

        # 3) asignar con bloqueo
        deal_id, result = assign_deal_locked(lead_id, owner_id, conn)
        logging.info(f"[lead_id={lead_id}] asignación result={result} deal_id={deal_id} owner_id={owner_id} strategy={strategy}")

        if not deal_id:
            return jsonify({"status":"error","error":result}), 500

        # Si fue idempotente, dejamos constancia
        final_strategy = "idempotent" if result == "idempotent" else strategy

        return jsonify({
            "status": "ok",
            "lead_id": lead_id,
            "company_id": company_id,
            "categoria": categoria,
            "deal_id": deal_id,
            "assigned_user_id": owner_id,
            "strategy": final_strategy
        }), 200

    except Exception as e:
        logging.exception(f"[lead_id={lead_id}] internal_error")
        return jsonify({"status":"error","error":"internal_error"}), 500
    finally:
        try:
            if conn: conn.close()
        except Exception:
            pass

if __name__ == "__main__":
    logging.info("Iniciando servicio Flask en 0.0.0.0:5020 /assign_lead")
    app.run(host="0.0.0.0", port=5020, debug=False)
        logging.info(f"[lead_id={lead_id}] asignación result={result} deal_id={deal_id} owner_id={owner_id} strategy={strategy}")

        if not deal_id:
            return jsonify({"status":"error","error":result}), 500

        # Si fue idempotente, dejamos constancia
        final_strategy = "idempotent" if result == "idempotent" else strategy

        return jsonify({
            "status": "ok",
            "lead_id": lead_id,
            "company_id": company_id,
            "categoria": categoria,
            "deal_id": deal_id,
            "assigned_user_id": owner_id,
            "strategy": final_strategy
        }), 200

    except Exception as e:
        logging.exception(f"[lead_id={lead_id}] internal_error")
        return jsonify({"status":"error","error":"internal_error"}), 500
    finally:
        try:
            if conn: conn.close()
        except Exception:
            pass

if __name__ == "__main__":
    logging.info("Iniciando servicio Flask en 0.0.0.0:5020 /assign_lead")
    app.run(host="0.0.0.0", port=5020, debug=False)
