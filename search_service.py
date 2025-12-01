#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import os
import configparser
from pathlib import Path
import re
import pg8000
from flask import Flask, request, jsonify
from flask_cors import CORS

# ----------------------------------------------------------------------------
# Flask + logging
# ----------------------------------------------------------------------------
app = Flask(__name__)
app.logger.setLevel(logging.DEBUG)
logging.getLogger("werkzeug").setLevel(logging.INFO)

# ----------------------------------------------------------------------------
# CORS Configuration
# ----------------------------------------------------------------------------
ALLOWED_ORIGINS = {
    "https://portal.eliminamostudeuda.com",
    "https://portal.solvify.es",
    "http://localhost:3000",
    "http://*.localhost:3000",
        "http://eliminamostudeuda.localhost:3000",
}


app.logger.info(f"🔧 CORS Config: ALLOWED_ORIGINS={ALLOWED_ORIGINS}")

def is_origin_allowed(origin):
    """Verifica si el origin está en la lista de permitidos."""
    if not origin:
        app.logger.debug("🔎 CORS check: no Origin header present -> denied")
        return False
    origin = origin.strip()
    allowed = origin in ALLOWED_ORIGINS
    app.logger.debug(f"🔎 CORS check origin={origin} allowed={allowed}")
    return allowed

@app.before_request
def handle_cors_preflight():
    origin = request.headers.get("Origin")
    allowed = origin in ALLOWED_ORIGINS if origin else False
    app.logger.debug(f"🔎 CORS check origin={origin} allowed={allowed}")

    # Preflight de CORS
    if request.method == "OPTIONS":
        if not allowed:
            # Opcional: puedes devolver 204 sin CORS para no exponer nada
            resp = make_response("", 204)
            return resp

        resp = make_response("", 204)
        resp.headers["Access-Control-Allow-Origin"] = origin
        resp.headers["Vary"] = "Origin"
        resp.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
        resp.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
        resp.headers["Access-Control-Allow-Credentials"] = "true"
        resp.headers["Access-Control-Max-Age"] = "86400"
        return resp

    # Para peticiones normales seguimos; los headers se añaden en after_request
    request._cors_allowed = allowed


@app.after_request
def add_cors_headers(response):
    origin = request.headers.get("Origin")
    if getattr(request, "_cors_allowed", False) and origin:
        response.headers["Access-Control-Allow-Origin"] = origin
        response.headers["Vary"] = "Origin"
        response.headers["Access-Control-Allow-Credentials"] = "true"
    return response

# ----------------------------------------------------------------------------
# Carga de configuración desde scripts.conf (igual que otros microservicios)
# ----------------------------------------------------------------------------
config_supabase = configparser.ConfigParser()
SCRIPTS_CONF_PATH = "scripts.conf"

if os.path.exists(SCRIPTS_CONF_PATH):
    try:
        config_supabase.read(SCRIPTS_CONF_PATH)
        app.logger.info("✅ scripts.conf cargado en search_service")
        app.logger.debug(f"Secciones: {config_supabase.sections()}")
    except Exception as e:
        app.logger.error(f"❌ Error cargando scripts.conf: {e}", exc_info=True)
else:
    app.logger.warning("⚠️ scripts.conf no encontrado. Revisa ruta/volumen.")

# ----------------------------------------------------------------------------
# Conexión a BD (usa sección [DB], igual que en task_engine_webhook)
# ----------------------------------------------------------------------------
def get_db_connection():
    """
    Devuelve una conexión pg8000 usando la sección [DB] de scripts.conf
    """
    try:
        if not config_supabase.has_section("DB"):
            raise RuntimeError("No se encontró la sección [DB] en scripts.conf")

        db_host = config_supabase.get("DB", "DB_HOST")
        db_port = config_supabase.getint("DB", "DB_PORT", fallback=5432)
        db_name = config_supabase.get("DB", "DB_NAME")
        db_user = config_supabase.get("DB", "DB_USER")
        db_pass = config_supabase.get("DB", "DB_PASS")
        search_path = config_supabase.get("DB", "DB_SEARCH_PATH", fallback="public")

        conn = pg8000.connect(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_pass,
            application_name="search_service",
        )
        with conn.cursor() as cur:
            cur.execute(f"SET search_path TO {search_path}")
        return conn
    except Exception as e:
        app.logger.error(f"❌ Error creando conexión a BD: {e}", exc_info=True)
        raise

# ----------------------------------------------------------------------------
# Lógica de búsqueda
#   - Siempre filtramos por company_id
#   - Buscamos en deals + leads + profiles
# ----------------------------------------------------------------------------

def run_search(company_id: str, query: str):
    """
    Ejecuta una búsqueda global para una company_id y un término q.
    Devuelve:
      - results: lista de entidades (deals, leads, profiles)
      - messages: mensajes asociados al teléfono buscado (si lo hay)
      - tasks: tareas asociadas a deals de ese teléfono (si las hay)
    """

    q = (query or "").strip()
    if not q:
        return [], [], []

    # Construimos patrón de búsqueda en Python
    pattern = f"%{q}%"
    digits = re.sub(r"\D", "", q or "")

    # ------------------------------------------------------------
    # 1) BÚSQUEDA DE ENTIDADES (deals, leads, profiles)
    # ------------------------------------------------------------
    search_sql = """
    WITH search AS (
        SELECT
            %s::uuid AS company_id,
            %s::text AS pattern,
            %s::text AS digits
    ),
    raw_results AS (
        -- 1) DEALS
        SELECT
            'deal'::text AS entity_type,
            d.id         AS entity_id,
            d.name       AS title,
            (l.first_name || ' ' || l.last_name) AS subtitle,
            l.phone      AS phone,
            l.email      AS email,
            json_build_object(
                'status', d.status,
                'sub_status', d.sub_status
            ) AS extra,
            '/admin/negocios/' || d.id::text || '/lead/' || l.id::text AS link
        FROM public.deals d
        JOIN public.leads l
        ON l.id = d.lead_id
        JOIN search s ON TRUE
        WHERE
            d.company_id = s.company_id
            AND d.is_deleted = FALSE
            AND l.is_deleted = FALSE
            AND (
                d.name ILIKE s.pattern
                OR (l.first_name || ' ' || l.last_name) ILIKE s.pattern
                OR l.email ILIKE s.pattern
                OR (
                    s.digits <> ''
                    AND regexp_replace(COALESCE(l.phone, ''), '\\D', '', 'g') <> ''
                    AND regexp_replace(COALESCE(l.phone, ''), '\\D', '', 'g')
                        LIKE s.digits || '%'
                )
            )


        UNION ALL

        -- 2) LEADS (asociados a esa company vía deals)
        SELECT
            'lead'::text AS entity_type,
            l.id         AS entity_id,
            (l.first_name || ' ' || l.last_name) AS title,
            l.email      AS subtitle,
            l.phone      AS phone,
            l.email      AS email,
            json_build_object(
                'channel', l.channel
            ) AS extra,
            '/admin/negocios/' || d.id::text || '/lead/' || l.id::text AS link
        FROM public.leads l
        JOIN public.deals d
          ON d.lead_id = l.id
        JOIN search s ON TRUE
        WHERE
            d.company_id = s.company_id
            AND d.is_deleted = FALSE
            AND l.is_deleted = FALSE
            AND (
                (l.first_name || ' ' || l.last_name) ILIKE s.pattern
                OR l.email ILIKE s.pattern
                OR (
                    s.digits <> ''
                    AND regexp_replace(COALESCE(l.phone, ''), '\\D', '', 'g') <> ''
                    AND regexp_replace(COALESCE(l.phone, ''), '\\D', '', 'g')
                        LIKE s.digits || '%'
                )
            )

        UNION ALL

        -- 3) PROFILES (usuarios de esa compañía)
        SELECT
            'profile'::text AS entity_type,
            p.id           AS entity_id,
            (p.first_name || ' ' || p.last_name) AS title,
            p.email        AS subtitle,
            p.phone        AS phone,
            p.email        AS email,
            json_build_object(
                'role_id', p.role_id
            ) AS extra,
            '/profiles/' || p.id::text AS link
        FROM public.profiles p
        JOIN public.profile_comp_addresses pca
          ON pca.user_id = p.id
         AND pca.is_deleted = FALSE
        JOIN public.company_addresses ca
          ON ca.id = pca.company_address_id
         AND ca.is_deleted = FALSE
        JOIN search s ON TRUE
        WHERE
            ca.company_id = s.company_id
            AND p.is_deleted = FALSE
            AND (
                (p.first_name || ' ' || p.last_name) ILIKE s.pattern
                OR p.email ILIKE s.pattern
                OR (
                    s.digits <> ''
                    AND regexp_replace(COALESCE(p.phone, ''), '\\D', '', 'g') <> ''
                    AND regexp_replace(COALESCE(p.phone, ''), '\\D', '', 'g')
                        LIKE s.digits || '%'
                )
            )
    )
    SELECT DISTINCT ON (entity_type, entity_id)
        entity_type,
        entity_id,
        title,
        subtitle,
        phone,
        email,
        extra,
        link
    FROM raw_results
    ORDER BY entity_type, entity_id, title
    LIMIT 50;
    """

    search_params = (
        company_id,  # search.company_id
        pattern,     # search.pattern
        digits,      # search.digits
    )

    conn = get_db_connection()
    results = []
    messages = []
    tasks = []

    try:
        cur = conn.cursor()

        # ------ ENTIDADES ------
        cur.execute(search_sql, search_params)
        rows = cur.fetchall()

        for row in rows:
            (
                entity_type,
                entity_id,
                title,
                subtitle,
                phone,
                email,
                extra,
                link,
            ) = row

            results.append({
                "entity_type": entity_type,
                "entity_id": str(entity_id),
                "title": title,
                "subtitle": subtitle,
                "phone": phone,
                "email": email,
                "extra": extra,
                "link": link,
            })

        # Si no hay dígitos en la búsqueda, no tiene sentido buscar mensajes/tareas por teléfono
        if digits:
            # ------------------------------------------------------------
            # 2) MENSAJES por teléfono (external_messages)
            # ------------------------------------------------------------
            msg_sql = """
            SELECT
                em.id,
                em.message,
                em.sender_phone,
                em.responsible_email,
                em.last_message_uid,
                em.last_message_timestamp,
                em.from_me,
                em.status,
                em.chat_url,
                em.chat_id,
                em.assigned_to_id
            FROM public.external_messages em
            WHERE
                em.is_deleted = FALSE
                AND em.company_id = %s
                AND %s <> ''
                AND regexp_replace(COALESCE(em.sender_phone, ''), '\\D', '', 'g')
                    LIKE %s || '%%'
            ORDER BY em.last_message_timestamp DESC
            LIMIT 50;
            """
            msg_params = (company_id, digits, digits)
            cur.execute(msg_sql, msg_params)
            msg_rows = cur.fetchall()

            for r in msg_rows:
                (
                    mid,
                    message,
                    sender_phone,
                    responsible_email,
                    last_message_uid,
                    last_message_timestamp,
                    from_me,
                    status,
                    chat_url,
                    chat_id,
                    assigned_to_id,
                ) = r

                messages.append({
                    "id": str(mid),
                    "message": message,
                    "sender_phone": sender_phone,
                    "responsible_email": responsible_email,
                    "last_message_uid": last_message_uid,
                    "last_message_timestamp": last_message_timestamp.isoformat() if last_message_timestamp else None,
                    "from_me": from_me,
                    "status": status,
                    "chat_url": chat_url,
                    "chat_id": chat_id,
                    "assigned_to_id": str(assigned_to_id) if assigned_to_id else None,
                })

            # ------------------------------------------------------------
            # 3) TAREAS ligadas a deals de ese teléfono
            #    annotation_tasks -> annotations -> deals -> leads(phone)
            # ------------------------------------------------------------
            task_sql = """
            SELECT
                at.id,
                at.annotation_type,
                at.content,
                at.status,
                at.due_date,
                at.is_completed,
                at.priority,
                at.user_assigned_id,
                at.annotation_id,
                a.object_reference_id,
                a.object_reference_type
            FROM public.annotation_tasks at
            JOIN public.annotations a
              ON a.id = at.annotation_id
            JOIN public.deals d
              ON d.id = a.object_reference_id
            JOIN public.leads l
              ON l.id = d.lead_id
            WHERE
                at.is_deleted = FALSE
                AND a.is_deleted = FALSE
                AND d.is_deleted = FALSE
                AND l.is_deleted = FALSE
                AND a.object_reference_type = 'deals'
                AND d.company_id = %s
                AND %s <> ''
                AND regexp_replace(COALESCE(l.phone, ''), '\\D', '', 'g')
                    LIKE %s || '%%'
            ORDER BY at.due_date NULLS LAST, at.created_at DESC
            LIMIT 50;
            """
            task_params = (company_id, digits, digits)
            cur.execute(task_sql, task_params)
            task_rows = cur.fetchall()

            for t in task_rows:
                (
                    tid,
                    annotation_type,
                    content,
                    status,
                    due_date,
                    is_completed,
                    priority,
                    user_assigned_id,
                    annotation_id,
                    object_reference_id,
                    object_reference_type,
                ) = t

                tasks.append({
                    "id": str(tid),
                    "annotation_type": annotation_type,
                    "content": content,
                    "status": status,
                    "due_date": due_date.isoformat() if due_date else None,
                    "is_completed": is_completed,
                    "priority": priority,
                    "user_assigned_id": str(user_assigned_id) if user_assigned_id else None,
                    "annotation_id": str(annotation_id),
                    "object_reference_id": str(object_reference_id),
                    "object_reference_type": object_reference_type,
                })

    finally:
        try:
            conn.close()
        except Exception:
            pass

    return results, messages, tasks


# ----------------------------------------------------------------------------
# Endpoints
# ----------------------------------------------------------------------------

@app.route("/healthz", methods=["GET", "OPTIONS"])
def healthz():
    """Health check endpoint con soporte OPTIONS para CORS."""
    if request.method == "OPTIONS":
        # Preflight ya manejado por @app.after_request
        return jsonify({}), 204
    return jsonify({"status": "ok", "service": "search_service"}), 200

@app.route("/search", methods=["GET", "OPTIONS"])
def search_endpoint():
    """
    GET /search?company_id=<uuid>&q=<texto>

    Respuesta:
      {
        "results": [...],
        "messages": [...],
        "tasks": [...]
      }
    """
    if request.method == "OPTIONS":
        # Preflight ya manejado por @app.after_request
        return jsonify({}), 204
    
    try:
        company_id = (request.args.get("company_id") or "").strip()
        q = (request.args.get("q") or "").strip()

        if not company_id:
            return jsonify({
                "error": "company_id es obligatorio como query param"
            }), 400

        if not q:
            return jsonify({"results": [], "messages": [], "tasks": []}), 200

        app.logger.info(f"🔎 /search company_id={company_id}, q={q!r}")

        try:
            results, messages, tasks = run_search(company_id=company_id, query=q)
        except Exception as e:
            app.logger.error(f"❌ Error ejecutando búsqueda: {e}", exc_info=True)
            return jsonify({"error": "Error ejecutando la búsqueda"}), 500

        return jsonify({
            "results": results,
            "messages": messages,
            "tasks": tasks
        }), 200

    except Exception as e:
        app.logger.error(f"💥 Error inesperado en /search: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

# ----------------------------------------------------------------------------
# Config de servidor (similar a get_task_engine_server_config)
# ----------------------------------------------------------------------------
def get_search_service_server_config():
    """
    Lee la sección [SEARCH_SERVICE] de scripts.conf para configurar
    el servidor de este microservicio.

    Ejemplo de sección:

      [SEARCH_SERVICE]
      SEARCH_SERVICE_HOST = 0.0.0.0
      SEARCH_SERVICE_PORT = 5107
      SSL_CERT_PATH = /ruta/cert.pem
      SSL_KEY_PATH  = /ruta/key.pem
    """
    host = "0.0.0.0"
    port = 5107
    ssl_cert = None
    ssl_key = None

    if config_supabase.has_section("SEARCH_SERVICE"):
        sec = config_supabase["SEARCH_SERVICE"]
        host = sec.get("SEARCH_SERVICE_HOST", host)
        port = int(sec.get("SEARCH_SERVICE_PORT", port))
        ssl_cert = sec.get("SSL_CERT_PATH", ssl_cert)
        ssl_key = sec.get("SSL_KEY_PATH", ssl_key)

    return host, port, ssl_cert, ssl_key

# ----------------------------------------------------------------------------
# Arranque
# ----------------------------------------------------------------------------
if __name__ == "__main__":
    host, port, ssl_cert, ssl_key = get_search_service_server_config()

    ssl_context = None
    if ssl_cert and ssl_key:
        cert_path = Path(ssl_cert)
        key_path = Path(ssl_key)
        if cert_path.exists() and key_path.exists():
            ssl_context = (str(cert_path), str(key_path))
            app.logger.info(
                f"🔐 Iniciando search_service HTTPS en https://{host}:{port}/search"
            )
        else:
            app.logger.warning(
                f"⚠️ Cert o key no encontrados ({cert_path}, {key_path}). "
                f"Arrancando en HTTP."
            )

    if ssl_context is None:
        app.logger.info(
            f"🌐 Iniciando search_service HTTP en http://{host}:{port}/search"
        )

    app.run(host=host, port=port, ssl_context=ssl_context)
