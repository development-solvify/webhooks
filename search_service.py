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
import re  # asegúrate de tener este import arriba del archivo

def run_search(company_id: str, query: str):
    """
    Ejecuta una búsqueda global para una company_id y un término q.
    Devuelve una lista de dicts ya listos para el front.
    """

    q = (query or "").strip()
    if not q:
        return []

    # Construimos patrón de búsqueda en Python
    pattern = f"%{q}%"
    digits = re.sub(r"\D", "", q or "")

    sql = """
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
            '/deals/' || d.id::text AS link
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
                -- quitamos l.status porque no existe en la BDD real
                'channel', l.channel
            ) AS extra,
            '/leads/' || l.id::text AS link
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

    params = (
        company_id,  # search.company_id
        pattern,     # search.pattern
        digits,      # search.digits
    )

    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall()
        cur.close()
    finally:
        try:
            conn.close()
        except Exception:
            pass

    results = []
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

    return results

# ----------------------------------------------------------------------------
# Flask + CORS
# ----------------------------------------------------------------------------
# Si luego lo pones detrás de Nginx en el mismo dominio, CORS casi no se usa,
# pero lo dejamos abierto para desarrollo / pruebas.
CORS(app, resources={r"/*": {"origins": "*"}})

@app.before_request
def log_request_info():
    app.logger.debug(f"--> {request.method} {request.url}")
    try:
        app.logger.debug(f"Headers: {dict(request.headers)}")
    except Exception:
        pass

# ----------------------------------------------------------------------------
# Endpoints
# ----------------------------------------------------------------------------

@app.route("/healthz", methods=["GET"])
def healthz():
    return jsonify({"status": "ok", "service": "search_service"}), 200


@app.route("/search", methods=["GET"])
def search_endpoint():
    """
    GET /search?company_id=<uuid>&q=<texto>

    Respuesta:
      { "results": [ { ... }, ... ] }
    """
    try:
        company_id = (request.args.get("company_id") or "").strip()
        q = (request.args.get("q") or "").strip()

        if not company_id:
            return jsonify({
                "error": "company_id es obligatorio como query param"
            }), 400

        if not q:
            return jsonify({"results": []}), 200

        app.logger.info(f"🔎 /search company_id={company_id}, q={q!r}")

        try:
            results = run_search(company_id=company_id, query=q)
        except Exception as e:
            app.logger.error(f"❌ Error ejecutando búsqueda: {e}", exc_info=True)
            return jsonify({"error": "Error ejecutando la búsqueda"}), 500

        return jsonify({"results": results}), 200

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
