#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import os
import configparser
from pathlib import Path
import re
import pg8000
from flask import Flask, request, jsonify, make_response
from flask_cors import CORS

# ----------------------------------------------------------------------------
# Flask + logging
# ----------------------------------------------------------------------------
app = Flask(__name__)

# Configuración de logging similar a otros microservicios
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("search_service")
app.logger.setLevel(logging.INFO)

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
    except Exception as e:
        app.logger.error(f"❌ Error leyendo scripts.conf: {e}")
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
        db_port = config_supabase.getint("DB", "DB_PORT")
        db_name = config_supabase.get("DB", "DB_NAME")
        db_user = config_supabase.get("DB", "DB_USER")
        db_password = config_supabase.get("DB", "DB_PASSWORD")

        conn = pg8000.connect(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_password,
        )
        return conn
    except Exception as e:
        app.logger.error(f"❌ Error conectando a BD: {e}")
        raise


# ----------------------------------------------------------------------------
# Utilidad: validación NIF/NIE (DNI_Valido)
# ----------------------------------------------------------------------------

DNI_LETTERS = "TRWAGMYFPDXBNJZSQVHLCKE"


def dni_valido(dni: str) -> bool:
    """
    Valida un NIF/NIE español.
    - Admite NIF: 8 dígitos + letra (ej: 12345678Z)
    - Admite NIE: X/Y/Z + 7 dígitos + letra (ej: X1234567L)
    - Ignora espacios y separadores (., -, etc.)
    Devuelve True si el dígito de control es correcto, False en caso contrario.
    """
    if not dni:
        return False

    # Quitamos espacios y caracteres no alfanuméricos, y pasamos a mayúsculas
    clean = re.sub(r"[^A-Za-z0-9]", "", dni).upper()

    if len(clean) < 2:
        return False

    # Caso NIF: 8 dígitos + letra
    m = re.fullmatch(r"(\d{8})([A-Z])", clean)
    if m:
        number_str, letter = m.groups()
        try:
            number = int(number_str)
        except ValueError:
            return False

        expected_letter = DNI_LETTERS[number % 23]
        return letter == expected_letter

    # Caso NIE: X/Y/Z + 7 dígitos + letra
    m = re.fullmatch(r"([XYZ])(\d{7})([A-Z])", clean)
    if m:
        prefix, digits_str, letter = m.groups()
        prefix_map = {"X": "0", "Y": "1", "Z": "2"}
        number_str = prefix_map[prefix] + digits_str  # ej: X1234567 -> 01234567

        try:
            number = int(number_str)
        except ValueError:
            return False

        expected_letter = DNI_LETTERS[number % 23]
        return letter == expected_letter

    # Si no encaja con ningún formato de NIF/NIE, es inválido
    return False


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
      - messages: lista de mensajes relacionados
      - tasks: lista de tareas relacionadas
    """
    conn = get_db_connection()
    try:
        cur = conn.cursor()

        # ----------------------------------------------------------------------------
        # Búsqueda en deals
        # ----------------------------------------------------------------------------
        sql_deals = """
        SELECT
            'deal' AS type,
            d.id,
            d.name,
            d.status,
            d.created_at
        FROM deals d
        WHERE d.company_id = %s
          AND d.name ILIKE %s
        ORDER BY d.created_at DESC
        LIMIT 50
        """

        like_pattern = f"%{query}%"
        cur.execute(sql_deals, (company_id, like_pattern))
        deal_rows = cur.fetchall()

        deals_results = [
            {
                "type": row[0],
                "id": row[1],
                "name": row[2],
                "status": row[3],
                "created_at": row[4].isoformat() if row[4] else None,
            }
            for row in deal_rows
        ]

        # ----------------------------------------------------------------------------
        # TODO: Añadir aquí búsquedas en leads, profiles, mensajes, tareas, etc.
        # De momento devolvemos solo deals_results para que funcione el MVP.
        # ----------------------------------------------------------------------------
        messages_results = []
        tasks_results = []

        return deals_results, messages_results, tasks_results
    finally:
        conn.close()


# ----------------------------------------------------------------------------
# Endpoints Flask
# ----------------------------------------------------------------------------
@app.route("/healthz", methods=["GET", "OPTIONS"])
def healthz():
    """Health check endpoint con soporte OPTIONS para CORS."""
    if request.method == "OPTIONS":
        # Preflight ya manejado por @app.after_request
        return jsonify({}), 204
    return jsonify({"status": "ok", "service": "search_service"}), 200


@app.route("/dni/valido", methods=["GET", "POST", "OPTIONS"])
def dni_valido_endpoint():
    """
    Servicio simple para validar un DNI/NIF/NIE.

    - GET  /dni/valido?dni=12345678Z
    - POST /dni/valido  { "dni": "12345678Z" }

    Respuesta:
      { "dni": "12345678Z", "is_valid": true }
    """
    if request.method == "OPTIONS":
        # Preflight ya manejado por @app.before_request / @app.after_request
        return jsonify({}), 204

    dni_str = ""
    if request.method == "GET":
        dni_str = (request.args.get("dni") or "").strip()
    else:
        data = request.get_json(silent=True) or {}
        dni_str = (data.get("dni") or "").strip()

    if not dni_str:
        return jsonify({
            "error": "dni es obligatorio",
            "is_valid": False
        }), 400

    is_valid = dni_valido(dni_str)

    return jsonify({
        "dni": dni_str,
        "is_valid": is_valid
    }), 200


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
            results, messages, tasks = run_search(company_id, q)
        except Exception as e:
            app.logger.error(f"❌ Error ejecutando run_search: {e}")
            return jsonify({"error": "Error interno ejecutando la búsqueda"}), 500

        return jsonify({
            "results": results,
            "messages": messages,
            "tasks": tasks,
        }), 200

    except Exception as e:
        app.logger.error(f"❌ Error en /search: {e}", exc_info=True)
        return jsonify({"error": "Error interno en el endpoint /search"}), 500


# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------
def main():
    """
    Arranca la aplicación Flask de search_service.
    Respeta SSL si en scripts.conf hay sección [SSL] con CERT_FILE y KEY_FILE.
    """
    host = os.environ.get("SEARCH_SERVICE_HOST", "0.0.0.0")
    port = int(os.environ.get("SEARCH_SERVICE_PORT", "5107"))

    ssl_context = None
    if config_supabase.has_section("SSL"):
        cert_path = config_supabase.get("SSL", "CERT_FILE", fallback="")
        key_path = config_supabase.get("SSL", "KEY_FILE", fallback="")
        if cert_path and key_path and Path(cert_path).exists() and Path(key_path).exists():
            ssl_context = (cert_path, key_path)
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


if __name__ == "__main__":
    main()
