#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import os
import configparser
from pathlib import Path
from datetime import datetime, timezone

import pg8000
from flask import Flask, request, jsonify
from flask_cors import CORS

try:
    import requests
except Exception:
    requests = None

# ----------------------------------------------------------------------------
# Configuración básica / logging
# ----------------------------------------------------------------------------
app = Flask(__name__)
app.logger.setLevel(logging.DEBUG)

logging.getLogger("werkzeug").setLevel(logging.INFO)

# ----------------------------------------------------------------------------
# Carga de configuración desde scripts.conf
# ----------------------------------------------------------------------------
config_supabase = configparser.ConfigParser()
SCRIPTS_CONF_PATH = "scripts.conf"

if os.path.exists(SCRIPTS_CONF_PATH):
    try:
        config_supabase.read(SCRIPTS_CONF_PATH)
        app.logger.info("✅ scripts.conf cargado")
        app.logger.debug(f"Secciones: {config_supabase.sections()}")
    except Exception as e:
        app.logger.error(f"❌ Error cargando scripts.conf: {e}", exc_info=True)
else:
    app.logger.warning("⚠️ scripts.conf no encontrado. Revisa ruta/volumen.")

# ----------------------------------------------------------------------------
# Conexión a BD
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
            application_name="task_info_webhook",
        )
        with conn.cursor() as cur:
            cur.execute(f"SET search_path TO {search_path}")
        return conn
    except Exception as e:
        app.logger.error(f"❌ Error creando conexión a BD: {e}", exc_info=True)
        raise


def get_task_info(task_id: str):
    """
    Ajusta la SQL a tu esquema real de tareas.
    Suposición:
      - Tabla: public.tasks
      - Campos: id, due_date, description, assigned_to_id
      - Join con profiles (asignado)
    """
    sql = """
        SELECT
            t.id,
            t.due_date,
            t.description,
            t.assigned_to_id,
            p.first_name,
            p.last_name,
            p.email
        FROM public.tasks t
        LEFT JOIN public.profiles p
          ON p.id = t.assigned_to_id
        WHERE t.id = %s
        LIMIT 1;
    """

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(sql, (task_id,))
            row = cur.fetchone()
        if not row:
            return None

        task = {
            "id": row[0],
            "due_date": row[1],         # datetime / date
            "description": row[2],
            "assigned_to_id": row[3],
            "assigned_first_name": row[4],
            "assigned_last_name": row[5],
            "assigned_email": row[6],
        }
        return task
    except Exception as e:
        app.logger.error(f"❌ Error obteniendo task {task_id}: {e}", exc_info=True)
        raise
    finally:
        if conn is not None:
            conn.close()

# ----------------------------------------------------------------------------
# Config Scheduler / Customer Journey
# ----------------------------------------------------------------------------
def get_scheduler_config():
    """
    Saca URL y API Key de scripts.conf o variables de entorno.
    Sección sugerida:

    [SCHEDULER]
    SCHEDULER_URL = https://scheduler.solvify.es:5100/api/flow/triggerFlow
    SCHEDULER_API_KEY = your-api-key-for-webhooks
    """
    url = os.environ.get("SCHEDULER_URL")
    api_key = os.environ.get("SCHEDULER_API_KEY")

    if config_supabase.has_section("SCHEDULER"):
        sec = config_supabase["SCHEDULER"]
        url = sec.get("SCHEDULER_URL", url)
        api_key = sec.get("SCHEDULER_API_KEY", api_key)

    # Por defecto, la URL que me has pasado
    if not url:
        url = "https://scheduler.solvify.es:5100/api/flow/triggerFlow"

    return url, api_key


def to_utc_iso_z(dt):
    """
    Convierte un datetime o date a string ISO8601 terminado en Z.
    """
    if dt is None:
        return None

    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
    else:
        # Si es date, le ponemos hora 00:00 UTC
        dt = datetime(dt.year, dt.month, dt.day, tzinfo=timezone.utc)

    iso = dt.isoformat()
    # Normalizar +00:00 a Z
    if iso.endswith("+00:00"):
        iso = iso[:-6] + "Z"
    return iso


def trigger_call_reminder_flow(task: dict):
    """
    Lanza el flow 'recordatorio_llamada' en el scheduler/customer_journey
    usando:
      - id         = assigned_to_id
      - schedule_at = due_date (ISO Z)
    """
    if requests is None:
        app.logger.error("❌ requests no disponible, no puedo llamar al scheduler.")
        return False, "requests_not_available"

    scheduler_url, api_key = get_scheduler_config()

    assigned_id = task.get("assigned_to_id")
    if not assigned_id:
        app.logger.warning("⚠️ Task sin assigned_to_id, no se lanza flow.")
        return False, "no_assigned_to_id"

    schedule_at = to_utc_iso_z(task.get("due_date"))
    if not schedule_at:
        app.logger.warning("⚠️ Task sin due_date, no se lanza flow.")
        return False, "no_due_date"

    payload = {
        "id": str(assigned_id),
        "flow_name": "recordatorio_llamada",
        "schedule_at": schedule_at,
    }

    headers = {
        "Content-Type": "application/json",
    }
    if api_key:
        headers["X-API-Key"] = api_key
    else:
        # Si no hay API key, seguimos pero avisando
        app.logger.warning("⚠️ SCHEDULER_API_KEY no configurada. Enviando sin X-API-Key.")

    app.logger.info("🚀 Lanzando flow de recordatorio de llamada en scheduler")
    app.logger.debug(f"POST {scheduler_url} payload={payload} headers={headers}")

    try:
        resp = requests.post(scheduler_url, json=payload, headers=headers, timeout=10)
        app.logger.info(f"✅ Respuesta scheduler: {resp.status_code} {resp.text[:500]}")
        return resp.ok, resp.text
    except Exception as e:
        app.logger.error(f"❌ Error llamando al scheduler: {e}", exc_info=True)
        return False, str(e)

# ----------------------------------------------------------------------------
# Flask + CORS
# ----------------------------------------------------------------------------
CORS(app, resources={r"/*": {"origins": "*"}})


@app.before_request
def log_request_info():
    app.logger.debug(f"--> {request.method} {request.url}")
    try:
        app.logger.debug(f"Headers: {dict(request.headers)}")
    except Exception:
        pass


@app.route("/task-info", methods=["POST", "GET"])
def task_info_webhook():
    """
    Webhook HTTPS/CORS que recibe un task_id y:
      1) Muestra info de la tarea por pantalla (logs)
      2) Devuelve la info en JSON
      3) Lanza el flow 'recordatorio_llamada' en scheduler con:
         - id = assigned_to_id
         - schedule_at = due_date
    """
    try:
        # --- Obtener task_id de querystring o body JSON ---
        task_id = request.args.get("task_id")
        if not task_id:
            data = request.get_json(silent=True) or {}
            task_id = data.get("task_id")

        if not task_id:
            return (
                jsonify(
                    {
                        "status": "error",
                        "message": "task_id es obligatorio (query param o JSON).",
                    }
                ),
                400,
            )

        app.logger.info(f"🔔 Webhook recibido para task_id={task_id}")

        # --- Consultar la tarea ---
        task = get_task_info(task_id)
        if not task:
            app.logger.warning(f"⚠️ Task {task_id} no encontrada")
            return (
                jsonify(
                    {
                        "status": "not_found",
                        "message": f"Tarea {task_id} no encontrada",
                    }
                ),
                404,
            )

        assigned_name = (
            f"{task['assigned_first_name'] or ''} {task['assigned_last_name'] or ''}"
        ).strip() or "Sin asignar"

        # --- Log bonito en consola ---
        app.logger.info(
            "📝 TASK INFO\n"
            f"   • ID          : {task['id']}\n"
            f"   • Due date    : {task['due_date']}\n"
            f"   • Descripción : {task['description']}\n"
            f"   • Asignado a  : {assigned_name} <{task['assigned_email']}>"
        )

        # --- Lanzar flow en scheduler ---
        ok, scheduler_response = trigger_call_reminder_flow(task)
        app.logger.info(f"📡 Resultado trigger flow: ok={ok} resp={scheduler_response}")

        # --- Respuesta JSON ---
        return (
            jsonify(
                {
                    "status": "ok",
                    "task": {
                        "id": str(task["id"]),
                        "due_date": task["due_date"].isoformat()
                        if hasattr(task["due_date"], "isoformat")
                        else task["due_date"],
                        "description": task["description"],
                        "assigned": {
                            "id": str(task["assigned_to_id"])
                            if task["assigned_to_id"]
                            else None,
                            "first_name": task["assigned_first_name"],
                            "last_name": task["assigned_last_name"],
                            "email": task["assigned_email"],
                            "full_name": assigned_name,
                        },
                    },
                    "flow_trigger": {
                        "ok": ok,
                        "raw_response": scheduler_response,
                    },
                }
            ),
            200,
        )

    except Exception as e:
        app.logger.error(f"💥 Error en /task-info: {e}", exc_info=True)
        return (
            jsonify(
                {
                    "status": "error",
                    "message": "Error interno",
                }
            ),
            500,
        )

# ----------------------------------------------------------------------------
# Arranque HTTPS
# ----------------------------------------------------------------------------
def get_webhook_server_config():
    """
    Lee [WEBHOOK] de scripts.conf si existe.
    """
    host = "0.0.0.0"
    port = 5105
    ssl_cert = None
    ssl_key = None

    if config_supabase.has_section("WEBHOOK"):
        w = config_supabase["WEBHOOK"]
        host = w.get("WEBHOOK_HOST", host)
        port = int(w.get("WEBHOOK_PORT", port))
        ssl_cert = w.get("SSL_CERT_PATH", ssl_cert)
        ssl_key = w.get("SSL_KEY_PATH", ssl_key)

    return host, port, ssl_cert, ssl_key


if __name__ == "__main__":
    host, port, ssl_cert, ssl_key = get_webhook_server_config()

    ssl_context = None
    if ssl_cert and ssl_key:
        cert_path = Path(ssl_cert)
        key_path = Path(ssl_key)
        if cert_path.exists() and key_path.exists():
            ssl_context = (str(cert_path), str(key_path))
            app.logger.info(
                f"🔐 Iniciando servidor HTTPS en https://{host}:{port}/task-info"
            )
        else:
            app.logger.warning(
                f"⚠️ Cert o key no encontrados ({cert_path}, {key_path}). "
                f"Arrancando en HTTP."
            )

    if ssl_context is None:
        app.logger.info(f"🌐 Iniciando servidor HTTP en http://{host}:{port}/task-info")

    app.run(host=host, port=port, ssl_context=ssl_context)
