#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import os
import configparser
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Optional
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


# Cargar configuración del scheduler como variables globales
SCHEDULER_URL, SCHEDULER_API_KEY = get_scheduler_config()
app.logger.info(f"🔧 Scheduler configurado: {SCHEDULER_URL}")

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


def get_task_info(task_id: str) -> Optional[dict]:
    """
    Obtiene info de la tarea a partir de annotation_tasks.id
    - due_date
    - content
    - user_assigned (profile)
    - compañía (si se puede resolver vía deals/companies)
    """
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        sql = """
        SELECT
            at.id                             AS task_id,
            at.content                        AS task_content,
            at.due_date                       AS task_due_date,
            at.status                         AS task_status,
            at.priority                       AS task_priority,
            at.user_assigned_id               AS user_assigned_id,
            at.annotation_type                AS task_annotation_type,
            at.is_completed                   AS task_is_completed,

            a.annotation_type                 AS parent_annotation_type,
            a.object_reference_type           AS object_reference_type,
            a.object_reference_id             AS object_reference_id,

            p.first_name                      AS assignee_first_name,
            p.last_name                       AS assignee_last_name,
            p.email                           AS assignee_email,

            d.id                              AS deal_id,
            d.company_id                      AS company_id,
            c.name                            AS company_name,

            -- 👇 lead_id resuelto según el tipo de objeto
            CASE
                WHEN a.object_reference_type = 'leads' THEN a.object_reference_id
                WHEN a.object_reference_type = 'deals' THEN d.lead_id
                ELSE NULL
            END                               AS lead_id
        FROM annotation_tasks at
        LEFT JOIN annotations a
               ON a.id = at.annotation_id
        LEFT JOIN profiles p
               ON p.id = at.user_assigned_id
        LEFT JOIN LATERAL (
            SELECT d.id, d.company_id, d.lead_id
            FROM deals d
            WHERE d.is_deleted = false
              AND (
                    (a.object_reference_type = 'deals' AND a.object_reference_id = d.id)
                 OR (a.object_reference_type = 'leads' AND a.object_reference_id = d.lead_id)
              )
            ORDER BY d.created_at DESC
            LIMIT 1
        ) d ON TRUE
        LEFT JOIN companies c
               ON c.id = d.company_id
        WHERE at.id = %s
          AND at.is_deleted = false
        """
        cur.execute(sql, (task_id,))
        row = cur.fetchone()
        cur.close()

        if not row:
            app.logger.warning(f"🔍 No se encontró annotation_task con id={task_id}")
            return None

        (
            task_id,
            task_content,
            task_due_date,
            task_status,
            task_priority,
            user_assigned_id,
            task_annotation_type,
            task_is_completed,
            parent_annotation_type,
            object_reference_type,
            object_reference_id,
            assignee_first_name,
            assignee_last_name,
            assignee_email,
            deal_id,
            company_id,
            company_name,
            lead_id,              # 👈 nuevo campo
        ) = row

        return {
            "task_id": str(task_id) if task_id else None,
            "content": task_content,
            "due_date": task_due_date,
            "status": task_status,
            "priority": task_priority,
            "user_assigned_id": str(user_assigned_id) if user_assigned_id else None,
            "task_annotation_type": task_annotation_type,
            "task_is_completed": task_is_completed,
            "parent_annotation_type": parent_annotation_type,
            "object_reference_type": object_reference_type,
            "object_reference_id": str(object_reference_id) if object_reference_id else None,
            "assignee_first_name": assignee_first_name,
            "assignee_last_name": assignee_last_name,
            "assignee_email": assignee_email,
            "deal_id": str(deal_id) if deal_id else None,
            "company_id": str(company_id) if company_id else None,
            "company_name": company_name,
            "lead_id": str(lead_id) if lead_id else None,   # 👈 lo devolvemos
        }

    finally:
        try:
            conn.close()
        except Exception:
            pass


from werkzeug.exceptions import BadRequest
import json

from werkzeug.exceptions import BadRequest
import json

@app.route("/task-info", methods=["POST"])
def task_info_webhook():
    """
    Webhook HTTPS + CORS.

    Recibe:
      { "task_id": "<uuid-annotation_tasks>" }
    (también acepta "id" o "annotation_task_id")

    1) Busca la tarea en BD.
    2) Muestra info por log.
    3) SOLO si el tipo es 'Llamada programada' dispara trigger_call_reminder_flow(task).
    """
    try:
        # 👀 Log raw body para ver qué llega (axios, curl, etc.)
        raw_body = request.get_data(as_text=True)
        app.logger.info(f"📜 Raw body recibido en /task-info: {raw_body!r}")

        try:
            data = request.get_json(force=True)
        except BadRequest as e:
            app.logger.error(f"❌ JSON inválido en /task-info: {e}")
            return jsonify({"error": "JSON inválido", "details": str(e)}), 400

        if not isinstance(data, dict):
            app.logger.error(f"❌ JSON no es un objeto: {data!r}")
            return jsonify({"error": "JSON debe ser un objeto", "details": str(data)}), 400

        # 🔑 Aceptamos varios nombres de campo
        task_id = (
            data.get("task_id")
            or data.get("id")
            or data.get("annotation_task_id")
        )

        app.logger.info(f"🔎 task_id resuelto desde JSON: {task_id}")

        if not task_id:
            return jsonify({
                "error": "task_id es obligatorio",
                "details": "Envía 'task_id' o 'id' en el cuerpo JSON",
            }), 400

        app.logger.info(f"📥 Webhook /task-info recibido para task_id={task_id}")

        # 🔄 Recuperar la tarea desde la BD
        task = get_task_info(task_id)
        if not task:
            return jsonify({"error": "Tarea no encontrada"}), 404

        # 👀 Logs bonitos
        app.logger.info("📝 TASK INFO")
        app.logger.info(f"   • ID tarea:       {task.get('task_id')}")
        app.logger.info(f"   • Contenido:      {task.get('content')}")
        app.logger.info(f"   • Due date:       {task.get('due_date')}")
        app.logger.info(f"   • Estado:         {task.get('status')}")
        app.logger.info(f"   • Prioridad:      {task.get('priority')}")
        app.logger.info(f"   • Tipo tarea:     {task.get('task_annotation_type')}")
        app.logger.info(
            f"   • Asignado a:     {task.get('assignee_first_name')} "
            f"{task.get('assignee_last_name')} <{task.get('assignee_email')}> "
            f"(profile_id={task.get('user_assigned_id')})"
        )
        app.logger.info(
            f"   • Compañía:       {task.get('company_name')} "
            f"(company_id={task.get('company_id')})"
        )
        app.logger.info(
            f"   • Ref:            {task.get('object_reference_type')} "
            f"{task.get('object_reference_id')}"
        )
        app.logger.info(f"   • Lead:           {task.get('lead_id')}")

        # ⚙️ “Switch”: solo si es Llamada programada lanzamos el flow
        task_type = (task.get("task_annotation_type") or "").strip()
        if task_type == "Llamada programada":
            app.logger.info("📞 Tipo 'Llamada programada' → disparando trigger_call_reminder_flow()")
            trigger_call_reminder_flow(task)
        else:
            app.logger.info(
                f"⏭️ No se dispara customer_journey: task_type='{task_type}' != 'Llamada programada'"
            )

        # Respuesta al caller
        return jsonify({"ok": True, "task": task}), 200

    except Exception as e:
        app.logger.error(f"💥 Error en /task-info: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500



# ----------------------------------------------------------------------------
# Helper para convertir fechas a ISO8601 UTC
# ----------------------------------------------------------------------------
def build_schedule_at(due_date: Optional[datetime]) -> str:
    """
    Convierte due_date (naive o con tz) a ISO8601 UTC con 'Z'.
    Si no hay due_date, usa ahora + 5 minutos.
    """
    if due_date is None:
        dt = datetime.now(timezone.utc) + timedelta(minutes=5)
    else:
        if due_date.tzinfo is None:
            # asumimos que está en UTC si viene naive
            dt = due_date.replace(tzinfo=timezone.utc)
        else:
            dt = due_date.astimezone(timezone.utc)

    dt = dt.replace(microsecond=0)
    return dt.isoformat().replace("+00:00", "Z")

def trigger_customer_journey(task: dict) -> None:
    """
    Dispara el flow 'recordatorio_llamada' al scheduler/customer_journey.
    Ahora:
      - id = lead_id (no el user_assigned_id)
    """
    if not task:
        app.logger.error("❌ trigger_customer_journey llamado sin task válida")
        return

    if requests is None:
        app.logger.error("❌ requests no disponible, no puedo llamar al scheduler.")
        return

    lead_id = task.get("lead_id")
    if not lead_id:
        # Fallback por si alguna tarea vieja viene solo con object_reference_id
        fallback = task.get("object_reference_id")
        app.logger.warning(
            f"⚠️ Tarea sin lead_id resuelto, usando object_reference_id como fallback: {fallback}"
        )
        lead_id = fallback

    if not lead_id:
        app.logger.error("❌ No hay lead_id ni fallback, no se lanza customer_journey")
        return

    due_date = task.get("due_date")
    if due_date and not isinstance(due_date, datetime):
        app.logger.warning(f"⚠️ due_date no es datetime: {due_date!r}")
        due_dt = None
    else:
        due_dt = due_date

    schedule_at = build_schedule_at(due_dt)

    payload = {
        "id": lead_id,                      # 👈 AHORA ES EL LEAD
        "flow_name": "recordatorio_llamada",
        "schedule_at": schedule_at,
    }

    url = SCHEDULER_URL
    headers = {
        "Content-Type": "application/json",
    }
    if SCHEDULER_API_KEY:
        headers["X-API-Key"] = SCHEDULER_API_KEY

    import json
    app.logger.info(f"🌊 Llamando a Customer Journey: POST {url}")
    app.logger.info(f"📦 Payload JSON real: {json.dumps(payload)}")

    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=10)
        app.logger.info(f"📡 Respuesta scheduler: {resp.status_code} - {resp.text}")
        resp.raise_for_status()
    except Exception as e:
        app.logger.error(f"❌ Error llamando a customer_journey: {e}")


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


# ----------------------------------------------------------------------------
# Arranque HTTPS
# ----------------------------------------------------------------------------
def get_task_engine_server_config():
    """
    Lee la sección [TASK_ENGINE] de scripts.conf para configurar
    el servidor de este microservicio, SIN tocar la sección [WEBHOOK].

    Claves esperadas:
      [TASK_ENGINE]
      TASK_ENGINE_HOST = 0.0.0.0
      TASK_ENGINE_PORT = 5105
      TASK_ENGINE_HTTP_PORT = 5104   (opcional, por si quieres usarlo)
      SSL_CERT_PATH = /ruta/cert.pem
      SSL_KEY_PATH  = /ruta/key.pem
    """
    host = "0.0.0.0"
    port = 5105
    http_port = None  # por si en el futuro quieres exponer HTTP también
    ssl_cert = None
    ssl_key = None

    if config_supabase.has_section("TASK_ENGINE"):
        t = config_supabase["TASK_ENGINE"]
        host = t.get("TASK_ENGINE_HOST", host)
        port = int(t.get("TASK_ENGINE_PORT", port))
        http_port = t.get("TASK_ENGINE_HTTP_PORT", None)
        if http_port is not None:
            try:
                http_port = int(http_port)
            except ValueError:
                http_port = None

        ssl_cert = t.get("SSL_CERT_PATH", ssl_cert)
        ssl_key = t.get("SSL_KEY_PATH", ssl_key)

    return host, port, http_port, ssl_cert, ssl_key


if __name__ == "__main__":
    host, port, http_port, ssl_cert, ssl_key = get_task_engine_server_config()

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