
import os
import logging
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from flask_cors import CORS
import psycopg2
import requests
import configparser
from flask import Flask, redirect, request, session, jsonify, render_template

# ============================================================
# CONFIGURACIÓN: scripts.conf + entorno
# ============================================================

DEFAULT_CONF_PATH = os.path.join(os.path.dirname(__file__), "scripts.conf")
CONF_PATH = os.environ.get("SCRIPTS_CONF_PATH", DEFAULT_CONF_PATH)

config = configparser.ConfigParser()
read_files = config.read(CONF_PATH)

if not read_files:
    raise RuntimeError(f"No se ha podido leer scripts.conf en: {CONF_PATH}")

if "GOOGLE" not in config:
    raise RuntimeError("No se ha encontrado la sección [GOOGLE] en scripts.conf")

GOOGLE_CLIENT_ID = config.get("GOOGLE", "GOOGLE_CLIENT_ID", fallback=None)
GOOGLE_CLIENT_SECRET = config.get("GOOGLE", "GOOGLE_CLIENT_SECRET", fallback=None)
GOOGLE_REDIRECT_URI = config.get("GOOGLE", "GOOGLE_REDIRECT_URI", fallback=None)

if not (GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET and GOOGLE_REDIRECT_URI):
    raise RuntimeError("Faltan GOOGLE_CLIENT_ID / GOOGLE_CLIENT_SECRET / GOOGLE_REDIRECT_URI en [GOOGLE]")

GOOGLE_SCOPES = "https://www.googleapis.com/auth/calendar.events"

if "DB" not in config:
    raise RuntimeError("No se ha encontrado la sección [DB] en scripts.conf")

DB_HOST = config.get("DB", "DB_HOST")
DB_PORT = config.getint("DB", "DB_PORT", fallback=5432)
DB_NAME = config.get("DB", "DB_NAME")
DB_USER = config.get("DB", "DB_USER")
DB_PASS = config.get("DB", "DB_PASS")

SECRET_KEY = os.environ.get("SECRET_KEY", "dev-secret-change-me")

DEMO_PROFILE_ID = os.environ.get(
    "DEMO_PROFILE_ID",
    "00000000-0000-0000-0000-000000000001"
)

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("google-calendar-etd")

BASE_FILES_DIR = os.path.join(os.path.dirname(__file__), "google_calendar_ETD_files")

app = Flask(
    __name__,
    template_folder=os.path.join(BASE_FILES_DIR, "templates"),
    static_folder=os.path.join(BASE_FILES_DIR, "static"),
)
app.secret_key = SECRET_KEY

# 🔹 Habilitar CORS para las rutas /google/*
CORS(
    app,
    resources={r"/google/*": {"origins": "*"}},
    supports_credentials=True
)

def get_db_conn():
    logger.debug(
        "LOG: Abriendo conexión a BD %s@%s:%s/%s",
        DB_USER,
        DB_HOST,
        DB_PORT,
        DB_NAME,
    )
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        sslmode="require",
    )
    return conn


def build_google_auth_url(state: str) -> str:
    from urllib.parse import urlencode

    params = {
        "client_id": GOOGLE_CLIENT_ID,
        "redirect_uri": GOOGLE_REDIRECT_URI,
        "response_type": "code",
        "scope": GOOGLE_SCOPES,
        "access_type": "offline",
        "include_granted_scopes": "true",
        "prompt": "consent",
        "state": state,
    }
    url = "https://accounts.google.com/o/oauth2/v2/auth?" + urlencode(params)
    logger.debug("LOG: URL de auth Google construida: %s", url)
    return url


def get_fresh_access_token(profile_id: str):
    logger.info("LOG: Buscando tokens para profile_id=%s", profile_id)
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT id, access_token, refresh_token, token_type, scope, token_expiry
            FROM profile_google_tokens
            WHERE profile_id = %s
            """,
            (profile_id,),
        )
        row = cur.fetchone()
        if not row:
            logger.warning("LOG: No se encontraron tokens para profile_id=%s", profile_id)
            return None, "No hay tokens para este profile_id"

        id_, access_token, refresh_token, token_type, scope, token_expiry = row
        now = datetime.now(timezone.utc)
        logger.debug("LOG: token_expiry=%s ahora=%s", token_expiry, now)

        if token_expiry and token_expiry > now + timedelta(minutes=2):
            logger.info("LOG: Access token aún válido para profile_id=%s", profile_id)
            return access_token, None

        logger.info("LOG: Renovando access token para profile_id=%s", profile_id)
        token_url = "https://oauth2.googleapis.com/token"
        data = {
            "client_id": GOOGLE_CLIENT_ID,
            "client_secret": GOOGLE_CLIENT_SECRET,
            "refresh_token": refresh_token,
            "grant_type": "refresh_token",
        }
        resp = requests.post(token_url, data=data)
        logger.debug("LOG: Respuesta renovación token: %s %s", resp.status_code, resp.text)

        if resp.status_code != 200:
            logger.error("LOG: Error renovando token: %s", resp.text)
            return None, f"Error renovando token: {resp.text}"

        tdata = resp.json()
        new_access = tdata["access_token"]
        expires_in = tdata.get("expires_in", 3600)
        new_expiry = now + timedelta(seconds=expires_in)

        cur.execute(
            """
            UPDATE profile_google_tokens
            SET access_token = %s,
                token_expiry = %s,
                updated_at   = now()
            WHERE id = %s
            """,
            (new_access, new_expiry, id_),
        )
        conn.commit()

        logger.info("LOG: Access token renovado y guardado para profile_id=%s", profile_id)
        return new_access, None
    finally:
        cur.close()
        conn.close()


def fetch_task_context(annotation_task_id: str):
    logger.info("LOG: Recuperando contexto de tarea para annotation_task_id=%s", annotation_task_id)

    sql = """
    SELECT
        at.id AS annotation_task_id,
        at.annotation_type,
        at.content AS task_content,
        at.status AS task_status,
        at.due_date,
        at.user_assigned_id AS profile_id,

        a.id AS annotation_id,
        a.object_reference_type,
        a.object_reference_id AS deal_id,

        d.lead_id,

        l.first_name AS lead_first_name,
        l.last_name AS lead_last_name,
        l.email AS lead_email,
        l.phone AS lead_phone,

        p.email AS profile_email,
        p.first_name AS profile_first_name,
        p.last_name AS profile_last_name
    FROM annotation_tasks at
    JOIN annotations a ON a.id = at.annotation_id
    JOIN deals d ON d.id = a.object_reference_id
    JOIN leads l ON l.id = d.lead_id
    LEFT JOIN profiles p ON p.id = at.user_assigned_id
    WHERE at.id = %s
      AND a.is_deleted = false
      AND at.is_deleted = false
      AND d.is_deleted = false
      AND l.is_deleted = false
    LIMIT 1;
    """

    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            logger.debug("LOG: Ejecutando SQL fetch_task_context con annotation_task_id=%s", annotation_task_id)
            cur.execute(sql, (annotation_task_id,))
            row = cur.fetchone()
            if not row:
                logger.warning("LOG: No se encontró tarea para annotation_task_id=%s", annotation_task_id)
                return None

            columns = [desc[0] for desc in cur.description]
            task = dict(zip(columns, row))
            logger.debug("LOG: Contexto de tarea recuperado: %s", task)
            return task
    except Exception as e:
        logger.exception("LOG: Error recuperando contexto de tarea: %s", e)
        raise
    finally:
        conn.close()


def create_calendar_event_from_task_context(task: dict):
    logger.debug("LOG: Creando evento desde contexto de tarea: %s", task)

    profile_id = task.get("profile_id")
    if not profile_id:
        logger.error("LOG: La tarea no tiene user_assigned_id (profile_id)")
        return None, "La tarea no tiene user_assigned_id (profile_id) asignado"

    logger.info("LOG: Usando profile_id=%s para crear evento", profile_id)

    access_token, err = get_fresh_access_token(str(profile_id))
    if err or not access_token:
        logger.error("LOG: No se pudo obtener access_token para profile_id=%s: %s", profile_id, err)
        return None, err or "No access token disponible"

    due_date = task.get("due_date")
    logger.debug("LOG: due_date recuperado de BD: %s (%s)", due_date, type(due_date))

    if not isinstance(due_date, datetime):
        logger.error("LOG: due_date no es datetime: %s", due_date)
        return None, "La tarea no tiene due_date válido"

    tz = ZoneInfo("Europe/Madrid")
    if due_date.tzinfo is None:
        start_dt = due_date.replace(tzinfo=tz)
    else:
        start_dt = due_date.astimezone(tz)

    end_dt = start_dt + timedelta(minutes=30)

    logger.debug("LOG: start_dt=%s end_dt=%s", start_dt.isoformat(), end_dt.isoformat())

    lead_first_name = task.get("lead_first_name") or ""
    lead_last_name = task.get("lead_last_name") or ""
    lead_name = (lead_first_name + " " + lead_last_name).strip() or "Cliente"

    annotation_type = task.get("annotation_type") or "Llamada programada"
    summary = f"{annotation_type} - {lead_name}"

    task_content = task.get("task_content") or ""
    lead_email = task.get("lead_email") or ""
    lead_phone = task.get("lead_phone") or ""
    deal_id = task.get("deal_id")
    annotation_task_id = task.get("annotation_task_id")

    description_lines = []
    if task_content:
        description_lines.append(task_content)
        description_lines.append("")

    description_lines.append(f"Cliente: {lead_name}")
    if lead_email:
        description_lines.append(f"Email cliente: {lead_email}")
    if lead_phone:
        description_lines.append(f"Teléfono cliente: {lead_phone}")

    description_lines.append("")
    if deal_id:
        description_lines.append(f"Deal ID: {deal_id}")
    if annotation_task_id:
        description_lines.append(f"Tarea (annotation_task_id): {annotation_task_id}")

    description = "\n".join(description_lines)

    logger.debug("LOG: summary=%s", summary)
    logger.debug("LOG: description=%s", description)

    attendees = []

    profile_email = task.get("profile_email")
    profile_first_name = task.get("profile_first_name") or ""
    profile_last_name = task.get("profile_last_name") or ""
    profile_name = (profile_first_name + " " + profile_last_name).strip()

    if profile_email:
        att = {"email": profile_email}
        if profile_name:
            att["displayName"] = profile_name
        attendees.append(att)

    if lead_email:
        att = {"email": lead_email}
        if lead_name:
            att["displayName"] = lead_name
        attendees.append(att)

    logger.debug("LOG: attendees=%s", attendees)

    event_body = {
        "summary": summary,
        "description": description,
        "start": {
            "dateTime": start_dt.isoformat(),
            "timeZone": "Europe/Madrid",
        },
        "end": {
            "dateTime": end_dt.isoformat(),
            "timeZone": "Europe/Madrid",
        },
        "attendees": attendees,
        "reminders": {
            "useDefault": True
        },
        "extendedProperties": {
            "private": {
                "annotation_task_id": str(annotation_task_id)
            }
        }
    }

    logger.info(
        "LOG: Enviando event_body a Google Calendar para profile_id=%s: %s",
        profile_id,
        event_body,
    )

    url = "https://www.googleapis.com/calendar/v3/calendars/primary/events"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    try:
        resp = requests.post(url, headers=headers, json=event_body)
        logger.info("LOG: Respuesta de Google Calendar: %s %s", resp.status_code, resp.text)

        if resp.status_code not in (200, 201):
            logger.error("LOG: Error creando evento: %s %s", resp.status_code, resp.text)
            return None, f"Error creando evento: {resp.status_code} {resp.text}"

        return resp.json(), None
    except Exception as e:
        logger.exception("LOG: Excepción creando evento en Google Calendar: %s", e)
        return None, f"Excepción hablando con Google Calendar: {e}"


@app.route("/")
def index():
    return render_template("index.html")

# 🔹 NUEVO: para que /google/ también funcione
@app.route("/google/")
def google_root():
    # puedes enseñar la misma landing
    return render_template("index.html")
    # o si prefieres redirigir:
    # from flask import url_for
    # return redirect(url_for("index"))

@app.route("/google/login")
def google_login():
    profile_id = request.args.get("profile_id") or DEMO_PROFILE_ID
    session["profile_id"] = profile_id

    logger.info("LOG: Iniciando OAuth para profile_id=%s", profile_id)
    auth_url = build_google_auth_url(state=profile_id)
    return redirect(auth_url)


@app.route("/google/oauth2callback")
def google_oauth2callback():
    try:
        logger.debug("LOG: Query string en oauth2callback: %s", dict(request.args))
        error = request.args.get("error")
        if error:
            logger.error("LOG: Error en OAuth: %s", error)
            return render_template("error.html", message=f"Error en OAuth: {error}", detail=""), 400

        code = request.args.get("code")
        state = request.args.get("state")

        if not code:
            logger.error("LOG: Falta 'code' en la respuesta de Google")
            return render_template("error.html", message="Falta 'code' en la respuesta de Google", detail=""), 400

        profile_id = state or session.get("profile_id")
        if not profile_id:
            logger.error("LOG: No se ha podido determinar el profile_id en callback")
            return render_template("error.html", message="No se ha podido determinar el profile_id", detail=""), 400

        logger.info("LOG: Recibido callback OAuth para profile_id=%s", profile_id)

        token_url = "https://oauth2.googleapis.com/token"
        data = {
            "code": code,
            "client_id": GOOGLE_CLIENT_ID,
            "client_secret": GOOGLE_CLIENT_SECRET,
            "redirect_uri": GOOGLE_REDIRECT_URI,
            "grant_type": "authorization_code",
        }
        logger.debug("LOG: Solicitando tokens a Google con data=%s", data)

        token_resp = requests.post(token_url, data=data)
        logger.info("LOG: Respuesta Google tokens: %s %s", token_resp.status_code, token_resp.text)

        if token_resp.status_code != 200:
            logger.error("LOG: Error al obtener tokens: %s", token_resp.text)
            return render_template("error.html", message="Error al obtener tokens de Google", detail=token_resp.text), 400

        token_data = token_resp.json()
        access_token = token_data["access_token"]
        refresh_token = token_data.get("refresh_token")
        token_type = token_data.get("token_type", "Bearer")
        scope = token_data.get("scope", GOOGLE_SCOPES)
        expires_in = token_data.get("expires_in", 3600)

        if not refresh_token:
            logger.error("LOG: No se recibió refresh_token.")
            return render_template(
                "error.html",
                message="No se recibió refresh_token de Google",
                detail="Revisa 'prompt=consent' y 'access_type=offline' en la configuración de OAuth."
            ), 400

        expiry = datetime.now(timezone.utc) + timedelta(seconds=expires_in)
        logger.debug("LOG: Tokens recibidos. Expira en: %s", expiry)

        conn = get_db_conn()
        conn.autocommit = True
        cur = conn.cursor()

        upsert_sql = """
        INSERT INTO profile_google_tokens (
            profile_id, access_token, refresh_token, token_type, scope, token_expiry
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (profile_id)
        DO UPDATE SET
            access_token = EXCLUDED.access_token,
            refresh_token = EXCLUDED.refresh_token,
            token_type  = EXCLUDED.token_type,
            scope       = EXCLUDED.scope,
            token_expiry = EXCLUDED.token_expiry,
            updated_at  = now();
        """
        logger.debug("LOG: Ejecutando UPSERT de tokens para profile_id=%s", profile_id)

        cur.execute(
            upsert_sql,
            (profile_id, access_token, refresh_token, token_type, scope, expiry),
        )
        cur.close()
        conn.close()

        logger.info("LOG: Tokens guardados/actualizados para profile_id=%s", profile_id)

        profile_email = None

        return render_template(
            "oauth_success.html",
            profile_id=profile_id,
            profile_email=profile_email,
        )
    except Exception as e:
        logger.exception("LOG: Excepción en oauth2callback: %s", e)
        return render_template(
            "error.html",
            message="Ha ocurrido un error al conectar con Google Calendar.",
            detail=str(e),
        ), 500


@app.route("/google/calendar/from_task", methods=["POST"])
def create_event_from_task():
    try:
        raw_data = request.data.decode("utf-8", errors="replace")
        logger.info("LOG: Petición /google/calendar/from_task body RAW: %s", raw_data)

        data = request.get_json(silent=True) or {}
        logger.info("LOG: JSON parseado en /from_task: %s", data)

        annotation_task_id = data.get("annotation_task_id")

        if not annotation_task_id:
            logger.warning("LOG: annotation_task_id no enviado en la petición")
            return jsonify({"error": "annotation_task_id es requerido"}), 400

        logger.info("LOG: Creando evento desde annotation_task_id=%s", annotation_task_id)

        task = fetch_task_context(annotation_task_id)
        if not task:
            logger.warning("LOG: No se encontró contexto de tarea para annotation_task_id=%s", annotation_task_id)
            return jsonify({"error": "No se ha encontrado la tarea o sus datos asociados"}), 404

        event, err = create_calendar_event_from_task_context(task)
        if err or not event:
            logger.error("LOG: Error creando evento desde tarea: %s", err)
            return jsonify({"error": err or "Error desconocido creando el evento"}), 500

        logger.info("LOG: Evento creado correctamente. google_event_id=%s", event.get("id"))

        return jsonify({
            "status": "ok",
            "google_event_id": event.get("id"),
            "htmlLink": event.get("htmlLink"),
            "summary": event.get("summary"),
        }), 200
    except Exception as e:
        logger.exception("LOG: Excepción en /google/calendar/from_task: %s", e)
        return jsonify({"error": f"Excepción interna: {e}"}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=3000, debug=True)
