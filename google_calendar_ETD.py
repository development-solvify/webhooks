import os
import logging
from datetime import datetime, timedelta, timezone

import psycopg2
import requests
import configparser
from flask import Flask, redirect, request, session

# ============================================================
# CONFIGURACIÓN: scripts.conf + entorno
# ============================================================

# Ruta del fichero de config.
# Puedes sobrescribirla con la variable SCRIPTS_CONF_PATH si quieres.
DEFAULT_CONF_PATH = os.path.join(os.path.dirname(__file__), "scripts.conf")
CONF_PATH = os.environ.get("SCRIPTS_CONF_PATH", DEFAULT_CONF_PATH)

config = configparser.ConfigParser()
read_files = config.read(CONF_PATH)

if not read_files:
    raise RuntimeError(f"No se ha podido leer scripts.conf en: {CONF_PATH}")

# ------- GOOGLE -------
if "GOOGLE" not in config:
    raise RuntimeError("No se ha encontrado la sección [GOOGLE] en scripts.conf")

GOOGLE_CLIENT_ID = config.get("GOOGLE", "GOOGLE_CLIENT_ID", fallback=None)
GOOGLE_CLIENT_SECRET = config.get("GOOGLE", "GOOGLE_CLIENT_SECRET", fallback=None)
GOOGLE_REDIRECT_URI = config.get("GOOGLE", "GOOGLE_REDIRECT_URI", fallback=None)

if not (GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET and GOOGLE_REDIRECT_URI):
    raise RuntimeError("Faltan GOOGLE_CLIENT_ID / GOOGLE_CLIENT_SECRET / GOOGLE_REDIRECT_URI en [GOOGLE]")

GOOGLE_SCOPES = "https://www.googleapis.com/auth/calendar.events"

# ------- DB (SUPABASE POSTGRES) -------
if "DB" not in config:
    raise RuntimeError("No se ha encontrado la sección [DB] en scripts.conf")

DB_HOST = config.get("DB", "DB_HOST")
DB_PORT = config.getint("DB", "DB_PORT", fallback=5432)
DB_NAME = config.get("DB", "DB_NAME")
DB_USER = config.get("DB", "DB_USER")
DB_PASS = config.get("DB", "DB_PASS")

# SECRET_KEY para Flask (puede ir también a scripts.conf si quieres)
SECRET_KEY = os.environ.get("SECRET_KEY", "dev-secret-change-me")

# Profile de pruebas (cámbialo por un profile_id real de tu tabla profiles)
DEMO_PROFILE_ID = os.environ.get(
    "DEMO_PROFILE_ID",
    "d20d2784-84eb-4495-9ba7-244d062f1d18"
)

# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("google-calendar-demo")

# ============================================================
# APP FLASK
# ============================================================

app = Flask(__name__)
app.secret_key = SECRET_KEY


def get_db_conn():
    """
    Abre una conexión a la BD Supabase (Postgres) usando [DB] de scripts.conf.
    """
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        sslmode="require",  # Supabase requiere SSL
    )
    return conn



def build_google_auth_url(state: str) -> str:
    """
    Construye la URL de autorización de Google OAuth 2.0.
    """
    from urllib.parse import urlencode

    params = {
        "client_id": GOOGLE_CLIENT_ID,
        "redirect_uri": GOOGLE_REDIRECT_URI,
        "response_type": "code",
        "scope": GOOGLE_SCOPES,
        "access_type": "offline",          # importante para refresh_token
        "include_granted_scopes": "true",
        "prompt": "consent",               # fuerza consentimiento y refresh_token
        "state": state,
    }
    return "https://accounts.google.com/o/oauth2/v2/auth?" + urlencode(params)


# ============================================================
# RUTAS
# ============================================================

@app.route("/")
def index():
    """
    Healthcheck simple.
    """
    return (
        "<h1>Google Calendar Demo</h1>"
        "<p>Ve a <a href='/google/login'>/google/login</a> para probar el flujo OAuth y crear un evento de prueba.</p>"
    )


@app.route("/google/login")
def google_login():
    """
    Inicia el flujo OAuth con Google.
    Para la demo, usamos DEMO_PROFILE_ID como profile_id del usuario.
    """
    profile_id = DEMO_PROFILE_ID
    session["profile_id"] = profile_id

    logger.info("Iniciando OAuth para profile_id=%s", profile_id)
    auth_url = build_google_auth_url(state=profile_id)
    return redirect(auth_url)


@app.route("/google/oauth2callback")
def google_oauth2callback():
    """
    Callback de Google OAuth:
      - Recibe ?code=...
      - Intercambia por tokens
      - Guarda/actualiza en profile_google_tokens
      - Crea un evento de prueba en el Calendar
      - Muestra info por pantalla
    """
    error = request.args.get("error")
    if error:
        logger.error("Error en OAuth: %s", error)
        return f"Error en OAuth: {error}", 400

    code = request.args.get("code")
    state = request.args.get("state")

    if not code:
        logger.error("Falta 'code' en la respuesta de Google")
        return "Falta 'code' en la respuesta de Google", 400

    profile_id = state or session.get("profile_id")
    if not profile_id:
        logger.error("No se ha podido determinar el profile_id")
        return "No se ha podido determinar el profile_id", 400

    logger.info("Recibido callback OAuth para profile_id=%s", profile_id)

    # 1) Intercambiar code por tokens
    token_url = "https://oauth2.googleapis.com/token"
    data = {
        "code": code,
        "client_id": GOOGLE_CLIENT_ID,
        "client_secret": GOOGLE_CLIENT_SECRET,
        "redirect_uri": GOOGLE_REDIRECT_URI,
        "grant_type": "authorization_code",
    }
    token_resp = requests.post(token_url, data=data)
    if token_resp.status_code != 200:
        logger.error("Error al obtener tokens: %s", token_resp.text)
        return f"Error al obtener tokens: {token_resp.text}", 400

    token_data = token_resp.json()
    access_token = token_data["access_token"]
    refresh_token = token_data.get("refresh_token")
    token_type = token_data.get("token_type", "Bearer")
    scope = token_data.get("scope", GOOGLE_SCOPES)
    expires_in = token_data.get("expires_in", 3600)

    if not refresh_token:
        logger.error("No se recibió refresh_token. Revisa 'prompt=consent' y 'access_type=offline'.")
        return "No se recibió refresh_token. Revisa 'prompt=consent' y 'access_type=offline'.", 400

    expiry = datetime.now(timezone.utc) + timedelta(seconds=expires_in)

    # 2) Guardar/actualizar tokens en la tabla profile_google_tokens
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
    cur.execute(
        upsert_sql,
        (profile_id, access_token, refresh_token, token_type, scope, expiry),
    )
    cur.close()
    conn.close()

    logger.info("Tokens guardados/actualizados para profile_id=%s", profile_id)

    # 3) Creamos una "tarea de pruebas" para verificar que todo funciona
    demo_task = {
        "title": "Tarea programada de PRUEBA",
        "description": "Creada desde Sicuel/Solvify para probar integración con Google Calendar.",
    }

    event, error_msg = create_demo_calendar_event(profile_id, demo_task)
    if error_msg:
        logger.error("Tokens guardados, pero error creando evento: %s", error_msg)
        return f"Tokens guardados, pero error creando evento: {error_msg}", 500

    logger.info("Evento de prueba creado correctamente: %s", event.get("id"))

    # 4) Mostrar info por pantalla
    html = f"""
    <h1>Google Calendar conectado correctamente ✅</h1>
    <p>Profile ID (demo): <code>{profile_id}</code></p>

    <h2>Evento de prueba creado</h2>
    <ul>
      <li><strong>Resumen:</strong> {event.get('summary')}</li>
      <li><strong>Inicio:</strong> {event.get('start')}</li>
      <li><strong>Fin:</strong> {event.get('end')}</li>
      <li><strong>Link:</strong> <a href="{event.get('htmlLink')}" target="_blank">Abrir en Google Calendar</a></li>
    </ul>

    <h3>Detalles técnicos</h3>
    <pre>{event}</pre>
    """
    return html


# ============================================================
# HELPERS PARA TOKENS Y EVENTOS
# ============================================================

def get_fresh_access_token(profile_id: str):
    """
    Obtiene un access_token válido para el profile_id.
    Si está caducado (o cerca de caducar), lo renueva con el refresh_token.
    """
    conn = get_db_conn()
    cur = conn.cursor()
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
        cur.close()
        conn.close()
        return None, "No hay tokens para este profile_id"

    id_, access_token, refresh_token, token_type, scope, token_expiry = row
    now = datetime.now(timezone.utc)

    # Si aún no caduca en menos de 2 minutos, lo usamos
    if token_expiry and token_expiry > now + timedelta(minutes=2):
        cur.close()
        conn.close()
        return access_token, None

    # Renovar access_token con el refresh_token
    token_url = "https://oauth2.googleapis.com/token"
    data = {
        "client_id": GOOGLE_CLIENT_ID,
        "client_secret": GOOGLE_CLIENT_SECRET,
        "refresh_token": refresh_token,
        "grant_type": "refresh_token",
    }
    resp = requests.post(token_url, data=data)
    if resp.status_code != 200:
        cur.close()
        conn.close()
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
    cur.close()
    conn.close()

    logger.info("Access token renovado para profile_id=%s", profile_id)
    return new_access, None


def create_demo_calendar_event(profile_id: str, task: dict):
    """
    Crea un evento de prueba en el calendario 'primary' del usuario autenticado,
    invitando a isidoro@gmail.com como asistente.
    """
    access_token, err = get_fresh_access_token(profile_id)
    if err or not access_token:
        return None, err or "No access token disponible"

    # Evento: ahora + 1h, duración 30 minutos
    start_dt = datetime.now(timezone.utc) + timedelta(hours=1)
    end_dt = start_dt + timedelta(minutes=30)

    event_body = {
        "summary": task["title"],
        "description": task.get("description", ""),
        "start": {
            "dateTime": start_dt.isoformat(),
            "timeZone": "Europe/Madrid",
        },
        "end": {
            "dateTime": end_dt.isoformat(),
            "timeZone": "Europe/Madrid",
        },
        "attendees": [
            {"email": "isidoro@gmail.com"}
        ]
    }

    url = "https://www.googleapis.com/calendar/v3/calendars/primary/events"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    logger.info("Creando evento de prueba en Calendar para profile_id=%s", profile_id)
    resp = requests.post(url, headers=headers, json=event_body)
    if resp.status_code not in (200, 201):
        logger.error("Error creando evento: %s %s", resp.status_code, resp.text)
        return None, f"Error creando evento: {resp.status_code} {resp.text}"

    return resp.json(), None


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    # Asegúrate de tener scripts.conf en el mismo directorio
    # y que NGiNX ya está proxy_pass /google/ -> 127.0.0.1:3000
    app.run(host="0.0.0.0", port=3000, debug=True)
