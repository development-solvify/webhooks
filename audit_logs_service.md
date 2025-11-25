📘 SICUEL – Audit Logs Humanizer Service

Microservicio Flask para conversión de audit logs a formato humano

Este microservicio forma parte del ecosistema SICUEL.
Su función es consultar la tabla audit_logs de Supabase y convertir los cambios técnicos en una descripción humana que pueda ser mostrada en el frontend (timeline de actividad del Deal).

✨ Características principales

API REST en Flask (/audit/deal/<deal_id>).

Devuelve cambios INSERT/UPDATE/DELETE en texto humano.

Analiza old_values y new_values y detecta diferencias campo a campo.

Traduce campos especiales (status, comercial asignado, oficina…).

Lookups: perfiles (profiles) y oficinas (company_addresses).

CORS activado solo para rutas /audit/*.

Conexión a Supabase Postgres con SSL.

Preparado para ejecutarse como servicio systemd.

Opción de servir vía HTTPS directo o mediante Nginx reverse proxy.

Certificados Let’s Encrypt o autofirmados.

🏗️ Estructura del proyecto
/webhooks
 ├── audit_logs_service.py        # Microservicio Flask
 ├── scripts.conf                 # Configuración: credenciales Supabase
 ├── cert.pem / key.pem           # Certificados SSL (opcional)
 └── README.md                    # Este archivo

🔧 Configuración
1. scripts.conf

El servicio carga automáticamente scripts.conf desde la misma carpeta.

Ejemplo:

[DB]
DB_HOST = your-db-host
DB_PORT = 6543
DB_NAME = postgres
DB_USER = your_user
DB_PASS = your_password

🚀 Ejecutar manualmente
HTTP
export HTTP_PORT=5115
python3 audit_logs_service.py

HTTPS
export HTTP_PORT=5115
export SSL_CERT=/home/isidoro/webhooks/cert.pem
export SSL_KEY=/home/isidoro/webhooks/key.pem
python3 audit_logs_service.py

🧩 Endpoints
GET /audit/deal/<deal_id>

Devuelve todos los logs asociados a un Deal en formato humano.

Ejemplo:

curl -X GET "https://<host>:5115/audit/deal/<deal_id>"


Respuesta:

{
  "deal_id": "b9f59...",
  "count": 4,
  "human_logs": [
    {
      "type": "INSERT",
      "at": "2025-11-21 17:20:50",
      "title": "Creación del Deal: LSO - Miguel fuentes jimenez",
      "detail": "Se creó el deal. Nombre: LSO - Miguel fuentes jimenez"
    },
    {
      "type": "UPDATE",
      "at": "2025-11-21 17:16:49",
      "title": "Actualización del Deal: LSO - Hugo Martin",
      "detail": [
        "Cambio de estado: 'No contesta' → 'NC 1er intento'",
        "Comercial asignado: Juan Pérez <juan@example.com> → María López <mlopez@example.com>"
      ]
    }
  ]
}

POST /audit/deal
curl -X POST "https://<host>:5115/audit/deal" \
  -H "Content-Type: application/json" \
  -d '{"deal_id": "b9f59..."}'

🛠️ Instalar como servicio systemd

Crear:

sudo nano /etc/systemd/system/audit_logs.service


Contenido:

[Unit]
Description=SICUEL Audit Logs Humanizer Service
After=network.target

[Service]
Type=simple
User=isidoro
WorkingDirectory=/home/isidoro/webhooks
ExecStart=/usr/bin/python3 /home/isidoro/webhooks/audit_logs_service.py
Restart=always
RestartSec=5

StandardOutput=append:/var/log/audit_logs_service.log
StandardError=append:/var/log/audit_logs_service_error.log

Environment="PYTHONUNBUFFERED=1"
Environment="SCRIPTS_CONF_PATH=/home/isidoro/webhooks/scripts.conf"
Environment="HTTP_PORT=5115"
Environment="SSL_CERT=/home/isidoro/webhooks/cert.pem"
Environment="SSL_KEY=/home/isidoro/webhooks/key.pem"

[Install]
WantedBy=multi-user.target


Activar:

sudo systemctl daemon-reload
sudo systemctl enable audit_logs.service
sudo systemctl start audit_logs.service
sudo systemctl status audit_logs.service

🌐 Integración con Nginx (Producción recomendada)

Usar Flask solo en HTTP interno y Nginx con Let’s Encrypt en 443.

Ejemplo:

server {
    listen 443 ssl;
    server_name api.sicuel.io;

    ssl_certificate /etc/letsencrypt/live/api.sicuel.io/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/api.sicuel.io/privkey.pem;

    location /audit/ {
        proxy_pass http://127.0.0.1:5115;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}

🧪 Testing rápido
HTTP
curl -i http://127.0.0.1:5115/audit/deal/<deal_id>

HTTPS (autofirmado)
curl -k -i https://127.0.0.1:5115/audit/deal/<deal_id>

Desde navegador
https://api.sicuel.io/audit/deal/<deal_id>

🛃 Troubleshooting
❌ “Address already in use”
sudo ss -lntp | grep 5115
sudo kill <PID>

❌ “no alternative certificate subject name”

El certificado no coincide con la IP → usar dominio o curl -k.

❌ “Bad request version '\x16\x03'”

Intentaste enviar HTTPS a un puerto HTTP.

❌ Error al leer scripts.conf

Verificar ruta en:

Environment="SCRIPTS_CONF_PATH=/home/isidoro/webhooks/scripts.conf"

📄 Licencia

Propietario © SICUEL / Lex Monkeys Solutions S.L.