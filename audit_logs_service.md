📘 SICUEL – Audit Logs Humanizer Service

Microservicio Flask para conversión de audit logs a formato humano

Este microservicio forma parte del ecosistema SICUEL.
Su función es consultar la tabla audit_logs en Supabase y convertir los cambios técnicos en descripciones comprensibles, pensadas para el timeline del Deal en el frontend.

✨ Características principales

API REST /audit/deal/<deal_id>

Conversión de INSERT / UPDATE / DELETE a texto humano

Detección de cambios campo a campo entre old_values y new_values

Normalización de campos: estado, comercial asignado, oficina…

Lookups automáticos a profiles y company_addresses

CORS restringido a /audit/*

Conexión SSL con Supabase

Compatible con systemd (servicio Linux)

Puede servir HTTPS directo o vía Nginx

🏗️ Estructura del proyecto
/webhooks
 ├── audit_logs_service.py        # Microservicio Flask
 ├── scripts.conf                 # Config Supabase
 ├── cert.pem / key.pem           # Certificados SSL (opcional)
 └── README.md                    # Este archivo

🔧 Configuración

El servicio lee automáticamente scripts.conf desde la misma carpeta:

[DB]
DB_HOST = your-db-host
DB_PORT = 6543
DB_NAME = postgres
DB_USER = your_user
DB_PASS = your_password

🚀 Ejecución manual
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
    }
  ]
}

POST /audit/deal
curl -X POST "https://<host>:5115/audit/deal" \
  -H "Content-Type: application/json" \
  -d '{"deal_id": "b9f59..."}'
