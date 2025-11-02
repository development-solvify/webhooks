# ☁️ CloudAPI2 – Multi-Tenant WhatsApp Gateway

## 🧩 Descripción general

`CloudAPI2` es un **gateway multi-tenant** que centraliza la integración con la **Meta WhatsApp Business Cloud API** para múltiples compañías (tenants) desde un solo servicio.

Cada tenant tiene sus propias credenciales (`WABA_ID`, `PHONE_NUMBER_ID`, `ACCESS_TOKEN`, etc.) almacenadas en su configuración en base de datos y precargadas en memoria a través de `CompanyConfigCache`.

El sistema gestiona:
- Envío de plantillas y mensajes de texto/media  
- Recepción de mensajes y actualizaciones de estado  
- Descarga de archivos multimedia  
- Persistencia en `external_messages`  
- Cobertura automática de tokens y cabeceras por tenant  

---

## ⚙️ Arquitectura general

```
                   ┌────────────────────────────┐
                   │ Meta WhatsApp Cloud API    │
                   │ (Webhook + Graph API v22)  │
                   └──────────────┬─────────────┘
                                  │
                                  ▼
 ┌─────────────────────────────────────────────────────────┐
 │                     CloudAPI2 (Flask)                   │
 │                                                         │
 │  ┌────────────────────────┐   ┌───────────────────────┐ │
 │  │ /<company_id>/webhook  │ → │ webhook_company()     │ │
 │  └────────────────────────┘   └───────────────────────┘ │
 │     │  mensajes entrantes / status                     │
 │     ▼                                                  │
 │  save_external_message() / update_status()             │
 │  + process_whatsapp_media_extended()                   │
 │                                                         │
 │  ┌────────────────────────┐   ┌───────────────────────┐ │
 │  │ /send_template_direct  │ → │ send_template_message │ │
 │  │ /send_text_direct      │ → │ send_text_message     │ │
 │  └────────────────────────┘   └───────────────────────┘ │
 │                                                         │
 │  company_cache  ←→  DB (companies, custom_properties)    │
 │                                                         │
 │  external_messages (mensajería unificada)               │
 └─────────────────────────────────────────────────────────┘
```

---

## 🏗️ Resolución multi-tenant

Cada request identifica el **tenant (compañía)** por alguno de estos métodos:

| Contexto | Resolución |
|-----------|-------------|
| Webhook inbound (`/<company_id>/webhook`) | `company_id` en la URL |
| Envío directo (`send_template_direct`) | `company_id` en el body JSON |
| Envío interno (`send_template_message`) | inferido desde teléfono (`get_whatsapp_credentials_for_phone`) |

### CompanyConfigCache

La clase `CompanyConfigCache` mantiene en memoria los datos de configuración de cada compañía:

```python
{
  'id': 'uuid',
  'name': 'Solvify',
  'config': {
      'custom_properties': {
          'WHATSAPP_ACCESS_TOKEN': 'EAA...',
          'WHATSAPP_PHONE_NUMBER_ID': '6743...',
          'WHATSAPP_BUSINESS_ID': '8197...',
          'WHATSAPP_COVER': 'https://.../cover.png'
      }
  }
}
```

Las configuraciones se cargan mediante:

```python
company_cache.preload_all_companies(db_manager)
```

y se acceden con:

```python
company_entry = company_cache.get(company_id)
custom_props = company_entry['config']['custom_properties']
```

---

## 💬 Envío de plantillas (multi-tenant)

### Endpoint

`POST /send_template_direct`

### Request JSON

```json
{
  "phone": "608684495",
  "template_name": "agendar_llamada_inicial",
  "template_data": {
    "first_name": "Paco",
    "deal_id": "b5078967-ff48-4f37-914f-52c5b64639e8"
  },
  "company_id": "a9242a58-4f5d-494c-8a74-45f8cee150e6",
  "phone_number_id": "674372385752523",
  "language": "es_ES"
}
```

### Flujo de ejecución

1. Valida los parámetros de entrada y normaliza el teléfono.
2. Llama a `whatsapp_service.send_template_message()` con `company_id` y `phone_number_id`.
3. El servicio:
   - Resuelve las credenciales del tenant.
   - Construye el payload mediante `_build_template_payload`.
   - Inserta automáticamente el *cover* definido en los settings del tenant.
   - Envía el mensaje a `https://graph.facebook.com/v22.0/{PHONE_NUMBER_ID}/messages`.
4. Registra el resultado en logs (no guarda en BD).

---

## 🖼️ Resolución del cover

Implementación minimalista — solo consulta la propiedad `WHATSAPP_COVER` de la compañía:

```python
def _resolve_cover_url(self, company_id: str | None) -> str | None:
    if not company_id:
        logger.warning("[COVER] No company_id provided")
        return None

    try:
        company_entry = company_cache.get(company_id)
        if not company_entry:
            logger.warning(f"[COVER] Company {company_id} not found in cache")
            return None

        custom_props = company_entry.get("config", {}).get("custom_properties", {})
        cover = custom_props.get("WHATSAPP_COVER")
        logger.info(f"[COVER] company_id={company_id} cover={cover}")
        return cover
    except Exception as e:
        logger.error(f"[COVER] Error resolving cover for company_id={company_id}: {e}")
        return None
```

Cada compañía puede definir su *cover* en `custom_properties`:
```
WHATSAPP_COVER = https://cdn.misitio.com/covers/solvify.png
```

---

## 📨 Persistencia en `external_messages`

Tabla: `public.external_messages`  
Índice único: `(company_id, last_message_uid)`

### Motivo del duplicado

WhatsApp envía dos tipos de webhooks con el mismo `wamid`:
- **messages** → creación del mensaje  
- **statuses** → actualización (`sent`, `delivered`, `read`)  

Antes ambos ejecutaban un `INSERT`, generando colisiones.  
Ahora:
- `messages` → `INSERT ... ON CONFLICT ... DO UPDATE`
- `statuses` → solo `UPDATE`.

### SQL idempotente

```sql
INSERT INTO public.external_messages (
  id, message, sender_phone, responsible_email, last_message_uid, last_message_timestamp,
  from_me, status, created_at, updated_at, is_deleted, chat_url, chat_id, assigned_to_id, company_id
)
VALUES (
  gen_random_uuid(), %s, %s, %s, %s, NOW(),
  %s, %s, NOW(), NOW(), FALSE, %s, %s, %s, %s
)
ON CONFLICT (company_id, last_message_uid) DO UPDATE
SET
  status = EXCLUDED.status,
  last_message_timestamp = NOW(),
  updated_at = NOW();
```

---

## 🧾 Webhook inbound (multi-tenant)

Cada tenant tiene su propio endpoint:

```
POST /<company_id>/webhook
```

Implementación:

```python
@app.route('/<company_id>/webhook', methods=['GET', 'POST'])
def webhook_company(company_id):
    if request.method == 'GET':
        return request.args.get("hub.challenge")
    elif request.method == 'POST':
        data = request.get_json()
        process_whatsapp_event(company_id, data)
        return jsonify({"status": "ok"}), 200
```

El webhook:
- Procesa `messages` → inserta y descarga media si aplica.  
- Procesa `statuses` → actualiza el registro existente.

---

## 🧱 Estructura del proyecto

```
CloudAPI2/
│
├── app/
│   ├── routes/
│   │   ├── webhooks.py
│   │   ├── send_template_direct.py
│   │   └── ...
│   ├── services/
│   │   ├── whatsapp_service.py
│   │   ├── file_service.py
│   │   └── ...
│   ├── utils/
│   │   ├── phone_utils.py
│   │   ├── rate_limit.py
│   │   └── ...
│   └── __init__.py
│
├── db/
│   ├── db_manager.py
│   └── ...
│
├── company_cache.py
│
├── CloudAPI2.py
│
└── README.md
```

---

## 🔒 Seguridad y control de flujo

- `@rate_limit(max_calls=20, window=60)` protege los endpoints.  
- Tokens de WhatsApp almacenados en `custom_properties`.  
- Normalización de teléfonos vía `PhoneUtils`.  
- Logs detallados con `company_id`, `pnid`, `template`, `cover`.  

---

## 🧪 Ejemplo de flujo completo

1️⃣ Envío desde `/send_template_direct`:
```
POST /send_template_direct
```

→ payload con credenciales de `company_id=a9242a58...`  
→ envía a Meta API  
→ log:

```
🔐 Sending template 'agendar_llamada_inicial' con tenant:
📱 Phone: 608684495 -> E164: 34608684495
🏢 Company ID: a9242a58-4f5d-494c-8a74-45f8cee150e6
📞 Phone Number ID: 674372385752523
🌐 Base URL: https://graph.facebook.com/v22.0/674372385752523/messages
[COVER] company_id=a9242a58... cover=https://pngimg.com/uploads/tree/tree_PNG2517.png
```

2️⃣ Webhook entrante:
```
POST /a9242a58-4f5d-494c-8a74-45f8cee150e6/webhook
```

→ `messages` → inserta registro  
→ `statuses` → actualiza estado (`sent`, `delivered`)

---

## 📦 Mejoras implementadas

| Área | Mejora |
|------|---------|
| **Multi-tenant** | Endpoints y servicios aceptan `company_id` y `phone_number_id`. |
| **Cover** | Resolución simple desde `custom_properties['WHATSAPP_COVER']`. |
| **Credenciales** | Dinámicas por tenant usando `CompanyConfigCache`. |
| **Persistencia** | Idempotencia con `ON CONFLICT` y separación `messages`/`statuses`. |
| **Webhook** | Endpoint único por tenant. |
| **Logs** | Contexto completo: tenant, PNID, template, cover. |
| **Errores** | Manejo seguro de duplicados y tokens inválidos. |

---

## 🧭 Próximos pasos

- Métricas por tenant (mensajes enviados/entregados/fallidos).  
- Endpoint de *healthcheck* por `company_id`.  
- Limpieza automática de mensajes antiguos.  
- Extender la arquitectura a email/SMS manteniendo el modelo multi-tenant.

---

### 🛠️ Autor

Equipo técnico – **Solvify / Sicuel Platform**

```
© 2025 – CloudAPI2 Multi-Tenant Gateway
```
