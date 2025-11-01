# Guía de Logging Estandarizado

## Niveles de Logging

### CRITICAL
Fallos catastróficos que requieren intervención inmediata.
- Ejemplo: "Base de datos inaccesible tras 3 reintentos"

### ERROR
Fallos que impiden completar una operación específica.
- Ejemplo: "No se pudo enviar mensaje a cliente (token inválido)"

### WARNING
Situaciones anómalas recuperables o degradación del servicio.
- Ejemplo: "Plantilla no disponible, usando configuración por defecto"

### INFO
**Eventos de negocio visibles para operadores humanos.**
- Ejemplo: "Cliente recibió notificación de cita agendada"
- Usar voz activa y lenguaje natural
- Máximo 120 caracteres
- Sin IDs técnicos (moverlos a extra_data)

### DEBUG
Detalles técnicos para debugging (payloads, IDs, estados internos).
- Ejemplo: "Payload enviado a WhatsApp API: {...}"

---

## Acciones Estandarizadas (action)

### Webhooks y Mensajería
- `inbound.webhook` - Webhook recibido
- `message.received` - Mensaje entrante procesado
- `message.sent` - Mensaje saliente enviado
- `message.auto_reply_sent` - Auto-respuesta enviada
- `message.failed` - Fallo al enviar mensaje
- `template.sent` - Plantilla enviada
- `template.list` - Plantillas listadas
- `media.uploaded` - Archivo multimedia subido
- `media.downloaded` - Archivo multimedia descargado

### Flujos Conversacionales
- `flow.started` - Flujo conversacional iniciado
- `flow.completed` - Flujo conversacional completado
- `flow.exit` - Salida de flujo solicitada

### Gestión de Leads
- `lead.created` - Lead creado
- `lead.updated` - Lead actualizado
- `lead.assigned` - Lead asignado a responsable

### Calendario y Tareas
- `calendar.task_created` - Tarea de calendario creada
- `calendar.reminder_sent` - Recordatorio enviado

### Sistema
- `config.loaded` - Configuración cargada
- `db.query` - Consulta a base de datos
- `api.call` - Llamada a API externa

---

## Outcomes Estandarizados

- `ok` - Operación exitosa
- `queued` - Operación encolada para procesamiento
- `skipped` - Operación omitida (ej: fuera de horario)
- `invalid_input` - Entrada inválida del usuario
- `rate_limited` - Limitación de tasa aplicada
- `not_found` - Recurso no encontrado
- `unauthorized` - Sin autorización
- `error` - Error genérico

---

## Estilo de Mensajes INFO

### ✅ CORRECTO (lenguaje natural, conciso)
```python
blog.event(
    "Cliente recibió plantilla de confirmación",
    action='template.sent',
    outcome='ok',
    phone=clean_phone,
    tenant=company_id,
    template='agendar_llamada_inicial'
)
```

### ❌ INCORRECTO (jerga técnica, verboso)
```python
logger.info(
    f"Template agendar_llamada_inicial sent successfully to {clean_phone} "
    f"with message_id {message_id} using WABA_ID {waba_id}"
)
```

### Migración del incorrecto al correcto:
```python
# Mensaje humano en INFO
blog.event(
    "Cliente recibió plantilla de confirmación",
    action='template.sent',
    outcome='ok',
    phone=clean_phone,
    tenant=company_id
)

# Detalles técnicos en DEBUG
blog.debug_detail(
    "Detalles técnicos de envío",
    message_id=message_id,
    waba_id=waba_id,
    template_name='agendar_llamada_inicial'
)
```

---

## Contexto Multi-Tenant

### Enlazar contexto al inicio de cada petición:
```python
from logging_setup import bind_context, clear_context

@app.before_request
def setup_request_logging():
    # Extraer tenant de headers, params o payload
    company_id = request.headers.get('X-Company-ID') or extract_company_from_payload()
    phone = extract_phone_from_request()
    trace_id = request.headers.get('X-Trace-ID') or str(uuid4())[:8]
    
    bind_context(tenant=company_id, phone=phone, trace_id=trace_id)

@app.after_request
def cleanup_request_logging(response):
    clear_context()
    return response
```

### Uso en código:
```python
# El contexto se propaga automáticamente
blog = BusinessLogger('whatsapp')
blog.event("Mensaje procesado correctamente", action='message.received', outcome='ok')
# Tendrá automáticamente: tenant, phone, trace_id del contexto
```
