# WhatsApp Webhook Service

Servicio de webhook para WhatsApp Business Cloud API con soporte extendido de MIME types.

## Características

- ✅ Soporte extendido de MIME types (cualquier tipo hasta 100MB)
- ✅ Integración con Supabase Storage
- ✅ Auto-respuestas fuera de horario
- ✅ Gestión de templates
- ✅ Procesamiento de archivos multimedia
- ✅ Flow exit management

## Instalación

1. **Clonar repositorio:**
   ```bash
   git clone <repo-url>
   cd webhooks
   ```

2. **Crear entorno virtual:**
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```

3. **Instalar dependencias:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configurar:**
   ```bash
   cp config/scripts.conf.example config/scripts.conf
   # Editar config/scripts.conf con tus credenciales
   ```

## Uso

1. **Activar entorno:**
   ```bash
   source venv/bin/activate
   ```

2. **Ejecutar:**
   ```bash
   python app.py
   ```

## Endpoints

- `GET /health` - Health check
- `POST /webhook` - WhatsApp webhook
- `POST /send_message` - Enviar mensaje
- `POST /send_template` - Enviar template
- `POST /send_file` - Enviar archivo
- `GET /supported_types` - Tipos MIME soportados

## Desarrollo

```bash
# Activar entorno
source venv/bin/activate

# Instalar nueva dependencia
pip install nueva-libreria
pip freeze > requirements.txt

# Desactivar entorno
deactivate
```

## Logs

Los logs se guardan en `logs/webhook.log` cuando está configurado.

## Testing

```bash
curl http://localhost:5041/health
```
