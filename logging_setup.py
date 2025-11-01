"""
Sistema de logging estandarizado multi-tenant para WhatsApp Webhook Service.

Características:
- Consola: formato legible y coloreado para humanos
- Archivo: JSON estructurado para procesamiento automático
- Contexto automático: tenant, phone, trace_id
- Rotación diaria de logs
- Filtrado de información sensible
"""

import logging
import json
import sys
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import Dict, Any, Optional
from contextvars import ContextVar
from uuid import uuid4

try:
    from rich.logging import RichHandler
    from rich.console import Console
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False


# =============================================================================
# CONTEXTO POR PETICIÓN (ContextVars)
# =============================================================================

_context_tenant: ContextVar[Optional[str]] = ContextVar('tenant', default=None)
_context_phone: ContextVar[Optional[str]] = ContextVar('phone', default=None)
_context_trace_id: ContextVar[Optional[str]] = ContextVar('trace_id', default=None)


def bind_context(tenant: Optional[str] = None, 
                 phone: Optional[str] = None, 
                 trace_id: Optional[str] = None):
    """
    Enlaza contexto para la petición actual (thread-safe con ContextVars).
    
    Uso típico:
        bind_context(tenant=company_id, phone=clean_phone, trace_id=str(uuid4()))
    """
    if tenant is not None:
        _context_tenant.set(tenant)
    if phone is not None:
        _context_phone.set(phone)
    if trace_id is not None:
        _context_trace_id.set(trace_id)
    elif _context_trace_id.get() is None:
        # Auto-generar trace_id si no existe
        _context_trace_id.set(str(uuid4())[:8])


def clear_context():
    """Limpia el contexto de la petición actual."""
    _context_tenant.set(None)
    _context_phone.set(None)
    _context_trace_id.set(None)


def get_context() -> Dict[str, Optional[str]]:
    """Obtiene el contexto actual."""
    return {
        'tenant': _context_tenant.get(),
        'phone': _context_phone.get(),
        'trace_id': _context_trace_id.get()
    }


# =============================================================================
# FORMATTERS
# =============================================================================

class HumanReadableFormatter(logging.Formatter):
    """
    Formatter para consola: mensajes concisos y legibles para humanos.
    Formato: [LEVEL] tenant/phone | action → msg (duration)
    """
    
    # Códigos de color ANSI (si Rich no está disponible)
    COLORS = {
        'DEBUG': '\033[36m',      # Cyan
        'INFO': '\033[32m',       # Green
        'WARNING': '\033[33m',    # Yellow
        'ERROR': '\033[31m',      # Red
        'CRITICAL': '\033[35m',   # Magenta
        'RESET': '\033[0m'
    }
    
    def format(self, record: logging.LogRecord) -> str:
        # Extraer contexto
        tenant = getattr(record, 'tenant', None) or _context_tenant.get() or 'unknown'
        phone = getattr(record, 'phone', None) or _context_phone.get()
        action = getattr(record, 'action', None)
        outcome = getattr(record, 'outcome', None)
        duration_ms = getattr(record, 'duration_ms', None)
        
        # Construir prefijo de contexto
        context_parts = [tenant]
        if phone:
            context_parts.append(phone[-4:])  # Solo últimos 4 dígitos
        context_str = '/'.join(context_parts)
        
        # Construir mensaje
        parts = []
        
        # Timestamp corto (HH:MM:SS)
        ts = datetime.fromtimestamp(record.created).strftime('%H:%M:%S')
        parts.append(ts)
        
        # Nivel
        level_str = f"[{record.levelname:8s}]"
        if not RICH_AVAILABLE and sys.stderr.isatty():
            level_str = f"{self.COLORS.get(record.levelname, '')}{level_str}{self.COLORS['RESET']}"
        parts.append(level_str)
        
        # Contexto
        parts.append(f"{context_str:20s}")
        
        # Acción (si existe)
        if action:
            parts.append(f"│ {action:25s} →")
        
        # Mensaje principal
        parts.append(record.getMessage())
        
        # Outcome y duración
        suffix_parts = []
        if outcome:
            suffix_parts.append(f"[{outcome}]")
        if duration_ms is not None:
            suffix_parts.append(f"({duration_ms}ms)")
        
        if suffix_parts:
            parts.append(' '.join(suffix_parts))
        
        return ' '.join(parts)


class StructuredJSONFormatter(logging.Formatter):
    """
    Formatter para archivo: JSON estructurado para procesamiento automático.
    """
    
    SENSITIVE_KEYS = {'access_token', 'token', 'password', 'api_key', 'Authorization'}
    
    def format(self, record: logging.LogRecord) -> str:
        # Base del log
        log_entry = {
            'ts': datetime.fromtimestamp(record.created).isoformat(),
            'level': record.levelname,
            'logger': record.name,
            'msg': record.getMessage(),
            'module': record.module,
            'func': record.funcName,
            'line': record.lineno
        }
        
        # Contexto multi-tenant
        log_entry['tenant'] = getattr(record, 'tenant', None) or _context_tenant.get() or 'unknown'
        log_entry['phone'] = getattr(record, 'phone', None) or _context_phone.get()
        log_entry['trace_id'] = getattr(record, 'trace_id', None) or _context_trace_id.get()
        
        # Campos de negocio
        if hasattr(record, 'action'):
            log_entry['action'] = record.action
        if hasattr(record, 'outcome'):
            log_entry['outcome'] = record.outcome
        if hasattr(record, 'duration_ms'):
            log_entry['duration_ms'] = record.duration_ms
        
        # Extra data (filtrado de info sensible)
        if hasattr(record, 'extra_data'):
            log_entry['extra'] = self._redact_sensitive(record.extra_data)
        
        # Exception info
        if record.exc_info:
            log_entry['exception'] = self.formatException(record.exc_info)
        
        return json.dumps(log_entry, ensure_ascii=False)
    
    def _redact_sensitive(self, data: Any) -> Any:
        """Redacta información sensible de forma recursiva."""
        if isinstance(data, dict):
            return {
                k: '***REDACTED***' if k in self.SENSITIVE_KEYS else self._redact_sensitive(v)
                for k, v in data.items()
            }
        elif isinstance(data, (list, tuple)):
            return [self._redact_sensitive(item) for item in data]
        return data


# =============================================================================
# CONFIGURACIÓN PRINCIPAL
# =============================================================================

def setup_logging(
    log_dir: str = 'logs',
    log_level: str = 'INFO',
    console_level: str = 'INFO',
    file_level: str = 'DEBUG',
    app_name: str = 'whatsapp_webhook'
) -> logging.Logger:
    """
    Configura el sistema de logging completo.
    
    Args:
        log_dir: Directorio para archivos de log
        log_level: Nivel base de logging
        console_level: Nivel para consola (humano)
        file_level: Nivel para archivo (máquina)
        app_name: Nombre de la aplicación
    
    Returns:
        Logger raíz configurado
    """
    # Crear directorio de logs
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)
    
    # Logger raíz
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level.upper()))
    
    # Limpiar handlers existentes
    root_logger.handlers.clear()
    
    # =================================================================
    # HANDLER DE CONSOLA (humano)
    # =================================================================
    
    if RICH_AVAILABLE:
        console_handler = RichHandler(
            show_time=False,  # Ya lo incluimos en el formatter
            show_path=False,
            markup=True,
            rich_tracebacks=True,
            tracebacks_show_locals=False
        )
    else:
        console_handler = logging.StreamHandler(sys.stderr)
    
    console_handler.setLevel(getattr(logging, console_level.upper()))
    console_handler.setFormatter(HumanReadableFormatter())
    root_logger.addHandler(console_handler)
    
    # =================================================================
    # HANDLER DE ARCHIVO (máquina, rotación diaria)
    # =================================================================
    
    file_handler = TimedRotatingFileHandler(
        filename=log_path / f'{app_name}.log',
        when='midnight',
        interval=1,
        backupCount=30,  # 30 días de retención
        encoding='utf-8'
    )
    file_handler.setLevel(getattr(logging, file_level.upper()))
    file_handler.setFormatter(StructuredJSONFormatter())
    root_logger.addHandler(file_handler)
    
    # =================================================================
    # SILENCIAR LOGGERS RUIDOSOS
    # =================================================================
    
    for noisy_logger in ['werkzeug', 'urllib3', 'requests', 'httpcore', 'hpack']:
        logging.getLogger(noisy_logger).setLevel(logging.WARNING)
        logging.getLogger(noisy_logger).propagate = False
    
    # Log inicial
    root_logger.info(
        f"Sistema de logging inicializado para {app_name}",
        extra={
            'action': 'logging.initialized',
            'outcome': 'ok',
            'extra_data': {
                'console_level': console_level,
                'file_level': file_level,
                'log_dir': str(log_path),
                'rich_enabled': RICH_AVAILABLE
            }
        }
    )
    
    return root_logger


# =============================================================================
# UTILIDADES DE LOGGING
# =============================================================================

class BusinessLogger:
    """
    Wrapper para simplificar el logging de eventos de negocio.
    
    Uso:
        blog = BusinessLogger('whatsapp')
        blog.event('Mensaje recibido de cliente', 
                   action='message.received',
                   outcome='ok',
                   phone='679609016',
                   tenant=company_id)
    """
    
    def __init__(self, logger_name: str):
        self.logger = logging.getLogger(logger_name)
    
    def event(self, message: str, *, 
              action: Optional[str] = None,
              outcome: Optional[str] = None,
              phone: Optional[str] = None,
              tenant: Optional[str] = None,
              duration_ms: Optional[float] = None,
              **extra_data):
        """Registra un evento de negocio (nivel INFO)."""
        extra = {
            'action': action,
            'outcome': outcome,
            'phone': phone or _context_phone.get(),
            'tenant': tenant or _context_tenant.get(),
            'trace_id': _context_trace_id.get()
        }
        
        if duration_ms is not None:
            extra['duration_ms'] = round(duration_ms, 2)
        
        if extra_data:
            extra['extra_data'] = extra_data
        
        self.logger.info(message, extra=extra)
    
    def debug_detail(self, message: str, **details):
        """Registra detalles técnicos (nivel DEBUG)."""
        extra = {
            'tenant': _context_tenant.get(),
            'phone': _context_phone.get(),
            'trace_id': _context_trace_id.get(),
            'extra_data': details
        }
        self.logger.debug(message, extra=extra)
    
    def error(self, message: str, *, 
              action: Optional[str] = None,
              exc_info: bool = True,
              **extra_data):
        """Registra un error de negocio."""
        extra = {
            'action': action,
            'outcome': 'error',
            'tenant': _context_tenant.get(),
            'phone': _context_phone.get(),
            'trace_id': _context_trace_id.get(),
            'extra_data': extra_data
        }
        self.logger.error(message, extra=extra, exc_info=exc_info)


# =============================================================================
# DECORATOR PARA MEDIR DURACIÓN
# =============================================================================

import time
from functools import wraps

def log_duration(action: str, outcome_on_success: str = 'ok'):
    """
    Decorator para medir y loguear la duración de una función.
    
    Uso:
        @log_duration('template.send')
        def send_template(...):
            ...
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_ms = time.time() * 1000
            success = True
            outcome = outcome_on_success
            
            try:
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                success = False
                outcome = 'error'
                raise
            finally:
                duration_ms = (time.time() * 1000) - start_ms
                
                logger = logging.getLogger(func.__module__)
                level = logging.INFO if success else logging.ERROR
                
                logger.log(
                    level,
                    f"Operación completada: {func.__name__}",
                    extra={
                        'action': action,
                        'outcome': outcome,
                        'duration_ms': round(duration_ms, 2),
                        'tenant': _context_tenant.get(),
                        'phone': _context_phone.get(),
                        'trace_id': _context_trace_id.get()
                    }
                )
        
        return wrapper
    return decorator
