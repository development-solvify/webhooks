import logging
import json
import requests
from requests.exceptions import HTTPError
from flask import Flask, request, jsonify
import os
import re
import configparser
import pg8000
import datetime
from pathlib import Path

# ----------------------------------------------------------------------------
# Configuración y constantes
# ----------------------------------------------------------------------------
TOKEN = os.getenv('SOLVIFY_API_TOKEN',
    'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IjEyMmZlYTI1LWQ1OWEtNGE2Zi04YzQ0LWIzZTVmZTExZTZmZSIsImVtYWlsIjoic2VydmljZUBzb2x2aWZ5LmVzIiwiZmlyc3RfbmFtZSI6IlNlcnZpY2UiLCJsYXN0X25hbWUiOiJTb2x2aWZ5IiwiaXNfYWN0aXZlIjp0cnVlLCJjcmVhdGVkX2F0IjoiMjAyNC0xMC0xN1QxNzowODozOC4xNjY3OTEiLCJjcmVhdGVkX2J5IjpudWxsLCJ1cGRhdGVkX2F0IjoiMjAyNC0xMC0xN1QxNTowODozOC45OCIsInVwZGF0ZWRfYnkiOm51bGwsImRlbGV0ZWRfYXQiOm51bGwsImRlbGV0ZWRfYnkiOm51bGwsImlzX2RlbGV0ZWQiOmZhbHNlLCJyb2xlX2lkIjoiODQ5ZmFiZTgtNDhjYi00ZWY4LWE0YWUtZTJiN2MzZjNlYTViIiwic3RyaXBlX2N1c3RvbWVyX2lkIjpudWxsLCJleHBvX3B1c2hfdG9rZW4iOm51bGwsInBob25lIjoiMCIsInJvbGVfbmFtZSI6IkFETUlOIiwicm9sZXMiOltdLCJpYXQiOjE3MjkxNzc4OTIsImV4cCI6Nzc3NzE3Nzg5Mn0.TJWtiOnLW8XyWjQDR_LAWvEiqrw50tWUmYiKXxo_5Wg')

app = Flask(__name__)
app.logger.setLevel(logging.DEBUG)

# ----------------------------------------------------------------------------
# Carga de configuración DB
# ----------------------------------------------------------------------------
config_supabase = configparser.ConfigParser()
if os.path.exists('scripts.conf'):
    try:
        config_supabase.read('scripts.conf')
        app.logger.debug(f"sections: {config_supabase.sections()}")
        app.logger.debug(f"DB_HOST = {config_supabase.get('DB','DB_HOST',fallback=None)}")        
        app.logger.debug(f"DB_PORT = {config_supabase.get('DB','DB_PORT',fallback=None)}")
        BASE_URL = config_supabase.get('APP', 'BASE_URL', fallback="https://test.solvify.es/api")
        app.logger.debug(f"🌐 BASE_URL en uso: {BASE_URL}")        
        app.logger.info("scripts.conf cargado")
    except Exception as e:
        app.logger.error(f"Error cargando scripts.conf: {e}")
        BASE_URL = "https://test.solvify.es/api"  # Fallback
else:
    app.logger.warning("scripts.conf no encontrado.")
    BASE_URL = "https://test.solvify.es/api"  # Fallback

# 🔍 DEBUG CONFIGURACIÓN GLOBAL
app.logger.info(f"=== CONFIGURACIÓN GLOBAL DEBUG ===")
app.logger.info(f"🔑 TOKEN disponible: {'SÍ' if TOKEN else 'NO'}")
app.logger.info(f"🔑 TOKEN (primeros 50 chars): {TOKEN[:50] if TOKEN else 'N/A'}...")
app.logger.info(f"🌐 BASE_URL final: {BASE_URL}")
app.logger.info(f"📂 Directorio actual: {os.getcwd()}")
app.logger.info(f"📋 Variables de entorno API: SOLVIFY_API_TOKEN={'SET' if os.getenv('SOLVIFY_API_TOKEN') else 'NOT SET'}")
app.logger.info("===================================")

# ----------------------------------------------------------------------------
# Sistema de Mappings Configurables
# ----------------------------------------------------------------------------
class FormMappingManager:
    def __init__(self, config_file='form_mappings.json'):
        self.config_file = config_file
        self.mappings = self._load_mappings()
        
    def _load_mappings(self):
        """Carga mappings desde archivo JSON"""
        try:
            p = Path(self.config_file)
            app.logger.debug(f"[Mappings] Intentando leer: {p.resolve()}")
            if p.exists():
                with open(p, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                # debug: keys de primer nivel y de sources
                app.logger.debug(f"[Mappings] Top-level keys: {list(data.keys())}")
                srcs = list((data.get("sources") or {}).keys())
                app.logger.debug(f"[Mappings] Sources disponibles: {srcs}")
                return data
            else:
                # Crear archivo inicial con mappings existentes
                initial_config = self._create_initial_config()
                with open(self.config_file, 'w', encoding='utf-8') as f:
                    json.dump(initial_config, f, indent=2, ensure_ascii=False)
                app.logger.info(f"Archivo {self.config_file} creado con configuración inicial")
                return initial_config
        except Exception as e:
            app.logger.error(f"Error cargando mappings: {e}")
            return self._get_fallback_mappings()
    
    def _create_initial_config(self):
        """Crear configuración inicial con mappings existentes"""
        return {
            "default_fb": {
                "description": "Mapping por defecto para formularios FB",
                "fields": {
                    "full name": "nombre_y_apellidos",
                    "email": "correo_electrónico", 
                    "phone_number": "número_de_teléfono"
                },
                "validations": {
                    "check_debt_questions": True,
                    "min_debt_amount": None
                }
            },
            "fb_extended": {
                "description": "Mapping extendido FB con preguntas adicionales",
                "inherits_from": "default_fb",
                "fields": {
                    "¿tienes_dos_o_más_deudas?": "tienes_más_de_1_acreedor?",
                    "¿tienes_más_de_12.000_euros_en_deudas?": "tienes_más_de_8000€_en_deudas?",
                    "¿de_estas_deudas_tienes_más_de_6.000€_de_deuda_privada_esto_es_con_bancos,_financieras,_procedentes_de_tarjetas_de_crédito?": "tienes_más_de_6000€_en_deuda_privada?",
                    "¿entran_en_casa_unos_ingresos_mensuales_superiores_a_600€?": "tienes_ingresos_superiores_a_600€?",
                    "¿tienes_algún_bien_como_una_casa,_parking,_terreno_o_local_en_propiedad?": "tienes_bienes?",
                    "¿has_pedido_algún_préstamo_desde_tu_empresa_(sociedad)_y_ahora_te_lo_están_reclamando_personalmente?": "has_pedido_algún_préstamo_desde_tu_empresa...?"
                }
            },
            "form_specific": {
                "1326789255723601": {
                    "description": "Formulario específico con campos Raw",
                    "inherits_from": "default_fb",
                    "fields": {
                        "Full Name": "nombre_y_apellidos",
                        "Email": "correo_electrónico",
                        "Phone Number": "número_de_teléfono", 
                        "Tienes Mas De 6 000 € En Deuda": "¿tienes_más_de_6000€_en_deuda_privada?",
                        "Dispone De Mas De Un Acreedor": "¿tienes_más_de_1_acreedor?",
                        "Ingresa Mas De 1 000 € Al Mes": "¿tienes_ingresos_superiores_a_1000€?",
                        "Raw Full Name": "nombre_y_apellidos",
                        "Raw Email": "correo_electrónico",
                        "Raw Phone Number": "número_de_teléfono",
                        "Raw Tienes Mas De 6 000 € En Deuda": "¿tienes_más_de_6000€_en_deuda_privada?",
                        "Raw Dispone De Mas De Un Acreedor": "¿tienes_más_de_1_acreedor?",
                        "Raw Ingresa Mas De 1 000 € Al Mes": "¿tienes_ingresos_superiores_a_1000€?",
                        "Form Id": "form_id",
                        "Form Name": "form_name",
                        "Campaign Name": "campaign_name",
                        "Ad Name": "ad_name",
                        "Leadgen Id": "leadgen_id"
                    },
                    "validations": {
                        "check_debt_questions": True,
                        "required_fields": ["Full Name", "Email", "Phone Number"]
                    }
                }
            },
            "sources": {
                "FBLexCorner": {
                    "description": "Mapping para FBLexCorner",
                    "company_name": "Lex Corner",
                    "fields": {
                        "Fecha": "created_time",
                        "Nombre": "nombre_y_apellidos",
                        "Correo electrónico": "correo_electrónico",
                        "Teléfono": "número_de_teléfono",
                        "¿A cuánto asciende el conjunto de tus deudas?": "monto_total_deudas",
                        "¿En qué situación laboral te encuentras?": "situacion_laboral",
                        "Campaña": "campaign_name",
                        "Audiencia": "audiencia", 
                        "Anuncio": "anuncio",
                        "Formulario": "form_name",
                        "Leadgen Id": "leadgen_id"
                    },
                    "validations": {
                        "check_debt_amount": True,
                        "rejected_amounts": ["menos_de_8000€", "sin_deudas"]
                    }
                },
                "Backup_FB": {
                    "description": "Sheet mapping backup",
                    "fields": {
                        "Fecha": "created_time",
                        "Nombre": "nombre_y_apellidos", 
                        "Teléfono": "número_de_teléfono",
                        "Mail": "correo_electrónico",
                        "Más de 8k": "tienes_más_de_8000€_en_deudas?",
                        "Más de 1 acreedor": "tienes_más_de_1_acreedor?",
                        "Bienes": "tienes_bienes?",
                        "Vivienda": "tipo_de_vivienda?",
                        "Estado Civil": "estado_civil?",
                        "Provincia": "provincia?"
                    }
                },
                "Alianza_FB": {
                    "description": "Sheet mapping alianza",
                    "fields": {
                        "Fecha": "created_time",
                        "Nombre": "nombre_y_apellidos",
                        "Teléfono": "número_de_teléfono", 
                        "Mail": "correo_electrónico",
                        "Mas de 8k": "tienes_más_de_8000€_en_deudas?",
                        "Estado civil": "estado_civil?",
                        "Bienes": "tienes_bienes?",
                        "Más de 1 acreedor": "tienes_más_de_1_acreedor?"
                    }
                },
                "Alianza": {
                    "description": "Mapping para endpoint Alianza desde n8n",
                    "company_name": "Alianza",
                    "fields": {
                        "Fecha": "created_time",
                        "Nombre": "nombre_y_apellidos",
                        "Lead ID": "lead_id",
                        "Teléfono": "número_de_teléfono",
                        "Mail": "correo_electrónico",
                        "Deuda": "monto_total_deudas",
                        "Cantidad deudas": "cantidad_acreedores"
                    },
                    "validations": {
                        "check_debt_questions": False,
                        "required_fields": ["Nombre", "Mail", "Teléfono"]
                    }
                },
                "Piqueras": {
                    "description": "Mapping para Piqueras Borisova desde N8N",
                    "company_name": "Piqueras Borisova",
                    "company_id": "47ab24c3-b918-46ac-b3c4-975095b001ca",
                    "fields": {
                        "Fecha": "created_time",
                        "Nombre": "nombre_y_apellidos",
                        "Teléfono": "número_de_teléfono",
                        "Mail": "correo_electrónico",
                        "Deuda": "monto_total_deudas",
                        "Cantidad de deudas": "cantidad_acreedores"
                    },
                    "validations": {
                        "check_debt_questions": False,
                        "required_fields": ["Nombre", "Mail", "Teléfono"]
                    }
                },
                                
            }
        }
    
    def _get_fallback_mappings(self):
        """Mappings de emergencia si falla la carga"""
        return {
            "default_fb": {
                "fields": {
                    "full name": "nombre_y_apellidos",
                    "email": "correo_electrónico",
                    "phone_number": "número_de_teléfono"
                }
            }
        }
    
    def get_mapping_for_form(self, form_id=None, source=None):
        try:
            # 1) por form_id
            if form_id and str(form_id) in self.mappings.get("form_specific", {}):
                config = self.mappings["form_specific"][str(form_id)]
                app.logger.debug(f"Usando mapping específico para form_id: {form_id}")
                return self._resolve_mapping(config)

            # 2) por source (normalizado)
            if source:
                src_norm = str(source).strip().casefold()
                sources = self.mappings.get("sources", {}) or {}

                # construir índice normalizado -> config
                index = {}
                for k, v in sources.items():
                    k_norm = str(k).strip().casefold()
                    index[k_norm] = v

                if src_norm in index:
                    config = index[src_norm]
                    app.logger.debug(f"Usando mapping para source: {source} (match normalizado)")
                    return self._resolve_mapping(config)

                # debug: keys disponibles
                app.logger.warning(
                    f"Mapping para source='{source}' no encontrado. "
                    f"Sources disponibles: {list(sources.keys())}"
                )

            # 3) por defecto
            if "default_fb" in self.mappings:
                config = self.mappings["default_fb"]
                app.logger.debug("Usando mapping por defecto")
                return self._resolve_mapping(config)

            app.logger.warning("Usando mapping de emergencia")
            return {"fields": self._get_fallback_mappings()["default_fb"]["fields"]}
        except Exception as e:
            app.logger.error(f"Error obteniendo mapping: {e}", exc_info=True)
            return {"fields": {}}
    
    def _resolve_mapping(self, config):
        """Resuelve herencia de mappings (inherits_from)"""
        result = {"fields": {}, "validations": {}, "company_name": None}
        
        # Si hereda de otro mapping, combinar
        if "inherits_from" in config:
            parent_name = config["inherits_from"]
            if parent_name in self.mappings:
                parent = self._resolve_mapping(self.mappings[parent_name])
                result["fields"].update(parent["fields"])
                result["validations"].update(parent.get("validations", {}))
        
        # Aplicar campos específicos (override)
        result["fields"].update(config.get("fields", {}))
        result["validations"].update(config.get("validations", {}))
        result["company_name"] = config.get("company_name")
        
        return result
    
    def reload_mappings(self):
        """Recarga mappings desde archivo (útil para desarrollo)"""
        self.mappings = self._load_mappings()
        app.logger.info("Mappings recargados desde archivo")

# Instancia global del manager
form_mapping_manager = FormMappingManager()

# ----------------------------------------------------------------------------
# Funciones auxiliares
# ----------------------------------------------------------------------------
def get_supabase_connection():
    params = {
        'user': config_supabase.get('DB','DB_USER',fallback=None),
        'password': config_supabase.get('DB','DB_PASS',fallback=None),
        'host': config_supabase.get('DB','DB_HOST',fallback=None),
        'port': config_supabase.getint('DB','DB_PORT',fallback=5432),
        'database': config_supabase.get('DB','DB_NAME',fallback=None),
    }
    return pg8000.connect(**params)

def strip_country_code(phone):
    return re.sub(r'^(?:\+?34)', '', str(phone).strip())

def normalize_key(key: str) -> str:
    return key.strip().lstrip('¿').rstrip('?').strip().lower()


def detect_source_and_get_mapping(raw_data):
    """
    Detecta el origen del lead y obtiene el mapping apropiado
    Returns: (source, mapping_dict, config)
    """
    # --- util ---
    def n(s):  # normaliza clave: quita tildes/¿? y baja a minúsculas (usa tu normalize_key)
        return normalize_key(str(s))

    keys = list(raw_data.keys())
    nkeys = [n(k) for k in keys]

    form_id = raw_data.get('form_id') or raw_data.get('Form Id')
    if form_id:
        source = 'fb'
        config = form_mapping_manager.get_mapping_for_form(form_id=form_id)
        return source, config["fields"], config

    # 2) Datos ya normalizados
    norm_targets = {n('nombre_y_apellidos'), n('número_de_teléfono'), n('correo_electrónico')}
    if any(nk in norm_targets for nk in nkeys):
        source = 'fb'
        config = form_mapping_manager.get_mapping_for_form(source='default_fb')
        return source, config["fields"], config

    # 3) Detecciones específicas (orden importa)

    # 3.1 MARTIN (al menos 2 de 3)
    martin_sig = ["Lead ID", "Nombre ", "Mas de 8k"]
    if sum(1 for k in martin_sig if k in raw_data) >= 2:
        source = 'MARTIN'
        config = form_mapping_manager.get_mapping_for_form(source=source)
        app.logger.debug("Detectado origen MARTIN por campos signature")
        return source, config["fields"], config

    # 3.2 Sheets
    backup_fb_signature = ["Más de 8k", "Más de 1 acreedor", "Vivienda"]
    alianza_fb_signature = ["Mas de 8k", "Estado civil", "Bienes"]

    if any(k in raw_data for k in backup_fb_signature):
        source = 'Backup_FB'
        config = form_mapping_manager.get_mapping_for_form(source=source)
        app.logger.debug("Detectado origen Backup_FB por campos signature")
        return source, config["fields"], config

    if any(k in raw_data for k in alianza_fb_signature):
        source = 'Alianza_FB'
        config = form_mapping_manager.get_mapping_for_form(source=source)
        app.logger.debug("Detectado origen Alianza_FB por campos signature")
        return source, config["fields"], config

    # 4) FB extendido (claves tipo preguntas)
    fb_extended_fields = [
        'tienes_más_de_1_acreedor?', 'tienes_más_de_8000€_en_deudas?',
        'tienes_más_de_6000€_en_deuda_privada?', 'tienes_ingresos_superiores_a_600€?',
        'tienes_bienes?', 'has_pedido_algún_préstamo_desde_tu_empresa'
    ]
    n_fb_ext = {n(x) for x in fb_extended_fields}
    if any(nk in n_fb_ext for nk in nkeys):
        source = 'fb'
        cfg_default = form_mapping_manager.get_mapping_for_form(source='default_fb')
        cfg_ext = form_mapping_manager._resolve_mapping(form_mapping_manager.mappings.get("fb_extended", {}))
        combined_fields = {**cfg_default["fields"], **cfg_ext.get("fields", {})}
        config = {
            "fields": combined_fields,
            "validations": cfg_ext.get("validations", {}),
            "company_name": cfg_ext.get("company_name")
        }
        app.logger.debug("Usando mapping FB extendido (detección por claves normalizadas)")
        return source, config["fields"], config

    # 5) Keywords de deuda → FB extendido (último recurso)
    debt_keywords = ['acreedor', 'deuda', 'ingres', 'bien', 'préstamo']
    if any(any(kw in n(k) for kw in debt_keywords) for k in keys):
        source = 'fb'
        cfg_default = form_mapping_manager.get_mapping_for_form(source='default_fb')
        cfg_ext = form_mapping_manager._resolve_mapping(form_mapping_manager.mappings.get("fb_extended", {}))
        combined_fields = {**cfg_default["fields"], **cfg_ext.get("fields", {})}
        config = {
            "fields": combined_fields,
            "validations": cfg_ext.get("validations", {}),
            "company_name": cfg_ext.get("company_name")
        }
        app.logger.debug("Usando mapping FB extendido (detección por keywords)")
        return source, config["fields"], config

    # 6) Desconocido
    source = 'unknown'
    config = form_mapping_manager.get_mapping_for_form(source=source)
    return source, config.get("fields", {}), config

def validate_lead_data(data, config, source):
    """
    Valida los datos del lead según la configuración específica
    
    Args:
        data: Datos mapeados del lead
        config: Configuración del mapping
        source: Origen del lead
        
    Returns:
        tuple: (is_valid, rejection_reason)
    """
    validations = config.get("validations", {})
    
    # Validaciones para FBLexCorner
    if source == 'FBLexCorner' and validations.get("check_debt_amount"):
        monto_deudas = str(data.get('monto_total_deudas', '')).strip().lower()
        rejected_amounts = validations.get("rejected_amounts", [])
        if monto_deudas in rejected_amounts:
            return False, f"Monto de deudas insuficiente ({monto_deudas})"
    
    # Validaciones estándar de deuda
    if validations.get("check_debt_questions"):
        debt_fields = [
            '¿tienes_más_de_1_acreedor?',
            '¿tienes_más_de_8000€_en_deudas?', 
            '¿tienes_más_de_6000€_en_deuda_privada?'
        ]
        for field in debt_fields:
            if str(data.get(field, '')).strip().lower() == 'no':
                return False, "Respuesta negativa explícita en validación de deuda"
    
    # Validar campos requeridos (trabaja con datos ya mapeados)
    required_fields = validations.get("required_fields", [])
    for field in required_fields:
        field_value = data.get(field, '').strip()
        if not field_value:
            # Debug adicional para campos requeridos
            app.logger.debug(f"CAMPO REQUERIDO FALTANTE: '{field}' = '{field_value}'")
            app.logger.debug(f"TODOS LOS CAMPOS DISPONIBLES: {list(data.keys())}")
            return False, f"Campo requerido faltante: {field}"
    
    return True, None

def build_info_lead_content(data: dict, source: str = None):
    """
    Construye contenido del Info Lead basado en los datos reales del formulario
    """
    # Mapeo dinámico de campos del formulario a preguntas del Info Lead
    question_mappings = {
        # Campos estándar que pueden venir de diferentes formularios
        '¿dispone_de_más_de_un_acreedor?': "Tiene dos o más deudas",
        '¿tienes_más_de_8.000€_en_deudas?': "Tiene más de 8.000€ de deuda",
        '¿tienes_más_de_6000€_en_deuda_privada?': "Tiene más de 6.000€ de deuda privada",
        '¿tienes_ingresos_superiores_a_1000€?': "Tiene ingresos mensuales superiores a 1.000€",
        '¿tienes_ingresos_superiores_a_600€?': "Tiene ingresos mensuales superiores a 600€",
        '¿tienes_bienes?': "Tiene bienes",
        '¿tienes_hipoteca_en_alguno_de_sus_bienes?': "Tiene hipoteca en alguno de sus bienes",
        
        # Campos específicos de FBLexCorner
        'monto_total_deudas': "Monto total de deudas",
        'situacion_laboral': "Situación laboral",
    }
    
    content = []
    questions_found = set()
    
    # 1. Procesar campos que tienen mapeo directo
    for field_key, field_value in data.items():
        if field_key in question_mappings:
            question_text = question_mappings[field_key]
            
            # Evitar duplicados
            if question_text in questions_found:
                continue
            questions_found.add(question_text)
            
            # Determinar respuesta
            if field_key in ["monto_total_deudas", "situacion_laboral"]:
                answer = str(field_value) if field_value else "No especificado"
            else:
                # Para preguntas de sí/no, considerar varias formas de respuesta afirmativa
                answer_str = str(field_value).strip().lower()
                answer = "Sí" if answer_str in ("sí", "si", "s", "yes", "true", "1") else "No"
            
            content.append({"question": question_text, "answer": answer})
            app.logger.debug(f"INFO LEAD MAPEADO: '{field_key}' = '{field_value}' → '{question_text}' = '{answer}'")
    
    # 2. Agregar preguntas estándar que no se encontraron (con respuesta "No")
    standard_questions = [
        "Tiene más de 8.000€ de deuda",
        "Tiene dos o más deudas", 
        "Tiene más de 6.000€ de deuda privada",
        "Tiene ingresos mensuales superiores a 600€",
        "Tiene ingresos mensuales superiores a 1.000€",
        "Tiene bienes",
        "Tiene hipoteca en alguno de sus bienes",
        "Monto total de deudas",
        "Situación laboral"
    ]
    
    for standard_question in standard_questions:
        if standard_question not in questions_found:
            content.append({"question": standard_question, "answer": "No especificado"})
            app.logger.debug(f"INFO LEAD DEFAULT: '{standard_question}' = 'No especificado'")
    
    # 3. Log del contenido final
    app.logger.debug("INFO LEAD CONTENIDO FINAL:")
    for item in content:
        app.logger.debug(f"  {item['question']}: {item['answer']}")
    
    return content

def _pretty_txt(v: str) -> str:
    t = str(v or '').strip()
    # Limpieza visual típica de Sheets y formularios
    t = t.replace('_', ' ')
    # Normalizaciones ligeras
    t = t.replace('mas de', 'más de').replace('Mas de', 'Más de').replace('mas_de', 'más de')
    t = t.replace('o mas', 'o más').replace('o_mas', 'o más').replace('o_más', 'o más')
    return t

def _yes_no_from_dispuesto(v: str) -> str:
    if v is None:
        return ""
    s = str(v).strip().lower()
    # Admite formatos como 'sí,_entiendo_que_tras_el_estudio_tendrá_un_coste'
    # y cualquier 'si/sí/yes/y/true/1'
    truthy = ['si', 'sí', 'yes', 'y', 'true', '1']
    # También si el texto contiene explícitamente "entiendo" y "coste"
    if any(tok in s for tok in truthy) or ('entiendo' in s and 'coste' in s):
        return "Sí"
    if any(tok in s for tok in ['no', 'false', '0']):
        return "No"
    return _pretty_txt(v)  # Si no sabemos, devolvemos tal cual pero bonito

def build_info_lead_content_from_mapping(data_norm: dict, raw_payload: dict, mapping: dict, source: str):
    """
    Construye el contenido del Info lead de forma DINÁMICA en base al mapping.
    - Incluye todos los campos mapeados que existan en data_norm (excepto nombre/email/teléfono y campos técnicos).
    - Para 'dispuesto' genera la pregunta 'Acepta coste de estudio' en Sí/No cuando sea posible.
    - Para Despacho Calero, muestra literalmente 'Deuda' y 'Cantidad de deudas' con sus valores.
    """
    content = []
    questions_added = set()

    fields_map = (mapping.get('fields') or {})
    # Invertimos para conocer la etiqueta original (src_key) a partir de la clave normalizada
    label_by_norm = {norm_key: src_key for src_key, norm_key in fields_map.items()}

    # 1) Reglas específicas “Despacho calero”: etiquetas legibles
    is_calero = (source or '').strip().lower() in ('despacho calero', 'despcaldero', 'despcalero')

    # 2) Recorremos TODOS los campos normalizados presentes en data_norm
    for norm_key, val in data_norm.items():
        # Excluir campos técnicos que no deben salir en el InfoLead
        if norm_key in ('nombre_y_apellidos', 'correo_electrónico', 'número_de_teléfono', 'leadgen_id'):
            continue  

        label = label_by_norm.get(norm_key, norm_key)

        # Caso especial: DISUESTO → Acepta coste de estudio (Sí/No)
        if norm_key == 'dispuesto':
            yn = _yes_no_from_dispuesto(val)
            q = "Acepta coste de estudio"
            content.append({"question": q, "answer": yn if yn else _pretty_txt(val)})
            questions_added.add(q)
            continue

        # Caso especial: etiquetas bonitas para Calero
        # extra: normaliza etiquetas genéricas aunque no sea Calero
        if norm_key == 'cantidad_acreedores' and 'Cantidad de deudas' not in questions_added:
            content.append({"question": "Cantidad de deudas", "answer": _pretty_txt(val)})
            questions_added.add("Cantidad de deudas")
            continue

        # Etiqueta por defecto
        q = _pretty_txt(label)
        content.append({"question": q, "answer": _pretty_txt(val)})
        questions_added.add(q)

    # Log de depuración
    app.logger.debug("INFO LEAD (dinámico) construido desde mapping:")
    for item in content:
        app.logger.debug(f"  {item['question']}: {item['answer']}")

    return content

def get_tracking_annotation(deal_id):
    url = f"{BASE_URL}/annotations/deals/{deal_id}"
    try:
        r = requests.get(url, headers={"Authorization": f"Bearer {TOKEN}"}, timeout=30)
        r.raise_for_status()
    except HTTPError as e:
        app.logger.error(f"Error obteniendo anotaciones para deal {deal_id}: {e}")
        return None
    for ann in r.json():
        if ann.get("annotation_type") == "Seguimiento":
            return ann
    return None

def create_tracking_annotation(deal_id, data):
    url = f"{BASE_URL}/annotations/deals/{deal_id}"
    payload = {"annotation_type":"Seguimiento","status":"en curso",
               "content":f"LSO - {data.get('nombre_y_apellidos','')}","is_private":True}
    try:
        r = requests.post(url, json=payload,
                          headers={"Authorization":f"Bearer {TOKEN}","Content-Type":"application/json"},
                          timeout=30)
        r.raise_for_status()
    except HTTPError as e:
        app.logger.error(f"Error creando anotación Seguimiento para deal {deal_id}: {e}")
        return None
    return r.json()

def create_info_lead_task(deal_id, data, content=None):
    seg = get_tracking_annotation(deal_id) or create_tracking_annotation(deal_id, data)
    if not seg:
        app.logger.warning(f"No se pudo obtener ni crear anotación Seguimiento para {deal_id}")
        return None
    ann_id = seg.get('id')
    url = f"{BASE_URL}/annotations/deals/{deal_id}/{ann_id}/tasks"
    body = {
        "annotation_type": "Info lead",
        "content": content if content is not None else build_info_lead_content(data),
        "spent_time": 0, "is_completed": True, "is_private": True,
        "priority": 1, "status": "completada",
        "due_date": datetime.datetime.now().astimezone().isoformat(),
        "object_reference_id": deal_id, "object_reference_type": "deals"
    }
    try:
        r = requests.post(url, json=body,
                          headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"},
                          timeout=30)
        r.raise_for_status()
    except HTTPError as e:
        app.logger.error(f"Error creando tarea Info lead para deal {deal_id}: {e}")
        return None
    return r.json()

def create_portal_user(data, source, config=None):
    # 🔍 DEBUG INICIO - DATOS DE ENTRADA
    app.logger.info(f"=== INICIANDO CREATE_PORTAL_USER ===")
    app.logger.info(f"📊 Data recibida: {data}")
    app.logger.info(f"🏷️ Source: {source}")
    app.logger.info(f"⚙️ Config: {config}")
    
    full = data.get('nombre_y_apellidos', '').strip()
    phone = strip_country_code(data.get('número_de_teléfono','') or data.get('phone_number',''))
    
    app.logger.info(f"👤 Nombre procesado: '{full}'")
    app.logger.info(f"📞 Teléfono original: '{data.get('número_de_teléfono', '') or data.get('phone_number', '')}'")
    app.logger.info(f"📞 Teléfono procesado: '{phone}'")
    
    # 1️⃣ Validar datos usando configuración específica
    if config:
        is_valid, rejection_reason = validate_lead_data(data, config, source)
        if not is_valid:
            app.logger.info(f"RECHAZADO PortalUser: {full} | TEL={phone} | MOTIVO={rejection_reason}")
            return None
    
    # 2️⃣ Determinar categoría (cat_id)
    try:
        ans_empresa = str(data.get(
            'has_pedido_algún_préstamo_desde_tu_empresa_(sociedad)_y_ahora_te_lo_están_reclamando_personalmente?',
            ''
        )).lower()
        cat_id = (
            '932bbf6f-b505-495c-be19-f4dc186b4bd3'
            if ans_empresa in ('si','sí') 
            else 'bcb1ae3e-4c23-4461-9dae-30ed137d53e2'
        )
    except Exception:
        cat_id = 'bcb1ae3e-4c23-4461-9dae-30ed137d53e2'

    cat_id = 'bcb1ae3e-4c23-4461-9dae-30ed137d53e2'
    
    app.logger.debug(f"Categoría de PortalUser para {full}: {cat_id}")    # 3️⃣ Determinar company_name desde config o fallback
    company_name = config.get("company_name") if config else None
    app.logger.debug(f"=== COMPANY NAME DEBUG ===")
    app.logger.debug(f"config.get('company_name'): '{company_name}'")
    app.logger.debug(f"source recibido: '{source}'")

    if not company_name:
        if source in ('Backup_FB', 'Alianza_FB', 'MARTIN'):
            company_name = 'sheets'
            app.logger.debug("Company name asignado: 'sheets'")
        else:
            company_name = source
            app.logger.debug(f"Company name asignado desde source: '{company_name}'")
    
    app.logger.debug(f"Company name final antes de DB lookup: '{company_name}'")

    # 4️⃣ Lookup dinámico de company_id
    company_id = None
    try:
        conn = get_supabase_connection()
        cur = conn.cursor()
        cur.execute("SELECT id FROM companies WHERE LOWER(name) = LOWER(%s) LIMIT 1", (company_name,))
        row = cur.fetchone()
        company_id = str(row[0]) if row else None
        if company_id:
            app.logger.debug(f"Company ID encontrado para '{company_name}': {company_id}")
        else:
            app.logger.warning(f"No se encontró company_id para '{company_name}', usando fallback estático")
    except Exception as e:
        app.logger.error(f"Error buscando company_id para '{company_name}': {e}")
    finally:
        try:
            cur.close()
            conn.close()
        except:
            pass

    # 4.5️⃣ Verificar si el cliente (número) ya existe para este company_id
    if phone and company_id:
        try:
            conn = get_supabase_connection()
            cur = conn.cursor()
            # Consulta exacta como la proporcionada:
            # select l.phone, c.name, d.company_id from leads l, deals d, companies c 
            # where d.lead_id = l.id and c.id = d.company_id and l.phone = '708684495'
            cur.execute("""
                SELECT l.phone, c.name, d.company_id 
                FROM leads l, deals d, companies c 
                WHERE d.lead_id = l.id 
                AND c.id = d.company_id 
                AND l.phone = %s
                LIMIT 1
            """, (phone,))
            existing_lead = cur.fetchone()
            
            if existing_lead:
                existing_phone, existing_company_name, existing_company_id = existing_lead
                app.logger.warning(f"RECHAZADO PortalUser: {full} | TEL={phone} | MOTIVO=Cliente ya existe en company '{existing_company_name}' (ID: {existing_company_id})")
                return None
            else:
                app.logger.debug(f"Cliente {phone} no existe en ningún deal, procediendo a crear")
                
        except Exception as e:
            app.logger.error(f"Error verificando duplicado para {phone}: {e}")
            # En caso de error en la verificación, continuamos con la creación para no bloquear el proceso
        finally:
            try:
                cur.close()
                conn.close()
            except:
                pass

    # 5️⃣ Construcción del payload
    parts = full.split()
    first = parts[0] if parts else ''
    last = ' '.join(parts[1:]) if len(parts) > 1 else ''
    email = data.get('correo_electrónico','') or data.get('email','')
    payload = {
        'first_name': first,
        'last_name':  last,
        'email':      email,
        **({'phone': phone} if phone else {}),
        'channel':    company_name,
        'form_name':  data.get('form_name',''),
        'campaign':   data.get('campaign_name',''),
        'lead_gen_id':data.get('leadgen_id',''),
        'company_id' : company_id or 'a9242a58-4f5d-494c-8a74-45f8cee150e6'
    }
    app.logger.debug(f"Payload PortalUser para {company_name}: {payload}")

    # 6️⃣ Petición a la API
    url = f"{BASE_URL}/leads/{cat_id}/"
    
    # 🔍 DEBUG DETALLADO - CREACIÓN PORTAL
    app.logger.info(f"=== DEBUG CREACIÓN PORTAL USER ===")
    app.logger.info(f"🌐 URL: {url}")
    app.logger.info(f"📋 Payload completo: {payload}")
    app.logger.info(f"🏢 Company ID usado: {company_id}")
    app.logger.info(f"🏷️ Category ID usado: {cat_id}")
    app.logger.info(f"📞 Teléfono procesado: {phone}")
    app.logger.info(f"👤 Nombre completo: {full}")
    app.logger.info(f"🔑 TOKEN disponible: {'SÍ' if TOKEN else 'NO'}")
    app.logger.info(f"🎯 BASE_URL: {BASE_URL}")
    
    try:
        headers = {
            "Authorization": f"Bearer {TOKEN}",
            "Content-Type": "application/json"
        }
        app.logger.info(f"📤 Headers: {headers}")
        
        r = requests.post(
            url, json=payload,
            headers=headers,
            timeout=30
        )
        
        # DEBUG RESPUESTA DETALLADO
        app.logger.info(f"📥 RESPUESTA API:")
        app.logger.info(f"   Status Code: {r.status_code}")
        app.logger.info(f"   Response Headers: {dict(r.headers)}")
        app.logger.info(f"   Response Text: {r.text}")
        
        try:
            response_json = r.json()
            app.logger.info(f"   Response JSON: {response_json}")
        except:
            app.logger.warning("   No se pudo parsear respuesta como JSON")
        
        if r.status_code == 409:
            app.logger.warning(f"RECHAZADO PortalUser: {full} | TEL={phone} | MOTIVO=Portal user duplicado")
            return None
        if r.status_code not in (200,201):
            app.logger.error(f"❌ ERROR API - Status Code: {r.status_code}")
            app.logger.error(f"❌ ERROR API - Response: {r.text}")
            app.logger.error(f"❌ ERROR API - Headers: {dict(r.headers)}")
            app.logger.error(f"RECHAZADO PortalUser: {full} | TEL={phone} | MOTIVO=Error creando portal user: {r.status_code} {r.text}")
            return None
            
        r.raise_for_status()
        
    except requests.exceptions.Timeout as e:
        app.logger.error(f"⏱️ TIMEOUT ERROR: {e}")
        app.logger.error(f"RECHAZADO PortalUser: {full} | TEL={phone} | MOTIVO=Timeout en petición API")
        return None
    except requests.exceptions.ConnectionError as e:
        app.logger.error(f"🔌 CONNECTION ERROR: {e}")
        app.logger.error(f"RECHAZADO PortalUser: {full} | TEL={phone} | MOTIVO=Error de conexión API")
        return None
    except requests.exceptions.RequestException as e:
        app.logger.error(f"🌐 REQUEST ERROR: {e}")
        app.logger.error(f"RECHAZADO PortalUser: {full} | TEL={phone} | MOTIVO=Error en petición HTTP: {e}")
        return None
    except Exception as e:
        app.logger.error(f"💥 EXCEPCIÓN GENERAL: {type(e).__name__}: {e}")
        app.logger.error(f"RECHAZADO PortalUser: {full} | TEL={phone} | MOTIVO=Excepción creando portal user: {e}")
        return None

    app.logger.info(f"✅ Portal user creado exitosamente para {company_name}: {full} | TEL={phone}")
    app.logger.info(f"✅ Respuesta exitosa: {r.json()}")
    return r.json()

def process_lead_common(source: str, data: dict, raw_payload: dict, config: dict):
    """
    Flujo común:
      - Crea PortalUser (estricto)
      - Si falla -> no crea tarea Info lead
      - Si OK -> busca deal_id y crea tarea Info lead (dinámico para DespCaldero)
    Devuelve dict con flags y objetos útiles para la respuesta del endpoint.
    """
    # 1) Portal user
    portal_resp = create_portal_user(data, source, config)
    app.logger.info(f"Lead de {source} procesado en portal: {portal_resp}")

    if portal_resp is None:
        full = data.get('nombre_y_apellidos','').strip()
        phone = strip_country_code(data.get('número_de_teléfono','') or '')
        app.logger.warning(
            f"SKIP InfoLead {source}: {full} | TEL={phone} | MOTIVO=PortalUser no creado"
        )
        return {
            "portal_user_created": False,
            "info_lead_created": False,
            "deal_id": None,
            "task": None,
        }

    # 2) Buscar deal_id
    deal_id = None
    try:
        conn = get_supabase_connection()
        cur = conn.cursor()
        phone = strip_country_code(data.get('número_de_teléfono','') or '')
        if phone:
            query = (
                "SELECT d.id FROM leads l JOIN deals d ON l.id=d.lead_id "
                "WHERE TRIM(REPLACE(REPLACE(l.phone,'+34',''),' ',''))=%s "
                "AND l.is_deleted=FALSE AND d.is_deleted=FALSE "
                "ORDER BY d.created_at DESC LIMIT 1"
            )
            cur.execute(query, (phone,))
        else:
            query = (
                "SELECT d.id FROM leads l JOIN deals d ON l.id=d.lead_id "
                "WHERE l.email=%s AND l.is_deleted=FALSE AND d.is_deleted=FALSE "
                "ORDER BY d.created_at DESC LIMIT 1"
            )
            cur.execute(query, (data.get('correo_electrónico',''),))
        row = cur.fetchone()
        deal_id = str(row[0]) if row else None
        if deal_id:
            app.logger.debug(f"Deal ID encontrado para {source}: {deal_id}")
        else:
            app.logger.debug(f"No se encontró deal_id para el lead de {source}")
    except Exception as e:
        full = data.get('nombre_y_apellidos','').strip()
        phone = strip_country_code(data.get('número_de_teléfono','') or '')
        app.logger.error(f"RECHAZADO InfoLead {source}: {full} | TEL={phone} | MOTIVO=Error buscando deal_id: {e}", exc_info=True)
    finally:
        try:
            if 'cur' in locals(): cur.close()
            if 'conn' in locals(): conn.close()
        except:
            pass

    if not deal_id:
        full = data.get('nombre_y_apellidos','').strip()
        phone = strip_country_code(data.get('número_de_teléfono','') or '')
        app.logger.warning(f"RECHAZADO InfoLead {source}: {full} | TEL={phone} | MOTIVO=Sin deal_id")
        return {
            "portal_user_created": True,
            "info_lead_created": False,
            "deal_id": None,
            "task": None,
        }

    # 3) Crear tarea Info lead
    # Si hay mapping (config con fields), usa SIEMPRE el builder dinámico.
    # Si no, fallback al builder estático.
    try:
        has_mapping = bool(config and (config.get("fields") or {}))
    except Exception:
        has_mapping = False

    if has_mapping:
        info_content = build_info_lead_content_from_mapping(data, raw_payload, config, source)
    else:
        info_content = build_info_lead_content(data)


    task = create_info_lead_task(deal_id, data, content=info_content)

    app.logger.info(f"Tarea Info lead creada para {source}: {task}")
    return {
        "portal_user_created": True,
        "info_lead_created": task is not None,
        "deal_id": deal_id,
        "task": task,
    }

# ----------------------------------------------------------------------------
# Endpoints
# ----------------------------------------------------------------------------
@app.route('/OnNewLead', methods=['POST'])
@app.route('/OnNewLeadFB', methods=['POST'])
def receive_lead():
    raw = request.json or {}
    if isinstance(raw, list):
        if not raw:
            return jsonify({"error": "lista vacía"}), 400
        raw = raw[0]

    
    # Debug completo sin truncar
    app.logger.debug("="*80)
    app.logger.debug("RAW LEAD COMPLETO:")
    for key, value in raw.items():
        app.logger.debug(f"  '{key}': '{value}'")
    app.logger.debug("="*80)

    # 1️⃣ Detectar origen y obtener mapping dinámico
    source, mapping, config = detect_source_and_get_mapping(raw)
    app.logger.debug(f"Origen detectado: {source}")
    app.logger.debug(f"Config obtenida: {config.get('description', 'Sin descripción')}")
    
    # Debug del mapping
    app.logger.debug("MAPPING APLICADO:")
    for orig, dest in mapping.items():
        app.logger.debug(f"  '{orig}' → '{dest}'")

# 2️⃣ Mapear datos usando mapping dinámico
    data = {}
    
    # Primero aplicar mapping específico
    for original_key, mapped_key in mapping.items():
        # Buscar la clave original o sin interrogaciones
        value = raw.get(original_key) or raw.get(original_key.lstrip('¿'))
        if value is not None:
            # Convertir a string si es necesario para consistencia
            data[mapped_key] = str(value) if not isinstance(value, str) else value
            app.logger.debug(f"MAPEADO: '{original_key}' → '{mapped_key}' = '{value}' (tipo: {type(value)})")
    
    # Fallback inteligente para campos críticos si no se encontraron
    critical_fields = {
        'nombre_y_apellidos': [
            'name', 'full_name', 'fullname', 'nombre', 'nombre_completo', 
            'full name', 'Full Name', 'Nombre', 'Nombre '  # ← Agregado "Nombre " con espacio
        ],
        'correo_electrónico': [
            'email', 'correo', 'mail', 'Email', 'e-mail', 'Mail'  # ← Agregado "Mail"
        ],
        'número_de_teléfono': [
            'phone', 'telefono', 'teléfono', 'phone_number', 'Phone Number', 
            'tel', 'mobile', 'Teléfono'  # ← Agregado "Teléfono"
        ]
    }
    
    for target_field, possible_sources in critical_fields.items():
        current_value = str(data.get(target_field, '')).strip()  # ← SEGURO: convertir a str primero
        if not current_value:
            app.logger.debug(f"FALLBACK: Buscando '{target_field}' (actual: '{current_value}')")
            for possible_source in possible_sources:
                raw_value_original = raw.get(possible_source, '')
                raw_value = str(raw_value_original).strip() if raw_value_original != '' else ''  # ← SEGURO: convertir a str
                if raw_value:
                    data[target_field] = raw_value
                    app.logger.debug(f"FALLBACK MAPEADO: '{possible_source}' → '{target_field}' = '{raw_value}' (tipo original: {type(raw_value_original)})")
                    break
                else:
                    app.logger.debug(f"FALLBACK: '{possible_source}' no encontrado o vacío")
        else:
            app.logger.debug(f"FALLBACK: '{target_field}' ya tiene valor: '{current_value}'")
    
    # Agregar campos que no estén en el mapping (por compatibilidad)
    for k, v in raw.items():
        mapped_key = mapping.get(k) or mapping.get(k.lstrip('¿'))
        if not mapped_key and k not in data:
            # Convertir a string si es necesario para consistencia
            data[k] = str(v) if not isinstance(v, str) else v
            app.logger.debug(f"SIN MAPEAR: '{k}' = '{v}' (tipo: {type(v)})")
    
    # Debug final de datos mapeados
    app.logger.debug("DATOS FINALES MAPEADOS:")
    for key, value in data.items():
        app.logger.debug(f"  '{key}': '{value}' (tipo: {type(value)})")    # 3️⃣ Crear portal user con configuración específica
        app.logger.debug(f"  '{key}': '{value}' (tipo: {type(value)})")    # 3️⃣ Crear portal user con configuración específica
    if source in ('fb', 'Backup_FB', 'Alianza_FB'):
        portal_resp = create_portal_user(data, source, config)
        app.logger.info(f"Lead de {source} procesado en portal: {portal_resp}")
    else:
        full = data.get('nombre_y_apellidos','').strip()
        phone = strip_country_code(data.get('número_de_teléfono','') or '')
        app.logger.warning(f"RECHAZADO PortalUser: {full} | TEL={phone} | MOTIVO=Origen desconocido ({source})")

    # 4️⃣ Buscar deal_id y crear tarea Info lead
    deal_id = None
    try:
        conn = get_supabase_connection()
        cur = conn.cursor()
        phone = strip_country_code(data.get('número_de_teléfono','') or '')
        if phone:
            query = (
                "SELECT d.id FROM leads l JOIN deals d ON l.id=d.lead_id "
                "WHERE TRIM(REPLACE(REPLACE(l.phone,'+34',''),' ',''))=%s "
                "AND l.is_deleted=FALSE AND d.is_deleted=FALSE "
                "ORDER BY d.created_at DESC LIMIT 1"
            )
            cur.execute(query, (phone,))
        else:
            query = (
                "SELECT d.id FROM leads l JOIN deals d ON l.id=d.lead_id "
                "WHERE l.email=%s AND l.is_deleted=FALSE AND d.is_deleted=FALSE "
                "ORDER BY d.created_at DESC LIMIT 1"
            )
            cur.execute(query, (data.get('correo_electrónico',''),))
        row = cur.fetchone()
        deal_id = str(row[0]) if row else None
    except Exception as e:
        full = data.get('nombre_y_apellidos','').strip()
        phone = strip_country_code(data.get('número_de_teléfono','') or '')
        app.logger.error(f"RECHAZADO InfoLead {source}: {full} | TEL={phone} | MOTIVO=Error buscando deal_id: {e}", exc_info=True)
    finally:
        try:
            cur.close()
            conn.close()
        except:
            pass

    if deal_id:
        task = create_info_lead_task(deal_id, data)
        app.logger.info(f"Tarea Info lead creada: {task}")
    else:
        full = data.get('nombre_y_apellidos','').strip()
        phone = strip_country_code(data.get('número_de_teléfono','') or '')
        app.logger.warning(f"RECHAZADO InfoLead: {full} | TEL={phone} | MOTIVO=Sin deal_id")

    return jsonify({"message":"Procesado","source":source}), 200

@app.route('/Alianza', methods=['POST'])
def receive_alianza_lead():
    """
    Endpoint específico para recibir leads de Alianza desde n8n
    Usa lógica común: crear PortalUser -> (si OK) deal_id -> tarea Info lead
    """
    raw = request.json or {}
    if isinstance(raw, list):
        if not raw:
            return jsonify({"error": "lista vacía"}), 400
        raw = raw[0]

    app.logger.debug("="*50)
    app.logger.debug("RAW ALIANZA LEAD:")
    for key, value in raw.items():
        app.logger.debug(f"  '{key}': '{value}'")
    app.logger.debug("="*50)

    # 1️⃣ Configuración dinámica para Alianza
    source = 'Alianza'
    config = form_mapping_manager.get_mapping_for_form(source=source)
    mapping = config.get("fields", {})

    app.logger.debug(f"Config {source}: {config.get('description', 'Configuración dinámica')}")
    app.logger.debug("MAPPING APLICADO:")
    for orig, dest in mapping.items():
        app.logger.debug(f"  '{orig}' → '{dest}'")

    # 2️⃣ Mapear datos
    data = {}

    # 2.1 Aplicar mapping
    for original_key, mapped_key in mapping.items():
        value = raw.get(original_key)
        if value is not None:
            data[mapped_key] = str(value) if not isinstance(value, str) else value
            app.logger.debug(f"MAPEADO: '{original_key}' → '{mapped_key}' = '{value}' (tipo: {type(value)})")

    # 2.2 Fallback para críticos
    critical_fields = {
        'nombre_y_apellidos': ['name', 'full_name', 'fullname', 'nombre', 'nombre_completo',
                               'full name', 'Full Name', 'Nombre', 'Nombre '],
        'correo_electrónico': ['email', 'correo', 'mail', 'Email', 'e-mail', 'Mail'],
        'número_de_teléfono': ['phone', 'telefono', 'teléfono', 'phone_number', 'Phone Number',
                               'tel', 'mobile', 'Teléfono'],
    }
    for target_field, possible_sources in critical_fields.items():
        current_value = str(data.get(target_field, '')).strip()
        if not current_value:
            app.logger.debug(f"FALLBACK: Buscando '{target_field}' (actual: '{current_value}')")
            for possible_source in possible_sources:
                raw_value_original = raw.get(possible_source, '')
                raw_value = str(raw_value_original).strip() if raw_value_original != '' else ''
                if raw_value:
                    data[target_field] = raw_value
                    app.logger.debug(f"FALLBACK MAPEADO: '{possible_source}' → '{target_field}' = '{raw_value}'")
                    break
        else:
            app.logger.debug(f"FALLBACK: '{target_field}' ya tiene valor: '{current_value}'")

    # 2.3 Agregar campos no mapeados
    for k, v in raw.items():
        mapped_key = mapping.get(k)
        if not mapped_key and k not in data:
            data[k] = str(v) if not isinstance(v, str) else v
            app.logger.debug(f"SIN MAPEAR: '{k}' = '{v}' (tipo: {type(v)})")

    # 2.4 Debug final
    app.logger.debug("DATOS FINALES MAPEADOS ALIANZA:")
    for key, value in data.items():
        app.logger.debug(f"  '{key}': '{value}' (tipo: {type(value)})")

    # 3️⃣ Lógica COMÚN
    result = process_lead_common(source, data, raw, config)

    return jsonify({
        "message": f"Lead de {source} procesado",
        "source": source,
        "portal_user_created": result["portal_user_created"],
        "info_lead_created": result["info_lead_created"],
        "deal_id": result["deal_id"],
        "task": result["task"],
    }), 200

@app.route('/B2B', methods=['POST'])
def receive_b2b_lead():
    """
    Endpoint B2B simplificado:
    - source == company_name (mismo literal que en la tabla companies.name)
    - mapping cargado desde form_mappings.json usando FormMappingManager
    - mapeo directo raw -> data según mapping["fields"]
    - fallback mínimo para nombre/email/teléfono
    - flujo común con process_lead_common()
    """
    # 0) Carga del payload
    raw = request.json or {}
    if isinstance(raw, list):
        if not raw:
            return jsonify({"error": "lista vacía"}), 400
        raw = raw[0]

    app.logger.debug("=" * 60)
    app.logger.debug("RAW B2B LEAD:")
    for k, v in raw.items():
        app.logger.debug(f"  {k!r}: {v!r}")
    app.logger.debug("=" * 60)

    # 1) source obligatorio (coincide con company_name)
    source = (raw.get("source") or raw.get("Source") or "").strip()
    if not source:
        return jsonify({"error": "Falta 'source'/'Source' en el payload"}), 400

    app.logger.debug(f"[B2B] source recibido: '{source}'")

    # 2) Cargar mapping para el source
    config = form_mapping_manager.get_mapping_for_form(source=source)
    mapping = config.get("fields", {}) or {}
    if not mapping:
        app.logger.warning(f"[B2B] No hay mapping para source='{source}'. Se usará mapping vacío.")
    app.logger.debug(f"[B2B] Mapping de '{source}': {len(mapping)} campos")

    # 3) Mapear campos según mapping (raw_key -> normalized_key)
    data = {}
    for original_key, mapped_key in mapping.items():
        # Permitimos variantes con '¿' inicial (formularios con preguntas)
        val = raw.get(original_key)
        if val is None and isinstance(original_key, str) and original_key.startswith('¿'):
            val = raw.get(original_key.lstrip('¿'))
        if val is not None:
            data[mapped_key] = val if isinstance(val, str) else str(val)
            app.logger.debug(f"MAP: '{original_key}' → '{mapped_key}' = {val!r}")

    # 4) Fallback mínimo para críticos si no vienen en el mapping
    critical_fallbacks = {
        'nombre_y_apellidos': ['Nombre', 'Full Name', 'full_name', 'fullname', 'name', 'Nombre '],
        'correo_electrónico': ['Mail', 'Email', 'email', 'correo', 'e-mail'],
        'número_de_teléfono': ['Teléfono', 'Phone Number', 'phone_number', 'telefono', 'teléfono', 'tel', 'mobile'],
    }
    for target, aliases in critical_fallbacks.items():
        cur = str(data.get(target, '')).strip()
        if not cur:
            for alias in aliases:
                raw_value_original = raw.get(alias, '')
                raw_value = str(raw_value_original).strip() if raw_value_original != '' else ''
                if raw_value:
                    data[target] = raw_value
                    app.logger.debug(f"FALLBACK: '{alias}' → '{target}' = {raw_value!r}")
                    break

    # 5) No copiamos 'source' a data (solo datos del lead). Campos no mapeados: no se añaden.
    #    (Si quisieras conservar TODOS los no mapeados, podrías agregarlos aquí, pero nos ceñimos al mapping.)

    # 6) Log final de los datos mapeados
    app.logger.debug(f"[B2B] DATOS MAPEADOS ({source}):")
    for k, v in data.items():
        app.logger.debug(f"  {k!r}: {v!r}")

    # 7) Flujo común: crea portal user -> busca deal_id -> crea tarea Info Lead
    result = process_lead_common(source, data, raw, config)

    # 8) Respuesta
    return jsonify({
        "message": f"Lead de {source} procesado",
        "source": source,
        "portal_user_created": result["portal_user_created"],
        "info_lead_created": result["info_lead_created"],
        "deal_id": result["deal_id"],
        "task": result["task"],
    }), 200


@app.route('/B2B1', methods=['POST'])
def receive_b2b_lead1():
    """
    Endpoint genérico para recibir leads B2B desde n8n
    Acepta 'source' y usa lógica común: crear PortalUser -> (si OK) deal_id -> tarea Info lead
    """
    raw = request.json or {}
    if isinstance(raw, list):
        if not raw:
            return jsonify({"error": "lista vacía"}), 400
        raw = raw[0]

    # --- Normalización de alias del source ---
    alias_map = {
        "despacho calero": "DespCaldero",
        "despcaldero": "DespCaldero",
        "despcalero": "DespCaldero",
        "economis": "economis",  
        "lexcorner": "lexcorner",  # Nuevo
        "lexcorner": "Lex C",  # Nuevo
        "buenalex": "buenalex",  
    }
    src_in = (raw.get("source") or raw.get("Source") or "").strip()
    src_norm_l = src_in.lower()
   
    source = alias_map.get(src_norm_l, src_in or "")
    
    app.logger.debug(f"Source normalizado: '{source}'")

    app.logger.debug("="*50)
    app.logger.debug("RAW GENERIC B2B LEAD:")
    for key, value in raw.items():
        app.logger.debug(f"  '{key}': '{value}'")
    app.logger.debug("="*50)
    app.logger.debug(f"Source detectado: '{source}'")

    # 1) Mapping dinámico para el source
    config = form_mapping_manager.get_mapping_for_form(source=source)
    mapping = config.get("fields", {})
    app.logger.debug(f"Config {source}: {config.get('description', 'Configuración dinámica')}")
    app.logger.debug("MAPPING APLICADO:")
    for orig, dest in mapping.items():
        app.logger.debug(f"  '{orig}' → '{dest}'")

    # 2) Mapear datos usando mapping dinámico
    data = {}

    # 2.1 Aplicar mapping específico
    for original_key, mapped_key in mapping.items():
        value = raw.get(original_key)
        if value is not None:
            data[mapped_key] = str(value) if not isinstance(value, str) else value
            app.logger.debug(f"MAPEADO: '{original_key}' → '{mapped_key}' = '{value}' (tipo: {type(value)})")

    # 2.2 Fallback inteligente para críticos
    critical_fields = {
        'nombre_y_apellidos': [
            'name', 'full_name', 'fullname', 'nombre', 'nombre_completo', 
            'full name', 'Full Name', 'Nombre', 'Nombre '  # ← Agregado "Nombre " con espacio
        ],
        'correo_electrónico': [
            'email', 'correo', 'mail', 'Email', 'e-mail', 'Mail'  # ← Agregado "Mail"
        ],
        'número_de_teléfono': [
            'phone', 'telefono', 'teléfono', 'phone_number', 'Phone Number', 
            'tel', 'mobile', 'Teléfono'  # ← Agregado "Teléfono"
        ]
    }
    for target_field, possible_sources in critical_fields.items():
        current_value = str(data.get(target_field, '')).strip()
        if not current_value:
            app.logger.debug(f"FALLBACK: Buscando '{target_field}' (actual: '{current_value}')")
            for possible_source in possible_sources:
                raw_value_original = raw.get(possible_source, '')
                raw_value = str(raw_value_original).strip() if raw_value_original != '' else ''
                if raw_value:
                    data[target_field] = raw_value
                    app.logger.debug(f"FALLBACK MAPEADO: '{possible_source}' → '{target_field}' = '{raw_value}' (tipo original: {type(raw_value_original)})")
                    break
                else:
                    app.logger.debug(f"FALLBACK: '{possible_source}' no encontrado o vacío")
        else:
            app.logger.debug(f"FALLBACK: '{target_field}' ya tiene valor: '{current_value}'")

    # 2.3 Agregar campos no mapeados (compatibilidad), excluyendo 'source'
    for k, v in raw.items():
        mapped_key = mapping.get(k)
        if not mapped_key and k not in data and k != 'source':
            data[k] = str(v) if not isinstance(v, str) else v
            app.logger.debug(f"SIN MAPEAR: '{k}' = '{v}' (tipo: {type(v)})")

    # 2.4 Debug final
    app.logger.debug(f"DATOS FINALES MAPEADOS {source}:")
    for key, value in data.items():
        app.logger.debug(f"  '{key}': '{value}' (tipo: {type(value)})")

    # --- Blindaje Despacho calero: completar campos críticos desde RAW si faltan ---
    src_norm = (source or "").strip().lower()
    app.logger.debug(f"=== BLINDAJE DEBUG: source='{source}', src_norm='{src_norm}' ===")
    app.logger.debug(f"¿src_norm en lista?: {src_norm in ('despacho calero', 'despcaldero', 'despcalero')}")

    if src_norm in ("despacho calero", "despcaldero", "despcalero"):
        app.logger.debug("ENTRANDO EN BLINDAJE DESPCALDERO")


    if src_norm in ("despacho calero", "despcaldero", "despcalero"):
        # Deuda
        if not data.get("monto_total_deudas"):
            v = raw.get("Deuda") or raw.get("deuda")
            if v is not None:
                data["monto_total_deudas"] = str(v)

        # Cantidad de deudas
        if not data.get("cantidad_acreedores"):
            v = (raw.get("Cantidad deudas") or raw.get("Cantidad de deudas")
                 or raw.get("cantidad_deudas") or raw.get("cantidad deudas"))
            if v is not None:
                data["cantidad_acreedores"] = str(v)

        # Dispuesto
        if not data.get("dispuesto"):
            v = raw.get("Dispuesto") or raw.get("dispuesto")
            if v is not None:
                data["dispuesto"] = str(v)

        app.logger.debug("AFTER FILL (DespCaldero): " +
                         f"monto_total_deudas={data.get('monto_total_deudas')}, " +
                         f"cantidad_acreedores={data.get('cantidad_acreedores')}, " +
                         f"dispuesto={data.get('dispuesto')}")


    # 3) Lógica COMÚN: portal user -> (si OK) deal_id -> tarea Info lead
    result = process_lead_common(source, data, raw, config)

    return jsonify({
        "message": f"Lead de {source} procesado",
        "source": source,
        "portal_user_created": result["portal_user_created"],
        "info_lead_created": result["info_lead_created"],
        "deal_id": result["deal_id"],
        "task": result["task"],
    }), 200

@app.route('/reload-mappings', methods=['POST'])
def reload_mappings():
    """Endpoint para recargar mappings sin reiniciar el servidor"""
    try:
        form_mapping_manager.reload_mappings()
        return jsonify({"message": "Mappings recargados exitosamente"}), 200
    except Exception as e:
        app.logger.error(f"Error recargando mappings: {e}")
        return jsonify({"error": f"Error recargando mappings: {e}"}), 500

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        "status": "ok",
        "mappings_loaded": len(form_mapping_manager.mappings),
        "timestamp": datetime.datetime.now().isoformat()
    }), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5008, debug=True)