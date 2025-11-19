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
from flask import Flask, request, jsonify
import datetime
from flask_cors import CORS
# ----------------------------------------------------------------------------
# Configuración y constantes
# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------
# Configuración y constantes
# ----------------------------------------------------------------------------
TOKEN = os.getenv('SOLVIFY_API_TOKEN',
    'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IjEyMmZlYTI1LWQ1OWEtNGE2Zi04YzQ0LWIzZTVmZTExZTZmZSIsImVtYWlsIjoic2VydmljZUBzb2x2aWZ5LmVzIiwiZmlyc3RfbmFtZSI6IlNlcnZpY2UiLCJsYXN0X25hbWUiOiJTb2x2aWZ5IiwiaXNfYWN0aXZlIjp0cnVlLCJjcmVhdGVkX2F0IjoiMjAyNC0xMC0xN1QxNzowODozOC4xNjY3OTEiLCJjcmVhdGVkX2J5IjpudWxsLCJ1cGRhdGVkX2F0IjoiMjAyNC0xMC0xN1QxNTowODozOC45OCIsInVwZGF0ZWRfYnkiOm51bGwsImRlbGV0ZWRfYXQiOm51bGwsImRlbGV0ZWRfYnkiOm51bGwsImlzX2RlbGV0ZWQiOmZhbHNlLCJyb2xlX2lkIjoiODQ5ZmFiZTgtNDhjYi00ZWY4LWE0YWUtZTJiN2MzZjNlYTViIiwic3RyaXBlX2N1c3RvbWVyX2lkIjpudWxsLCJleHBvX3B1c2hfdG9rZW4iOm51bGwsInBob25lIjoiMCIsInJvbGVfbmFtZSI6IkFETUlOIiwicm9sZXMiOltdLCJpYXQiOjE3MjkxNzc4OTIsImV4cCI6Nzc3NzE3Nzg5Mn0.TJWtiOnLW8XyWjQDR_LAWvEiqrw50tWUmYiKXxo_5Wg')

app = Flask(__name__)
app.logger.setLevel(logging.DEBUG)

CORS(
    app,
    origins=[
        r"https://app\.solvify\.es",
        r"https://clientes\.solvify\.es",
        r"https://portal\..*",      # mejor regex para portal.*
        r"http://localhost:3000",
    ],
    supports_credentials=True,   # ponlo a True solo si usas cookies / auth de navegador
)

FALLBACK_COMPANY_ID = "9d4c6ef7-b5fa-4890-893f-51cafc247875"  # SICUEL

# Nombre del rol GESTOR_LEADS para asignaciones ETD (se busca en user_roles)
GESTOR_LEADS_ROLE_NAME = "GESTOR_LEADS"

# Oficinas para round-robin cuando no hay gestores disponibles
ROUND_ROBIN_OFFICES = ["Bilbao", "Madrid", "Zaragoza"]

# Variable global para tracking de round-robin (en producción usar Redis/DB)
_round_robin_counter = {"index": 0}
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
        result = {"fields": {}, "validations": {}, "company_name": None, "labels": {}}
        
        # Si hereda de otro mapping, combinar
        if "inherits_from" in config:
            parent_name = config["inherits_from"]
            if parent_name in self.mappings:
                parent = self._resolve_mapping(self.mappings[parent_name])
                result["fields"].update(parent["fields"])
                result["validations"].update(parent.get("validations", {}))
                result["labels"].update(parent.get("labels", {}))                
        
        # Aplicar campos específicos (override)
        result["fields"].update(config.get("fields", {}))
        result["validations"].update(config.get("validations", {}))
        result["company_name"] = config.get("company_name")
        result["labels"].update(config.get("labels", {}))        
        
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


def _sanitize_phone(raw_phone: str) -> str | None:
    if not raw_phone:
        return None

    # Convertir a string siempre
    s = str(raw_phone).strip()

    # Si viene de Google con #ERROR, lo detectamos:
    if "#ERROR" in s.upper():
        return None

    # Nos quedamos solo con dígitos
    digits = re.sub(r"\D", "", s)

    if not digits:
        return None

    # Normalización típica España: quitar 34 delante si lo han puesto
    if digits.startswith("34") and len(digits) > 9:
        digits = digits[2:]

    # Teléfonos de 9 dígitos (España) – puedes ajustar esta regla
    if len(digits) < 8:
        return None

    return digits

def _normalize_office_token(raw: str) -> str:
    """
    Normaliza el texto de la oficina a un token simple:
    - minúsculas
    - sin tildes/ñ
    - espacios → '_'
    """
    if raw is None:
        return ""
    text = str(raw).strip().lower()

    # quitar tildes y ñ
    replacements = {
        "á": "a", "à": "a",
        "é": "e", "è": "e",
        "í": "i", "ì": "i",
        "ó": "o", "ò": "o",
        "ú": "u", "ù": "u",
        "ü": "u",
        "ñ": "n",
    }
    for orig, repl in replacements.items():
        text = text.replace(orig, repl)

    # espacios → guiones bajos
    text = text.replace(" ", "_")

    return text

def _get_round_robin_office():
    """
    Retorna la siguiente oficina en round-robin entre Bilbao, Madrid y Zaragoza.
    En producción, usar Redis o DB para persistir el contador.
    """
    global _round_robin_counter
    
    office = ROUND_ROBIN_OFFICES[_round_robin_counter["index"] % len(ROUND_ROBIN_OFFICES)]
    _round_robin_counter["index"] += 1
    
    app.logger.info(
        f"[ROUND_ROBIN] Oficina seleccionada: {office} "
        f"(índice: {_round_robin_counter['index'] - 1})"
    )
    
    return office


def _get_gestores_leads_for_office(company_address_id, office_alias, cur):
    """
    Busca gestores de leads para una oficina específica.
    IMPORTANTE: Busca en user_roles (roles secundarios), NO en profiles.role_id
    
    Retorna lista de tuplas (user_id, first_name, last_name, email, role_id, total_deals)
    """
    app.logger.info(
        f"[GESTORES_LEADS] Buscando para oficina '{office_alias}' "
        f"(company_address_id={company_address_id})"
    )
    
    # Query CORRECTA: usa user_roles (roles secundarios) en lugar de profiles.role_id
    query_gestores = """
        SELECT 
            p.id AS user_id,
            p.first_name,
            p.last_name,
            p.email,
            r.id AS role_id,
            COUNT(d.id) AS total_deals
        FROM public.profile_comp_addresses pca
        JOIN public.profiles p
          ON p.id = pca.user_id
         AND p.is_deleted = FALSE
        JOIN public.user_roles ur
          ON ur.user_id = p.id
         AND ur.is_deleted = FALSE
        JOIN public.roles r
          ON r.id = ur.role_id
         AND r.is_deleted = FALSE
         AND r.name = %s  -- ✅ FILTRO POR NOMBRE DE ROL EN user_roles
        LEFT JOIN public.deals d
          ON d.user_assigned_id = p.id
         AND d.is_deleted = FALSE
         AND d.status != 'Negocio perdido'  -- Excluir deals perdidos
        WHERE pca.company_address_id = %s
          AND pca.is_deleted = FALSE
        GROUP BY p.id, p.first_name, p.last_name, p.email, r.id
        ORDER BY COUNT(d.id) ASC, p.created_at ASC  -- Menor carga primero
    """
    
    app.logger.debug(f"[GESTORES_LEADS] Query:\n{query_gestores}")
    app.logger.debug(f"[GESTORES_LEADS] Parámetros:")
    app.logger.debug(f"    - role_name: {GESTOR_LEADS_ROLE_NAME}")
    app.logger.debug(f"    - company_address_id: {company_address_id}")
    
    cur.execute(query_gestores, (GESTOR_LEADS_ROLE_NAME, company_address_id))
    rows_all = cur.fetchall()
    
    app.logger.info(f"[GESTORES_LEADS] Total encontrados: {len(rows_all)}")
    
    if rows_all:
        app.logger.info(f"[GESTORES_LEADS] Lista completa para '{office_alias}':")
        for i, row in enumerate(rows_all, 1):
            uid, fname, lname, email, role_id, deals = row
            app.logger.info(
                f"  {i}. {fname} {lname} <{email}> | "
                f"ID: {uid} | Role ID: {role_id} | Deals: {deals}"
            )
    else:
        app.logger.warning(
            f"[GESTORES_LEADS] ⚠️ No se encontraron gestores para '{office_alias}'"
        )
    
    return rows_all


def c_assign_deal_ETD(deal_id: str, source: str, data: dict):
    """
    Asigna el deal al GESTOR DE LEADS de la oficina con MENOR carga de trabajo.
    
    NUEVA LÓGICA CON FALLBACK:
    1. Si viene oficina informada → buscar gestores en esa oficina
    2. Si NO hay gestores en esa oficina → round-robin entre Bilbao, Madrid, Zaragoza
    3. Si NO viene oficina → round-robin directo entre Bilbao, Madrid, Zaragoza
    4. Contar deals activos de cada gestor (no perdidos, no borrados)
    5. Asignar al que tiene MENOS deals
    
    IMPORTANTE: Los GESTORES_LEADS se buscan en user_roles (roles secundarios)
    
    - Si algo falla, NO toca company_id (se queda el fallback SICUEL).
    """
    
    app.logger.info("="*80)
    app.logger.info(f"[ASSIGN_ETD] INICIO - deal_id={deal_id}, source={source}")
    app.logger.info(f"[ASSIGN_ETD] Datos recibidos: {data}")
    app.logger.info(f"[ASSIGN_ETD] 🎯 ROL GESTOR_LEADS: {GESTOR_LEADS_ROLE_NAME}")
    app.logger.info("="*80)

    if not deal_id:
        app.logger.warning("[ASSIGN_ETD] ❌ deal_id vacío, no se puede asignar.")
        return None

    # 1) Resolver oficina -> company_address_id
    app.logger.info(f"[ASSIGN_ETD] 🔍 Paso 1: Resolviendo oficina para source={source}")
    office_info = c_lead_assigment_ETD(source, data)
    
    app.logger.info(f"[ASSIGN_ETD] 📋 office_info completo: {office_info}")

    conn = None
    cur = None
    user_assigned_id = None
    company_id = None
    user_company_address_id = None
    company_address_id = None
    office_alias = None
    rows_all = []

    try:
        app.logger.info(f"[ASSIGN_ETD] 🔌 Conectando a base de datos...")
        conn = get_supabase_connection()
        cur = conn.cursor()
        app.logger.info(f"[ASSIGN_ETD] ✅ Conexión establecida")

        # --- 2) LÓGICA CON FALLBACK ROUND-ROBIN ---
        
        # 2A) Si tenemos oficina del formulario, intentar buscar gestores ahí
        if office_info and office_info.get("company_address_id"):
            company_address_id = office_info["company_address_id"]
            office_alias = office_info.get("alias")
            
            app.logger.info(
                f"[ASSIGN_ETD] ✅ Oficina del formulario: "
                f"alias='{office_alias}', company_address_id={company_address_id}"
            )
            
            # Obtener company_id
            cur.execute(
                """
                SELECT company_id
                FROM public.company_addresses
                WHERE id = %s
                  AND (is_deleted = FALSE OR is_deleted IS NULL)
                LIMIT 1;
                """,
                (company_address_id,),
            )
            row_company = cur.fetchone()
            
            if row_company:
                company_id = row_company[0]
                app.logger.info(f"[ASSIGN_ETD] ✅ company_id obtenido: {company_id}")
                
                # Buscar gestores en esta oficina
                rows_all = _get_gestores_leads_for_office(company_address_id, office_alias, cur)
            else:
                app.logger.warning(
                    f"[ASSIGN_ETD] ⚠️ No se encontró company_id para "
                    f"company_address_id={company_address_id}"
                )
        
        # 2B) Si NO hay oficina o NO hay gestores → FALLBACK ROUND-ROBIN
        if not rows_all:
            if office_info and office_info.get("company_address_id"):
                app.logger.warning(
                    f"[ASSIGN_ETD] ⚠️ No hay GESTORES DE LEADS en oficina '{office_alias}'. "
                    f"Aplicando FALLBACK: round-robin entre {ROUND_ROBIN_OFFICES}"
                )
            else:
                app.logger.info(
                    f"[ASSIGN_ETD] 💡 No viene oficina informada. "
                    f"Aplicando FALLBACK: round-robin entre {ROUND_ROBIN_OFFICES}"
                )
            
            # Intentar oficinas de round-robin hasta encontrar gestores
            max_attempts = len(ROUND_ROBIN_OFFICES)
            attempts = 0
            
            while not rows_all and attempts < max_attempts:
                fallback_alias = _get_round_robin_office()
                attempts += 1
                
                app.logger.info(
                    f"[ASSIGN_ETD] 🔄 Intento {attempts}/{max_attempts}: "
                    f"Buscando en oficina '{fallback_alias}'"
                )
                
                # Buscar company_address_id de la oficina fallback
                cur.execute(
                    """
                    SELECT id, company_id
                    FROM public.company_addresses
                    WHERE alias = %s
                      AND (is_deleted = FALSE OR is_deleted IS NULL)
                    LIMIT 1;
                    """,
                    (fallback_alias,),
                )
                row_office = cur.fetchone()
                
                if row_office:
                    company_address_id, company_id = row_office
                    office_alias = fallback_alias
                    
                    app.logger.info(
                        f"[ASSIGN_ETD] ✅ Oficina fallback encontrada: "
                        f"'{office_alias}' (company_address_id={company_address_id}, "
                        f"company_id={company_id})"
                    )
                    
                    # Buscar gestores en esta oficina
                    rows_all = _get_gestores_leads_for_office(
                        company_address_id, office_alias, cur
                    )
                else:
                    app.logger.warning(
                        f"[ASSIGN_ETD] ⚠️ Oficina '{fallback_alias}' no encontrada en DB"
                    )
            
            if not rows_all:
                app.logger.error(
                    f"[ASSIGN_ETD] ❌ CRÍTICO: No se encontraron GESTORES DE LEADS "
                    f"en ninguna oficina de round-robin: {ROUND_ROBIN_OFFICES}"
                )
                return {
                    "deal_id": deal_id,
                    "company_id": None,
                    "user_assigned_id": None,
                    "company_address_id": None,
                    "office_info": office_info,
                    "error": "No hay GESTORES DE LEADS disponibles en ninguna oficina"
                }

        # --- 3) Seleccionar gestor con MENOR carga ---
        row = rows_all[0]
        user_assigned_id, first_name, last_name, email, role_id, total_deals = row
        comercial_nombre = f"{first_name} {last_name}".strip()

        app.logger.info(
            f"[ASSIGN_ETD] 🎯 GESTOR DE LEADS SELECCIONADO: {comercial_nombre} <{email}>"
        )
        app.logger.info(
            f"[ASSIGN_ETD] 📊 ID: {user_assigned_id} | "
            f"Role ID: {role_id} | "
            f"Carga actual: {total_deals} deals activos | "
            f"Oficina: '{office_alias}'"
        )

        # --- 4) Obtener company_address_id del usuario asignado ---
        app.logger.info(
            f"[ASSIGN_ETD] 🔍 Paso 4: Obteniendo company_address_id del usuario "
            f"{user_assigned_id}"
        )
        
        cur.execute(
            """
            SELECT pca.company_address_id
            FROM public.profile_comp_addresses pca
            WHERE pca.user_id = %s
              AND pca.is_deleted = FALSE
            ORDER BY pca.created_at DESC
            LIMIT 1;
            """,
            (user_assigned_id,),
        )
        row_address = cur.fetchone()
        
        if row_address:
            user_company_address_id = row_address[0]
            app.logger.info(
                f"[ASSIGN_ETD] ✅ company_address_id del usuario: "
                f"{user_company_address_id}"
            )
        else:
            app.logger.warning(
                f"[ASSIGN_ETD] ⚠️ No se encontró company_address_id para el usuario. "
                f"Se usará el de la oficina: {company_address_id}"
            )
            user_company_address_id = company_address_id

        # --- 5) Actualizar deal ---
        app.logger.info(f"[ASSIGN_ETD] 🔍 Paso 5: Actualizando deal {deal_id}")
        app.logger.info(f"[ASSIGN_ETD] 📝 Valores a actualizar:")
        app.logger.info(f"    - user_assigned_id: {user_assigned_id} ({comercial_nombre})")
        app.logger.info(f"    - company_id: {company_id}")
        app.logger.info(f"    - company_address_id: {user_company_address_id}")
        
        cur.execute(
            """
            UPDATE public.deals
               SET user_assigned_id = %s,
                   company_id       = %s,
                   company_address_id = %s,
                   updated_at       = now()
             WHERE id = %s
               AND is_deleted = FALSE;
            """,
            (user_assigned_id, company_id, user_company_address_id, deal_id),
        )
        
        rows_affected = cur.rowcount
        app.logger.info(f"[ASSIGN_ETD] 📊 Filas afectadas por UPDATE: {rows_affected}")
        
        if rows_affected == 0:
            app.logger.warning(
                f"[ASSIGN_ETD] ⚠️ UPDATE no afectó ninguna fila. "
                f"Verificar que el deal {deal_id} existe y no está borrado."
            )
        
        conn.commit()
        app.logger.info(f"[ASSIGN_ETD] ✅ Commit exitoso")

        resultado = {
            "deal_id": deal_id,
            "company_id": str(company_id) if company_id else None,
            "user_assigned_id": str(user_assigned_id) if user_assigned_id else None,
            "company_address_id": str(user_company_address_id) if user_company_address_id else None,
            "office_info": office_info,
            "office_final": office_alias,
            "comercial_nombre": comercial_nombre,
            "comercial_email": email,
            "comercial_role_id": str(role_id),
            "carga_actual": int(total_deals),
            "total_gestores_disponibles": len(rows_all),
            "fallback_usado": office_alias in ROUND_ROBIN_OFFICES and (
                not office_info or 
                not office_info.get("company_address_id") or 
                office_info.get("alias") != office_alias
            )
        }

        app.logger.info("="*80)
        app.logger.info(f"[ASSIGN_ETD] ✅ ÉXITO - Deal {deal_id} asignado correctamente")
        app.logger.info(f"[ASSIGN_ETD] 📊 Resultado final:")
        for key, value in resultado.items():
            app.logger.info(f"    {key}: {value}")
        app.logger.info("="*80)

        return resultado

    except Exception as e:
        app.logger.error("="*80)
        app.logger.error(f"[ASSIGN_ETD] ❌ ERROR CRÍTICO asignando deal {deal_id}")
        app.logger.error(f"[ASSIGN_ETD] Tipo de error: {type(e).__name__}")
        app.logger.error(f"[ASSIGN_ETD] Mensaje: {e}")
        app.logger.error(f"[ASSIGN_ETD] Traceback completo:", exc_info=True)
        app.logger.error(f"[ASSIGN_ETD] Contexto:")
        app.logger.error(f"    - source: {source}")
        app.logger.error(f"    - office_alias: {office_alias}")
        app.logger.error(f"    - company_address_id: {company_address_id}")
        app.logger.error(f"    - company_id: {company_id}")
        app.logger.error("="*80)
        
        return {
            "deal_id": deal_id,
            "company_id": None,
            "user_assigned_id": None,
            "company_address_id": None,
            "office_info": office_info,
            "error": f"{type(e).__name__}: {str(e)}"
        }
    finally:
        try:
            if cur:
                cur.close()
                app.logger.debug("[ASSIGN_ETD] 🔌 Cursor cerrado")
            if conn:
                conn.close()
                app.logger.debug("[ASSIGN_ETD] 🔌 Conexión cerrada")
        except Exception as cleanup_error:
            app.logger.error(
                f"[ASSIGN_ETD] ⚠️ Error cerrando conexión: {cleanup_error}"
            )

def c_lead_assigment_ETD(source: str, data: dict):
    """
    1) Lee la oficina del formulario:
       - ETD  -> P2 (Oficina Seleccionada)
       - ETD2 -> P1 (Oficina Seleccionada)
    2) Mapea el valor del formulario a un alias de company_addresses.alias
    3) Busca la oficina en company_addresses.
    4) Saca los comerciales (profiles) ligados a esa oficina vía profile_comp_addresses.
    5) Devuelve info básica + lista de comerciales.
    """
    # 1) Leer oficina desde el payload normalizado
    office_raw = None
    if source == "ETD":
        office_raw = data.get("P2") or data.get("p2")
    elif source == "ETD2":
        office_raw = data.get("P1") or data.get("p1")
    else:
        app.logger.debug(
            f"[ASSIGN_ETD] Source '{source}' no usa lógica de oficinas (solo ETD / ETD2)."
        )
        return None

    office_raw = (office_raw or "").strip()
    if not office_raw:
        app.logger.info(
            f"[ASSIGN_ETD] {source}: sin oficina en formulario, no se hace nada."
        )
        return None

    # 2) Normalizar valor de oficina a un token
    token = _normalize_office_token(office_raw)

    # 3) Mapeo de token → alias de company_addresses.alias
    OFFICE_TOKEN_TO_ALIAS = {
        # ciudades directas
        "alicante": "Alicante",
        "jerez": "Jerez",
        "las_palmas_de_gran_canaria": "Las Palmas",
        "las_palmas": "Las Palmas",
        "madrid": "Madrid",
        "malaga": "Málaga",
        "palma_de_mallorca": "Palma de Mallorca",
        "mallorca": "Palma de Mallorca",
        "murcia": "Murcia",
        "tenerife": "Tenerife",
        "santa_cruz_de_tenerife": "Tenerife",
        "barcelona": "Barcelona",
        "badalona": "Badalona",
        "hospitalet": "Hospitalet del Llobregat",
        "hospitalet_del_llobregat": "Hospitalet del Llobregat",
        "sevilla": "Sevilla",
        "terrassa": "Terrassa",
        "tarragona": "Tarragona",
        "zaragoza": "Zaragoza",
        "valencia": "Valencia",
        "bilbao": "Bilbao",

        # casos de "no tengo ninguna cerca"
        "no_tengo_ninguna_cerca_(te_llamaremos)": None,
        "no_tengo_ninguna_cerca_te_llamaremos": None,
        "no_tengo_ninguna_cerca": None,
    }

    # Caso especial: "no tengo ninguna cerca..."
    if token.startswith("no_tengo_ninguna_cerca"):
        app.logger.info(
            f"[ASSIGN_ETD] {source}: el cliente no tiene oficina cercana "
            f"('{office_raw}'), de momento no asignamos a ninguna oficina."
        )
        return {
            "office_raw": office_raw,
            "alias": None,
            "company_address_id": None,
            "sales_reps": [],
        }

    alias = OFFICE_TOKEN_TO_ALIAS.get(token)

    if not alias:
        app.logger.warning(
            f"[ASSIGN_ETD] {source}: oficina '{office_raw}' "
            f"(token='{token}') no mapeada a ningún alias de company_addresses."
        )
        return {
            "office_raw": office_raw,
            "alias": None,
            "company_address_id": None,
            "sales_reps": [],
        }

    conn = None
    cur = None
    try:
        conn = get_supabase_connection()
        cur = conn.cursor()

        # 4) Buscar la oficina en company_addresses por alias
        cur.execute(
            """
            SELECT id, alias
            FROM public.company_addresses
            WHERE alias = %s
              AND (is_deleted = FALSE OR is_deleted IS NULL)
            LIMIT 1;
            """,
            (alias,),
        )
        row = cur.fetchone()

        if not row:
            app.logger.warning(
                f"[ASSIGN_ETD] {source}: alias '{alias}' no encontrado "
                f"en company_addresses (office_raw='{office_raw}', token='{token}')."
            )
            return {
                "office_raw": office_raw,
                "alias": alias,
                "company_address_id": None,
                "sales_reps": [],
            }

        company_address_id, alias_db = row

        app.logger.info(
            f"[ASSIGN_ETD] {source}: oficina formulario '{office_raw}' "
            f"→ alias '{alias_db}' → company_address_id={company_address_id}"
        )

        # 5) Obtener comerciales (profiles) ligados a esa oficina
        cur.execute(
            """
            SELECT p.id,
                   p.first_name,
                   p.last_name,
                   p.email
            FROM public.profile_comp_addresses pca
            JOIN public.profiles p
              ON p.id = pca.user_id
            WHERE pca.company_address_id = %s
              AND pca.is_deleted = FALSE
              AND p.is_deleted = FALSE;
            """,
            (company_address_id,),
        )
        reps = cur.fetchall()

        sales_reps = []
        for rep in reps:
            rep_id, first_name, last_name, email = rep
            full_name = f"{first_name} {last_name}".strip()
            sales_reps.append(
                {
                    "id": str(rep_id),
                    "full_name": full_name,
                    "email": email,
                }
            )

        if sales_reps:
            app.logger.info(
                f"[ASSIGN_ETD] {source}: comerciales para alias '{alias_db}' "
                f"(company_address_id={company_address_id}): "
                + ", ".join([f"{r['full_name']} <{r['email']}>" for r in sales_reps])
            )
        else:
            app.logger.warning(
                f"[ASSIGN_ETD] {source}: sin comerciales activos para "
                f"company_address_id={company_address_id} (alias='{alias_db}')"
            )

        return {
            "office_raw": office_raw,
            "alias": alias_db,
            "company_address_id": str(company_address_id),
            "sales_reps": sales_reps,
        }

    except Exception as e:
        app.logger.error(
            f"[ASSIGN_ETD] Error buscando oficina/comerciales para '{office_raw}' "
            f"(token='{token}', alias='{alias}') : {e}",
            exc_info=True,
        )
        return {
            "office_raw": office_raw,
            "alias": alias,
            "company_address_id": None,
            "sales_reps": [],
        }
    finally:
        try:
            if cur:
                cur.close()
            if conn:
                conn.close()
        except Exception:
            pass

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
    labels_map = mapping.get('labels') or {}
    
    # 1) Reglas específicas “Despacho calero”: etiquetas legibles
    is_calero = (source or '').strip().lower() in ('despacho calero', 'despcaldero', 'despcalero')

    # 2) Recorremos TODOS los campos normalizados presentes en data_norm
    for norm_key, val in data_norm.items():
        # Excluir campos técnicos que no deben salir en el InfoLead
        if norm_key in ('nombre_y_apellidos', 'correo_electrónico', 'número_de_teléfono', 'leadgen_id', 'lead_gen_id'):
            continue  

        label = labels_map.get(norm_key) or label_by_norm.get(norm_key, norm_key)


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
    phone = _sanitize_phone(strip_country_code(data.get('número_de_teléfono','') or data.get('phone_number','')))

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
        if source in ('Backup_FB', 'Alianza_FB', 'MARTIN','fb'):
            company_name = 'Solvify'
            app.logger.debug("Company name asignado: 'Solvify'")
        else:
            company_name = source
            app.logger.debug(f"Company name asignado desde source: '{company_name}'")
    
    app.logger.debug(f"Company name final antes de DB lookup: '{company_name}'")

    # 4️⃣ Lookup dinámico de company_id y company_address_id
    # NUEVO: Si config ya trae company_id directo, usarlo
    if config and config.get("company_id"):
        company_id = config.get("company_id")
        company_address_id = config.get("company_address_id")
        app.logger.debug(f"Company ID desde config (directo): {company_id}")
        app.logger.debug(f"Company Address ID desde config (directo): {company_address_id}")
    else:
        # Lookup normal por company_name
        company_id = None
        company_address_id = None
        
        try:
            conn = get_supabase_connection()
            cur = conn.cursor()

            cur.execute("""
                SELECT 
                    c.id AS company_id,
                    ca.id AS company_address_id
                FROM companies c
                LEFT JOIN LATERAL (
                    SELECT 
                        a.id,
                        a.created_at
                    FROM company_addresses a
                    WHERE a.company_id = c.id
                      AND a.is_deleted = FALSE
                    ORDER BY a.created_at ASC
                    LIMIT 1
                ) ca ON TRUE
                WHERE c.is_deleted = FALSE
                  AND LOWER(c.name) = LOWER(%s)
                LIMIT 1;
            """, (company_name,))

            row = cur.fetchone()

            if row:
                company_id = str(row[0]) if row[0] is not None else None
                company_address_id = str(row[1]) if row[1] is not None else None

                app.logger.debug(f"Company ID encontrado para '{company_name}': {company_id}")
                app.logger.debug(f"Company Address ID encontrado para '{company_name}': {company_address_id}")
            else:
                app.logger.warning(f"No se encontró company para '{company_name}'")

        except Exception as e:
            app.logger.error(f"Error buscando company_id / company_address_id para '{company_name}': {e}")

        finally:
            try:
                cur.close()
                conn.close()
            except Exception:
                pass

    # 4.5️⃣ Verificar si el cliente (número) ya existe para ESTE company_id
    if phone and company_id:
        try:
            conn = get_supabase_connection()
            cur = conn.cursor()
            
            # Buscar deal existente
            cur.execute("""
                SELECT d.id, d.status
                FROM leads l
                JOIN deals d ON d.lead_id = l.id AND d.is_deleted = FALSE
                WHERE TRIM(REPLACE(REPLACE(l.phone, '+34', ''), ' ', '')) = %s
                AND l.is_deleted = FALSE
                AND d.company_id = %s
                ORDER BY d.created_at DESC
                LIMIT 1
            """, (phone, company_id))
            existing_deal = cur.fetchone()

            if existing_deal:
                deal_id, current_status = existing_deal
                app.logger.warning(
                    f"⚠️ DUPLICADO DETECTADO: {full} | TEL={phone} | "
                    f"Company ID: {company_id} | Deal ID: {deal_id} | Status actual: {current_status}"
                )
                
                # Actualizar deal a "Nuevo Contacto"
                cur.execute("""
                    UPDATE public.deals
                    SET status = 'Nuevo contacto',
                        updated_at = now()
                    WHERE id = %s
                    AND is_deleted = FALSE
                """, (deal_id,))
                conn.commit()
                
                app.logger.info(
                    f"✅ Deal {deal_id} actualizado a 'Nuevo contacto' | "
                    f"Cliente: {full} | TEL={phone} | Status anterior: {current_status}"
                )
                
                # Retornar None para no crear nuevo portal user
                return None
            else:
                app.logger.debug(
                    f"Cliente {phone} no existe en deals de company_id {company_id}, procediendo a crear"
                )

        except Exception as e:
            app.logger.error(f"Error verificando/actualizando duplicado para {phone} en company {company_id}: {e}")
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

    
    #mappeo específico de origen
    
    if source=='ETD2':
        origin = 'gads'
    elif source=='ETD':
        origin = 'fb'
    else:
        origin = 'fb'   
#mappeo específico de origen

    
    payload = {
        'first_name': first,
        'last_name':  last,
        'email':      email,
        **({'phone': phone} if phone else {}),
        'channel':    origin,
        'form_name':  data.get('form_name',''),
        'campaign':   data.get('campaign_name',''),
        'lead_gen_id':data.get('lead_gen_id') or data.get('leadgen_id',''),
        'company_id': company_id or FALLBACK_COMPANY_ID,
        'company_address_id': company_address_id or FALLBACK_COMPANY_ID
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

    # 4️⃣ Asignación final de deal (solo ETD / ETD2 de momento)
    assignment_result = None
    try:
        if deal_id and source in ("ETD", "ETD2"):
            # ❌ antes: c_assign_deal_ETD(deal_id, source, data, config)
            assignment_result = c_assign_deal_ETD(deal_id, source, data)
            app.logger.info(f"[ASSIGN_ETD] Resultado final: {assignment_result}")
    except Exception as e:
        app.logger.error(
            f"[ASSIGN_ETD] Error inesperado al asignar deal {deal_id} para source={source}: {e}",
            exc_info=True,
               )
    return {
        "portal_user_created": True,
        "info_lead_created": task is not None,
        "deal_id": deal_id,
        "task": task,
        "assignment": assignment_result,  # útil para debug en logs/respuesta
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
        'nombre_y_apellidos': ['name', 'full_name', 'fullname', 'nombre', 'nombre_completo'],
        'correo_electrónico': ['mail', 'email', 'correo', 'e-mail'],
        'número_de_teléfono': ['teléfono', 'phone_number', 'telefono', 'tel', 'mobile'],
    }
    for target_field, aliases in critical_fields.items():
        cur = str(data.get(target_field, '')).strip()
        if not cur:
            for alias in aliases:
                raw_value_original = raw.get(alias, '')
                raw_value = str(raw_value_original).strip() if raw_value_original != '' else ''
                if raw_value:
                    data[target_field] = raw_value
                    app.logger.debug(f"FALLBACK: '{alias}' → '{target_field}' = {raw_value!r}")
                    break

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
        'nombre_y_apellidos': ['Nombre', 'Full Name', 'email'],
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

@app.route('/B2B_Manual', methods=['POST', 'OPTIONS'])
def receive_b2b_manual_lead():
    if request.method == 'OPTIONS':
        # flask-cors se encarga de las cabeceras, solo respondemos vacío
        return '', 204
    """
    Endpoint para leads creados manualmente en el sistema (presencial, etc.)
    
    Payload esperado:
    {
        "first_name": "nombre",
        "last_name": "apellido",
        "email": "email@example.com",
        "phone": "123456789",
        "channel": "presencial",
        "company_id": "uuid",
        "company_address_id": "uuid",
        "campaign": null,
        "form_name": null
    }
    
    Normaliza el payload y reutiliza process_lead_common().
    """
    raw = request.json or {}
    
    app.logger.info("="*80)
    app.logger.info("NUEVO LEAD B2B_MANUAL RECIBIDO:")
    for key, value in raw.items():
        app.logger.info(f"  {key}: {value}")
    app.logger.info("="*80)
    
    # 1️⃣ Validar campos requeridos
    required_fields = ['first_name', 'last_name', 'email', 'phone', 'company_id', 'company_address_id']
    missing_fields = [field for field in required_fields if not raw.get(field)]
    
    if missing_fields:
        error_msg = f"Campos requeridos faltantes: {', '.join(missing_fields)}"
        app.logger.error(f"[B2B_MANUAL] ❌ {error_msg}")
        return jsonify({
            "success": False,
            "error": error_msg,
            "missing_fields": missing_fields
        }), 400
    
    # 2️⃣ Sanitizar teléfono
    phone = _sanitize_phone(raw.get('phone', ''))
    
    if not phone:
        error_msg = "Teléfono inválido después de sanitización"
        app.logger.error(f"[B2B_MANUAL] ❌ {error_msg}: {raw.get('phone')}")
        return jsonify({
            "success": False,
            "error": error_msg,
            "original_phone": raw.get('phone')
        }), 400
    
    # 3️⃣ NORMALIZAR a formato esperado por process_lead_common
    # Transformar payload manual → formato estándar
    data_normalized = {
        'nombre_y_apellidos': f"{raw['first_name']} {raw['last_name']}".strip(),
        'correo_electrónico': raw['email'],
        'número_de_teléfono': phone,
        'form_name': raw.get('form_name', 'Manual'),
        'campaign_name': raw.get('campaign', 'Manual'),
        'lead_gen_id': raw.get('lead_gen_id', '')
    }
    
    # 4️⃣ Config especial que incluye company_id y company_address_id directos
    config_manual = {
        "fields": {},
        "validations": {},
        "company_name": None,  # No usamos company_name, tenemos IDs directos
        "company_id": raw['company_id'],
        "company_address_id": raw['company_address_id'],
        "channel": raw.get('channel', 'presencial')
    }
    
    source = 'B2B_Manual'
    
    app.logger.info(f"[B2B_MANUAL] Datos normalizados: {data_normalized}")
    app.logger.info(f"[B2B_MANUAL] Config: {config_manual}")
    
    # 5️⃣ MODIFICAR create_portal_user para aceptar company_id directo desde config
    # (necesitamos un pequeño ajuste en create_portal_user)
    
    # 6️⃣ REUTILIZAR process_lead_common (con pequeño ajuste)
    result = process_lead_common(source, data_normalized, raw, config_manual)
    
    # 7️⃣ Si hay deal_id, REUTILIZAR lógica de asignación
    if result.get("deal_id") and config_manual.get("company_address_id"):
        try:
            # Simular data para asignación con company_address_id
            data_for_assignment = {
                "company_address_id": config_manual["company_address_id"]
            }
            
            # Crear un pseudo office_info
            conn = get_supabase_connection()
            cur = conn.cursor()
            
            cur.execute(
                """
                SELECT alias, company_id
                FROM public.company_addresses
                WHERE id = %s AND is_deleted = FALSE
                LIMIT 1;
                """,
                (config_manual['company_address_id'],)
            )
            
            row = cur.fetchone()
            
            if row:
                office_alias = row[0]
                company_id = row[1]
                
                # REUTILIZAR _get_gestores_leads_for_office
                rows_gestores = _get_gestores_leads_for_office(
                    config_manual['company_address_id'],
                    office_alias,
                    cur
                )
                
                if not rows_gestores:
                    # REUTILIZAR round-robin
                    app.logger.info(f"[B2B_MANUAL] Sin gestores en {office_alias}, round-robin")
                    
                    for attempt in range(len(ROUND_ROBIN_OFFICES)):
                        rr_office = _get_round_robin_office()
                        
                        cur.execute(
                            """
                            SELECT id, company_id
                            FROM public.company_addresses
                            WHERE alias = %s AND is_deleted = FALSE
                            LIMIT 1;
                            """,
                            (rr_office,)
                        )
                        
                        row_rr = cur.fetchone()
                        
                        if row_rr:
                            config_manual['company_address_id'] = row_rr[0]
                            company_id = row_rr[1]
                            office_alias = rr_office
                            
                            rows_gestores = _get_gestores_leads_for_office(
                                config_manual['company_address_id'],
                                office_alias,
                                cur
                            )
                            
                            if rows_gestores:
                                break
                
                if rows_gestores:
                    # Asignar al gestor con menor carga
                    gestor = rows_gestores[0]
                    user_id, fname, lname, email, role_id, deals = gestor
                    
                    # Obtener company_address_id del usuario
                    cur.execute(
                        """
                        SELECT company_address_id
                        FROM public.profile_comp_addresses
                        WHERE user_id = %s AND is_deleted = FALSE
                        ORDER BY created_at DESC
                        LIMIT 1;
                        """,
                        (user_id,)
                    )
                    
                    user_ca = cur.fetchone()
                    user_company_address_id = user_ca[0] if user_ca else config_manual['company_address_id']
                    
                    # UPDATE del deal
                    cur.execute(
                        """
                        UPDATE public.deals
                        SET user_assigned_id = %s,
                            company_id = %s,
                            company_address_id = %s,
                            updated_at = now()
                        WHERE id = %s AND is_deleted = FALSE;
                        """,
                        (user_id, company_id, user_company_address_id, result['deal_id'])
                    )
                    
                    conn.commit()
                    
                    result['assignment'] = {
                        "success": True,
                        "user_assigned": f"{fname} {lname}",
                        "office": office_alias,
                        "carga": int(deals)
                    }
                    
                    app.logger.info(f"[B2B_MANUAL] ✅ Deal asignado a {fname} {lname}")
                else:
                    app.logger.warning(f"[B2B_MANUAL] ⚠️ Sin gestores disponibles")
            
            cur.close()
            conn.close()
            
        except Exception as e:
            app.logger.error(f"[B2B_MANUAL] Error en asignación: {e}", exc_info=True)
    
    # 8️⃣ Respuesta
    return jsonify({
        "success": True,
        "portal_user_created": result["portal_user_created"],
        "deal_id": result["deal_id"],
        "assignment": result.get("assignment"),
        "info_lead_created": result["info_lead_created"],
        "message": "Lead manual procesado"
    }), 200