#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import socket
import urllib.parse
import time as _time
from requests import Session, PreparedRequest
import os
import logging
import configparser
import json
import time
from uuid import uuid4
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from threading import Thread
from functools import wraps
from urllib.parse import urlparse, urlunparse
from flask import Flask, request, jsonify
from flask_cors import CORS
from werkzeug.exceptions import BadRequest
import logging
import pg8000
import requests
from requests.exceptions import HTTPError
from werkzeug.utils import secure_filename
from enum import Enum
from typing import Dict, Optional, Tuple
import requests
import mimetypes
import os
from urllib.parse import urlparse
from supabase import create_client, Client
import hashlib
import re
from enum import Enum



import os

GRAPH_API_VERSION = (os.getenv("GRAPH_API_VERSION") or "v22.0").strip() or "v22.0"

class CompanyConfigCache:
    """Cache manager for company configurations"""
    def __init__(self):
        self._cache = {}
        self._last_reload = None
        self.reload_interval = 3600  # 1 hora entre recargas forzadas
        self._logger = logging.getLogger(__name__)

    def get(self, company_id: str) -> dict:
        """Get company config from cache"""
        return self._cache.get(company_id)

    def set(self, company_id: str, config: dict):
        """Set company config in cache"""
        self._cache[company_id] = config
        
    def preload_all_companies(self, db_manager):
        """Load all company configurations into cache"""
        try:
            # Obtener todos los IDs de compañías activas
            companies_query = """
                SELECT id 
                FROM public.companies 
                WHERE is_deleted = false 
            """
            company_results = db_manager.execute_query(companies_query, fetch_all=True)
            
            if not company_results:
                self._logger.warning("No se encontraron compañías activas")
                return
                
            # Extraer los IDs de los resultados
            company_ids = [str(row[0]) for row in company_results]
            self._logger.info(f"Encontradas {len(company_ids)} compañías activas para cargar")
            loaded = 0
            for company_id in company_ids:
                try:
                    # Llamar directamente a get_company_data para cada compañía
                    result = db_manager.execute_query(
                        "SELECT public.get_company_data(%s) as config",
                        [company_id],
                        fetch_one=True
                    )
                    
                    if result and result[0]:
                        company_data = result[0]
                        
                        # Verificar que tenemos los datos necesarios
                        if isinstance(company_data, dict) and company_data.get('id'):
                            self._cache[company_id] = {
                                'id': company_id,
                                'name': company_data.get('name', 'Unknown'),
                                'config': company_data
                            }
                            loaded += 1
                            
                            # Log de configuración cargada
                            self._logger.info(f"✅ Configuración cargada para {company_data.get('name')} (id: {company_id})")
                            
                            # Log de valores críticos de WhatsApp si existen
                            custom_props = company_data.get('custom_properties', {})
                            if custom_props:
                                self._logger.debug(f"   • WhatsApp config para {company_id}:")
                                self._logger.debug(f"     - Business ID: {custom_props.get('WHATSAPP_BUSINESS_ID')}")
                                self._logger.debug(f"     - Phone ID: {custom_props.get('WHATSAPP_PHONE_NUMBER_ID')}")
                                token = custom_props.get('WHATSAPP_ACCESS_TOKEN', '')
                                if token:
                                    self._logger.debug(f"     - Token: {token[:20]}...")
                            else:
                                self._logger.warning(f"   • No custom_properties encontradas para {company_id}")
                        else:
                            self._logger.warning(f"❌ Datos inválidos o incompletos para compañía {company_id}")
                    else:
                        self._logger.warning(f"❌ No se encontraron datos para compañía {company_id}")
                            
                except Exception as e:
                    self._logger.error(f"Error cargando config para compañía {company_id}: {e}")
                    continue
            
            self._last_reload = time.time()
            self._logger.info(f"✅ Precargadas {loaded}/{len(company_ids)} configuraciones de compañías")
            
            # Log del estado final de la caché
            self._logger.info(f"📦 Caché contiene {len(self._cache)} configuraciones en total")
            
        except Exception as e:
            self._logger.exception(f"Error precargando configuraciones de compañías: {e}")



    def get_config_by_phone(self, phone: str, db_manager) -> tuple[dict, str, str]:
        """Get company config by phone number, returns (config, company_name, company_id)"""
        try:
            clean_phone = PhoneUtils.strip_34(phone)
            query = """
                SELECT c.id, c.name
                FROM public.companies c
                JOIN public.deals d ON d.company_id = c.id
                JOIN public.leads l ON d.lead_id = l.id
                WHERE l.phone = %s 
                AND l.is_deleted = false
                AND d.is_deleted = false
                LIMIT 1
            """
            result = db_manager.execute_query(query, [clean_phone], fetch_one=True)
            
            if not result:
                return None, None, None
                
            
            company_id = str(result[0])  # ✅ Convertir UUID a string
            company_name = result[1]
            company_cache = self.get(company_id)
            
            if not company_cache:
                # Si no está en caché, recargar solo esta compañía
                query_company = "SELECT public.get_company_data(%s) as config"
                company_data = db_manager.execute_query(query_company, [company_id], fetch_one=True)
                if company_data and company_data[0]:
                    company_cache = {
                        'id': company_id,
                        'name': company_name,
                        'config': company_data[0]
                    }
                    self.set(company_id, company_cache)
            
            if company_cache:
                return (
                    company_cache['config'].get('custom_properties', {}),
                    company_cache['name'],
                    company_id
                )
                
            return None, company_name, company_id
            
        except Exception as e:
            self._logger.exception(f"Error getting config by phone: {e}")
            return None, None, None

# Instancia global de la caché
company_cache = CompanyConfigCache()

class ExtendedFileService:
    """
    Service para manejar uploads/downloads con soporte completo para todos los MIME types
    soportados por WhatsApp Business Cloud API según la documentación oficial de Meta.
    """
    
    # Configuración actualizada según documentación oficial de WhatsApp Cloud API
    WHATSAPP_MEDIA_CONFIG = {
        'image': {
            'mime_types': [
                'image/jpeg', 'image/jpg', 'image/png'
            ],
            'extensions': ['.jpg', '.jpeg', '.png'],
            'max_size': 5 * 1024 * 1024,  # 5MB
            'document_type': 'whatsapp_image',
            'whatsapp_type': 'image'
        },
        'audio': {
            'mime_types': [
                'audio/aac', 'audio/mp4', 'audio/amr', 'audio/mpeg', 
                'audio/ogg', 'audio/opus', 'audio/wav'
            ],
            'extensions': ['.aac', '.m4a', '.amr', '.mp3', '.ogg', '.opus', '.wav'],
            'max_size': 16 * 1024 * 1024,  # 16MB
            'document_type': 'whatsapp_audio',
            'whatsapp_type': 'audio'
        },
        'video': {
            'mime_types': [
                'video/mp4', 'video/3gpp', 'video/quicktime', 'video/avi', 'video/mkv'
            ],
            'extensions': ['.mp4', '.3gp', '.mov', '.avi', '.mkv'],
            'max_size': 16 * 1024 * 1024,  # 16MB
            'document_type': 'whatsapp_video',
            'whatsapp_type': 'video'
        },
        'sticker': {
            'mime_types': ['image/webp'],
            'extensions': ['.webp'],
            'max_size': 500 * 1024,  # 500KB
            'document_type': 'whatsapp_sticker',
            'whatsapp_type': 'sticker'
        },
        'voice': {
            'mime_types': ['audio/ogg'],  # Solo OGG con codec Opus para voice
            'extensions': ['.ogg'],
            'max_size': 16 * 1024 * 1024,  # 16MB
            'document_type': 'whatsapp_voice',
            'whatsapp_type': 'voice'
        },
        'document': {
            # WhatsApp acepta CUALQUIER MIME type válido para documentos
            'mime_types': [
                # Documentos comunes
                'application/pdf',
                'application/msword',
                'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
                'application/vnd.ms-powerpoint',
                'application/vnd.openxmlformats-officedocument.presentationml.presentation',
                'application/vnd.ms-excel',
                'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                'text/plain', 'text/csv', 'text/rtf',
                
                # Archivos comprimidos
                'application/zip', 'application/x-rar-compressed', 'application/x-7z-compressed',
                'application/gzip', 'application/x-tar',
                
                # Código y desarrollo
                'application/json', 'application/xml', 'text/html', 'text/css',
                'application/javascript', 'text/javascript',
                
                # Imágenes adicionales (como documentos)
                'image/gif', 'image/bmp', 'image/tiff', 'image/svg+xml',
                
                # Otros formatos
                'application/octet-stream',  # Binarios genéricos
                'application/x-executable',  # Ejecutables
                'application/vnd.google-earth.kml+xml',  # KML
                'application/epub+zip',  # EPUB
                
                # Placeholder para cualquier otro MIME type
                '*/*'  # Acepta cualquier tipo como documento
            ],
            'extensions': [
                '.pdf', '.doc', '.docx', '.ppt', '.pptx', '.xls', '.xlsx',
                '.txt', '.csv', '.rtf', '.zip', '.rar', '.7z', '.tar', '.gz',
                '.json', '.xml', '.html', '.css', '.js', '.gif', '.bmp', 
                '.tiff', '.svg', '.bin', '.exe', '.kml', '.epub'
            ],
            'max_size': 100 * 1024 * 1024,  # 100MB según documentación oficial
            'document_type': 'whatsapp_document',
            'whatsapp_type': 'document'
        }
    }

    # Lista de MIME types conocidos que deben ser tratados como tipos específicos
    SPECIFIC_TYPE_OVERRIDES = {
        'image/webp': 'sticker',  # WEBP siempre es sticker
        'audio/ogg': 'voice',     # OGG específicamente para voice messages
    }

    def __init__(self, config, db_manager):
        self.config = config
        self.db_manager = db_manager
        
        # Configuración de Supabase
        supabase_config = config.config['SUPABASE'] if config.config.has_section('SUPABASE') else {}
        self.supabase_url = os.getenv('SUPABASE_URL') or supabase_config.get('SUPABASE_URL')
        self.supabase_key = os.getenv('SUPABASE_KEY') or supabase_config.get('SUPABASE_KEY')
        self.storage_bucket = supabase_config.get('STORAGE_BUCKET', 'whatsapp-files')
        
        if not self.supabase_url or not self.supabase_key:
            raise RuntimeError("Supabase URL y KEY son requeridos")
            
        self.supabase: Client = create_client(self.supabase_url, self.supabase_key)
        logger.info(f"ExtendedFileService initialized with bucket: {self.storage_bucket}")

    def detect_media_type_from_content(self, content: bytes, filename: str, content_type: str) -> str:
        """
        Detecta el tipo de media más apropiado para WhatsApp basado en contenido,
        filename y content_type, con soporte para cualquier MIME type.
        """
        # Normalizar content_type
        if content_type:
            content_type = content_type.lower().split(';')[0].strip()
        
        # Verificar overrides específicos primero
        if content_type in self.SPECIFIC_TYPE_OVERRIDES:
            return self.SPECIFIC_TYPE_OVERRIDES[content_type]
        
        # Detectar por categoría principal de MIME type
        if content_type:
            main_type = content_type.split('/')[0]
            
            if main_type == 'image':
                if content_type == 'image/webp':
                    return 'sticker'
                elif content_type in ['image/jpeg', 'image/jpg', 'image/png']:
                    return 'image'
                else:
                    # Otras imágenes como documentos
                    return 'document'
                    
            elif main_type == 'audio':
                if content_type == 'audio/ogg':
                    return 'voice'
                elif content_type in self.WHATSAPP_MEDIA_CONFIG['audio']['mime_types']:
                    return 'audio'
                else:
                    return 'document'
                    
            elif main_type == 'video':
                if content_type in self.WHATSAPP_MEDIA_CONFIG['video']['mime_types']:
                    return 'video'
                else:
                    return 'document'
        
        # Si no se puede clasificar específicamente, es un documento
        return 'document'

    def validate_file_extended(self, content: bytes, filename: str, content_type: str) -> dict:
        """
        Validación extendida que acepta cualquier MIME type válido,
        aplicando las reglas específicas de WhatsApp Cloud API.
        """
        file_size = len(content)
        
        # Detectar tipo de media
        media_type = self.detect_media_type_from_content(content, filename, content_type)
        media_config = self.WHATSAPP_MEDIA_CONFIG[media_type]
        
        # Validar tamaño según el tipo detectado
        if file_size > media_config['max_size']:
            max_mb = media_config['max_size'] / (1024 * 1024)
            raise ValueError(f"Archivo demasiado grande. Máximo {max_mb}MB para {media_type}")
        
        # Para tipos específicos, validar MIME type exacto
        if media_type in ['image', 'audio', 'video', 'sticker', 'voice']:
            if content_type not in media_config['mime_types']:
                # Si no está en la lista exacta pero es del tipo general correcto,
                # lo tratamos como documento
                if media_type != 'document':
                    logger.warning(f"MIME type {content_type} no está en lista específica para {media_type}, tratando como documento")
                    media_type = 'document'
                    media_config = self.WHATSAPP_MEDIA_CONFIG['document']
        
        return {
            'media_type': media_type,
            'document_type': media_config['document_type'],
            'whatsapp_type': media_config['whatsapp_type'],
            'file_size': file_size,
            'content_type': content_type,
            'valid': True
        }

    def upload_to_supabase(self, file_content: bytes, filename: str, content_type: str = None) -> dict:
        """Upload file to Supabase Storage with auto-generated path"""
        try:
            # Generate file path with date organization
            date_folder = datetime.now().strftime("%Y/%m/%d")
            safe_filename = self._sanitize_filename(filename)
            file_hash = hashlib.md5(file_content).hexdigest()[:8]
            file_path = f"whatsapp-media/{date_folder}/{file_hash}_{safe_filename}"
            
            # Upload options
            file_options = {
                "x-upsert": "true",
                "content-type": content_type or "application/octet-stream"
            }
            
            # Upload to Supabase
            result = self.supabase.storage.from_(self.storage_bucket).upload(
                file_path, file_content, file_options
            )
            
            if hasattr(result, 'error') and result.error:
                raise RuntimeError(f"Supabase upload error: {result.error}")
            
            # Generate public URL
            public_url_result = self.supabase.storage.from_(self.storage_bucket).get_public_url(file_path)
            public_url = public_url_result if isinstance(public_url_result, str) else public_url_result.get('publicURL', '')
            
            # 🔧 AGREGAR ESTA LÍNEA para limpiar el ? extra
            if public_url.endswith("?"):
                public_url = public_url[:-1]
            
            logger.info(f"File uploaded successfully: {file_path}")
            
            return {
                'file_path': file_path,
                'public_url': public_url,
                'file_size': len(file_content),
                'upload_timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error uploading to Supabase: {e}")
            raise

    def _sanitize_filename(self, filename: str) -> str:
        """Sanitize filename for safe storage"""
        if not filename:
            return f"file_{uuid4().hex[:8]}.bin"
        
        # Remove dangerous characters
        import re
        safe_name = re.sub(r'[^\w\-_\.]', '_', filename)
        
        # Ensure reasonable length
        if len(safe_name) > 100:
            name_part, ext = os.path.splitext(safe_name)
            safe_name = name_part[:95] + ext
        
        return safe_name

    def get_whatsapp_media_url(self, media_id: str, phone: str = None) -> str:
        """Get media URL from WhatsApp API. If phone is provided, resolve token for that company."""
        try:
            logger.info(f"[MEDIA DEBUG] Starting get_whatsapp_media_url - media_id: {media_id}, phone: {phone}")
            
            # Resolver credenciales por teléfono si se pasa
            if phone:
                logger.info(f"[MEDIA DEBUG] Phone provided: {phone}, getting credentials...")
                creds = get_whatsapp_credentials_for_phone(phone)
                logger.info(f"[MEDIA DEBUG] Credentials result: {creds}")
                token = creds.get('access_token')
                logger.info(f"[MEDIA DEBUG] Token from credentials: {token[:20] if token else 'None'}...")
            else:
                logger.info("[MEDIA DEBUG] No phone provided, using default config")
                token = self.config.whatsapp_config["access_token"]
                logger.info(f"[MEDIA DEBUG] Default token: {token[:20] if token else 'None'}...")

            if not token:
                raise RuntimeError("No WhatsApp access token available to fetch media URL")

            headers = {'Authorization': f'Bearer {token}'}
            logger.info(f"[MEDIA DEBUG] Making request to: https://graph.facebook.com/v22.0/{media_id}")
            logger.info(f"[MEDIA DEBUG] Authorization header: Bearer {token[:20]}...")

            response = requests.get(
                f'https://graph.facebook.com/v22.0/{media_id}',
                headers=headers,
                timeout=15
            )
            
            logger.info(f"[MEDIA DEBUG] Response status: {response.status_code}")
            if not response.ok:
                logger.error(f"[MEDIA DEBUG] Response text: {response.text}")
            
            response.raise_for_status()
            media_info = response.json()
            
            logger.info(f"Media info retrieved for {media_id}: {media_info}")
            return media_info.get('url')

        except Exception as e:
            logger.error(f"Error getting media URL for {media_id}: {e}")
            raise


    def download_whatsapp_media(self, media_url: str, phone: str = None) -> tuple[bytes, str, str]:
        """Download media from WhatsApp and return content, filename, mime_type.
           If phone provided, uses company-specific token."""
        try:
            # Resolver token por teléfono si se proporciona
            if phone:
                creds = get_whatsapp_credentials_for_phone(phone)
                token = creds.get('access_token')
            else:
                token = self.config.whatsapp_config.get("access_token")

            if not token:
                raise RuntimeError("No WhatsApp access token available to download media")

            headers = {
                'Authorization': f'Bearer {token}',
            }

            response = requests.get(media_url, headers=headers, timeout=60)
            response.raise_for_status()

            # Detectar tipo MIME del response
            content_type = response.headers.get('content-type', 'application/octet-stream')
            content_type = content_type.split(';')[0].strip().lower()

            # Generar nombre de archivo basado en content type
            filename = self._generate_filename_from_content_type(content_type)

            logger.info(f"Downloaded media: {len(response.content)} bytes, type: {content_type}")
            return response.content, filename, content_type

        except Exception as e:
            logger.error(f"Error downloading media from {media_url}: {e}", exc_info=True)
            raise
    def _generate_filename_from_content_type(self, content_type: str) -> str:
        """Generate appropriate filename based on content type"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        random_id = uuid4().hex[:8]
        
        # Mapeo de content types a extensiones apropiadas
        extension_map = {
            'image/jpeg': '.jpg',
            'image/png': '.png', 
            'image/webp': '.webp',
            'image/gif': '.gif',
            'audio/mpeg': '.mp3',
            'audio/aac': '.aac',
            'audio/ogg': '.ogg',
            'audio/mp4': '.m4a',
            'video/mp4': '.mp4',
            'video/3gpp': '.3gp',
            'application/pdf': '.pdf',
            'application/msword': '.doc',
            'application/vnd.openxmlformats-officedocument.wordprocessingml.document': '.docx',
            'text/plain': '.txt',
            'application/json': '.json',
            'application/zip': '.zip',
        }
        
        extension = extension_map.get(content_type)
        if not extension:
            # Intentar deducir de mimetypes standard
            extension = mimetypes.guess_extension(content_type) or '.bin'
        
        return f"whatsapp_{timestamp}_{random_id}{extension}"

    def process_whatsapp_media_extended(self, media_id: str, object_reference_type: str, 
                                        object_reference_id: str, original_filename: str = None, phone: str = None) -> dict:
            """
            Pipeline completo extendido: download desde WhatsApp -> validar -> subir a Supabase -> guardar metadata
            Soporta cualquier MIME type válido según WhatsApp Cloud API
            Ahora acepta phone opcional para resolver token/company específico.
            """
            try:
                # Get company-specific credentials if phone provided
                if phone:
                    creds = get_whatsapp_credentials_for_phone(phone)
                    # Log detailed credential info
                    logger.info("=" * 80)
                    logger.info(f"🔐 Processing media {media_id} with credentials for phone {phone}:")
                    logger.info(f"📱 Company: {creds.get('company_name', 'Default')}")
                    logger.info(f"🆔 Company ID: {creds.get('company_id', 'Default')}")
                    logger.info(f"🔑 Token: {creds.get('access_token', '')[:20]}...")
                    logger.info(f"📞 Phone Number ID: {creds.get('phone_number_id', '')}")
                    logger.info(f"💼 Business ID: {creds.get('business_id', '')}")
                    logger.info("=" * 80)

                # 1. Obtener URL del media (usa token adecuado si phone provisto)
                media_url = self.get_whatsapp_media_url(media_id, phone)
                if not media_url:
                    raise ValueError("Could not get media URL from WhatsApp")

                # 2. Descargar media (pasa phone para usar token correcto)
                content, filename, content_type = self.download_whatsapp_media(media_url, phone)

                # 3. Validación extendida
                validation = self.validate_file_extended(content, filename, content_type)

                # 4. Subir a Supabase
                upload_result = self.upload_to_supabase(content, filename, content_type)

                # 5. Guardar metadata en base de datos
                upload_result['original_filename'] = original_filename or filename
                document_id = self.save_file_metadata_extended(
                    upload_result, validation, object_reference_type, object_reference_id
                )

                result = {
                    'success': True,
                    'document_id': document_id,
                    'media_id': media_id,
                    'filename': filename,
                    'original_filename': original_filename,
                    'content_type': content_type,
                    'media_type': validation['media_type'],
                    'whatsapp_type': validation['whatsapp_type'],
                    'file_size': validation['file_size'],
                    'supabase_path': upload_result['file_path'],
                    'public_url': upload_result['public_url']
                }

                logger.info(f"Media processed successfully: {media_id} -> {document_id} (type: {validation['media_type']})")
                return result

            except Exception as e:
                logger.error(f"Error processing WhatsApp media {media_id}: {e}", exc_info=True)
                raise
            
    def save_file_metadata_extended(self, file_info: dict, validation_result: dict, 
                                   object_reference_type: str, object_reference_id: str) -> str:
        """Save file metadata with extended type support"""
        try:
            # Obtener document_type_id
            doc_type_query = """
                SELECT id FROM public.document_types 
                WHERE name = %s AND is_deleted = false 
                LIMIT 1
            """
            doc_type_row = self.db_manager.execute_query(
                doc_type_query, 
                [validation_result['document_type']], 
                fetch_one=True
            )
            
            if not doc_type_row:
                # Crear document_type si no existe
                try:
                    insert_doc_type_query = """
                        INSERT INTO public.document_types (
                            id, name, description, help_text, is_active, created_at, is_deleted
                        ) VALUES (
                            uuid_generate_v4(), %s, %s, %s, true, NOW(), false
                        ) RETURNING id
                    """
                except:
                    insert_doc_type_query = """
                        INSERT INTO public.document_types (
                            id, name, description, help_text, is_active, created_at, is_deleted
                        ) VALUES (
                            gen_random_uuid(), %s, %s, %s, true, NOW(), false
                        ) RETURNING id
                    """
                doc_type_params = [
                    validation_result['document_type'],
                    f"Auto-created for {validation_result['media_type']} files",
                    f"WhatsApp {validation_result['whatsapp_type']} media type"
                ]
                doc_type_row = self.db_manager.execute_query(
                    insert_doc_type_query, doc_type_params, fetch_one=True
                )
            
            document_type_id = doc_type_row[0]
            
            # Insertar documento con metadata extendida
            document_id = str(uuid4())
            insert_query = """
                INSERT INTO public.documents (
                    id, name, document_type_id, object_reference_id, 
                    object_reference_type, path, status, uploaded_at,
                    created_at, is_deleted
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """
            
            params = [
                document_id,
                file_info.get('original_filename', os.path.basename(file_info['file_path'])),
                document_type_id,
                object_reference_id,
                object_reference_type,
                file_info['file_path'],
                'uploaded',
                now_madrid_naive(),
                now_madrid_naive(),
                False
            ]
            
            self.db_manager.execute_query(insert_query, params)
            logger.info(f"Extended file metadata saved with ID: {document_id}")
            
            return document_id
            
        except Exception as e:
            logger.error(f"Error saving extended file metadata: {e}")
            raise

    def send_media_to_whatsapp_extended(self, phone: str, file_path: str, 
                                       media_type: str, filename: str = None, 
                                       caption: str = None) -> tuple[bool, str]:
        """
        Envía media a WhatsApp con el payload correcto según el tipo.
        - image/video: permite caption
        - document: usa link público
        - audio/voice: NO caption, NO filename (evita 400)
        Devuelve (success, wamid)
        """
        try:
            # 1) Descargar el archivo desde Supabase
            file_bytes = self.supabase.storage.from_(self.storage_bucket).download(file_path)
            if not file_bytes:
                raise ValueError("Could not download file from Supabase")

            # 2) Detectar content-type
            content_type = self._detect_content_type_from_path(file_path)

            # 3) Determinar tipo WhatsApp (normalizado)
            #    Tu método detect_media_type_from_content puede devolver 'voice'/'sticker';
            #    aquí normalizamos a lo que acepta /messages: image | video | document | audio
            detected_type = self.detect_media_type_from_content(
                file_bytes, filename or file_path, content_type
            )

            # Config original (si lo usas) y tipo WA base
            whatsapp_config = self.WHATSAPP_MEDIA_CONFIG.get(detected_type, {})
            wa_message_type = whatsapp_config.get('whatsapp_type', detected_type)

            # Normalización:
            # - 'voice' se envía como 'audio' (nota de voz ≈ audio) y NUNCA con caption/filename
            # - 'sticker' (si llegara) también se manda como media simple sin caption
            is_voice = False
            if wa_message_type == "voice":
                wa_message_type = "audio"
                is_voice = True

            # 4) Rama por tipo
            if wa_message_type in ("image", "video", "audio", "sticker"):
                # Subir primero a WhatsApp (devuelve media_id)
                media_id = self._upload_media_to_whatsapp_extended(
                    file_bytes, wa_message_type, content_type
                )
                if not media_id:
                    raise RuntimeError("WhatsApp /media upload failed (no media_id)")

                # Reglas de caption/filename por tipo
                send_caption = caption if wa_message_type in ("image", "video") else None
                send_filename = None  # nunca filename en audio/sticker; en image/video no se usa

                # Enviar mensaje por ID de media
                return self._send_whatsapp_media_message_extended(
                    phone, media_id, wa_message_type, send_caption, send_filename
                )

            else:
                # DOCUMENTOS u otros -> enviar por LINK público
                public_url = self.supabase.storage.from_(self.storage_bucket).get_public_url(file_path)
                # Algunos helpers de supabase añaden '?' al final; lo limpiamos
                if public_url.endswith("?"):
                    public_url = public_url[:-1]

                # Para documento puedes pasar filename y caption
                return self._send_whatsapp_document_message(
                    phone, public_url, filename, caption
                )

        except Exception as e:
            logger.error(f"Error sending extended media to WhatsApp: {e}")
            return False, None

    def _detect_content_type_from_path(self, file_path: str) -> str:
        """Detect content type from file path"""
        content_type, _ = mimetypes.guess_type(file_path)
        return content_type or 'application/octet-stream'

    def _upload_media_to_whatsapp_extended(self, file_content: bytes, media_type: str, content_type: str) -> str:
        """Upload media to WhatsApp with extended type support"""
        headers = {
            'Authorization': f'Bearer {self.config.whatsapp_config["access_token"]}',
        }
        
        files = {
            'file': ('file', file_content, content_type),
            'type': (None, media_type),
            'messaging_product': (None, 'whatsapp')
        }
        
        response = requests.post(
            f'https://graph.facebook.com/v22.0/{self.config.whatsapp_config["phone_number_id"]}/media',
            headers={'Authorization': headers['Authorization']},
            files=files,
            timeout=60
        )
        
        response.raise_for_status()
        result = response.json()
        return result['id']

    def build_whatsapp_media_payload(to_e164: str, media_kind: str, media_id: str = None, link: str = None, mime_type: str = None, caption: str = None, voice: bool = False):
        """
        media_kind: 'image' | 'video' | 'document' | 'audio'
        Usa 'media_id' (preferente si has llamado a /media) o 'link' (URL pública HTTPS).
        Para audio: NO se permite caption. Puedes usar 'voice' para nota de voz.
        """
        payload = {
            "messaging_product": "whatsapp",
            "to": to_e164,
            "type": media_kind
        }

        if media_kind == "audio":
            audio_obj = {}
            if media_id:
                audio_obj["id"] = media_id
            elif link:
                audio_obj["link"] = link
                if mime_type:
                    audio_obj["mime_type"] = mime_type
            # voice opcional (nota de voz)
            if voice:
                audio_obj["voice"] = True
            payload["audio"] = audio_obj
            return payload

        if media_kind == "image":
            image_obj = {}
            if media_id:
                image_obj["id"] = media_id
            elif link:
                image_obj["link"] = link
                if mime_type:
                    image_obj["mime_type"] = mime_type
            if caption:
                image_obj["caption"] = caption[:1024]
            payload["image"] = image_obj
            return payload

        if media_kind == "video":
            video_obj = {}
            if media_id:
                video_obj["id"] = media_id
            elif link:
                video_obj["link"] = link
                if mime_type:
                    video_obj["mime_type"] = mime_type
            if caption:
                video_obj["caption"] = caption[:1024]
            payload["video"] = video_obj
            return payload

        if media_kind == "document":
            doc_obj = {}
            if media_id:
                doc_obj["id"] = media_id
            elif link:
                doc_obj["link"] = link
                if mime_type:
                    doc_obj["mime_type"] = mime_type
            if caption:
                doc_obj["caption"] = caption[:1024]
            payload["document"] = doc_obj
            return payload

        raise ValueError(f"Unsupported media_kind: {media_kind}")

    def _send_whatsapp_media_message_extended(self, phone: str, media_id: str, 
                                            media_type: str, caption: str = None,
                                            filename: str = None) -> tuple[bool, str]:
        """Send media message with extended type support"""
        destination = PhoneUtils.add_34(phone)
        
        payload = {
            "messaging_product": "whatsapp",
            "to": destination,
            "type": media_type,
            media_type: {
                "id": media_id
            }
        }
        
        # Agregar caption según el tipo
        if caption and media_type in ['image', 'video', 'document']:
            payload[media_type]["caption"] = caption
        
        # Para documentos, agregar filename
        if filename and media_type == 'document':
            payload[media_type]["filename"] = filename
        
        try:
            response = requests.post(
                self.config.whatsapp_config['base_url'],
                headers=self.config.whatsapp_config['headers'],
                json=payload,
                timeout=30
            )
            
            response.raise_for_status()
            message_id = response.json()['messages'][0]['id']
            
            logger.info(f"Extended media message sent successfully: {message_id} (type: {media_type})")
            return True, message_id
            
        except Exception as e:
            logger.error(f"Error sending extended media message: {e}")
            return False, None

    def _send_whatsapp_document_message(self, phone: str, public_url: str, 
                                      filename: str = None, caption: str = None) -> tuple[bool, str]:
        """Send document via public URL"""
        destination = PhoneUtils.add_34(phone)
        
        payload = {
            "messaging_product": "whatsapp",
            "to": destination,
            "type": "document",
            "document": {
                "link": public_url
            }
        }
        
        if filename:
            payload["document"]["filename"] = filename
        if caption:
            payload["document"]["caption"] = caption
        
        try:
            response = requests.post(
                self.config.whatsapp_config['base_url'],
                headers=self.config.whatsapp_config['headers'],
                json=payload,
                timeout=30
            )
            
            response.raise_for_status()
            message_id = response.json()['messages'][0]['id']
            
            logger.info(f"Document message sent successfully: {message_id}")
            return True, message_id
            
        except Exception as e:
            logger.error(f"Error sending document message: {e}")
            return False, None

    def get_supported_types_info(self) -> dict:
        """Retorna información completa sobre tipos soportados"""
        return {
            'whatsapp_media_config': self.WHATSAPP_MEDIA_CONFIG,
            'specific_overrides': self.SPECIFIC_TYPE_OVERRIDES,
            'max_sizes': {
                media_type: f"{config['max_size'] / (1024*1024):.1f}MB"
                for media_type, config in self.WHATSAPP_MEDIA_CONFIG.items()
            },
            'total_mime_types_supported': sum(
                len(config['mime_types']) for config in self.WHATSAPP_MEDIA_CONFIG.values()
            ),
            'document_accepts_any_mime': True,
            'note': "Los documentos pueden ser de cualquier MIME type válido hasta 100MB según WhatsApp Cloud API"
        }

    # Métodos de compatibilidad con la clase original
    def validate_file(self, content: bytes, filename: str, content_type: str) -> dict:
        """Compatibility method"""
        return self.validate_file_extended(content, filename, content_type)

    def process_whatsapp_media(self, media_id: str, object_reference_type: str, 
                              object_reference_id: str, original_filename: str = None) -> dict:
        """Compatibility method"""
        return self.process_whatsapp_media_extended(media_id, object_reference_type, object_reference_id, original_filename)

    def send_media_to_whatsapp(self, phone: str, file_path: str, 
                              media_type: str, caption: str = None) -> tuple[bool, str]:
        """Compatibility method"""
        return self.send_media_to_whatsapp_extended(phone, file_path, media_type, caption=caption)

    def save_file_metadata(self, file_info: dict, validation_result: dict, 
                          object_reference_type: str, object_reference_id: str) -> str:
        """Compatibility method"""
        return self.save_file_metadata_extended(file_info, validation_result, object_reference_type, object_reference_id)

class FlowExitClient:
    """
    Cliente para notificar salidas de flow al Scheduler.

    Acepta ambos estilos de construcción:
      - FlowExitClient(flow_config_dict, api_key, logger=...)
      - FlowExitClient(flow_config=flow_config_dict, api_key="...", logger=...)

    flow_config esperado:
      {
        'base_url': 'https://scheduler-dev.solvify.es',
        'https_port': 5100,
        'http_port': 5101,
        'exit_path': '/api/exit',
        'try_candidates': True,
        'timeout': 8
      }
    """

    def __init__(self, *args, **kwargs):
        # Compatibilidad con llamadas posicionales y por keyword
        if args and isinstance(args[0], dict):
            flow_config = args[0]
            api_key = args[1] if len(args) > 1 else kwargs.get('api_key')
            logger = kwargs.get('logger', None)
        else:
            flow_config = kwargs.get('flow_config')
            api_key = kwargs.get('api_key')
            logger = kwargs.get('logger', None)

        if not isinstance(flow_config, dict):
            raise RuntimeError("FlowExitClient: flow_config debe ser un dict con base_url/ports/exit_path")

        self.base_url = flow_config.get('base_url', '').rstrip('/')
        if not self.base_url:
            raise RuntimeError("FlowExitClient: falta flow_config['base_url']")

        self.exit_path = flow_config.get('exit_path', '/api/exit')
        if not self.exit_path.startswith('/'):
            self.exit_path = '/' + self.exit_path

        self.https_port = int(flow_config.get('https_port', 5100))
        self.http_port = int(flow_config.get('http_port', 5101))
        self.try_candidates = bool(flow_config.get('try_candidates', True))
        self.timeout = int(flow_config.get('timeout', 8))

        if not api_key:
            raise RuntimeError("FlowExitClient: falta api_key")
        self.api_key = api_key

        self.logger = logger or logging.getLogger(__name__)

    def _candidates(self):
        """
        Devuelve lista de URLs candidatas según configuración.
        Orden: https:PORT, http:PORT, https sin puerto (por si hay proxy/Nginx).
        """
        urls = [
            f"{self.base_url}:{self.https_port}{self.exit_path}",
            f"{self.base_url.replace('https://', 'http://')}:{self.http_port}{self.exit_path}",
            f"{self.base_url}{self.exit_path}",
        ]
        # Dedup conservando orden
        seen = set()
        ordered = []
        for u in urls:
            if u not in seen:
                ordered.append(u)
                seen.add(u)
        return ordered

    def send_exit(self, lead_id: str, flow_name: str = "welcome_email_flow",
                  motivo: str = "Usuario quiere salir del flow") -> bool:
        """
        POST al endpoint de salida. Devuelve True si el POST fue 2xx,
        o si devuelve 400 con 'No active flow nodes found' (case típico de idempotencia).
        """
        headers = {
            "Content-Type": "application/json",
            "X-API-Key": self.api_key,
        }
        payload = {
            "id": lead_id,
            "flow_name": flow_name,
            "motivo": motivo,
        }

        candidates = self._candidates() if self.try_candidates else [f"{self.base_url}{self.exit_path}"]

        for idx, url in enumerate(candidates, start=1):
            try:
                self.logger.info(f"[FLOW EXIT] intentando POST ({idx}/{len(candidates)}): {url}")
                self.logger.debug({
                    'event': 'flow.exit.request',
                    'url': url,
                    'headers': headers,
                    'payload': payload
                })
                resp = requests.post(url, headers=headers, json=payload, timeout=self.timeout)
                text_prev = (resp.text or '')[:2000]
                self.logger.debug({
                    'event': 'flow.exit.response',
                    'status_code': resp.status_code,
                    'text_preview': text_prev
                })

                # Caso OK
                if 200 <= resp.status_code < 300:
                    self.logger.info(f"[FLOW EXIT] OK {resp.status_code} en {url}")
                    return True

                # Caso idempotente / sin nodos activos => lo damos por bueno
                if resp.status_code == 400 and 'No active flow nodes found' in text_prev:
                    self.logger.warning(f"[FLOW EXIT] 400 pero sin nodos activos; lo consideramos completado: {url}")
                    return True

                # Errores típicos
                if resp.status_code == 401:
                    self.logger.error(f"[FLOW EXIT] HTTP 401 en {url} (credenciales faltan o no válidas).")
                elif resp.status_code == 404:
                    self.logger.error(f"[FLOW EXIT] 404 en {url} — la ruta no existe o no está publicada.")
                else:
                    self.logger.error(f"[FLOW EXIT] HTTP {resp.status_code} en {url}")

            except Exception as e:
                self.logger.exception(f"[FLOW EXIT] excepción realizando POST a {url}: {e}")

        self.logger.error("[FLOW EXIT] Fallaron todos los candidatos; revisa Nginx/puerto/route del scheduler")
        return False

class Config:
    """Centralized configuration management with company cache support"""
    def __init__(self, config_path=None, company_id=None, supabase_client=None):
        self._logger = logging.getLogger(__name__)
        self.config = configparser.ConfigParser()
        if config_path is None:
            config_path = 'scripts.conf'
        self.config.read(config_path)
        self.company_id = company_id
        self.supabase_client = supabase_client
        self.company_config = None
        
        # Setup inicial sin errores críticos
        self._setup_config(raise_on_missing_whatsapp=False)
        
        # Precargar configs si es la primera instancia
        global company_cache
        if not company_cache._last_reload:
            db_manager = DatabaseManager(self.db_config)
            company_cache.preload_all_companies(db_manager)
        
        # Si hay company_id, cargar de caché
        if self.company_id:
            cached_config = company_cache.get(self.company_id)
            if cached_config:
                self.company_config = cached_config.get('config', {})
                self._apply_company_config()
        
        # Validar config crítica
        self._validate_critical_config()

    def _apply_company_config(self):
        """Apply cached company configuration"""
        if not self.company_config:
            return
            
        custom = self.company_config.get('custom_properties', {})
        if not custom:
            return
            
        # Actualizar WhatsApp config
        if all(custom.get(k) for k in ['WHATSAPP_ACCESS_TOKEN', 'WHATSAPP_PHONE_NUMBER_ID', 'WHATSAPP_BUSINESS_ID']):
            self.whatsapp_config.update({
                'access_token': custom['WHATSAPP_ACCESS_TOKEN'],
                'phone_number_id': custom['WHATSAPP_PHONE_NUMBER_ID'],
                'business_id': custom['WHATSAPP_BUSINESS_ID'],
                'base_url': f"https://graph.facebook.com/v22.0/{custom['WHATSAPP_PHONE_NUMBER_ID']}/messages",
                'headers': {
                    'Authorization': f"Bearer {custom['WHATSAPP_ACCESS_TOKEN']}",
                    'Content-Type': 'application/json'
                }
            })

    def _validate_critical_config(self):



        if not self.company_id:
            logger.info("Skipping critical config validation for temporary config object")
            return
        
        # Solo lanza error si faltan datos críticos después de cargar DB
        if not hasattr(self, 'whatsapp_config') or not self.whatsapp_config.get('access_token'):
            raise RuntimeError("WHATSAPP_ACCESS_TOKEN no configurado (ni en env, ni en [WHATSAPP], ni en DB).")
        if not self.whatsapp_config.get('phone_number_id'):
            raise RuntimeError("WHATSAPP_PHONE_NUMBER_ID no configurado (ni en env, ni en [WHATSAPP], ni en DB).")
        if not self.whatsapp_config.get('business_id'):
            raise RuntimeError("WHATSAPP_BUSINESS_ID no configurado (ni en env, ni en [WHATSAPP], ni en DB).")

    def load_company_config(self, company_id):
        """
        Carga configuración de la empresa desde la API de propiedades personalizadas.
        Primero carga todo de fichero, luego la API sobrescribe solo las variables presentes.
        """
        logger = self._logger
        try:
            # Configurar la llamada a la API
            api_url = f"https://test.solvify.es/api/custom-properties/companies/{company_id}/property-value"
            headers = {
                'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IjEyMmZlYTI1LWQ1OWEtNGE2Zi04YzQ0LWIzZTVmZTExZTZmZSIsImVtYWlsIjoic2VydmljZUBzb2x2aWZ5LmVzIiwiZmlyc3RfbmFtZSI6IlNlcnZpY2UiLCJsYXN0X25hbWUiOiJTb2x2aWZ5IiwiaXNfYWN0aXZlIjp0cnVlLCJjcmVhdGVkX2F0IjoiMjAyNC0xMC0xN1QxNzowODozOC4xNjY3OTEiLCJjcmVhdGVkX2J5IjpudWxsLCJ1cGRhdGVkX2F0IjoiMjAyNC0xMC0xN1QxNTowODozOC45OCIsInVwZGF0ZWRfYnkiOm51bGwsImRlbGV0ZWRfYXQiOm51bGwsImRlbGV0ZWRfYnkiOm51bGwsImlzX2RlbGV0ZWQiOmZhbHNlLCJyb2xlX2lkIjoiODQ5ZmFiZTgtNDhjYi00ZWY4LWE0YWUtZTJiN2MzZjNlYTViIiwic3RyaXBlX2N1c3RvbWVyX2lkIjpudWxsLCJleHBvX3B1c2hfdG9rZW4iOm51bGwsInBob25lIjoiMCIsInJvbGVfbmFtZSI6IkFETUlOIiwicm9sZXMiOltdLCJpYXQiOjE3MjkxNzc4OTIsImV4cCI6Nzc3NzE3Nzg5Mn0.TJWtiOnLW8XyWjQDR_LAWvEiqrw50tWUmYiKXxo_5Wg',
                'Content-Type': 'application/json'
            }

            # Hacer la llamada a la API
            response = requests.get(api_url, headers=headers)
            response.raise_for_status()
            properties = response.json()

            # Crear un diccionario con las propiedades
            config_map = {}
            for prop in properties:
                if prop['is_deleted']:
                    continue
                
                # Convertir el valor según el tipo de propiedad
                value = prop['value']
                if prop['property_type'] == 'boolean':
                    value = value.lower() == 'true'
                elif prop['property_type'] == 'number':
                    value = float(value) if '.' in value else int(value)
                
                config_map[prop['property_name']] = value

            # Actualizar la configuración de WhatsApp
            self.whatsapp_config = {
                'access_token': config_map.get('WHATSAPP_ACCESS_TOKEN'),
                'phone_number_id': config_map.get('WHATSAPP_PHONE_NUMBER_ID'),
                'business_id': config_map.get('WHATSAPP_BUSINESS_ID'),
                'base_url': f"https://graph.facebook.com/v22.0/{config_map.get('WHATSAPP_PHONE_NUMBER_ID')}/messages",
                'headers': {
                    'Authorization': f"Bearer {config_map.get('WHATSAPP_ACCESS_TOKEN')}",
                    'Content-Type': 'application/json'
                }
            }

            # Configuración de horario comercial
            self.business_hours = {
                'enabled': config_map.get('WHATSAPP_BUSINESS_HOURS_ENABLED', False),
                'start_time': config_map.get('BUSINESS_HOURS_START_TIME', '09:00:00'),
                'end_time': config_map.get('BUSINESS_HOURS_END_TIME', '18:00:00'),
                'weekdays': [int(d) for d in config_map.get('BUSINESS_HOURS_WEEKDAYS', '0,1,2,3,4').split(',')]
            }

            # Otras configuraciones
            self.hours_ahead = config_map.get('HOURS_AHEAD', 24)
            self.message_file = config_map.get('MESSAGE_FILE', 'messages/')
            self.templates_file = config_map.get('MESSAGE_TEMPLATES_FILE', 'templates/whatsapp.json')
            self.default_from_email = config_map.get('DEFAULT_FROM_EMAIL')

            # Guardar la configuración completa para acceso posterior
            self.company_config = config_map
            logger.info(f"[Config] Loaded configuration for company {company_id} from API")

        except Exception as e:
            logger.error(f"[Config] Error loading company config from API: {str(e)}")
            raise
            # Llama a la función RPC de Supabase
            resp = self.supabase_client.rpc('get_company_data', {"company_id": company_id}).execute()
            if not resp.data:
                logger.warning(f"No company data found in Supabase for company_id={company_id}")
                return
            company_data = resp.data[0] if isinstance(resp.data, list) else resp.data
            self.company_config = company_data
            custom = company_data.get('custom_properties', {})
            logger.info(f"[Config] Company data loaded from Supabase for company_id={company_id}")
            # Lista de variables a sobreescribir si existen en custom_properties
            override_vars = [
                'WHATSAPP_ACCESS_TOKEN',
                'WHATSAPP_PHONE_NUMBER_ID',
                'WHATSAPP_BUSINESS_ID',
                'HOURS_AHEAD',
                'MESSAGE_FILE',
                'MESSAGE_TEMPLATES_FILE',
                'DEFAULT_FROM_EMAIL',
                'WHATSAPP_BUSINESS_HOURS_ENABLED',
                'BUSINESS_HOURS_TIMEZONE',
                'BUSINESS_HOURS_START_TIME',
                'BUSINESS_HOURS_END_TIME',
                'BUSINESS_HOURS_WEEKDAYS',
                'COVER_WB'
            ]
            # Primero loguea todo lo que hay en fichero
            for var in override_vars:
                file_val = None
                # Buscar en configparser
                for section in self.config.sections():
                    if var in self.config[section]:
                        file_val = self.config[section][var]
                        break
                if file_val is not None:
                    logger.info(f"[Config] {var} loaded from file: {file_val}")
            # Luego sobrescribe con DB si existe
            for var in override_vars:
                db_val = custom.get(var)
                if db_val is not None:
                    logger.info(f"[Config] {var} OVERRIDDEN from DB: {db_val}")
                    # Actualiza en config y/o atributos
                    if var.startswith('WHATSAPP_') or var in ['HOURS_AHEAD']:
                        # WhatsApp config
                        if hasattr(self, 'whatsapp_config'):
                            if var == 'WHATSAPP_ACCESS_TOKEN':
                                self.whatsapp_config['access_token'] = db_val
                                self.whatsapp_config['headers']['Authorization'] = f'Bearer {db_val}'
                            elif var == 'WHATSAPP_PHONE_NUMBER_ID':
                                self.whatsapp_config['phone_number_id'] = db_val
                                self.whatsapp_config['base_url'] = f'https://graph.facebook.com/v22.0/{db_val}/messages'
                            elif var == 'WHATSAPP_BUSINESS_ID':
                                self.whatsapp_config['business_id'] = db_val
                    # También en configparser para compatibilidad
                    if self.config.has_section('WHATSAPP'):
                        self.config['WHATSAPP'][var] = str(db_val)
                    elif var == 'HOURS_AHEAD':
                        self.hours_ahead = db_val
                    else:
                        # Otros valores
                        setattr(self, var.lower(), db_val)
        except Exception as e:
            logger.error(f"[Config] Error loading company config from Supabase: {e}")

    def _setup_config(self, raise_on_missing_whatsapp=True):
        # ---------- Selección de entorno ----------
        env_use_test = os.getenv('USE_TEST_CONFIG')
        if env_use_test is not None:
            self.use_test = env_use_test.strip().lower() in ('1', 'true', 'yes', 'y')
        else:
            self.use_test = self.config.getboolean('APP', 'USE_TEST_CONFIG', fallback=True)  # Default to True

        # En test, por defecto sí escribimos con WRITE_ENABLED=true en [APP]
        self.write_enabled = self.config.getboolean('APP', 'WRITE_ENABLED', fallback=True)

        # ---------- Base URL y API token ----------
        self.base_url = self.config.get('APP', 'BASE_URL', fallback='https://test.solvify.es/api')
        
        # API Token - prioridad: 1) ENV, 2) Config, 3) Default token
        self.api_token = os.getenv('SOLVIFY_API_TOKEN')
        if not self.api_token:
            self.api_token = self.config.get('APP', 'SOLVIFY_API_TOKEN', fallback=None)
        if not self.api_token:
            self.api_token = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IjEyMmZlYTI1LWQ1OWEtNGE2Zi04YzQ0LWIzZTVmZTExZTZmZSIsImVtYWlsIjoic2VydmljZUBzb2x2aWZ5LmVzIiwiZmlyc3RfbmFtZSI6IlNlcnZpY2UiLCJsYXN0X25hbWUiOiJTb2x2aWZ5IiwiaXNfYWN0aXZlIjp0cnVlLCJjcmVhdGVkX2F0IjoiMjAyNC0xMC0xN1QxNzowODozOC4xNjY3OTEiLCJjcmVhdGVkX2J5IjpudWxsLCJ1cGRhdGVkX2F0IjoiMjAyNC0xMC0xN1QxNTowODozOC45OCIsInVwZGF0ZWRfYnkiOm51bGwsImRlbGV0ZWRfYXQiOm51bGwsImRlbGV0ZWRfYnkiOm51bGwsImlzX2RlbGV0ZWQiOmZhbHNlLCJyb2xlX2lkIjoiODQ5ZmFiZTgtNDhjYi00ZWY4LWE0YWUtZTJiN2MzZjNlYTViIiwic3RyaXBlX2N1c3RvbWVyX2lkIjpudWxsLCJleHBvX3B1c2hfdG9rZW4iOm51bGwsInBob25lIjoiMCIsInJvbGVfbmFtZSI6IkFETUlOIiwicm9sZXMiOltdLCJpYXQiOjE3MjkxNzc4OTIsImV4cCI6Nzc3NzE3Nzg5Mn0.TJWtiOnLW8XyWjQDR_LAWvEiqrw50tWUmYiKXxo_5Wg'
            logger.info(f"Using default API token (no token in env or config)")
        else:
            logger.info(f"Using API token from {'environment' if os.getenv('SOLVIFY_API_TOKEN') else 'config'}")

        # ---------- DB ----------
        desired_section = 'DB_TEST' if self.use_test else 'DB'
        if self.config.has_section(desired_section):
            db_cfg = self.config[desired_section]
            used_section = desired_section
        else:
            used_section = 'DB'
            if not self.config.has_section('DB'):
                raise RuntimeError(f"[Config] No se encontró sección de DB. Esperaba [{desired_section}] o [DB].")
            db_cfg = self.config['DB']
            logging.warning("[Config] Sección [DB_TEST] ausente. Usando [DB]. (Respetando WRITE_ENABLED del conf)")

        self.db_config = {
            'host': db_cfg.get('DB_HOST'),
            'port': int(db_cfg.get('DB_PORT', '6543')) if db_cfg.get('DB_PORT') else None,
            'database': db_cfg.get('DB_NAME'),
            'user': db_cfg.get('DB_USER'),
            'password': db_cfg.get('DB_PASS'),
            'search_path': db_cfg.get('DB_SEARCH_PATH', None),
        }
        missing = [k for k, v in self.db_config.items() if k != 'search_path' and v in (None, '')]
        if missing:
            raise RuntimeError(f"[Config] Faltan claves en sección [{used_section}]: {missing}")

        try:
            dsn_preview = {
                'host': self.db_config['host'],
                'port': self.db_config['port'],
                'database': self.db_config['database'],
                'user': self.db_config['user'],
                'password': '***'
            }
            logging.info(f"[Config] DB section usada: {used_section}")
            logging.info(f"[Config] DB DSN: {dsn_preview}")
            logging.info(f"[Config] Write enabled: {self.write_enabled}")
        except Exception:
            logging.exception("[Config] No se pudo loguear DSN")

        # ---------- WhatsApp / Facebook ----------
        wa_cfg = self.config['WHATSAPP'] if self.config.has_section('WHATSAPP') else {}
        fb_cfg = self.config['FACEBOOK'] if self.config.has_section('FACEBOOK') else {}

        access_token = os.getenv('WHATSAPP_ACCESS_TOKEN') or wa_cfg.get('WHATSAPP_ACCESS_TOKEN')
        phone_number_id = os.getenv('WHATSAPP_PHONE_NUMBER_ID') or wa_cfg.get('WHATSAPP_PHONE_NUMBER_ID')
        verify_token = wa_cfg.get('VERIFY_TOKEN', 'SICUEL2025')
        business_id = os.getenv('WHATSAPP_BUSINESS_ID') or wa_cfg.get('WHATSAPP_BUSINESS_ID')
        self.whatsapp_config = {
            'access_token': access_token,
            'phone_number_id': phone_number_id,
            'verify_token': verify_token,
            'business_id': business_id,
            'base_url': f'https://graph.facebook.com/v22.0/{phone_number_id}/messages' if phone_number_id else None,
            'headers': {
                'Authorization': f'Bearer {access_token}' if access_token else '',
                'Content-Type': 'application/json',
            }
        }
        # Solo lanzar error si se pide (por compatibilidad con inicialización por DB)
        if raise_on_missing_whatsapp:
            if not access_token:
                raise RuntimeError("WHATSAPP_ACCESS_TOKEN no configurado (env o [WHATSAPP]).")
            if not phone_number_id:
                raise RuntimeError("WHATSAPP_PHONE_NUMBER_ID no configurado (env o [WHATSAPP]).")
            if not business_id:
                raise RuntimeError("WHATSAPP_BUSINESS_ID no configurado (env o [WHATSAPP]).")

        # ---------- Supabase Storage ----------
        supabase_cfg = self.config['SUPABASE'] if self.config.has_section('SUPABASE') else {}

        supabase_url = os.getenv('SUPABASE_URL') or supabase_cfg.get('SUPABASE_URL')
        supabase_key = os.getenv('SUPABASE_KEY') or supabase_cfg.get('SUPABASE_KEY')

        if not supabase_url or not supabase_key:
            raise RuntimeError("SUPABASE_URL y SUPABASE_KEY son requeridos")

        self.supabase_config = {
            'url': supabase_url,
            'key': supabase_key,
            'bucket': supabase_cfg.get('STORAGE_BUCKET', 'whatsapp-files'),
            'max_file_size': int(supabase_cfg.get('MAX_FILE_SIZE', 16777216)),
            'public_url_expiry': int(supabase_cfg.get('PUBLIC_URL_EXPIRY', 3600))
        }

        logger.info(f"[Config] Supabase bucket: {self.supabase_config['bucket']}")

        # ---------- Servidor webhook ----------
        import pathlib
        webhook_cfg = self.config['WEBHOOK'] if self.config.has_section('WEBHOOK') else {}
        base_dir = pathlib.Path(__file__).parent.resolve()
        # Permitir override por variable de entorno
        ssl_cert_env = os.getenv('SSL_CERT_PATH')
        ssl_key_env = os.getenv('SSL_KEY_PATH')
        ssl_cert = ssl_cert_env or webhook_cfg.get('SSL_CERT_PATH')
        ssl_key = ssl_key_env or webhook_cfg.get('SSL_KEY_PATH')
        # Si la ruta no es absoluta, hacerla relativa al proyecto
        if ssl_cert and not pathlib.Path(ssl_cert).is_absolute():
            ssl_cert = str((base_dir / ssl_cert).resolve())
        if ssl_key and not pathlib.Path(ssl_key).is_absolute():
            ssl_key = str((base_dir / ssl_key).resolve())
        # Comprobar si existen los certificados
        cert_exists = ssl_cert and pathlib.Path(ssl_cert).is_file()
        key_exists = ssl_key and pathlib.Path(ssl_key).is_file()
        if not (cert_exists and key_exists):
            self._logger.warning(f"[Config] SSL cert or key not found. Falling back to HTTP only. Cert: {ssl_cert}, Key: {ssl_key}")
            ssl_cert = None
            ssl_key = None
        self.server_config = {
            'http_port': int(webhook_cfg.get('HTTP_PORT', webhook_cfg.get('WEBHOOK_HTTP', '5041'))),
            'https_port': int(webhook_cfg.get('WEBHOOK_PORT', '5042')),
            'host': webhook_cfg.get('WEBHOOK_HOST', '0.0.0.0'),
            'ssl_cert': ssl_cert,
            'ssl_key': ssl_key,
            'public_url': webhook_cfg.get('WEBHOOK_PUBLIC_URL'),
        }

        # ---------- Logging ----------
        log_cfg = self.config['LOGGING'] if self.config.has_section('LOGGING') else {}
        self.log_config = {
            'level': getattr(logging, log_cfg.get('LOG_LEVEL', 'INFO'), logging.DEBUG),
            'file': log_cfg.get('LOG_FILE', None),
            'format': log_cfg.get('LOG_FORMAT', '%(asctime)s | %(levelname)s | %(name)s | %(message)s'),
        }

        # ---------- FLOW (scheduler) ----------
        # Solo necesitas la URL base del servidor del scheduler y la API key.
        flow_cfg = self.config['FLOW'] if self.config.has_section('FLOW') else {}

        # base_url OBLIGATORIA (p.ej. https://scheduler-dev.solvify.es ó https://scheduler.solvify.es)
        flow_base_url = os.getenv('FLOW_BASE_URL') or flow_cfg.get('SERVER_BASE_URL') or flow_cfg.get('base_url')
        if not flow_base_url:
            raise RuntimeError("[Config] Falta FLOW base_url (FLOW.SERVER_BASE_URL o FLOW.base_url)")

        # api_key OBLIGATORIA para 401 "Missing API key"
        self.api_key = os.getenv('FLOW_API_KEY') or flow_cfg.get('api_key')
        if not self.api_key:
            raise RuntimeError("[Config] Falta FLOW api_key (FLOW.api_key o env FLOW_API_KEY)")

        # puertos opcionales (por tus logs: 5100 https / 5101 http)
        https_port = int(flow_cfg.get('https_port', 5100))
        http_port = int(flow_cfg.get('http_port', 5101))

        # endpoint de salida configurable (por defecto /api/exit)
        exit_path = flow_cfg.get('exit_path', '/api/exit')

        # intentar varios candidatos (5100, 5101, host base)
        try_candidates = flow_cfg.get('try_candidates', 'true').strip().lower() in ('1', 'true', 'yes', 'y')

        self.flow_config = {
            'base_url': flow_base_url.rstrip('/'),
            'https_port': https_port,
            'http_port': http_port,
            'exit_path': exit_path if exit_path.startswith('/') else f'/{exit_path}',
            'try_candidates': try_candidates,
            'timeout': int(flow_cfg.get('timeout', 8)),  # segundos
        }

        # ---------- Logs de resumen seguro ----------
        logging.info(f"[Config] Test mode: {self.use_test}")
        logging.info(f"[Config] App BASE_URL: {self.base_url}")
        logging.info(f"[Config] Access token (preview): {self.whatsapp_config['access_token'][:8]}...{self.whatsapp_config['access_token'][-6:]}")
        logging.info(f"[Config] Flow base_url: {self.flow_config['base_url']}")
        try:
            logging.info(f"[Config] Flow https_port: {self.flow_config.get('https_port')}")
            logging.info(f"[Config] Flow http_port: {self.flow_config.get('http_port')}")
            logging.info(f"[Config] Flow exit_path: {self.flow_config.get('exit_path')}")
            logging.info(f"[Config] Flow try_candidates: {self.flow_config.get('try_candidates')}")
            logging.info(f"[Config] Flow api_key: {self.api_key[:4]}...{self.api_key[-5:]}")
        except Exception:
            logging.exception("[Config] No se pudo loguear flow_config")

class DatabaseManager:
    """Database operations manager"""
    def __init__(self, db_config):
        self.db_config = db_config

    def get_connection(self):
        """Get database connection with retry logic"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # Filtra solo los kwargs que pg8000.connect acepta
                conn_kwargs = {k: self.db_config.get(k) for k in ('host', 'port', 'database', 'user', 'password')}
                logger.debug(f"DB Connection attempt {attempt + 1} -> {conn_kwargs | {'password':'***'}}")
                conn = pg8000.connect(**conn_kwargs)

                # Timezone + (opcional) search_path
                with conn.cursor() as cur:
                    cur.execute("SET timezone = 'Europe/Madrid'")
                    sp = self.db_config.get('search_path')
                    if sp:
                        cur.execute(f"SET search_path TO {sp}")
                    cur.execute("SELECT CURRENT_SETTING('timezone')")
                    tz_result = cur.fetchone()
                    logger.debug(f"Database connection timezone set to: {tz_result[0] if tz_result else 'unknown'}")
                    if sp:
                        cur.execute("SHOW search_path")
                        sp_result = cur.fetchone()
                        logger.debug(f"Database search_path set to: {sp_result[0] if sp_result else 'unknown'}")
                    conn.commit()

                return conn

            except Exception as e:
                logger.warning(f"DB connection attempt {attempt + 1} failed: {e}")
                if attempt == max_retries - 1:
                    raise
                time.sleep(1)

    def execute_query(self, query, params=None, fetch_one=False, fetch_all=False):
        """
        Ejecuta una consulta con logging robusto y manejo opcional de escritura deshabilitada.
        - Soporta pg8000 (%s + lista/tupla en params).
        - Evita UnboundLocalError con q_preview.
        """
        start = time.time()
        try:
            q_preview = ' '.join((query or '').split())[:500]
        except Exception:
            q_preview = str(query)[:500] if query is not None else '<empty-query>'

        params = params or []

        try:
            is_read = bool(fetch_one or fetch_all)
            if not is_read and hasattr(config, 'write_enabled') and not config.write_enabled:
                logging.warning({
                    'event': 'db.write_blocked',
                    'reason': 'WRITE_ENABLED=false',
                    'query_preview': q_preview
                })
                return 0

            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    logging.debug({
                        'event': 'db.execute.start',
                        'query_preview': q_preview,
                        'params_type': type(params).__name__,
                        'fetch_one': fetch_one,
                        'fetch_all': fetch_all
                    })

                    cur.execute(query, params)

                    if fetch_one:
                        row = cur.fetchone()
                        conn.commit()
                        logging.debug({
                            'event': 'db.execute.done',
                            'elapsed_ms': int((time.time() - start) * 1000),
                            'mode': 'fetch_one',
                            'row_is_none': row is None
                        })
                        return row

                    if fetch_all:
                        rows = cur.fetchall()
                        conn.commit()
                        logging.debug({
                            'event': 'db.execute.done',
                            'elapsed_ms': int((time.time() - start) * 1000),
                            'mode': 'fetch_all',
                            'rows_count': len(rows)
                        })
                        return rows

                    affected_rows = cur.rowcount
                    conn.commit()
                    logging.info({
                        'event': 'db.execute.done',
                        'elapsed_ms': int((time.time() - start) * 1000),
                        'mode': 'write',
                        'affected_rows': affected_rows
                    })
                    return affected_rows

        except Exception as e:
            logging.exception(f"[DB] query failed: {q_preview}")
            raise


class PhoneUtils:
    """Phone number utilities"""
    @staticmethod
    def strip_34(phone: str) -> str:
        phone = phone.strip().replace('+', '')
        if phone.startswith('0034'):
            return phone[4:]
        if phone.startswith('34'):
            return phone[2:]
        return phone

    @staticmethod
    def add_34(phone: str) -> str:
        return f"34{PhoneUtils.strip_34(phone)}"

    @staticmethod
    def validate_spanish_phone(phone: str) -> bool:
        clean_phone = PhoneUtils.strip_34(phone)
        return len(clean_phone) == 9 and clean_phone.startswith(('6', '7', '8', '9'))


class WhatsAppService:
    """WhatsApp API service con templates y logging de errores"""
    def __init__(self, config=None):
        self.config = config  # Guardar la configuración completa
        if config is None:
            self.access_token    = ACCESS_TOKEN
            self.phone_number_id = PHONE_NUMBER_ID
            self.base_url        = WHATSAPP_BASE_URL
            self.headers         = WHATSAPP_HEADERS
            # URL base para API de propiedades personalizadas
            self.api_base_url    = "https://test.solvify.es/api"
        else:
            # Acceder a través de whatsapp_config que es donde están los valores
            self.access_token    = config.whatsapp_config['access_token']
            self.phone_number_id = config.whatsapp_config['phone_number_id']
            self.base_url        = config.whatsapp_config['base_url']
            self.headers         = config.whatsapp_config['headers']
            # URL base para API de propiedades personalizadas
            self.api_base_url    = getattr(config, 'api_base_url', "https://test.solvify.es/api")

    def get_debug_info(self):
        return {
            'access_token_preview': f"{self.access_token[:20]}..." if self.access_token else "None",
            'phone_number_id':      self.phone_number_id,
            'base_url':             self.base_url,
            'headers': {
                'Authorization': self.headers.get('Authorization', ''),
                'Content-Type':  self.headers.get('Content-Type', '')
            }
        }

    def _get_company_data(self, phone: str) -> tuple[dict, str, str]:
        """
        Método interno para obtener los datos de la compañía y su configuración de WhatsApp.
        Retorna: (company_config, company_name, company_id)
        """
        clean_phone = PhoneUtils.strip_34(phone)
        if not PhoneUtils.validate_spanish_phone(clean_phone):
            raise ValueError(f"Número de teléfono inválido: {phone}")

        try:
            query = """
                SELECT l.id as lead_id, d.company_id, c.name as company_name, 
                       public.get_company_data(d.company_id) as company_data
                FROM public.leads l
                INNER JOIN public.deals d ON d.lead_id = l.id AND d.is_deleted = false
                INNER JOIN public.companies c ON d.company_id = c.id
                WHERE l.phone = %s AND l.is_deleted = false
                LIMIT 1
            """
            with db_manager.get_connection() as conn:
                cur = conn.cursor()
                cur.execute(query, [clean_phone])
                result = cur.fetchone()

            if not result:
                logger.warning(f"[WhatsApp] No se encontró lead/deal/company para teléfono {phone}, usando config por defecto")
                return (
                    {
                        'access_token': self.access_token,
                        'phone_number_id': self.phone_number_id,
                        'business_id': getattr(self, 'business_id', None)
                    },
                    None,
                    None
                )

            lead_id, company_id, company_name, company_data = result
            logger.info(f"[WhatsApp] Lead {lead_id} encontrado para company: {company_name} (id: {company_id})")

            if not company_data or 'custom_properties' not in company_data:
                logger.warning(f"[WhatsApp] No se encontraron custom_properties para company {company_id}, usando config por defecto")
                return (
                    {
                        'access_token': self.access_token,
                        'phone_number_id': self.phone_number_id,
                        'business_id': getattr(self, 'business_id', None)
                    },
                    company_name,
                    company_id
                )

            custom_props = company_data['custom_properties']
            whatsapp_config = {
                'access_token': custom_props.get('WHATSAPP_ACCESS_TOKEN'),
                'phone_number_id': custom_props.get('WHATSAPP_PHONE_NUMBER_ID'),
                'business_id': custom_props.get('WHATSAPP_BUSINESS_ID')
            }

            if not all(whatsapp_config.values()):
                logger.warning(f"[WhatsApp] Config incompleta para company {company_id}, usando por defecto")
                return (
                    {
                        'access_token': self.access_token,
                        'phone_number_id': self.phone_number_id,
                        'business_id': getattr(self, 'business_id', None)
                    },
                    company_name,
                    company_id
                )

            logger.info(f"[WhatsApp] Usando config específica de company {company_name} (id: {company_id})")
            return whatsapp_config, company_name, company_id

        except Exception as e:
            logger.error(f"[WhatsApp] Error obteniendo config: {str(e)}")
            logger.info("[WhatsApp] Usando configuración por defecto debido al error")
            return (
                {
                    'access_token': self.access_token,
                    'phone_number_id': self.phone_number_id,
                    'business_id': getattr(self, 'business_id', None)
                },
                None,
                None
            )

    def send_template_message(self, to_phone: str, template_name: str, template_data: dict, timeout=15):
        try:
            clean_phone = PhoneUtils.strip_34(to_phone)
            if not PhoneUtils.validate_spanish_phone(clean_phone):
                raise ValueError(f"Número de teléfono inválido: {to_phone}")

            # Get company-specific credentials first
            creds = get_whatsapp_credentials_for_phone(clean_phone)
            
            # Log detailed credential info
            logger.info("=" * 80)
            logger.info(f"🔐 Sending template '{template_name}' with credentials for phone {clean_phone}:")
            logger.info(f"📱 Company: {creds.get('company_name', 'Default')}")
            logger.info(f"🆔 Company ID: {creds.get('company_id', 'Default')}")
            logger.info(f"🔑 Token: {creds.get('access_token', '')[:20]}...")
            logger.info(f"📞 Phone Number ID: {creds.get('phone_number_id', '')}")
            logger.info(f"🌐 Base URL: {creds.get('base_url', '')}")
            logger.info(f"💼 Business ID: {creds.get('business_id', '')}")
            logger.info(f"📋 Template Data: {template_data}")
            logger.info("=" * 80)

            # 1. Obtener información de la compañía y su configuración
            query = """
                SELECT l.id as lead_id, d.company_id, c.name as company_name, 
                       public.get_company_data(d.company_id) as company_data
                FROM public.leads l
                INNER JOIN public.deals d ON d.lead_id = l.id AND d.is_deleted = false
                INNER JOIN public.companies c ON d.company_id = c.id
                WHERE l.phone = %s AND l.is_deleted = false
                LIMIT 1
            """
            with db_manager.get_connection() as conn:
                cur = conn.cursor()
                cur.execute(query, [clean_phone])
                result = cur.fetchone()

            if not result:
                logger.warning(f"[WhatsApp] No se encontró lead/deal/company para teléfono {to_phone}, usando config por defecto")
                company_config = {
                    'access_token': self.access_token,
                    'phone_number_id': self.phone_number_id,
                    'business_id': getattr(self, 'business_id', None)
                }
            else:
                lead_id, company_id, company_name, company_data = result
                logger.info(f"[WhatsApp] Lead {lead_id} encontrado para company: {company_name} (id: {company_id})")

                if not company_data or 'custom_properties' not in company_data:
                    logger.warning(f"[WhatsApp] No se encontraron custom_properties para company {company_id}, usando config por defecto")
                    company_config = {
                        'access_token': self.access_token,
                        'phone_number_id': self.phone_number_id,
                        'business_id': getattr(self, 'business_id', None)
                    }
                else:
                    custom_props = company_data['custom_properties']
                    company_config = {
                        'access_token': custom_props.get('WHATSAPP_ACCESS_TOKEN'),
                        'phone_number_id': custom_props.get('WHATSAPP_PHONE_NUMBER_ID'),
                        'business_id': custom_props.get('WHATSAPP_BUSINESS_ID')
                    }
                    if not all(company_config.values()):
                        logger.warning(f"[WhatsApp] Config incompleta para company {company_id}, usando por defecto")
                        company_config = {
                            'access_token': self.access_token,
                            'phone_number_id': self.phone_number_id,
                            'business_id': getattr(self, 'business_id', None)
                        }
                    else:
                        logger.info(f"[WhatsApp] Usando config específica de company {company_name} (id: {company_id})")

            # 2. Actualizar la configuración de WhatsApp para este envío
            headers = {
                'Authorization': f"Bearer {company_config['access_token']}",
                'Content-Type': 'application/json'
            }
            base_url = f"https://graph.facebook.com/v22.0/{company_config['phone_number_id']}/messages"
            
            # 3. Construir y enviar el mensaje
            payload = self._build_template_payload(template_name, template_data, to_phone)
            logger.debug(f"Enviando template '{template_name}' a {to_phone} → payload: {json.dumps(payload)}")
            
            response = requests.post(base_url, headers=headers, json=payload, timeout=timeout)
            if not response.ok:
                logger.error(f"❌ WhatsApp template error {response.status_code}: {response.text}")
            response.raise_for_status()
            
            message_id = response.json()['messages'][0]['id']
            logger.info(f"✅ Template '{template_name}' enviado exitosamente. ID: {message_id}")
            return True, message_id, payload
            
        except ValueError as e:
            logger.error(f"Error de validación: {str(e)}")
            return False, None, None
        except HTTPError as e:
            logger.error(f"Error de HTTP: {str(e)}")
            return False, None, None
        except Exception as e:
            logger.error(f"Error inesperado: {str(e)}")
            return False, None, None

    def send_text_message(self, to_phone: str, message: str, company_id: str | None = None, timeout: int = 10):
        try:
            clean_phone = PhoneUtils.strip_34(to_phone)
            if not PhoneUtils.validate_spanish_phone(clean_phone):
                raise ValueError(f"Número de teléfono inválido: {to_phone}")

            # 1) Credenciales: prioriza el tenant si viene company_id
            creds = get_whatsapp_credentials_for_phone(clean_phone, company_id=company_id)
            headers = creds.get('headers', self.headers)
            base_url = creds.get('base_url', self.base_url)

            logger.info("=" * 80)
            logger.info(f"🔐 Sending text message with credentials for phone {clean_phone}:")
            logger.info(f"📱 Company: {creds.get('company_name', 'Default')}")
            logger.info(f"🆔 Company ID: {creds.get('company_id', company_id or 'Default')}")
            logger.info(f"🔑 Token: {creds.get('access_token', '')[:20]}...")
            logger.info(f"📞 Phone Number ID: {creds.get('phone_number_id', '')}")
            logger.info(f"🌐 Base URL: {base_url}")
            logger.info(f"💼 Business ID: {creds.get('business_id', '')}")
            logger.info("=" * 80)

            logger.debug(f"Using credentials - base_url: {base_url}")
            logger.debug(f"Using credentials - headers: {headers}")

            # 2) (Opcional) Fallback si no hay credenciales del tenant
            if not creds.get('phone_number_id') or not creds.get('access_token'):
                # Tu bloque actual que consulta lead/deal/company por teléfono
                query = """
                    SELECT l.id as lead_id, d.company_id, c.name as company_name, 
                        public.get_company_data(d.company_id) as company_data
                    FROM public.leads l
                    INNER JOIN public.deals d ON d.lead_id = l.id AND d.is_deleted = false
                    INNER JOIN public.companies c ON d.company_id = c.id
                    WHERE l.phone = %s AND l.is_deleted = false
                    LIMIT 1
                """
                with db_manager.get_connection() as conn:
                    cur = conn.cursor()
                    cur.execute(query, [clean_phone])
                    result = cur.fetchone()

                if result:
                    lead_id, _company_id, company_name, company_data = result
                    logger.info(f"[WhatsApp] Lead {lead_id} encontrado para company: {company_name} (id: {_company_id})")
                    custom_props = (company_data or {}).get('custom_properties') or {}
                    access_token = custom_props.get('WHATSAPP_ACCESS_TOKEN') or creds.get('access_token')
                    phone_number_id = custom_props.get('WHATSAPP_PHONE_NUMBER_ID') or creds.get('phone_number_id')
                    headers = {'Authorization': f"Bearer {access_token}", 'Content-Type': 'application/json'}
                    base_url = f"https://graph.facebook.com/{GRAPH_API_VERSION}/{phone_number_id}/messages"

            # 3) Construir y enviar el mensaje
            payload = {
                "messaging_product": "whatsapp",
                "to": f"34{clean_phone}" if not clean_phone.startswith("34") else clean_phone,
                "type": "text",
                "text": {"body": message}
            }

            logger.debug(f"Enviando texto a {clean_phone} → payload: {payload}")
            resp = requests.post(base_url, headers=headers, json=payload, timeout=timeout)
            if not resp.ok:
                logger.error(f"❌ Error {resp.status_code} enviando texto: {resp.text}")
            resp.raise_for_status()

            data = resp.json() or {}
            msg_id = (data.get("messages") or [{}])[0].get("id")
            if not msg_id:
                logger.error(f"WhatsApp API no devolvió WAMID: {data}")
                return False, None

            logger.info(f"✅ Texto enviado. ID: {msg_id}")
            return True, msg_id

        except ValueError as e:
            logger.error(f"Error de validación: {str(e)}", exc_info=True)
            return False, None
        except requests.exceptions.RequestException as e:
            logger.error(f"Error de HTTP: {str(e)}", exc_info=True)
            return False, None
        except Exception as e:
            logger.error(f"❌ Excepción enviando texto: {e}", exc_info=True)
            return False, None

    def _build_template_payload(self, template_name: str, template_data: dict, to_phone: str) -> dict:
        """
        Construye el payload de envío de plantilla para la Cloud API de WhatsApp.
        - template_name: nombre EXACTO del template en WBM
        - template_data: dict con datos de la plantilla. Soporta:
            - language: "es_ES" (por defecto)
            - cover_url: URL imagen header (si no se pasa, usa default)
            - first_name, last_name, deal_id, slot_text, new_phone, responsible_name, etc.
            - body_params: lista[str] para modo genérico
            - buttons: lista de botones, p.ej:
                [
                {"type":"url", "index":0, "text_param":"<valor>"},
                {"type":"quick_reply", "index":1, "payload":"CONFIRMAR"}
                ]
        - to_phone: teléfono destino; se normaliza a E.164 español (34)
        """
        def normalize_es(phone: str) -> str:
            p = PhoneUtils.strip_34(str(phone))
            return p if p.startswith("34") else f"34{p}"

        # Idioma y cover
        lang = (template_data or {}).get("language") or "es_ES"
        cover_url = (
            (template_data or {}).get("cover_url")
            or getattr(self, "default_cover_url", None)
            or "https://app.solvify.es/cover-whats.jpg"
        )

        to_e164 = normalize_es(to_phone)
        components = []

        # Header (imagen) si hay cover_url
        if cover_url:
            components.append({
                "type": "header",
                "parameters": [{
                    "type": "image",
                    "image": {"link": cover_url}
                }]
            })

        name = (template_name or "").strip()

        # ======== PLANTILLAS CONOCIDAS (ajusta a tus definiciones reales en WBM) ========

        if name == "agendar_llamada_inicial":
            # Body: {{1}} = first_name
            # Botón URL dinámico con {{1}} = deal_id (definido así en WBM)
            first_name = (template_data or {}).get("first_name") or ""
            deal_id = (template_data or {}).get("deal_id") or ""
            components.append({
                "type": "body",
                "parameters": [
                    {"type": "text", "text": first_name}
                ]
            })
            components.append({
                "type": "button",
                "sub_type": "url",
                "index": 0,
                "parameters": [
                    {"type": "text", "text": deal_id}
                ]
            })

        elif name == "recordatorio_llamada_agendada":
            # Body: {{1}} = first_name, {{2}} = slot_text (fecha/hora legible)
            first_name = (template_data or {}).get("first_name") or ""
            slot_text = (template_data or {}).get("slot_text") or ""
            components.append({
                "type": "body",
                "parameters": [
                    {"type": "text", "text": first_name},
                    {"type": "text", "text": slot_text}
                ]
            })

        elif name == "retomar_contacto":
            # Body: {{1}} = first_name, {{2}} = responsible_name
            first_name = (template_data or {}).get("first_name") or ""
            responsible_name = (template_data or {}).get("responsible_name") or ""
            components.append({
                "type": "body",
                "parameters": [
                    {"type": "text", "text": first_name},
                    {"type": "text", "text": responsible_name}
                ]
            })

        elif name == "nuevo_numero":
            # Body: {{1}} = first_name, {{2}} = new_phone
            first_name = (template_data or {}).get("first_name") or ""
            new_phone = (template_data or {}).get("new_phone") or PhoneUtils.strip_34(to_e164)
            components.append({
                "type": "body",
                "parameters": [
                    {"type": "text", "text": first_name},
                    {"type": "text", "text": new_phone}
                ]
            })

        elif name == "baja_comercial":
            # Body: {{1}} = first_name
            first_name = (template_data or {}).get("first_name") or ""
            components.append({
                "type": "body",
                "parameters": [
                    {"type": "text", "text": first_name}
                ]
            })

        else:
            # ======== MODO GENÉRICO ========
            # Permite construir cualquier template pasando body_params/buttons desde template_data
            body_params = (template_data or {}).get("body_params") or []
            if body_params:
                components.append({
                    "type": "body",
                    "parameters": [{"type": "text", "text": str(x)} for x in body_params]
                })

            buttons = (template_data or {}).get("buttons") or []
            # Soporta botones URL (con parámetro de texto) y quick_reply (con payload)
            for i, btn in enumerate(buttons):
                btype = (btn.get("type") or "").lower()
                if btype == "url":
                    components.append({
                        "type": "button",
                        "sub_type": "url",
                        "index": int(btn.get("index", i)),
                        "parameters": [
                            {"type": "text", "text": str(btn.get("text_param", ""))}
                        ]
                    })
                elif btype in ("quick_reply", "quickreply", "quick-reply"):
                    components.append({
                        "type": "button",
                        "sub_type": "quick_reply",
                        "index": int(btn.get("index", i)),
                        "parameters": [
                            {"type": "payload", "payload": str(btn.get("payload", ""))}
                        ]
                    })
                # Otros tipos se pueden añadir aquí (COPY_CODE, OTP autofill, etc.)

        payload = {
            "messaging_product": "whatsapp",
            "to": to_e164,
            "type": "template",
            "template": {
                "name": name,
                "language": {"code": lang},
                "components": components
            }
        }
        return payload

class AutoReplyService:
    """Auto-reply service for office hours management"""
    def __init__(self, db_manager):
        self.db_manager = db_manager
        self._last_auto_replies = {}
        self._cache_duration = 3600  # 1 hora

    def is_office_hours(self, madrid_datetime=None):
        if madrid_datetime is None:
            madrid_datetime = now_madrid()
        weekday = madrid_datetime.weekday()
        if weekday > 4:
            return False
        hour = madrid_datetime.hour
        if hour < 8 or hour >= 22:
            return False
        return True

    def get_auto_reply_message(self, madrid_datetime=None):
        if madrid_datetime is None:
            madrid_datetime = now_madrid()
        weekday = madrid_datetime.weekday()
        hour = madrid_datetime.hour
        if weekday > 4:
            return (
                "¡Gracias por tu mensaje! 😊\n\n"
                "En este momento estamos fuera de oficina ya que es fin de semana. "
                "Nuestro horario de atención es de lunes a viernes de 9:00 a 19:00h.\n\n"
                "Te responderemos el próximo día laborable. "
                "¡Que tengas un buen fin de semana!"
            )
        elif hour < 9:
            return (
                "¡Gracias por tu mensaje! 🌅\n\n"
                "En este momento estamos fuera de oficina. "
                "Nuestro horario de atención es de lunes a viernes de 9:00 a 19:00h.\n\n"
                "Abrimos a las 9:00h. Te responderemos lo antes posible durante nuestro horario de oficina."
            )
        elif hour >= 19:
            return (
                "¡Gracias por tu mensaje! 🌙\n\n"
                "En este momento estamos fuera de oficina. "
                "Nuestro horario de atención es de lunes a viernes de 9:00 a 19:00h.\n\n"
                "Te responderemos mañana durante nuestro horario de oficina. ¡Que tengas una buena noche!"
            )
        else:
            return (
                "¡Gracias por tu mensaje! 📞\n\n"
                "En este momento estamos fuera de oficina. "
                "Nuestro horario de atención es de lunes a viernes de 9:00 a 19:00h.\n\n"
                "Te responderemos lo antes posible durante nuestro horario de oficina."
            )

    def should_send_auto_reply(self, phone_number):
        clean_phone = PhoneUtils.strip_34(phone_number)
        current_time = time.time()

        # Purge cache
        expired_keys = [k for k, ts in self._last_auto_replies.items() if current_time - ts > self._cache_duration]
        for k in expired_keys:
            del self._last_auto_replies[k]

        # Already replied recently?
        if clean_phone in self._last_auto_replies and current_time - self._last_auto_replies[clean_phone] < self._cache_duration:
            return False

        # Check DB (last hour)
        try:
            madrid_time_threshold = now_madrid_naive() - timedelta(hours=1)
            query = """
                SELECT COUNT(*) FROM external_messages
                WHERE sender_phone = %s
                AND from_me = 'true'
                AND status IN ('sent', 'auto_reply')
                AND created_at > %s
            """
            result = self.db_manager.execute_query(query, [clean_phone, madrid_time_threshold], fetch_one=True)
            if result and result[0] > 0:
                return False
        except Exception:
            logger.exception('Error checking recent auto-replies in DB')
            return False

        return True
    # AutoReplyService
    def send_auto_reply(self, phone_number, whatsapp_service, message_service, company_id: str | None = None):
        try:
            madrid_time = now_madrid()
            if self.is_office_hours(madrid_time):
                return False, "Office hours - no auto-reply needed"
            if not self.should_send_auto_reply(phone_number):
                return False, "Auto-reply already sent recently"

            auto_message = self.get_auto_reply_message(madrid_time)

            destination = PhoneUtils.add_34(phone_number)
            # ✅ ahora acepta company_id y lo pasa al envío
            success, message_id = whatsapp_service.send_text_message(destination, auto_message, company_id=company_id)

            if success:
                clean_phone = PhoneUtils.strip_34(phone_number)
                current_time_naive = now_madrid_naive()

                assigned_to_id, responsible_email = message_service.lead_service.get_lead_assigned_info(clean_phone)
                lead = message_service.lead_service.get_lead_data_by_phone(clean_phone)
                deal_id = (lead.get('deal_id') if lead and lead.get('deal_id') else None)

                chat_id = deal_id              # ✅ chat_id = deal_id (UUID)
                chat_url = clean_phone         # ✅ teléfono como “url” visual

                query = """
                    INSERT INTO external_messages (
                        id, message, sender_phone, responsible_email,
                        last_message_uid, last_message_timestamp, from_me,
                        status, created_at, updated_at, is_deleted,
                        chat_url, chat_id, is_read, assigned_to_id, company_id
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,NOW(),NOW(),FALSE,%s,%s,%s,%s,%s)
                """
                params = [
                    str(uuid4()), auto_message, clean_phone, (responsible_email or ""),
                    message_id, current_time_naive, 'true',
                    'sent',                          # FSM limpia: sent → delivered → read
                    chat_url, chat_id, False, assigned_to_id, company_id
                ]
                self.db_manager.execute_query(query, params)

                self._last_auto_replies[clean_phone] = time.time()

                log_sent_message(f'+{destination}', auto_message, message_id)
                logger.info(f"🤖 Auto-reply sent to {clean_phone} (outside office hours) -> sent (wamid={message_id})")
                return True, f"Auto-reply sent: {message_id}"
            else:
                return False, "Failed to send auto-reply message"

        except Exception:
            logger.exception('Error in send_auto_reply')
            return False, "Error sending auto-reply"

class LeadService:
    """Lead and deal management service"""

    def __init__(self, db_manager):
        self.db_manager = db_manager

    def get_lead_data_by_phone(self, phone: str):
        """Devuelve datos completos del lead + deal + responsable + compañía a partir del teléfono."""
        clean_phone = PhoneUtils.strip_34(phone)
        query = """
            SELECT
                l.id, l.first_name, l.last_name, l.email,
                d.id, d.user_assigned_id,
                p.email, p.first_name, p.last_name,
                c.name , c.id
            FROM public.leads l
            JOIN public.deals d ON d.lead_id = l.id
            LEFT JOIN public.profiles p ON p.id = d.user_assigned_id
            LEFT JOIN public.companies c ON d.company_id = c.id
            WHERE l.phone = %s
            LIMIT 1
        """
        row = self.db_manager.execute_query(query, [clean_phone], fetch_one=True)
        if not row:
            return None
        return {
            'lead_id': str(row[0]),
            'first_name': row[1] or '',
            'last_name': row[2] or '',
            'email': row[3] or '',
            'deal_id': str(row[4]),
            'user_assigned_id': str(row[5]) if row[5] else None,
            'responsible_email': row[6] or '',
            'responsible_first_name': row[7] or '',
            'responsible_name': f"{row[7] or ''} {row[8] or ''}".strip(),
            'company_name': row[9] or '',  # ← NUEVO CAMPO,
            'company_id': str(row[10]) if row[10] else None,
            'phone': clean_phone,
        }

    def get_lead_assigned_info(self, phone: str):
        """Devuelve (user_assigned_id, email del responsable) o (None, None)."""
        clean_phone = PhoneUtils.strip_34(phone)
        query = """
            SELECT d.user_assigned_id, p.email
            FROM public.leads AS l
            JOIN public.deals AS d ON d.lead_id = l.id
            LEFT JOIN public.profiles AS p ON p.id = d.user_assigned_id
            WHERE l.phone = %s
            LIMIT 1
        """
        row = self.db_manager.execute_query(query, [clean_phone], fetch_one=True)
        if row and row[0]:
            return str(row[0]), row[1]
        return None, None

    def update_deal_assignee(self, phone: str, assigned_to_id: str) -> bool:
        """Actualiza el responsable de un deal basado en el teléfono del lead."""
        try:
            clean_phone = PhoneUtils.strip_34(phone)
            
            # Primero verificar que el assigned_to_id es válido
            if not self.validate_assigned_to_id(assigned_to_id):
                logger.warning(f"assigned_to_id inválido: {assigned_to_id}")
                return False
            
            # Actualizar el deal del lead
            update_query = """
                UPDATE public.deals 
                SET user_assigned_id = %s, updated_at = NOW()
                WHERE lead_id = (
                    SELECT id FROM public.leads 
                    WHERE phone = %s 
                    LIMIT 1
                )
            """
            
            affected_rows = self.db_manager.execute_query(update_query, [assigned_to_id, clean_phone])
            
            if affected_rows > 0:
                logger.info(f"Deal assignee updated for phone {clean_phone} -> {assigned_to_id}")
                return True
            else:
                logger.warning(f"No deal found to update for phone {clean_phone}")
                return False
                
        except Exception as e:
            logger.exception(f"Error updating deal assignee for phone {phone}: {e}")
            return False

    def validate_assigned_to_id(self, assigned_to_id: str) -> bool:
        """Valida que el assigned_to_id existe en la tabla profiles."""
        if not assigned_to_id:
            return False
            
        try:
            # Verificar que el ID existe en profiles
            query = """
                SELECT 1 FROM public.profiles 
                WHERE id = %s AND is_deleted = false 
                LIMIT 1
            """
            result = self.db_manager.execute_query(query, [assigned_to_id], fetch_one=True)
            return bool(result)
            
        except Exception as e:
            logger.exception(f"Error validating assigned_to_id {assigned_to_id}: {e}")
            return False
    
class MessageService:
    """Message persistence service"""
    def __init__(self, db_manager, lead_service):
        self.db_manager = db_manager
        self.lead_service = lead_service

    # ---------- Utilidades flow-exit ----------
    def was_template_message(self, context_id: str) -> bool:
        """True si context_id corresponde a un mensaje 'template_sent'."""
        if not context_id:
            return False
        q = """
            SELECT 1
            FROM public.external_messages
            WHERE last_message_uid = %s
              AND status = 'template_sent'
            LIMIT 1
        """
        row = self.db_manager.execute_query(q, [context_id], fetch_one=True)
        return bool(row)

    def has_flow_exit_marker(self, context_id: str, phone: str) -> bool:
        """Evita duplicar el exit para un mismo context_id + teléfono."""
        if not context_id or not phone:
            return False
        q = """
            SELECT 1
            FROM public.external_messages
            WHERE last_message_uid = %s
              AND sender_phone = %s
              AND status = 'flow_exit_triggered'
            LIMIT 1
        """
        row = self.db_manager.execute_query(q, [context_id, PhoneUtils.strip_34(phone)], fetch_one=True)
        return bool(row)

    def mark_flow_exit_triggered(self, context_id: str, phone: str, chat_id: str) -> None:
        """Inserta un marcador para no repetir el flow-exit en el mismo contexto."""
        clean_phone = PhoneUtils.strip_34(phone)
        now_ts = now_madrid_naive()
        insert_sql = """
            INSERT INTO public.external_messages (
                id, message, sender_phone, responsible_email,
                last_message_uid, last_message_timestamp,
                from_me, status, created_at, updated_at, is_deleted,
                chat_id, chat_url, assigned_to_id
            ) VALUES (
                %s, %s, %s, %s,
                %s, %s,
                %s, %s, NOW(), NOW(), FALSE,
                %s, %s, %s
            )
        """
        params = [
            str(uuid4()), 'flow_exit_ok', clean_phone, '',
            context_id, now_ts,
            'false', 'flow_exit_triggered',
            chat_id or clean_phone, clean_phone, None
        ]
        self.db_manager.execute_query(insert_sql, params)

    def get_recent_template_context_for_phone(self, phone: str, window_minutes: int = 15) -> str | None:
        """
        Fallback: obtiene el last_message_uid del template más reciente
        enviado a 'phone' dentro de una ventana temporal.
        """
        clean_phone = PhoneUtils.strip_34(phone)
        threshold = now_madrid_naive() - timedelta(minutes=window_minutes)
        q = """
            SELECT last_message_uid
            FROM public.external_messages
            WHERE sender_phone = %s
              AND from_me = 'true'
              AND status = 'template_sent'
              AND created_at > %s
              AND last_message_uid IS NOT NULL
            ORDER BY created_at DESC
            LIMIT 1
        """
        row = self.db_manager.execute_query(q, [clean_phone, threshold], fetch_one=True)
        return row[0] if row else None

    # ---------- Guardado de mensajes (TENANT-AWARE) ----------
    def save_incoming_message(self, msg: dict, wa_id: str, company_id: str | None = None) -> bool:
        """Guarda mensaje entrante. Upsert manual por last_message_uid, aislado por company_id."""
        try:
            sender = PhoneUtils.strip_34(msg.get('from', ''))
            uid = msg.get('id')  # wamid.* si llega
            text = (msg.get('text') or {}).get('body')
            body_text_or_json = text if text else json.dumps(msg, ensure_ascii=False)

            wa_timestamp = msg.get('timestamp')
            last_message_ts = timestamp_to_madrid_naive(wa_timestamp) if wa_timestamp else now_madrid_naive()

            assigned_to_id, responsible_email = self.lead_service.get_lead_assigned_info(sender)
            lead = self.lead_service.get_lead_data_by_phone(sender)

            # chat_id/chat_url como antes
            chat_id = (lead.get('deal_id') if lead and lead.get('deal_id') else sender)
            chat_url = sender

            # --- Resolver company_id efectivo ---
            effective_company_id = company_id or (lead.get('company_id') if lead else None)

            # --- DEDUPE por last_message_uid (aislado por tenant si lo conocemos) ---
            if uid:
                if effective_company_id:
                    # Busca SOLO dentro del tenant
                    check_sql = """
                        SELECT id, company_id
                          FROM public.external_messages
                         WHERE last_message_uid = %s
                           AND company_id       = %s
                         LIMIT 1
                    """
                    row = self.db_manager.execute_query(check_sql, [uid, effective_company_id], fetch_one=True)
                else:
                    # Sin tenant, busca global (compatibilidad)
                    check_sql = "SELECT id FROM public.external_messages WHERE last_message_uid = %s LIMIT 1"
                    row = self.db_manager.execute_query(check_sql, [uid], fetch_one=True)

                if row and row[0]:
                    set_chat = ", chat_id = %s" if deal_id else ""
                    update_sql = f"""
                    UPDATE public.external_messages
                    SET message = %s,
                        sender_phone = %s,
                        responsible_email = %s,
                        last_message_timestamp = %s,
                        from_me = %s,
                        status = %s,
                        chat_url = %s,
                        assigned_to_id = %s,
                        updated_at = NOW()
                    {set_chat}
                    WHERE id = %s
                    """
                    params_upd = [
                    body_text_or_json, sender, (responsible_email or ""),
                    last_message_ts, 'false', 'received',
                    sender, assigned_to_id
                    ]
                    if deal_id:
                       params_upd.append(deal_id)
                       params_upd.append(row[0])
                       self.db_manager.execute_query(update_sql, params_upd)
                       return True

            deal_id = lead.get('deal_id') if lead else None

            insert_sql = """
            INSERT INTO public.external_messages (
            id, message, sender_phone, responsible_email,
            last_message_uid, last_message_timestamp,
            from_me, status, created_at, updated_at, is_deleted,
            chat_url, chat_id, assigned_to_id, company_id
            ) VALUES (
            %s, %s, %s, %s,
            %s, %s,
            %s, %s, NOW(), NOW(), FALSE,
            %s, %s, %s, %s
            )
            """
            params_ins = [
            str(uuid4()), body_text_or_json, sender, (responsible_email or ""),
            uid, last_message_ts,
            'false', 'received',
            sender,      # chat_url = teléfono
            deal_id,     # chat_id = deal_id (UUID o NULL)
            assigned_to_id, effective_company_id
            ]
            self.db_manager.execute_query(insert_sql, params_ins)
            return True

        except Exception:
            logging.exception('Failed to save incoming message')
            return False
        
    def save_outgoing_message(
        self,
        phone: str,
        text: str,
        wamid: str | None,
        responsible_email: str | None,
        assigned_to_id: str | None,
        company_id: str | None = None
    ) -> bool:
        """Registra un mensaje saliente (from_me=true), aislado por tenant si se conoce."""
        try:
            sender = PhoneUtils.strip_34(str(phone))
            last_message_ts = now_madrid_naive()

            effective_company_id = company_id
            if not effective_company_id:
                # Resolver por lead si no viene explicitamente
                lead = self.lead_service.get_lead_data_by_phone(sender)
                effective_company_id = (lead.get('company_id') if lead else None)

            deal_id = lead.get('deal_id') if lead else None

            insert_sql = """
            INSERT INTO public.external_messages (
            id, message, sender_phone, responsible_email,
            last_message_uid, last_message_timestamp,
            from_me, status, created_at, updated_at, is_deleted,
            chat_url, chat_id, assigned_to_id, company_id
            ) VALUES (
            %s, %s, %s, %s,
            %s, %s,
            %s, %s, NOW(), NOW(), FALSE,
            %s, %s, %s, %s
            )
            """
            params = [
            str(uuid4()), (text or ""), sender, (responsible_email or ""),
            wamid, last_message_ts,
            'true', 'sent',
            sender,      # chat_url = teléfono
            deal_id,     # chat_id = deal_id
            assigned_to_id, effective_company_id
            ]
            self.db_manager.execute_query(insert_sql, params)
            return True
        except Exception:
            logging.exception("Failed to save outgoing message")
            return False
    def update_outgoing_status(wamid: str, status: str, company_id: str | None):
        try:
            if company_id:
                sql = """
                    UPDATE public.external_messages
                    SET status = %s, updated_at = NOW()
                    WHERE last_message_uid = %s
                    AND company_id = %s
                """
                db_manager.execute_query(sql, [status, wamid, company_id])
            else:
                sql = """
                    UPDATE public.external_messages
                    SET status = %s, updated_at = NOW()
                    WHERE last_message_uid = %s
                """
                db_manager.execute_query(sql, [status, wamid])
        except Exception:
            logging.exception("Failed to update outgoing status")

    def save_template_message(
        self,
        payload: dict,
        wamid: str | None,
        company_id: str | None = None
    ) -> bool:
        """
        Registra un mensaje saliente de tipo TEMPLATE (from_me=true) con status 'template_sent'.
        Se intenta resolver phone y metadatos desde el payload y, si no, por la BBDD.
        """
        try:
            # 1) Resolver teléfono (en distintos posibles lugares del payload)
            phone = (
                (payload.get("phone")) or
                (payload.get("to")) or
                # algunos endpoints usan "template" con "to" en el payload real de envío
                ((payload.get("template_payload") or {}).get("to")) or
                ""
            )
            sender = PhoneUtils.strip_34(str(phone)) if phone else None

            # 2) Template name (si viene)
            template_name = (
                payload.get("template_name")
                or (payload.get("template_data") or {}).get("template_name")
                or (payload.get("template") or {}).get("name")
                or ""
            )

            # 3) Timestamp
            last_message_ts = now_madrid_naive()

            # 4) Resolver asignaciones/lead
            assigned_to_id = None
            responsible_email = ""
            lead = None

            if sender:
                try:
                    assigned_to_id, responsible_email = self.lead_service.get_lead_assigned_info(sender)
                except Exception:
                    logging.exception("Failed to get lead assigned info for sender=%s", sender)

                try:
                    lead = self.lead_service.get_lead_data_by_phone(sender)
                except Exception:
                    logging.exception("Failed to get lead data for sender=%s", sender)

            chat_id = None
            chat_url = None
            if lead:
                # Si tienes deal_id como chat_id, úsalo; si no, fallback al teléfono
                chat_id = lead.get("deal_id") or sender
                chat_url = sender
                # company_id efectivo
                if not company_id:
                    company_id = lead.get("company_id")

            if not chat_id:
                chat_id = sender or str(uuid4())
            if not chat_url:
                chat_url = sender or ""

            # 5) Cuerpo a guardar: si tienes el payload de template, lo guardamos como JSON
            body_json = json.dumps({
                "type": "template",
                "template_name": template_name,
                "payload": payload
            }, ensure_ascii=False)

            # 6) Insert con status template_sent y from_me='true'
            insert_sql = """
                INSERT INTO public.external_messages (
                    id, message, sender_phone, responsible_email,
                    last_message_uid, last_message_timestamp,
                    from_me, status, created_at, updated_at, is_deleted,
                    chat_id, chat_url, assigned_to_id, company_id
                ) VALUES (
                    %s, %s, %s, %s,
                    %s, %s,
                    %s, %s, NOW(), NOW(), FALSE,
                    %s, %s, %s, %s
                )
                ON CONFLICT DO NOTHING
            """
            params = [
                str(uuid4()),
                body_json,
                (sender or ""),
                (responsible_email or ""),
                wamid,
                last_message_ts,
                'true',
                'template_sent',
                chat_id,
                chat_url,
                assigned_to_id,
                company_id
            ]
            self.db_manager.execute_query(insert_sql, params)
            return True

        except Exception:
            logging.exception("Failed to save template message")
            return False

# ---------- Gestión de credenciales WhatsApp por company_id (tenant) ----------

# --- Cache opcional para mapear phone -> company_id (simple diccionario en memoria)
_phone_company_cache: dict[str, str] = {}

def _resolve_company_id_from_phone(phone: str) -> str | None:
    """Intenta resolver company_id a partir del teléfono normalizado."""
    try:
        if not phone:
            return None
        phone_norm = PhoneUtils.strip_34(str(phone))
        if phone_norm in _phone_company_cache:
            return _phone_company_cache[phone_norm]

        sql = """
            SELECT d.company_id
              FROM public.leads l
              JOIN public.deals d ON d.lead_id = l.id
             WHERE l.phone = %s
             LIMIT 1
        """
        row = db_manager.execute_query(sql, [phone_norm], fetch_one=True)
        company_id = row[0] if row else None
        if company_id:
            _phone_company_cache[phone_norm] = company_id
        return company_id
    except Exception:
        logging.exception("Failed to resolve company_id from phone")
        return None


def get_whatsapp_credentials_for_phone(phone: str | None, company_id: str | None = None) -> dict:
    """
    Devuelve credenciales de WhatsApp priorizando:
      1) company_id explícito (tenant)
      2) company_id resuelto por phone
      3) DEFAULT_* de entorno (modo global)
    """
    # 1) Si nos pasan company_id explícito, úsalo
    if company_id:
        return get_whatsapp_credentials_for_company(company_id)

    # 2) Intentar resolver company_id a partir del phone
    if phone:
        cid = _resolve_company_id_from_phone(phone)
        if cid:
            return get_whatsapp_credentials_for_company(cid)

    # 3) Fallback a variables globales (ya definidas en tu app)
    #    Asegúrate de tener WABA_ID, ACCESS_TOKEN, PHONE_NUMBER_ID y GRAPH_API_VERSION
    base_url = f"https://graph.facebook.com/{GRAPH_API_VERSION}"
    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }
    return {
        "waba_id": WABA_ID,
        "access_token": ACCESS_TOKEN,
        "phone_number_id": PHONE_NUMBER_ID,
        "base_url": base_url,
        "headers": headers,
    }



from datetime import datetime
import logging

logger = logging.getLogger(__name__)
   
def _safe_json(text):
    try:
        return json.loads(text)
    except Exception:
        return None
        
def send_flow_exit(lead_id: str):
    """
    POST a {SERVER_BASE_URL}/api/exit con:
      { "id": <lead_id>, "flow_name": "...", "motivo": "..." }

    Loga: base_url, exit_url, DNS->IP, timings, headers y cuerpo de respuesta.
    """
    base = config.flow_config.get('server_base_url', '').strip() if getattr(config, 'flow_config', None) else ''
    flow_name = (config.flow_config or {}).get('flow_name', 'welcome_email_flow')
    exit_reason = (config.flow_config or {}).get('exit_reason', 'Usuario quiere salir del flow')
    timeout_s = int((config.flow_config or {}).get('http_timeout', 5))

    if not base:
        logger.error("[FLOW EXIT] server_base_url no configurado")
        return False

    # Normaliza la URL final -> {base}/api/exit (sin dobles '/')
    exit_path = "/api/exit"
    exit_url = base.rstrip("/") + exit_path

    payload = {"id": str(lead_id), "flow_name": flow_name, "motivo": exit_reason}
    headers = {"Content-Type": "application/json"}

    # --- log pre ---
    logger.info("[FLOW EXIT] base_url=%s", base)
    logger.info("[FLOW EXIT] exit_url=%s", exit_url)
    logger.info("[FLOW EXIT] payload=%s", json.dumps(payload, ensure_ascii=False))
    logger.info("[FLOW EXIT] headers=%s", headers)
    logger.info("[FLOW EXIT] timeout_s=%s", timeout_s)

    # DNS/IP + puerto
    try:
        parsed = urllib.parse.urlparse(exit_url)
        host = parsed.hostname
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        ip = socket.gethostbyname(host) if host else None
        logger.info("[FLOW EXIT] host=%s resolved_ip=%s port=%s scheme=%s", host, ip, port, parsed.scheme)
    except Exception as e:
        logger.warning("[FLOW EXIT] DNS resolve failed: %s", e)

    # Request preparado (para ver URL final exacta, headers y body que envía requests)
    try:
        s = Session()
        req = PreparedRequest()
        req.prepare(
            method="POST",
            url=exit_url,
            headers=headers,
            json=payload
        )
        logger.info("[FLOW EXIT] prepared.url=%s", req.url)
        logger.info("[FLOW EXIT] prepared.headers=%s", dict(req.headers))
        # Body: en JSON
        try:
            logger.info("[FLOW EXIT] prepared.body=%s", req.body.decode("utf-8") if isinstance(req.body, bytes) else str(req.body))
        except Exception:
            logger.info("[FLOW EXIT] prepared.body=<binary>")
    except Exception as e:
        logger.exception("[FLOW EXIT] Error preparando la request: %s", e)
        return False

    # Timings + envío
    t0 = _time.monotonic()
    try:
        resp = s.send(req, timeout=timeout_s, allow_redirects=True)
    except requests.exceptions.RequestException as e:
        t1 = _time.monotonic()
        logger.exception("[FLOW EXIT] RequestException tras %.0f ms: %s", (t1 - t0) * 1000, e)
        return False

    t1 = _time.monotonic()
    elapsed_ms = int((t1 - t0) * 1000)

    # --- log post ---
    logger.info("[FLOW EXIT] status=%s elapsed_ms=%s", resp.status_code, elapsed_ms)
    logger.info("[FLOW EXIT] response.headers=%s", dict(resp.headers))

    # Muestra JSON si es posible, si no el texto
    js = None
    try:
        js = resp.json()
        logger.info("[FLOW EXIT] response.json=%s", js)
    except Exception:
        logger.info("[FLOW EXIT] response.text=%s", resp.text[:2000])  # evita logs infinitos

    # Rastrea redirecciones si las hubo
    if resp.history:
        logger.info("[FLOW EXIT] redirects=%s", len(resp.history))
        for i, h in enumerate(resp.history, 1):
            logger.info("  ↪ [%d] %s %s -> %s", i, h.status_code, h.request.method, h.headers.get("Location"))

    # Diagnóstico común para 404
    if resp.status_code == 404:
        logger.error("[FLOW EXIT] ERROR 404: la ruta no existe en el servidor destino. "
                     "Comprueba que el servicio expone %s exactamente (método POST) en %s",
                     exit_path, base)
        # pistas extra
        logger.error("[FLOW EXIT] pistas: 1) ¿nginx tiene location /api/exit? 2) ¿proxy a puerto correcto? "
                     "3) ¿hay barra final en base_url? (actual: %s) 4) ¿SSL correcto/host header?",
                     base)

    return 200 <= resp.status_code < 300

# Utility functions
def now_madrid():
    return datetime.now(ZoneInfo("Europe/Madrid"))

def now_madrid_naive():
    return datetime.now(ZoneInfo("Europe/Madrid")).replace(tzinfo=None)

def utc_to_madrid(utc_datetime):
    if utc_datetime.tzinfo is None:
        utc_datetime = utc_datetime.replace(tzinfo=timezone.utc)
    return utc_datetime.astimezone(ZoneInfo("Europe/Madrid"))

def timestamp_to_madrid(timestamp):
    utc_dt = datetime.fromtimestamp(int(timestamp), tz=timezone.utc)
    return utc_dt.astimezone(ZoneInfo("Europe/Madrid"))

def timestamp_to_madrid_naive(timestamp):
    utc_dt = datetime.fromtimestamp(int(timestamp), tz=timezone.utc)
    madrid_dt = utc_dt.astimezone(ZoneInfo("Europe/Madrid"))
    return madrid_dt.replace(tzinfo=None)

def ensure_madrid_timezone(dt):
    if dt is None:
        return now_madrid()
    if dt.tzinfo is None:
        return dt.replace(tzinfo=ZoneInfo("Europe/Madrid"))
    return dt.astimezone(ZoneInfo("Europe/Madrid"))

def madrid_to_naive(madrid_dt):
    if madrid_dt.tzinfo is None:
        return madrid_dt
    return madrid_dt.replace(tzinfo=None)

def convert_uuids_to_strings(obj):
    import uuid
    if isinstance(obj, uuid.UUID):
        return str(obj)
    if isinstance(obj, dict):
        return {k: convert_uuids_to_strings(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [convert_uuids_to_strings(v) for v in obj]
    return obj

def log_received_message(msg: dict, wa_id: str = None):
    sender = msg.get('from')
    text = msg.get('text', {}).get('body', '')
    wa_timestamp = msg.get('timestamp', 0)
    if wa_timestamp:
        ts_madrid = timestamp_to_madrid(wa_timestamp)
    else:
        ts_madrid = now_madrid()
    logger.info(f"[RECEIVED] {ts_madrid.strftime('%Y-%m-%d %H:%M:%S %Z')} | {sender} | '{text}' | wa_id={wa_id}")

def _normalize_base_url(url: str) -> str:
    """Quita la barra final para construir rutas de forma estable."""
    return url.rstrip('/')

def handle_possible_flow_exit_only_if_reply_to_template(msg: dict, lead_service, db_manager, config) -> None:
    """
    Llama al FlowExitClient SOLO si el mensaje entrante es una RESPUESTA (context.id)
    a un mensaje nuestro cuyo status en external_messages sea 'template_sent'.
    - Obtiene lead_id a partir del teléfono del remitente.
    - Si encuentra que el "context.id" apunta a 'template_sent', dispara exit_flow.
    """
    try:
        # 1) ¿Tiene contexto de respuesta?
        ctx = msg.get('context') or {}
        reply_to_uid = ctx.get('id')
        if not reply_to_uid:
            logger.debug("[FLOW EXIT] mensaje sin context.id → no es respuesta; no se dispara.")
            return

        # 2) ¿El 'reply_to_uid' corresponde a un template enviado?
        check_sql = """
            SELECT status
            FROM public.external_messages
            WHERE last_message_uid = %s
            LIMIT 1
        """
        row = db_manager.execute_query(check_sql, [reply_to_uid], fetch_one=True)
        if not row:
            logger.debug(f"[FLOW EXIT] no hay registro en external_messages con last_message_uid={reply_to_uid}; no se dispara.")
            return

        status = row[0]
        if status != 'template_sent':
            logger.debug(f"[FLOW EXIT] el reply apunta a un mensaje con status='{status}', no 'template_sent'; no se dispara.")
            return

        # 3) Resolver lead_id desde el teléfono del remitente
        sender_phone = PhoneUtils.strip_34(msg.get('from', ''))
        lead = lead_service.get_lead_data_by_phone(sender_phone)
        if not lead or not lead.get('lead_id'):
            logger.warning(f"[FLOW EXIT] no se pudo resolver lead_id para phone={sender_phone}; abortando.")
            return

        lead_id = lead['lead_id']

        # 4) Disparar exit_flow
        flow_client = FlowExitClient(config)
        ok = flow_client.exit_flow(lead_id=lead_id)
        if not ok:
            logger.error(f"[FLOW EXIT] no se pudo completar para lead_id={lead_id}")
        else:
            logger.info(f"[FLOW EXIT] solicitado con éxito para lead_id={lead_id}")

    except Exception as e:
        logger.exception(f"[FLOW EXIT] excepción procesando respuesta a template: {e}")

def _candidate_exit_urls(base_url: str):
    """
    Genera URLs candidatas para /api/exit:
    1) base_url + /api/exit  (sin puerto)
    2) si es https -> puerto 5100
       si es http  -> puerto 5101
    Mantiene orden y evita duplicados.
    """
    base_url = _normalize_base_url(base_url or '')
    if not base_url:
        return []

    p = urlparse(base_url)
    path = '/api/exit'
    candidates = []

    def build(parsed, port=None):
        netloc = parsed.hostname
        if port:
            netloc = f"{netloc}:{port}"
        elif parsed.port:  # ya venía con puerto en BASE_URL
            netloc = f"{netloc}:{parsed.port}"
        # conserva esquema; fuerza path = /api/exit
        return urlunparse((parsed.scheme, netloc, path, '', '', ''))

    # 1) sin forzar puerto (tal cual BASE_URL)
    candidates.append(build(p, None))

    # 2) forzar puerto dev según esquema si BASE_URL no lo tenía
    if p.port is None:
        if p.scheme == 'https':
            candidates.append(build(p, 5100))
        elif p.scheme == 'http':
            candidates.append(build(p, 5101))

    # Elimina duplicados preservando orden
    seen = set()
    ordered = []
    for u in candidates:
        if u not in seen:
            seen.add(u)
            ordered.append(u)
    return ordered

def post_flow_exit(lead_id: str, reason: str = "Usuario quiere salir del flow"):
    """
    Llama al endpoint del scheduler para forzar la salida del flow.
    Usa BASE_URL de config y reintenta con puertos dev si hace falta.
    Loguea request/response para diagnóstico.
    """
    payload = {
        "id": lead_id,
        "flow_name": "welcome_email_flow",
        "motivo": reason
    }
    headers = {"Content-Type": "application/json"}
    urls = _candidate_exit_urls(getattr(config, 'flow_base_url', None))

    if not urls:
        logger.error("[FLOW EXIT] BASE_URL vacío o inválido; no se puede llamar al scheduler")
        return False

    for idx, url in enumerate(urls, start=1):
        try:
            logger.info(f"[FLOW EXIT] intento {idx}/{len(urls)} → POST {url} | payload={payload}")
            r = requests.post(url, headers=headers, json=payload, timeout=8)
            logger.info(f"[FLOW EXIT] respuesta {r.status_code} | body={r.text[:500]}")
            if 200 <= r.status_code < 300:
                logger.info("[FLOW EXIT] OK")
                return True
            elif r.status_code == 404:
                # Prueba siguiente candidato (p.ej. con puerto 5100)
                logger.error("[FLOW EXIT] 404 en %s — pruebo siguiente candidato (si hay)", url)
                continue
            else:
                logger.error(f"[FLOW EXIT] HTTP {r.status_code} en {url}")
                # En códigos ≠404, aún probamos el siguiente por si fuera routing
                continue
        except requests.RequestException as e:
            logger.exception(f"[FLOW EXIT] Error de red al llamar {url}: {e}")

    logger.error("[FLOW EXIT] Fallaron todos los candidatos; revisa Nginx/puerto/route del scheduler")
    return False


def log_sent_message(to_phone: str, text: str, wa_id: str = None):
    ts = now_madrid()
    logger.info(f"[SENT] {ts.strftime('%Y-%m-%d %H:%M:%S %Z')} | {to_phone} | '{text}' | wa_id={wa_id}")

def rate_limit(max_calls=10, window=60):
    calls = []
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            now = time.time()
            while calls and calls[0] <= now - window:
                calls.pop(0)
            if len(calls) >= max_calls:
                return jsonify({'error': 'Rate limit exceeded'}), 429
            calls.append(now)
            return f(*args, **kwargs)
        return wrapper
    return decorator

def build_flow_exit_client(config, logger):
    """
    Crea y devuelve un FlowExitClient usando la configuración ya cargada en `config`.
    Lee la API key desde `config.api_key`.
    """
    fc = config.flow_config  # dict con base_url, ports, etc.
    return FlowExitClient(
        flow_config=fc,   # <-- ahora sí acepta keyword
        api_key=config.api_key,
        logger=logger
    )

# Initialize configuration and services
def configure_logging():
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s | %(levelname)s | %(name)s | %(message)s'
    )
    return logging.getLogger("webhook")

def get_next_message_status(current_status: str, whatsapp_status: str) -> str:
    """
    Determina el siguiente estado basado en el estado actual y el evento de WhatsApp.
    
    Flujos implementados:
    - Templates: template_sent → template_delivered → template_read | template_failed
    - Texto: sent → message_delivered → message_read | message_failed  
    - Media: media_sent → media_delivered → media_read | media_failed
    - Auto-respuestas: autoresponse_delivered (sin cambios)
    
    Args:
        current_status: Estado actual del mensaje en BD
        whatsapp_status: Estado recibido de WhatsApp ('delivered', 'read', 'failed')
    
    Returns:
        Nuevo estado según los flujos definidos
    """
    
    # Mapeo de transiciones válidas según los flujos definidos
    transitions = {
        # Plantillas (templates)
        'template_sent': {
            'delivered': 'template_delivered',
            'failed': 'template_failed'
        },
        'template_delivered': {
            'read': 'template_read',
            'failed': 'template_failed'  # Puede fallar aún después de entregado
        },
        
        # Mensajes de texto
        'sent': {
            'delivered': 'message_delivered',
            'failed': 'message_failed'
        },
        'message_delivered': {
            'read': 'message_read',
            'failed': 'message_failed'  # Puede fallar aún después de entregado
        },
        
        # Archivos multimedia
        'media_sent': {
            'delivered': 'media_delivered',
            'failed': 'media_failed'
        },
        'media_delivered': {
            'read': 'media_read',
            'failed': 'media_failed'  # Puede fallar aún después de entregado
        },
        
        # Auto-respuestas (se mantienen sin cambios según requerimiento)
        'autoresponse_delivered': {
            # No hay transiciones definidas - se mantiene como está
        }
    }
    
    # Obtener transiciones válidas para el estado actual
    valid_transitions = transitions.get(current_status, {})
    
    # Retornar nuevo estado o mantener actual si no hay transición válida
    new_status = valid_transitions.get(whatsapp_status, current_status)
    
    # Log de la transición para debugging
    if new_status != current_status:
        logger.info(f"🔄 Estado actualizado: {current_status} → {new_status} (evento: {whatsapp_status})")
    else:
        logger.debug(f"⏸️ Estado mantenido: {current_status} (evento: {whatsapp_status} no válido o ya final)")
    
    return new_status


def update_message_status(db_manager, message_id: str, whatsapp_status: str) -> bool:
    """
    Actualiza el estado de un mensaje siguiendo los flujos definidos.
    
    Args:
        db_manager: Instancia del DatabaseManager
        message_id: ID del mensaje de WhatsApp (wamid)
        whatsapp_status: Estado recibido de WhatsApp
    
    Returns:
        True si se actualizó el estado, False si no se encontró o no se pudo actualizar
    """
    try:
        # 1. Obtener el estado actual del mensaje
        get_current_sql = """
            SELECT status, from_me, sender_phone, message 
            FROM public.external_messages 
            WHERE last_message_uid = %s 
            LIMIT 1
        """
        current_record = db_manager.execute_query(get_current_sql, [message_id], fetch_one=True)
        
        if not current_record:
            logger.warning(f"❌ Mensaje no encontrado para actualizar estado: {message_id}")
            return False
        
        current_status, from_me, sender_phone, message_content = current_record
        
        # 2. Solo actualizamos mensajes salientes (from_me = 'true')
        if from_me != 'true':
            logger.debug(f"⏭️ Ignorando update de estado para mensaje entrante: {message_id}")
            return False
        
        # 3. Determinar el nuevo estado usando la lógica de flujos
        new_status = get_next_message_status(current_status, whatsapp_status)
        
        # 4. Si no hay cambio de estado, no hacer nada
        if new_status == current_status:
            logger.debug(f"⏸️ Sin cambio de estado para {message_id}: {current_status}")
            return False
        
        # 5. Actualizar el estado en la base de datos
        update_sql = """
            UPDATE public.external_messages 
            SET status = %s, updated_at = NOW()
            WHERE last_message_uid = %s
        """
        affected_rows = db_manager.execute_query(update_sql, [new_status, message_id])
        
        if affected_rows > 0:
            logger.info(f"✅ Estado actualizado para {message_id}: {current_status} → {new_status} (teléfono: {sender_phone})")
            
            # Log adicional para casos importantes
            if new_status.endswith('_failed'):
                logger.warning(f"⚠️ Mensaje fallido: {message_id} ({sender_phone}) - {new_status}")
            elif new_status.endswith('_read'):
                logger.info(f"👁️ Mensaje leído: {message_id} ({sender_phone}) - {new_status}")
            
            return True
        else:
            logger.error(f"❌ No se pudo actualizar el estado para {message_id}")
            return False
            
    except Exception as e:
        logger.exception(f"💥 Error actualizando estado del mensaje {message_id}: {e}")
        return False


def is_valid_whatsapp_status(status: str) -> bool:
    """
    Valida si el estado recibido de WhatsApp es uno de los esperados.
    
    Args:
        status: Estado recibido de WhatsApp
        
    Returns:
        True si es un estado válido para procesar
    """
    valid_statuses = {'delivered', 'read', 'failed', 'sent'}
    return status in valid_statuses


def get_status_statistics(db_manager) -> dict:
    """
    Obtiene estadísticas de estados de mensajes para monitoreo.
    
    Args:
        db_manager: Instancia del DatabaseManager
        
    Returns:
        Dict con estadísticas de estados
    """
    try:
        stats_sql = """
            SELECT 
                status,
                COUNT(*) as count,
                COUNT(CASE WHEN from_me = 'true' THEN 1 END) as outgoing,
                COUNT(CASE WHEN from_me = 'false' THEN 1 END) as incoming
            FROM public.external_messages 
            WHERE created_at > NOW() - INTERVAL '24 hours'
            GROUP BY status
            ORDER BY count DESC
        """
        
        results = db_manager.execute_query(stats_sql, fetch_all=True)
        
        statistics = {
            'total_messages_24h': 0,
            'by_status': {},
            'summary': {
                'templates': 0,
                'text_messages': 0,
                'media_messages': 0,
                'auto_responses': 0,
                'failed_messages': 0,
                'read_messages': 0
            }
        }
        
        for row in results:
            status, count, outgoing, incoming = row
            statistics['total_messages_24h'] += count
            statistics['by_status'][status] = {
                'total': count,
                'outgoing': outgoing,
                'incoming': incoming
            }
            
            # Clasificar por tipo para resumen
            if status.startswith('template_'):
                statistics['summary']['templates'] += count
            elif status.startswith('message_'):
                statistics['summary']['text_messages'] += count
            elif status.startswith('media_'):
                statistics['summary']['media_messages'] += count
            elif status.startswith('autoresponse_'):
                statistics['summary']['auto_responses'] += count
            
            if status.endswith('_failed'):
                statistics['summary']['failed_messages'] += count
            elif status.endswith('_read'):
                statistics['summary']['read_messages'] += count
        
        return statistics
        
    except Exception as e:
        logger.exception(f"Error obteniendo estadísticas de estados: {e}")
        return {'error': str(e)}


# Función auxiliar para determinar el estado inicial según el tipo de mensaje
def get_initial_message_status(message_type: str, is_template: bool = False, is_auto_response: bool = False) -> str:
    """
    Determina el estado inicial de un mensaje según su tipo.
    
    Args:
        message_type: Tipo de mensaje ('text', 'image', 'video', 'audio', 'document', etc.)
        is_template: Si es un mensaje de template
        is_auto_response: Si es una auto-respuesta
        
    Returns:
        Estado inicial correspondiente
    """
    if is_auto_response:
        return 'autoresponse_delivered'
    elif is_template:
        return 'template_sent'
    elif message_type in ['image', 'video', 'audio', 'document', 'sticker', 'voice']:
        return 'media_sent'
    else:  # text y otros
        return 'sent'


# =========================================================================
# CÓDIGO PARA REEMPLAZAR EN EL WEBHOOK - SECCIÓN ESTADOS DE MENSAJE
# =========================================================================

def handle_message_statuses_webhook(value: dict, db_manager) -> None:
    """
    Maneja los updates de estado de mensajes en el webhook con los nuevos flujos.
    Esta función reemplaza la sección "ESTADOS DE MENSAJE" en el webhook.
    """
    if 'statuses' not in value:
        return
        
    for status in value.get('statuses', []):
        message_id = status.get('id')
        whatsapp_status = status.get('status')
        timestamp = status.get('timestamp')
        
        if not message_id or not whatsapp_status:
            logger.warning("⚠️ Status update incompleto - falta message_id o status")
            continue
            
        # Validar que el estado es uno que procesamos
        if not is_valid_whatsapp_status(whatsapp_status):
            logger.debug(f"⏭️ Estado ignorado: {whatsapp_status} para mensaje {message_id}")
            continue
        
        logger.info(f"📊 Procesando update de estado: {message_id} → {whatsapp_status}")
        
        # Intentar actualizar usando la nueva lógica de flujos
        success = update_message_status(db_manager, message_id, whatsapp_status)
        
        if success:
            # Log adicional para casos especiales
            if whatsapp_status == 'failed':
                logger.error(f"💥 Mensaje fallido detectado: {message_id}")
            elif whatsapp_status == 'read':
                logger.info(f"👁️ Mensaje leído confirmado: {message_id}")
            elif whatsapp_status == 'delivered':
                logger.info(f"📫 Mensaje entregado confirmado: {message_id}")
        else:
            logger.warning(f"⚠️ No se pudo actualizar estado para mensaje {message_id}")



import os



logger = configure_logging()
# Reducir verbosidad de hpack y httpcore
import logging
logging.getLogger('hpack').setLevel(logging.WARNING)
logging.getLogger('httpcore').setLevel(logging.WARNING)


# --- Inicialización dinámica por compañía ---
import configparser
from supabase import create_client

# 1. Leer config para obtener datos de Supabase
temp_config = configparser.ConfigParser()
temp_config.read('scripts.conf')
supabase_cfg = temp_config['SUPABASE'] if temp_config.has_section('SUPABASE') else {}
SUPABASE_URL = os.getenv('SUPABASE_URL') or supabase_cfg.get('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY') or supabase_cfg.get('SUPABASE_KEY')

supabase_client = None
if SUPABASE_URL and SUPABASE_KEY:
    try:
        # Try creating client without proxy first
        try:
            supabase_client = create_client(SUPABASE_URL, SUPABASE_KEY)
        except TypeError as e:
            if 'proxy' in str(e):
                # If error mentions proxy, try alternative initialization
                from supabase import Client, create_client
                supabase_client = Client(
                    supabase_url=SUPABASE_URL,
                    supabase_key=SUPABASE_KEY
                )
            else:
                raise
        logger.info(f"Supabase client created successfully with URL: {SUPABASE_URL[:30]}...")
    except Exception as e:
        logger.error(f"Error creating Supabase client: {e}")
        logger.warning("⚠️ Running without Supabase support")
else:
    logger.warning(f"Supabase client not created. URL: {SUPABASE_URL}, KEY: {SUPABASE_KEY[:20] if SUPABASE_KEY else None}...")

# Multi-tenant support - no need for command line arguments
logger.info("[Startup] Starting multi-tenant WhatsApp webhook service")

# Preload all company configs into memory at startup
if supabase_client:
    logger.info("[PRELOAD] Starting preload of all company configs into memory")
    # Use a temp config to get DB config for preload
    try:
        logger.info("[PRELOAD] Creating temporary Config object for DB config (company_id=None)")
        temp_config_obj = Config(company_id=None, supabase_client=supabase_client)
        logger.info("[PRELOAD] Temporary Config object created successfully")
        logger.debug(f"[PRELOAD] temp_config_obj.whatsapp_config: {getattr(temp_config_obj, 'whatsapp_config', None)}")
        db_manager_for_cache = DatabaseManager(temp_config_obj.db_config)
        logger.info("[PRELOAD] DatabaseManager for cache created")
        company_cache.preload_all_companies(db_manager_for_cache)
        logger.info("[PRELOAD] Finished preload_all_companies")
    except Exception as e:
        logger.error(f"[PRELOAD] Exception during temp Config preload: {e}", exc_info=True)
        raise

    # Pick a default company_id if not provided
    default_company_id = None
    logger.info(f"[PRELOAD] company_cache._cache keys: {list(company_cache._cache.keys())}")
    if company_cache._cache:
        default_company_id = next(iter(company_cache._cache.keys()))
        logger.info(f"[PRELOAD] Default company_id selected: {default_company_id}")
    else:
        logger.error("[PRELOAD] No company configs loaded in cache!")

    # Use the default company_id if not explicitly set
    try:
        logger.info(f"[PRELOAD] Creating main Config object with company_id={default_company_id}")
        config = Config(company_id=default_company_id, supabase_client=supabase_client)
        logger.info("[PRELOAD] Main Config object created successfully")
        logger.debug(f"[PRELOAD] config.whatsapp_config: {getattr(config, 'whatsapp_config', None)}")
    except Exception as e:
        logger.error(f"[PRELOAD] Exception during main Config creation: {e}", exc_info=True)
        raise
else:
    logger.warning("[PRELOAD] Supabase client not created, skipping company config preload.")
    try:
        config = Config(company_id=None, supabase_client=None)
        logger.info("[PRELOAD] Config object created without Supabase client")
    except Exception as e:
        logger.error(f"[PRELOAD] Exception during fallback Config creation: {e}", exc_info=True)
        raise

# Mostrar variables clave y su origen
def log_config_summary(config, company_name=None):
    logger.info("==============================")
    logger.info(f"[Startup] CONFIG SUMMARY for company: {company_name or 'N/A'}")
    db_vars = {}
    file_vars = {}
    # Variables de la base de datos
    if hasattr(config, 'company_config') and config.company_config:
        db_vars = config.company_config.get('custom_properties', {})
        if db_vars:
            logger.info("[Startup] Variables loaded from DB (custom_properties):")
            for k, v in db_vars.items():
                logger.info(f"   • {k} = {v}   [DB]")
    # Variables del fichero (solo las que no están en DB)
    logger.info("[Startup] Variables loaded from file (not overridden by DB):")
    for section in config.config.sections():
        for k, v in config.config[section].items():
            if not db_vars or k not in db_vars:
                logger.info(f"   • {k} = {v}   [file:{section}]")
    logger.info("==============================")

log_config_summary(config, None)

flow_exit_client = build_flow_exit_client(config, logger)

# Global vars for compatibility
ACCESS_TOKEN = config.whatsapp_config['access_token']
PHONE_NUMBER_ID = config.whatsapp_config['phone_number_id']
WHATSAPP_PHONE_NUMBER_ID = config.whatsapp_config['phone_number_id']
# Token de verificación del webhook (desde scripts.conf)
VERIFY_TOKEN = config.config['WHATSAPP']['VERIFY_TOKEN']
WHATSAPP_BASE_URL = config.whatsapp_config['base_url']
WHATSAPP_HEADERS = config.whatsapp_config['headers']
WABA_ID = config.whatsapp_config['business_id']

# Setup logging
handlers = [logging.StreamHandler()]
if config.log_config['file']:
    handlers.append(logging.FileHandler(config.log_config['file'], encoding='utf-8'))

logging.basicConfig(
    level=config.log_config['level'],
    format=config.log_config['format'],
    handlers=handlers
)
logger = logging.getLogger(__name__)

logger.info("🚀 Iniciando WhatsApp Webhook Service v2.0 con caché de configuraciones")
logger.info(f"📊 Configuración cargada:")
logger.info(f"   • Test mode: {config.use_test}")
logger.info(f"   • ACCESS_TOKEN: {ACCESS_TOKEN[:20]}..." if ACCESS_TOKEN else "   • ACCESS_TOKEN: None")
logger.info(f"   • PHONE_NUMBER_ID: {PHONE_NUMBER_ID}")
logger.info(f"   • VERIFY_TOKEN: {VERIFY_TOKEN}")
logger.info(f"   • WABA_ID: {WABA_ID}")
logger.info(f"   • BASE_URL: {WHATSAPP_BASE_URL}")
logger.info(f"   • HTTP Port: {config.server_config['http_port']}")
logger.info(f"   • HTTPS Port: {config.server_config['https_port']}")

db_manager = DatabaseManager(config.db_config)
lead_service = LeadService(db_manager)
message_service = MessageService(db_manager, lead_service)
whatsapp_service = WhatsAppService(config)
auto_reply_service = AutoReplyService(db_manager)

# --- Inicialización del FileService extendido ---
try:
    file_service  # noqa
except NameError:
    file_service = None

def get_file_service():
    global file_service, config, db_manager
    if file_service is not None:
        return file_service
    file_service = ExtendedFileService(config, db_manager)  # Usar ExtendedFileService
    return file_service
    
logger.debug("🔧 WhatsApp Service configurado:")
debug_info = whatsapp_service.get_debug_info()
for key, value in debug_info.items():
    logger.debug(f"   • {key}: {value}")

if not ACCESS_TOKEN:
    logger.error("❌ ACCESS_TOKEN no configurado!")
if not PHONE_NUMBER_ID:
    logger.error("❌ PHONE_NUMBER_ID no configurado!")
if not VERIFY_TOKEN:
    logger.error("❌ VERIFY_TOKEN no configurado!")
if not WABA_ID:
    logger.error("❌ WABA_ID no configurado!")
else:
    logger.info(f"✅ WABA_ID configurado correctamente: {WABA_ID}")

logger.info("✅ Todos los servicios inicializados correctamente con soporte extendido")

# Flask App
app = Flask(__name__)
CORS(app)

@app.before_request
def log_request_info():
    logger.debug(f"--> {request.method} {request.url}")
    logger.debug(f"Headers: {dict(request.headers)}")
    if request.content_length and request.content_length < 1000:
        logger.debug(f"Body: {request.get_data()!r}")

@app.errorhandler(400)
def handle_bad_request(e):
    raw = request.get_data()
    logger.error(f"400 Bad Request: {e}. Raw: {raw!r}")
    return jsonify({'status': 'error', 'message': 'Bad request'}), 400

# ================================================
# Endpoints con soporte extendido de MIME types
# ================================================

@app.route('/send_file', methods=['POST'])
def send_file_endpoint():
    import os, json, re, mimetypes
    from datetime import datetime
    from werkzeug.utils import secure_filename

    try:
        # Helper functions
        def _detect_content_type(file_storage, filename: str) -> str:
            ct = getattr(file_storage, "mimetype", None)
            if ct and ct != "application/octet-stream":
                return ct
            guess, _ = mimetypes.guess_type(filename or "")
            return guess or "application/octet-stream"

        def _normalize_msisdn(raw: str, default_cc: str = "34") -> str:
            if not raw:
                return raw
            p = re.sub(r"\D", "", raw)
            if p.startswith("00"):
                p = p[2:]
            if p.startswith(default_cc):
                return p
            return default_cc + p

        def _strip_cc_34(msisdn: str) -> str:
            return re.sub(r"\D", "", msisdn)[-9:] if msisdn else msisdn

        def _head_ct(url: str) -> str:
            try:
                r = requests.head(url, timeout=10)
                return (r.headers.get("Content-Type") or "").split(";")[0].strip().lower()
            except Exception:
                return ""

        def get_wa_credentials():
            token = config.whatsapp_config["access_token"]
            pnid = config.whatsapp_config["phone_number_id"]
            if not token or not pnid:
                raise RuntimeError("Credenciales WhatsApp no encontradas.")
            return token, pnid

        def get_fs():
            return get_file_service()

        def get_dbm():
            return db_manager

        def get_supabase_client(fs):
            return fs.supabase

        def get_bucket_name(fs):
            return fs.storage_bucket

        def db_exec(dbm, sql, params=None, fetch_one=False, fetch_all=False):
            return dbm.execute_query(sql, params or (), fetch_one=fetch_one, fetch_all=fetch_all)

        def _one(obj):
            if obj is None:
                return None
            if isinstance(obj, list):
                return obj[0] if obj else None
            return obj

        def _first_scalar(val):
            if val is None:
                return None
            if isinstance(val, dict):
                if "id" in val and val["id"]:
                    return val["id"]
                try:
                    return next(iter(val.values()))
                except Exception:
                    return None
            if isinstance(val, (list, tuple)):
                if not val:
                    return None
                return _first_scalar(val[0])
            return val

        # Entrada
        data = request.form if request.form else request.json
        if not data:
            return jsonify({"status": "error", "message": "Cuerpo vacío"}), 400

        any_id = data.get('lead_id') or data.get('id') or data.get('actor_id')
        file = request.files.get('file') if request.files else None
        if not any_id or not file:
            return jsonify({"status": "error", "message": "Missing fields: id/lead_id and file"}), 400

        safe_name = secure_filename(file.filename or "upload.bin")
        file_bytes = file.read()
        content_type = _detect_content_type(file, safe_name)

        logger.info(f"[send_file_extended] Recibido id={any_id} filename={safe_name} ct={content_type}")

        # Servicios
        fs = get_fs()
        dbm = get_dbm()
        supa_client = get_supabase_client(fs)
        bucket = get_bucket_name(fs)

        # Resolver teléfono del actor
        actor_phone = None
        actor_is_lead = False

        # 1) ¿Es un lead?
        row = _one(db_exec(
            dbm,
            "SELECT phone FROM public.leads WHERE id = %s LIMIT 1",
            (any_id,),
            fetch_one=True
        ))
        if row is not None:
            actor_is_lead = True
            actor_phone = _first_scalar(row)

        # 2) Si no es lead, probamos en profiles
        if not actor_phone:
            row = _one(db_exec(
                dbm,
                "SELECT phone FROM public.profiles WHERE id = %s LIMIT 1",
                (any_id,),
                fetch_one=True
            ))
            if row is not None:
                actor_phone = _first_scalar(row)

        if not actor_phone:
            return jsonify({"status": "error", "message": f"Phone vacío para id {any_id}"}), 400

        # Validación extendida
        validation = fs.validate_file_extended(file_bytes, safe_name, content_type)
        
        # Subir a Supabase
        upload_result = fs.upload_to_supabase(file_bytes, safe_name, content_type)
        
        # Guardar metadata extendida
        upload_result['original_filename'] = safe_name
        document_id = fs.save_file_metadata_extended(
            upload_result, validation, 
            "leads" if actor_is_lead else "profiles", 
            any_id
        )

        logger.info(f"[send_file_extended] Subido a Supabase: {upload_result['file_path']} -> {upload_result['public_url']}")

        # Envío a WhatsApp con soporte extendido

        wa_token, wa_pnid = get_wa_credentials()

        # Detectar tipo para WhatsApp
        wa_type = validation['whatsapp_type']   # 'image' | 'video' | 'document' | 'audio'
        caption = None
        if wa_type in ("image", "video", "document"):
            # Solo estos soportan caption
            caption = data.get("caption", f"Documento: {safe_name}")

        success, wamid = fs.send_media_to_whatsapp_extended(
            actor_phone,
            upload_result['file_path'],
            wa_type,
            safe_name,
            caption  # será None si es audio
        )

        if not success:
            return jsonify({"status": "error", "message": "Failed to send to WhatsApp"}), 500


        # Guardar en external_messages
        msisdn_db = _strip_cc_34(actor_phone)
        msisdn = _normalize_msisdn(actor_phone, "34")
        
        # Datos de responsable/assigned_to
        assigned_id = None
        assigned_email = None
        if actor_is_lead:
            arow = _one(db_exec(dbm,
                                "SELECT d.user_assigned_id FROM public.deals d WHERE d.lead_id = %s ORDER BY d.created_at DESC NULLS LAST LIMIT 1",
                                (any_id,), fetch_one=True))
            assigned_id = str(_first_scalar(arow)) if arow else None
            if assigned_id:
                prow = _one(db_exec(dbm, "SELECT email FROM public.profiles WHERE id = %s LIMIT 1", (assigned_id,), fetch_one=True))
                assigned_email = _first_scalar(prow)

        chat_id = msisdn_db
        chat_url = f"https://wa.me/{msisdn_db}"

        message_json = {
            "type": validation['whatsapp_type'],
            "url": upload_result['public_url'],
            "caption": caption,
            "filename": safe_name,
            "mime_type": content_type,
            "detected_type": validation['media_type'],
            "extended_support": True
        }

        db_exec(
            dbm,
            """
            INSERT INTO public.external_messages
            ( id, message, sender_phone, responsible_email, last_message_uid, last_message_timestamp,
              from_me, status, created_at, updated_at, is_deleted, chat_id, chat_url, assigned_to_id )
            VALUES
            ( gen_random_uuid(), %s, %s, %s, %s, NOW(),
              TRUE, %s, NOW(), NOW(), FALSE, %s, %s, %s )
            """,
            [
                json.dumps(message_json, ensure_ascii=False),
                msisdn_db,
                assigned_email,
                wamid,
                'media_sent',
                chat_id,
                chat_url,
                assigned_id
            ],
            fetch_one=False
        )

        logger.info(f"[send_file_extended] Enviado a {msisdn} (wamid={wamid}) y registrado en external_messages")

        return jsonify({
            "status": "success",
            "id": any_id,
            "document_id": document_id,
            "file_url": upload_result['public_url'],
            "content_type": content_type,
            "detected_media_type": validation['media_type'],
            "whatsapp_type": validation['whatsapp_type'],
            "file_size": validation['file_size'],
            "whatsapp_message_id": wamid,
            "extended_support": True
        }), 200

    except Exception as e:
        logger.exception("Error en send_file_endpoint_extended")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/send_media_extended', methods=['POST'])
@rate_limit(max_calls=10, window=60)
def send_media_message_extended():
    """Send media file to WhatsApp with extended type support"""
    try:
        data = request.get_json(force=True)
        required_fields = ['customer_phone', 'document_id']
        missing = [f for f in required_fields if f not in data]
        if missing:
            return jsonify({'status': 'error', 'message': f'Missing fields: {missing}'}), 400

        customer_phone = data['customer_phone']
        document_id = data['document_id']
        caption = data.get('caption', '')

        if not PhoneUtils.validate_spanish_phone(customer_phone):
            return jsonify({'status': 'error', 'message': 'Invalid phone number'}), 400

        # Obtener información del documento
        doc_query = """
            SELECT d.path, d.name, dt.name as doc_type
            FROM public.documents d
            JOIN public.document_types dt ON d.document_type_id = dt.id
            WHERE d.id = %s AND d.is_deleted = false
        """
        doc_result = db_manager.execute_query(doc_query, [document_id], fetch_one=True)
        
        if not doc_result:
            return jsonify({'status': 'error', 'message': 'Document not found'}), 404

        file_path, filename, doc_type = doc_result
        
        # Usar ExtendedFileService para envío
        fs = get_file_service()
        success, message_id = fs.send_media_to_whatsapp_extended(
            customer_phone, file_path, doc_type, filename, caption
        )

        if success:
            # Guardar mensaje enviado
            media_description = f"Media enviado (extended support)"
            file_info = {'filename': filename, 'document_id': document_id}
            
            message_service.save_outgoing_media_message(
                customer_phone, media_description, message_id, 
                file_info=file_info
            )
            
            destination = PhoneUtils.add_34(customer_phone)
            log_sent_message(f'+{destination}', f"📎 {media_description}: {filename}")
            
            return jsonify({
                'status': 'success', 
                'message_id': message_id,
                'sent_to': f'+{destination}',
                'filename': filename,
                'extended_support': True
            }), 200
        else:
            return jsonify({'status': 'error', 'message': 'Failed to send media'}), 500

    except Exception as e:
        logger.exception('Error in send_media_message_extended')
        return jsonify({'status': 'error', 'message': 'Internal error'}), 500

@app.route('/upload_file_extended', methods=['POST'])
@rate_limit(max_calls=5, window=60)
def upload_file_endpoint_extended():
    """Upload file with extended MIME type support"""
    try:
        if 'file' not in request.files:
            return jsonify({'status': 'error', 'message': 'No file provided'}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({'status': 'error', 'message': 'No file selected'}), 400

        object_reference_type = request.form.get('object_reference_type', 'external_messages')
        object_reference_id = request.form.get('object_reference_id')
        
        if not object_reference_id:
            return jsonify({'status': 'error', 'message': 'object_reference_id required'}), 400

        # Leer contenido del archivo
        file_content = file.read()
        filename = file.filename
        content_type = file.content_type or mimetypes.guess_type(filename)[0] or 'application/octet-stream'

        # Usar ExtendedFileService
        fs = get_file_service()
        
        # Validar archivo con soporte extendido
        validation = fs.validate_file_extended(file_content, filename, content_type)
        
        # Subir a Supabase
        upload_result = fs.upload_to_supabase(file_content, filename, content_type)
        
        # Guardar metadata extendida
        upload_result['original_filename'] = filename
        document_id = fs.save_file_metadata_extended(
            upload_result, validation, object_reference_type, object_reference_id
        )

        return jsonify({
            'status': 'success',
            'document_id': document_id,
            'filename': filename,
            'content_type': content_type,
            'detected_media_type': validation['media_type'],
            'whatsapp_type': validation['whatsapp_type'],
            'file_size': validation['file_size'],
            'public_url': upload_result['public_url'],
            'extended_support': True
        }), 200

    except ValueError as e:
        return jsonify({'status': 'error', 'message': str(e)}), 400
    except Exception as e:
        logger.exception('Error in upload_file_endpoint_extended')
        return jsonify({'status': 'error', 'message': 'Internal error'}), 500

@app.route('/supported_types', methods=['GET'])
def get_supported_types():
    """Endpoint para obtener información sobre tipos MIME soportados"""
    try:
        fs = get_file_service()
        support_info = fs.get_supported_types_info()
        
        return jsonify({
            'status': 'success',
            'message': 'Extended MIME type support active',
            'support_info': support_info,
            'capabilities': {
                'images': 'JPEG, PNG (5MB max)',
                'audio': 'AAC, MP4, AMR, MPEG, OGG, Opus, WAV (16MB max)',
                'video': 'MP4, 3GPP, QuickTime, AVI, MKV (16MB max)',
                'documents': 'ANY valid MIME type (100MB max)',
                'stickers': 'WEBP only (500KB max)',
                'voice': 'OGG with Opus codec (16MB max)'
            }
        }), 200
        
    except Exception as e:
        logger.exception('Error getting supported types info')
        return jsonify({
            'status': 'error', 
            'message': 'Could not retrieve type information'
        }), 500

# ================================================
# Endpoints originales (compatibilidad)
# ================================================

@app.route('/send_media', methods=['POST'])
@rate_limit(max_calls=10, window=60)
def send_media_message():
    """Send media file to WhatsApp (legacy compatibility)"""
    return send_media_message_extended()

@app.route('/upload_file', methods=['POST'])
@rate_limit(max_calls=5, window=60)
def upload_file_endpoint():
    """Upload file directly to storage (legacy compatibility)"""
    return upload_file_endpoint_extended()

@app.route('/files/<customer_phone>', methods=['GET'])
def get_customer_files(customer_phone):
    """Get all files for a customer"""
    try:
        clean_phone = PhoneUtils.strip_34(customer_phone)
        
        # Obtener lead
        lead = lead_service.get_lead_data_by_phone(clean_phone)
        if not lead:
            return jsonify({'status': 'error', 'message': 'Lead not found'}), 404

        # Obtener archivos del lead
        files_query = """
            SELECT d.id, d.name, d.path, d.uploaded_at, d.status,
                   dt.name as document_type, dt.description
            FROM public.documents d
            JOIN public.document_types dt ON d.document_type_id = dt.id
            WHERE d.object_reference_type = 'leads' 
              AND d.object_reference_id = %s
              AND d.is_deleted = false
            ORDER BY d.uploaded_at DESC
        """
        
        files = db_manager.execute_query(files_query, [lead['lead_id']], fetch_all=True)
        
        result = []
        for file_row in files:
            # Generar URL pública temporal si es necesario
            public_url = None
            try:
                fs = get_file_service()
                public_url = fs.supabase.storage.from_(
                    fs.storage_bucket
                ).get_public_url(file_row[2])
            except Exception:
                logger.warning(f"Could not generate public URL for {file_row[2]}")
            
            result.append({
                'document_id': str(file_row[0]),
                'filename': file_row[1],
                'file_path': file_row[2],
                'uploaded_at': file_row[3].isoformat() if file_row[3] else None,
                'status': file_row[4],
                'document_type': file_row[5],
                'description': file_row[6],
                'public_url': public_url
            })

        return jsonify({
            'status': 'success',
            'customer_phone': customer_phone,
            'lead_id': lead['lead_id'],
            'files_count': len(result),
            'files': result,
            'extended_support_enabled': True
        }), 200

    except Exception:
        logger.exception('Error in get_customer_files')
        return jsonify({'status': 'error', 'message': 'Internal error'}), 500

@app.route('/file/<document_id>/download', methods=['GET'])
def download_file(document_id):
    """Generate download URL for a file"""
    try:
        # Obtener información del documento
        doc_query = """
            SELECT d.path, d.name
            FROM public.documents d
            WHERE d.id = %s AND d.is_deleted = false
        """
        doc_result = db_manager.execute_query(doc_query, [document_id], fetch_one=True)
        
        if not doc_result:
            return jsonify({'status': 'error', 'message': 'Document not found'}), 404

        file_path, filename = doc_result
        
        # Generar URL de descarga temporal (válida por 1 hora)
        try:
            fs = get_file_service()
            download_url = fs.supabase.storage.from_(
                fs.storage_bucket
            ).create_signed_url(file_path, 3600)  # 1 hora
            
            return jsonify({
                'status': 'success',
                'document_id': document_id,
                'filename': filename,
                'download_url': download_url,
                'expires_in': 3600
            }), 200
            
        except Exception as e:
            logger.error(f"Error generating download URL: {e}")
            return jsonify({'status': 'error', 'message': 'Could not generate download URL'}), 500

    except Exception:
        logger.exception('Error in download_file')
        return jsonify({'status': 'error', 'message': 'Internal error'}), 500

@app.route('/config_debug', methods=['GET'])
def config_debug():
    return jsonify({
        'status': 'ok',
        'config': {
            'test_mode': config.use_test,
            'access_token_preview': f"{ACCESS_TOKEN[:20]}..." if ACCESS_TOKEN else "None",
            'phone_number_id': PHONE_NUMBER_ID,
            'verify_token': VERIFY_TOKEN,
            'waba_id': WABA_ID,
            'base_url': WHATSAPP_BASE_URL,
            'http_port': config.server_config['http_port'],
            'https_port': config.server_config['https_port'],
            'extended_mime_support': True
        },
        'whatsapp_service': whatsapp_service.get_debug_info(),
        'services_initialized': True
    }), 200

@app.route('/send_message', methods=['POST'])
@rate_limit(max_calls=30, window=60)
def send_direct_message():
    try:
        data = request.get_json(force=True)
        required_fields = ['customer_phone', 'message_text']
        missing = [f for f in required_fields if f not in data]
        if missing:
            return jsonify({'status': 'error', 'message': f'Missing fields: {missing}'}), 400

        customer_phone = data['customer_phone']
        message_text = data['message_text']
        assigned_to_id = data.get('assigned_to_id')
        responsible_email = data.get('responsible_email')

        if not PhoneUtils.validate_spanish_phone(customer_phone):
            return jsonify({'status': 'error', 'message': 'Invalid phone number'}), 400

        if assigned_to_id:
            lead_service.update_deal_assignee(customer_phone, assigned_to_id)

        destination = PhoneUtils.add_34(customer_phone)
        success, message_id = whatsapp_service.send_text_message(destination, message_text)

        if success:
            message_service.save_outgoing_message(customer_phone, message_text, message_id, responsible_email, assigned_to_id)
            log_sent_message(f'+{destination}', message_text)
            return jsonify({'status': 'success', 'message_id': message_id, 'sent_to': f'+{destination}'}), 200
        else:
            return jsonify({'status': 'error', 'message': 'Failed to send message'}), 500

    except BadRequest:
        return handle_bad_request(BadRequest())
    except Exception:
        logger.exception('Error in send_direct_message')
        return jsonify({'status': 'error', 'message': 'Internal error'}), 500

@app.route('/send_template', methods=['POST'])
@rate_limit(max_calls=20, window=60)
def send_template_endpoint():
    try:
        data = request.get_json(force=True)
        required_fields = ['customer_phone', 'template_name']
        missing = [f for f in required_fields if f not in data]
        if missing:
            return jsonify({'status': 'error', 'message': f'Missing fields: {missing}'}), 400

        customer_phone = data['customer_phone']
        template_name = data['template_name']

        if not PhoneUtils.validate_spanish_phone(customer_phone):
            return jsonify({'status': 'error', 'message': 'Invalid phone number'}), 400

        lead_data = lead_service.get_lead_data_by_phone(customer_phone)
        if not lead_data:
            return jsonify({'status': 'error', 'message': 'Lead not found'}), 404

        destination = PhoneUtils.add_34(customer_phone)
        success, message_id, payload = whatsapp_service.send_template_message(destination, template_name, lead_data)

        if success:
            message_service.save_template_message(payload, message_id)
            return jsonify({'status': 'success', 'message_id': message_id, 'sent_to': f'+{destination}'}), 200
        else:
            return jsonify({'status': 'error', 'message': 'Failed to send template'}), 500

    except Exception:
        logger.exception('Error in send_template_endpoint')
        return jsonify({'status': 'error', 'message': 'Internal error'}), 500

@app.route('/WBhook', methods=['POST'])
@rate_limit(max_calls=50, window=60)
def handle_template():
    try:
        data = request.get_json(force=True)
        template_data = data.get('body', data)

        required = ['categoria', 'email', 'lead_id', 'nombre', 'propietario', 'telefono']
        missing = [f for f in required if f not in template_data]
        if missing:
            return jsonify({'error': f'Missing fields: {missing}'}), 400

        if not template_data.get('assigned_to_id'):
            aid, email = lead_service.get_lead_assigned_info(PhoneUtils.strip_34(template_data['telefono']))
            template_data['assigned_to_id'] = aid
            if not template_data.get('email'):
                template_data['email'] = email or template_data['email']

        destination = PhoneUtils.add_34(template_data['telefono'])
        clean_phone = PhoneUtils.strip_34(template_data['telefono'])
        lead_extra = lead_service.get_lead_data_by_phone(clean_phone)
        if lead_extra:
            template_data.setdefault('responsible_first_name', lead_extra.get('responsible_first_name'))
            template_data.setdefault('responsible_name', lead_extra.get('responsible_name'))
            template_data.setdefault('deal_id', lead_extra.get('deal_id'))
        success, message_id, payload = whatsapp_service.send_template_message(
            destination, 'agendar_llamada_inicial', template_data
        )

        if success:
            message_service.save_template_message(payload, message_id)
            return jsonify({'status': 'success', 'message_id': message_id}), 200
        else:
            return jsonify({'status': 'error', 'message': 'Failed to send template'}), 500

    except Exception:
        logger.exception('Error in handle_template')
        return jsonify({'status': 'error', 'message': 'Internal error'}), 500



def get_cover_wb_for_phone(phone: str) -> str:
    """
    Devuelve la URL de COVER_WB según el teléfono/company_id.
    Si no encuentra configuración específica, usa la URL por defecto.
    """
    # URL por defecto (fallback)
    default_cover = "https://app.solvify.es/cover-whats.jpg"
    
    try:
        if not phone:
            return default_cover

        clean_phone = PhoneUtils.strip_34(phone)
        # Usar el caché existente para obtener la configuración de la compañía
        custom_props, company_name, company_id = company_cache.get_config_by_phone(clean_phone, db_manager)

        if not custom_props:
            logger.debug(f"[COVER_WB] No company config for phone {phone}, using default cover")
            return default_cover

        # Buscar COVER_WB en las custom properties
        cover_wb = custom_props.get('COVER_WB')
        
        if cover_wb:
            logger.info(f"[COVER_WB] Using company cover for company_id={company_id}, phone={phone}: {cover_wb}")
            return cover_wb

        logger.debug(f"[COVER_WB] No COVER_WB config for company {company_id}, using default")
        return default_cover

    except Exception:
        logger.exception(f"[COVER_WB] Error resolving cover for phone {phone}")
        return default_cover
  
    """
    Devuelve las credenciales a usar para llamadas a la API de WhatsApp según el teléfono.
    - Intenta resolver company config desde la caché (company_cache.get_config_by_phone)
    - Si encuentra WHATSAPP_ACCESS_TOKEN y WHATSAPP_PHONE_NUMBER_ID en custom_properties
      devuelve headers y base_url concretos para esa compañía.
    - Si no, devuelve las credenciales globales (ACCESS_TOKEN, PHONE_NUMBER_ID, WABA_ID).
    Retorna dict con keys: access_token, phone_number_id, business_id, headers, base_url, company_name, company_id
    """
    # Valores por defecto (globales)
    default = {
        'access_token': ACCESS_TOKEN,
        'phone_number_id': PHONE_NUMBER_ID,
        'business_id': WABA_ID,
        'headers': WHATSAPP_HEADERS,
        'base_url': WHATSAPP_BASE_URL,
        'company_name': None,
        'company_id': None
    }

    try:
        if not phone:
            return default

        clean_phone = PhoneUtils.strip_34(phone)
        # company_cache.get_config_by_phone devuelve (custom_props_dict, company_name, company_id)
        custom_props, company_name, company_id = company_cache.get_config_by_phone(clean_phone, db_manager)

        if not custom_props:
            logger.debug(f"[get_whatsapp_credentials_for_phone] No company config for phone {phone}, using default credentials")
            return default

        # custom_props viene de get_company_data().get('custom_properties', {})
        token = custom_props.get('WHATSAPP_ACCESS_TOKEN')
        pnid = custom_props.get('WHATSAPP_PHONE_NUMBER_ID')
        bid = custom_props.get('WHATSAPP_BUSINESS_ID') or WABA_ID

        if token and pnid:
            headers = {
                'Authorization': f"Bearer {token}",
                'Content-Type': 'application/json'
            }
            base_url = f"https://graph.facebook.com/v22.0/{pnid}/messages"
            logger.info(f"[get_whatsapp_credentials_for_phone] Using company credentials for company_id={company_id}, phone={phone}")
            return {
                'access_token': token,
                'phone_number_id': pnid,
                'business_id': bid,
                'headers': headers,
                'base_url': base_url,
                'company_name': company_name,
                'company_id': company_id
            }

        logger.debug(f"[get_whatsapp_credentials_for_phone] Company config incomplete for phone {phone}, using default")
        return default

    except Exception:
        logger.exception(f"[get_whatsapp_credentials_for_phone] Error resolving credentials for phone {phone}")
        return default
# Helper function to get company credentials
def get_whatsapp_credentials_for_company(company_id: str) -> dict:
    """
    Lee credenciales de properties/object_property_values para el tenant.
    Devuelve dict con headers, base_url y metadatos. Hace fallback a DEFAULT_* si falta algo.
    """
    try:
        sql = """
            SELECT p.property_name, opv.value
            FROM object_property_values opv
            JOIN properties p ON p.id = opv.property_id
            WHERE opv.object_reference_type = 'companies'
              AND opv.object_reference_id = %s
              AND p.property_name IN ('WHATSAPP_ACCESS_TOKEN','WHATSAPP_PHONE_NUMBER_ID','WHATSAPP_BUSINESS_ID','COMPANY_NAME')
        """
        rows = db_manager.execute_query(sql, [company_id], fetch_all=True) or []
        kv = {name: val for (name, val) in rows if name}  # <-- convierte lista de tuplas a dict

        access_token   = kv.get('WHATSAPP_ACCESS_TOKEN')   or ACCESS_TOKEN
        phone_number_id= kv.get('WHATSAPP_PHONE_NUMBER_ID')or PHONE_NUMBER_ID
        business_id    = kv.get('WHATSAPP_BUSINESS_ID')    or WABA_ID
        company_name   = kv.get('COMPANY_NAME')            or "Default"

        base_url = f"https://graph.facebook.com/{GRAPH_API_VERSION}/{phone_number_id}/messages"
        headers  = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}

        return {
            "waba_id": business_id,
            "access_token": access_token,
            "phone_number_id": phone_number_id,
            "base_url": base_url,
            "headers": headers,
            "company_id": company_id,
            "company_name": company_name,
        }
    except Exception:
        logging.exception("Error getting company credentials")
        # Fallback absoluto a defaults
        base_url = f"https://graph.facebook.com/{GRAPH_API_VERSION}/{PHONE_NUMBER_ID}/messages"
        return {
            "waba_id": WABA_ID,
            "access_token": ACCESS_TOKEN,
            "phone_number_id": PHONE_NUMBER_ID,
            "base_url": base_url,
            "headers": {"Authorization": f"Bearer {ACCESS_TOKEN}", "Content-Type": "application/json"},
            "company_id": "Default",
            "company_name": "Default",
        }


@app.route('/get_templates', methods=['GET'])
def get_templates():
    try:
        logger.info("🔍 Obteniendo lista de templates de WhatsApp Business API")
        logger.info(f"📊 Usando DEFAULT WABA_ID configurado: {WABA_ID}")
        logger.info(f"📊 Usando DEFAULT ACCESS_TOKEN preview: {ACCESS_TOKEN[:20]}...")
        logger.info(f"📊 Usando DEFAULT PHONE_NUMBER_ID: {PHONE_NUMBER_ID}")

        # Aceptar optional query params ?id=<uuid>&phone=<phone>
        lead_info = None
        query_phone = request.args.get('phone')
        query_id = request.args.get('company_id') or request.args.get('id')

        logger.info(f"📊 Usando PHONE NUMBER: {query_phone}...")
        logger.info(f"📊 Usando COMPANY_ID: {query_id}")

        # Resolver teléfono desde id si hace falta
        resolved_phone = None
        try:
            if query_phone:
                resolved_phone = PhoneUtils.strip_34(query_phone)
            elif query_id and _is_valid_uuid(query_id):
                phone_from_id = _get_lead_phone(db_manager, query_id)
                if phone_from_id:
                    resolved_phone = PhoneUtils.strip_34(phone_from_id)
        except Exception:
            logger.exception("Error resolviendo teléfono desde query params")

        # Obtener lead_info si hay teléfono
        try:
            if resolved_phone:
                lead_info = lead_service.get_lead_data_by_phone(resolved_phone)
        except Exception:
            logger.exception("Error obteniendo lead_info desde lead_service")

        # RESOLVER CREDENCIALES por teléfono (usa cache/company config) -> helper
        creds = get_whatsapp_credentials_for_phone(resolved_phone)


        used_company_name = creds.get('company_name')
        used_company_id = creds.get('company_id')
        headers = creds.get('headers') or WHATSAPP_HEADERS
        waba_id = creds.get('business_id') or WABA_ID

        logger.info(f"📌 Usando WABA_ID: {waba_id} | phone_number_id: {creds.get('phone_number_id')} | company: {used_company_name}")

        # Llamada a Facebook para obtener templates (usando credenciales resueltas)
        templates_resp = requests.get(
            f'https://graph.facebook.com/v22.0/{waba_id}/message_templates',
            headers=headers,
            params={'fields': 'name,status,category,language,components,id,rejected_reason', 'limit': 100},
            timeout=15
        )

        logger.info(f"📊 Templates response status: {templates_resp.status_code}")

        if templates_resp.status_code != 200:
            error_detail = templates_resp.text
            logger.error(f"❌ Error obteniendo templates: {error_detail}")
            return jsonify({
                'status': 'error',
                'message': f'Error obteniendo templates: {templates_resp.status_code}',
                'details': error_detail,
                'waba_id_used': waba_id,
                'phone_used': resolved_phone,
                'company': used_company_name,
                'company_id': used_company_id
            }), templates_resp.status_code

        data_js = templates_resp.json().get('data', [])
        logger.info(f"✅ Se encontraron {len(data_js)} templates")

        processed = []
        for t in data_js:
            processed.append({
                'id': t.get('id'),
                'name': t.get('name'),
                'status': t.get('status'),
                'category': t.get('category'),
                'language': t.get('language'),
                'components': t.get('components', []),
                'rejected_reason': t.get('rejected_reason')
            })

        stats = {
            'approved': sum(1 for t in processed if t['status'] == 'APPROVED'),
            'pending': sum(1 for t in processed if t['status'] == 'PENDING'),
            'rejected': sum(1 for t in processed if t['status'] == 'REJECTED'),
            'total': len(processed)
        }

        logger.info(f"📊 Estadísticas de templates: {stats}")

        response_payload = {
            'status': 'success',
            'message': f'Templates obtenidos exitosamente usando WABA_ID: {waba_id}',
            'waba_id': waba_id,
            'used_phone': resolved_phone,
            'used_company_name': used_company_name,
            'used_company_id': used_company_id,
            'total_templates': len(processed),
            'statistics': stats,
            'templates': processed,
            'extended_mime_support': True
        }

        # Si resolvimos un lead, incluimos su info para pre‑llenado en cliente
        if lead_info:
            response_payload['lead'] = lead_info
            response_payload['prefill_suggestions'] = {
                'first_name': lead_info.get('first_name'),
                'deal_id': lead_info.get('deal_id'),
                'responsible_first_name': lead_info.get('responsible_first_name'),
                'company_name': lead_info.get('company_name')
            }

        return jsonify(response_payload), 200

    except Exception as e:
        logger.error(f"💥 Error interno en get_templates: {e}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': f'Error interno: {str(e)}',
            'waba_id_configured': WABA_ID if WABA_ID else 'Not configured'
        }), 500
# ...existing code...

def log_message_with_company_info(phone: str, message_type: str, direction: str, content: str = "", template_name: str = None, message_id: str = None):
    """
    Registra un mensaje con información clara de cliente y compañía
    
    Args:
        phone: Número de teléfono
        message_type: Tipo de mensaje (text, template, media, etc.)
        direction: INCOMING o OUTGOING
        content: Contenido del mensaje (opcional)
        template_name: Nombre del template (si aplica)
        message_id: ID del mensaje de WhatsApp
    """
    try:
        clean_phone = PhoneUtils.strip_34(phone)
        
        # Obtener información del lead/cliente y compañía
        lead_data = lead_service.get_lead_data_by_phone(clean_phone)
        
        if lead_data:
            client_name = f"{lead_data.get('first_name', '')} {lead_data.get('last_name', '')}".strip()
            company_name = lead_data.get('company_name', 'Sin compañía')
            company_id = lead_data.get('company_id', 'N/A')
            deal_id = lead_data.get('deal_id', 'N/A')
            responsible = lead_data.get('responsible_name', 'Sin asignar')
        else:
            client_name = "Cliente no registrado"
            company_name = "Sin compañía"
            company_id = "N/A"
            deal_id = "N/A"
            responsible = "Sin asignar"
        
        # Construir mensaje de log
        log_parts = [
            f"📱 {direction}",
            f"👤 Cliente: {client_name} ({clean_phone})",
            f"🏢 Compañía: {company_name} (ID: {company_id})",
            f"📋 Deal: {deal_id}",
            f"👨‍💼 Responsable: {responsible}",
            f"📨 Tipo: {message_type}"
        ]
        
        if template_name:
            log_parts.append(f"📄 Template: {template_name}")
        
        if message_id:
            log_parts.append(f"🆔 Message ID: {message_id}")
        
        if content:
            content_preview = content[:100] + "..." if len(content) > 100 else content
            log_parts.append(f"💬 Contenido: {content_preview}")
        
        # Log principal muy visible
        logger.info("=" * 80)
        for part in log_parts:
            logger.info(f"  {part}")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error(f"Error en log_message_with_company_info: {e}")
        logger.info(f"📱 {direction} - Teléfono: {phone} - Tipo: {message_type} (Error obteniendo detalles)")

def log_request_with_company_info(endpoint: str, method: str, data: dict = None):
    """
    Registra una petición HTTP con información de cliente/compañía si está disponible
    """
    try:
        # Extraer teléfono de diferentes campos posibles
        phone = None
        company_id = None
        
        if data:
            phone = (data.get('customer_phone') or 
                    data.get('phone') or 
                    data.get('to_phone') or
                    data.get('telefono'))
            company_id = data.get('company_id')
        
        log_parts = [
            f"🌐 HTTP {method} {endpoint}",
        ]
        
        if phone:
            clean_phone = PhoneUtils.strip_34(phone)
            lead_data = lead_service.get_lead_data_by_phone(clean_phone)
            
            if lead_data:
                client_name = f"{lead_data.get('first_name', '')} {lead_data.get('last_name', '')}".strip()
                company_name = lead_data.get('company_name', 'Sin compañía')
                log_parts.extend([
                    f"👤 Cliente: {client_name} ({clean_phone})",
                    f"🏢 Compañía: {company_name}"
                ])
            else:
                log_parts.append(f"📱 Teléfono: {clean_phone} (no registrado)")
        
        elif company_id:
            # Si solo tenemos company_id, obtener info de la caché
            cached_config = company_cache.get(company_id)
            if cached_config:
                company_name = cached_config.get('name', 'Desconocida')
                log_parts.append(f"🏢 Compañía: {company_name} (ID: {company_id})")
            else:
                log_parts.append(f"🏢 Company ID: {company_id}")
        
        logger.info("🔔 " + " | ".join(log_parts))
        
    except Exception as e:
        logger.error(f"Error en log_request_with_company_info: {e}")
        logger.info(f"🌐 HTTP {method} {endpoint}")

# ...existing code...


from flask import request, jsonify, abort
from uuid import uuid4
import json
from datetime import timedelta


from uuid import uuid4
from datetime import timedelta
import json, re

# Acepta UUID con guiones (validación rápida; Flask también tiene converter uuid, pero así no rompes nada)
UUID_RE = re.compile(r"^[0-9a-fA-F-]{36}$")

@app.route('/<company_id>/webhook', methods=['GET', 'POST'], strict_slashes=False)
def webhook_company(company_id):
    # --- Validación de ruta ---
    if not UUID_RE.match(company_id):
        abort(404)
    print(f"Webhook called for company_id={company_id} with method={request.method}")
    # --- Verificación Webhook (GET) ---
    if request.method == 'GET':
        token = request.args.get('hub.verify_token')
        challenge = request.args.get('hub.challenge')
        mode = request.args.get('hub.mode')
        print(f"Webhook verify: company_id={company_id}, mode={mode}, token={token}, challenge={challenge}")
        ok = (mode == 'subscribe' and token == VERIFY_TOKEN and challenge)
        logger.info(f"[{company_id}] Webhook verify -> ok={bool(ok)} mode={mode}")
        return (challenge, 200) if ok else ('Verify token incorrect', 403)

    # --- Recepción de eventos (POST) ---
    try:
        data = request.get_json(force=True)
        if not data or 'entry' not in data:
            logger.info(f"[{company_id}] Empty/invalid payload")
            return 'ok', 200

        for entry in data['entry']:
            for change in entry.get('changes', []):
                value = change.get('value', {})

                # -------- MENSAJES ENTRANTES --------
                if 'messages' in value:
                    contacts = value.get('contacts', [])
                    messages = value.get('messages', [])

                    for idx, msg in enumerate(messages):
                        contact = contacts[idx] if idx < len(contacts) else {}
                        wa_id = contact.get('wa_id')
                        sender_phone = PhoneUtils.strip_34(msg.get('from', ''))

                        logger.info(f"[{company_id}] 📨 Processing message from {sender_phone}, type: {msg.get('type', 'unknown')}")

                        # -------- MENSAJES DE TEXTO --------
                        if msg.get('type') == 'text':
                            log_received_message(msg, wa_id)
                            # MINIMO: pasar company_id (haz que el método lo acepte como opcional)
                            try:
                                message_service.save_incoming_message(msg, wa_id, company_id=company_id)
                            except TypeError:
                                # compat si aún no acepta el parámetro
                                message_service.save_incoming_message(msg, wa_id)

                            # Flow EXIT logic (con filtro por tenant)
                            try:
                                context = msg.get('context') or {}
                                context_id = context.get('id')
                                madrid_ahora = now_madrid_naive()

                                if not context_id:
                                    query_last_template = """
                                        SELECT last_message_uid
                                          FROM public.external_messages
                                         WHERE sender_phone = %s
                                           AND company_id   = %s
                                           AND from_me      = 'true'
                                           AND status       = 'template_sent'
                                           AND created_at   > %s
                                           AND last_message_uid IS NOT NULL
                                         ORDER BY created_at DESC
                                         LIMIT 1
                                    """
                                    umbral = madrid_ahora - timedelta(minutes=15)
                                    row = db_manager.execute_query(
                                        query_last_template, [sender_phone, company_id, umbral], fetch_one=True
                                    )
                                    if row and row[0]:
                                        context_id = row[0]
                                    else:
                                        if not auto_reply_service.is_office_hours():
                                            auto_reply_service.send_auto_reply(
                                                sender_phone, whatsapp_service, message_service , company_id=company_id
                                            )
                                        continue

                                if context_id:
                                    chk_template = """
                                        SELECT 1
                                          FROM public.external_messages
                                         WHERE last_message_uid = %s
                                           AND company_id       = %s
                                           AND status           = 'template_sent'
                                         LIMIT 1
                                    """
                                    ok_template = db_manager.execute_query(
                                        chk_template, [context_id, company_id], fetch_one=True
                                    )
                                    if not ok_template:
                                        if not auto_reply_service.is_office_hours():
                                            auto_reply_service.send_auto_reply(
                                                sender_phone, whatsapp_service, message_service , company_id=company_id
                                            )
                                        continue

                                    chk_dedup = """
                                        SELECT 1
                                          FROM public.external_messages
                                         WHERE last_message_uid = %s
                                           AND sender_phone     = %s
                                           AND company_id       = %s
                                           AND status           = 'flow_exit_triggered'
                                         LIMIT 1
                                    """
                                    ya_triggered = db_manager.execute_query(
                                        chk_dedup, [context_id, sender_phone, company_id], fetch_one=True
                                    )
                                    if ya_triggered:
                                        continue

                                    lead = lead_service.get_lead_data_by_phone(sender_phone)
                                    if not lead or not lead.get('lead_id'):
                                        if not auto_reply_service.is_office_hours():
                                            auto_reply_service.send_auto_reply(
                                                sender_phone, whatsapp_service, message_service , company_id=company_id
                                            )
                                        continue

                                    lead_id = lead['lead_id']
                                    ok = flow_exit_client.send_exit(lead_id)
                                    if ok:
                                        flow_name = "welcome_email_flow"
                                        motivo = "Usuario quiere salir del flow"
                                        exit_message = f"Exit Flow: {flow_name} por: {motivo}"

                                        # MINIMO: añadir company_id al INSERT
                                        mark_sql = """
                                            INSERT INTO public.external_messages (
                                                id, message, sender_phone, responsible_email,
                                                last_message_uid, last_message_timestamp,
                                                from_me, status, created_at, updated_at, is_deleted,
                                                chat_id, chat_url, assigned_to_id, company_id
                                            ) VALUES (
                                                %s, %s, %s, %s,
                                                %s, %s,
                                                %s, %s, NOW(), NOW(), FALSE,
                                                %s, %s, %s, %s
                                            )
                                        """
                                        params = [
                                            str(uuid4()),
                                            json.dumps({'text': exit_message}, ensure_ascii=False),
                                            sender_phone, '', context_id, madrid_ahora,
                                            'true', 'flow_exit_triggered',
                                            sender_phone, sender_phone, None,
                                            company_id
                                        ]
                                        db_manager.execute_query(mark_sql, params)

                            except Exception:
                                logger.exception(f"[{company_id}] Error procesando disparo de flow exit")

                            # Auto-reply si es fuera de horario
                            if not auto_reply_service.is_office_hours():
                                auto_reply_service.send_auto_reply(
                                    sender_phone, whatsapp_service, message_service , company_id=company_id
                                )

                        # -------- MENSAJES CON ARCHIVOS MULTIMEDIA (EXTENDIDO) --------
                        elif msg.get('type') in ['image', 'audio', 'video', 'document', 'sticker', 'voice']:
                            media_type = msg.get('type')
                            logger.info(f"[{company_id}] 📎 Received {media_type} from {sender_phone}")

                            try:
                                media_info = msg.get(media_type, {})
                                media_id = media_info.get('id')
                                original_filename = media_info.get('filename')
                                caption = media_info.get('caption', '')

                                if not media_id:
                                    logger.error(f"[{company_id}] No media ID found in {media_type} message")
                                    try:
                                        message_service.save_incoming_message(msg, wa_id, company_id=company_id)
                                    except TypeError:
                                        message_service.save_incoming_message(msg, wa_id)
                                    continue

                                # Determinar objeto de referencia
                                lead = lead_service.get_lead_data_by_phone(sender_phone)
                                if lead:
                                    object_ref_type = 'leads'
                                    object_ref_id = lead['lead_id']
                                else:
                                    object_ref_type = 'external_messages'
                                    object_ref_id = str(uuid4())

                                # Procesar media con ExtendedFileService
                                if file_service:
                                    try:
                                        file_result = file_service.process_whatsapp_media_extended(
                                            media_id, object_ref_type, object_ref_id, original_filename, sender_phone
                                        )
                                        file_info = {
                                            'document_id': file_result['document_id'],
                                            'filename': file_result['filename'],
                                            'original_filename': file_result.get('original_filename'),
                                            'media_type': file_result['media_type'],
                                            'whatsapp_type': file_result['whatsapp_type'],
                                            'content_type': file_result['content_type'],
                                            'file_size': file_result['file_size'],
                                            'public_url': file_result.get('public_url'),
                                            'supabase_path': file_result.get('supabase_path')
                                        }

                                        # MINIMO: pasar company_id
                                        try:
                                            message_service.save_media_message(msg, wa_id, file_info, company_id=company_id)
                                        except TypeError:
                                            message_service.save_media_message(msg, wa_id, file_info)

                                        file_size_mb = file_result['file_size'] / (1024 * 1024)
                                        log_message = f"📎 {file_result['media_type'].title()}: {file_result['filename']} ({file_size_mb:.2f}MB)"
                                        if file_result['content_type']:
                                            log_message += f" [{file_result['content_type']}]"
                                        if caption:
                                            log_message += f" - Caption: {caption}"

                                        log_received_message({
                                            'from': msg.get('from'),
                                            'timestamp': msg.get('timestamp'),
                                            'text': {'body': log_message},
                                            'type': 'media_extended'
                                        }, wa_id)

                                        logger.info(f"[{company_id}] ✅ Processed {file_result['whatsapp_type']} media: {file_result['filename']} (Extended MIME)")

                                    except Exception as e:
                                        logger.error(f"[{company_id}] ❌ Error processing media {media_id}: {e}")
                                        error_info = {
                                            'error': str(e),
                                            'media_id': media_id,
                                            'media_type': media_type,
                                            'whatsapp_type': media_type,
                                            'filename': f'error_{media_type}.bin',
                                            'content_type': 'application/octet-stream',
                                            'file_size': 0,
                                            'public_url': '',
                                            'note': 'Failed with extended MIME type support'
                                        }
                                        try:
                                            message_service.save_media_message(msg, wa_id, error_info, company_id=company_id)
                                        except TypeError:
                                            message_service.save_media_message(msg, wa_id, error_info)
                                else:
                                    logger.warning(f"[{company_id}] 📎 ExtendedFileService not available")
                                    fallback_info = {
                                        'error': 'ExtendedFileService not available',
                                        'media_type': media_type,
                                        'whatsapp_type': media_type,
                                        'filename': f'unavailable_{media_type}.bin',
                                        'content_type': 'application/octet-stream',
                                        'file_size': 0,
                                        'public_url': ''
                                    }
                                    try:
                                        message_service.save_media_message(msg, wa_id, fallback_info, company_id=company_id)
                                    except TypeError:
                                        message_service.save_media_message(msg, wa_id, fallback_info)

                                # Auto-reply si es fuera de horario
                                if not auto_reply_service.is_office_hours():
                                    auto_reply_service.send_auto_reply(
                                        sender_phone, whatsapp_service, message_service , company_id=company_id
                                    )

                            except Exception as e:
                                logger.exception(f"[{company_id}] ❌ Error processing {media_type} message: {e}")
                                try:
                                    error_info = {
                                        'error': str(e),
                                        'media_type': media_type or 'unknown',
                                        'whatsapp_type': media_type or 'unknown',
                                        'filename': f'failed_{media_type or "unknown"}.bin',
                                        'content_type': 'application/octet-stream',
                                        'file_size': 0,
                                        'public_url': ''
                                    }
                                    try:
                                        message_service.save_media_message(msg, wa_id, error_info, company_id=company_id)
                                    except TypeError:
                                        message_service.save_media_message(msg, wa_id, error_info)
                                except Exception:
                                    logger.exception(f"[{company_id}] Failed to save fallback message")

                # -------- ESTADOS DE MENSAJE --------
                elif 'statuses' in value:
                    # (mínimo: lo dejamos igual; si tu handler soporta company_id, pásaselo)
                    handle_message_statuses_webhook(value, db_manager)

                # -------- LLAMADAS (CALLS) --------
                elif 'calls' in value:
                    calls = value.get('calls', [])
                    for call in calls:
                        call_event = call.get('event', 'unknown')
                        call_status = call.get('status', 'unknown')
                        call_from = call.get('from', '')
                        call_to = call.get('to', '')
                        logger.info(f"[{company_id}] 📞 Call received: {call_event} - {call_status} from {call_from} to {call_to}")

                # -------- OTROS EVENTOS --------
                else:
                    logger.info(f"[{company_id}] 📱 Webhook event not processed: {list(value.keys())}")

        return 'ok', 200

    except Exception:
        logger.exception(f'[{company_id}] ❌ Error processing webhook')
        return 'error', 500


@app.route('/webhook', methods=['GET', 'POST'])
def webhook():
    # --- Verificación Webhook (GET) ---
    if request.method == 'GET':
        token = request.args.get('hub.verify_token')
        challenge = request.args.get('hub.challenge')
        mode = request.args.get('hub.mode')

        if mode == 'subscribe' and token == VERIFY_TOKEN:
            logger.info("Webhook verified successfully")
            return challenge, 200
        else:
            logger.warning("Webhook verification failed")
            return 'Verify token incorrect', 403

    # --- Recepción de eventos (POST) ---
    try:
        data = request.get_json(force=True)
        if not data or 'entry' not in data:
            return 'ok', 200

        for entry in data['entry']:
            for change in entry.get('changes', []):
                value = change.get('value', {})

                # -------- MENSAJES ENTRANTES --------
                if 'messages' in value:
                    contacts = value.get('contacts', [])
                    messages = value.get('messages', [])

                    for idx, msg in enumerate(messages):
                        contact = contacts[idx] if idx < len(contacts) else {}
                        wa_id = contact.get('wa_id')
                        sender_phone = PhoneUtils.strip_34(msg.get('from', ''))
                        
                        logger.info(f"📨 Processing message from {sender_phone}, type: {msg.get('type', 'unknown')}")

                        # -------- MENSAJES DE TEXTO --------
                        if msg.get('type') == 'text':
                            log_received_message(msg, wa_id)
                            message_service.save_incoming_message(msg, wa_id)

                            # Flow EXIT logic
                            try:
                                context = msg.get('context') or {}
                                context_id = context.get('id')
                                madrid_ahora = now_madrid_naive()

                                if not context_id:
                                    query_last_template = """
                                        SELECT last_message_uid
                                        FROM public.external_messages
                                        WHERE sender_phone = %s
                                          AND from_me = 'true'
                                          AND status = 'template_sent'
                                          AND created_at > %s
                                          AND last_message_uid IS NOT NULL
                                        ORDER BY created_at DESC
                                        LIMIT 1
                                    """
                                    umbral = madrid_ahora - timedelta(minutes=15)
                                    row = db_manager.execute_query(
                                        query_last_template, [sender_phone, umbral], fetch_one=True
                                    )
                                    if row and row[0]:
                                        context_id = row[0]
                                    else:
                                        if not auto_reply_service.is_office_hours():
                                            auto_reply_service.send_auto_reply(
                                                sender_phone, whatsapp_service, message_service , company_id=company_id
                                            )
                                        continue

                                if context_id:
                                    chk_template = """
                                        SELECT 1
                                        FROM public.external_messages
                                        WHERE last_message_uid = %s
                                          AND status = 'template_sent'
                                        LIMIT 1
                                    """
                                    ok_template = db_manager.execute_query(
                                        chk_template, [context_id], fetch_one=True
                                    )
                                    if not ok_template:
                                        if not auto_reply_service.is_office_hours():
                                            auto_reply_service.send_auto_reply(
                                                sender_phone, whatsapp_service, message_service , company_id=company_id
                                            )
                                        continue

                                    chk_dedup = """
                                        SELECT 1
                                        FROM public.external_messages
                                        WHERE last_message_uid = %s
                                          AND sender_phone = %s
                                          AND status = 'flow_exit_triggered'
                                        LIMIT 1
                                    """
                                    ya_triggered = db_manager.execute_query(
                                        chk_dedup, [context_id, sender_phone], fetch_one=True
                                    )
                                    if ya_triggered:
                                        continue

                                    lead = lead_service.get_lead_data_by_phone(sender_phone)
                                    if not lead or not lead.get('lead_id'):
                                        if not auto_reply_service.is_office_hours():
                                            auto_reply_service.send_auto_reply(
                                                sender_phone, whatsapp_service, message_service , company_id=company_id
                                            )
                                        continue
                                    
                                    lead_id = lead['lead_id']
                                    ok = flow_exit_client.send_exit(lead_id)
                                    if ok:
                                        flow_name = "welcome_email_flow"
                                        motivo = "Usuario quiere salir del flow"
                                        exit_message = f"Exit Flow: {flow_name} por: {motivo}"

                                        mark_sql = """
                                            INSERT INTO public.external_messages (
                                                id, message, sender_phone, responsible_email,
                                                last_message_uid, last_message_timestamp,
                                                from_me, status, created_at, updated_at, is_deleted,
                                                chat_id, chat_url, assigned_to_id
                                            ) VALUES (
                                                %s, %s, %s, %s,
                                                %s, %s,
                                                %s, %s, NOW(), NOW(), FALSE,
                                                %s, %s, %s
                                            )
                                        """
                                        params = [
                                            str(uuid4()),
                                            json.dumps({'text': exit_message}, ensure_ascii=False),
                                            sender_phone, '', context_id, madrid_ahora,
                                            'true', 'flow_exit_triggered',
                                            sender_phone, sender_phone, None
                                        ]
                                        db_manager.execute_query(mark_sql, params)

                            except Exception:
                                logger.exception("Error procesando disparo de flow exit")

                            # Auto-reply si es fuera de horario
                            if not auto_reply_service.is_office_hours():
                                auto_reply_service.send_auto_reply(
                                    sender_phone, whatsapp_service, message_service , company_id=company_id
                                )

                        # -------- MENSAJES CON ARCHIVOS MULTIMEDIA (EXTENDIDO) --------
                        elif msg.get('type') in ['image', 'audio', 'video', 'document', 'sticker', 'voice']:
                            media_type = msg.get('type')
                            logger.info(f"📎 Received {media_type} from {sender_phone}")
                            
                            try:
                                media_info = msg.get(media_type, {})
                                media_id = media_info.get('id')
                                original_filename = media_info.get('filename')
                                caption = media_info.get('caption', '')
                                
                                if not media_id:
                                    logger.error(f"No media ID found in {media_type} message")
                                    message_service.save_incoming_message(msg, wa_id)
                                    continue

                                # Determinar objeto de referencia
                                lead = lead_service.get_lead_data_by_phone(sender_phone)
                                if lead:
                                    object_ref_type = 'leads'
                                    object_ref_id = lead['lead_id']
                                else:
                                    object_ref_type = 'external_messages'
                                    object_ref_id = str(uuid4())

                                # Procesar media con ExtendedFileService
                                if file_service:
                                    try:
                                        # PASAR sender_phone para que get_whatsapp_media_url reciba el teléfono correcto
                                        file_result = file_service.process_whatsapp_media_extended(
                                            media_id, object_ref_type, object_ref_id, original_filename, sender_phone
                                        )
                                        # 🔧 MEJORAR: Construir file_info más completo para el JSON
                                        file_info = {
                                            'document_id': file_result['document_id'],
                                            'filename': file_result['filename'],
                                            'original_filename': file_result.get('original_filename'),
                                            'media_type': file_result['media_type'],
                                            'whatsapp_type': file_result['whatsapp_type'],
                                            'content_type': file_result['content_type'],
                                            'file_size': file_result['file_size'],
                                            'public_url': file_result.get('public_url'),
                                            'supabase_path': file_result.get('supabase_path')  # Agregar path de Supabase
                                        }
                                        
                                        # 🔧 IMPORTANTE: Guardar con formato JSON estructurado
                                        message_service.save_media_message(msg, wa_id, file_info)
                                        
                                        file_size_mb = file_result['file_size'] / (1024 * 1024)
                                        log_message = f"📎 {file_result['media_type'].title()}: {file_result['filename']} ({file_size_mb:.2f}MB)"
                                        if file_result['content_type']:
                                            log_message += f" [{file_result['content_type']}]"
                                        if caption:
                                            log_message += f" - Caption: {caption}"
                                        
                                        log_received_message({
                                            'from': msg.get('from'),
                                            'timestamp': msg.get('timestamp'),
                                            'text': {'body': log_message},
                                            'type': 'media_extended'
                                        }, wa_id)
                                        
                                        logger.info(f"✅ Processed {file_result['whatsapp_type']} media: {file_result['filename']} (Extended MIME support)")
                                        
                                    except Exception as e:
                                        logger.error(f"❌ Error processing media {media_id}: {e}")
                                        # 🔧 MEJORAR: Guardar error también en formato JSON
                                        error_info = {
                                            'error': str(e),
                                            'media_id': media_id,
                                            'media_type': media_type,
                                            'whatsapp_type': media_type,
                                            'filename': f'error_{media_type}.bin',
                                            'content_type': 'application/octet-stream',
                                            'file_size': 0,
                                            'public_url': '',
                                            'note': 'Failed with extended MIME type support'
                                        }
                                        message_service.save_media_message(msg, wa_id, error_info)
                                        
                                else:
                                    logger.warning("📎 ExtendedFileService not available")
                                    # 🔧 MEJORAR: Aún así guardar en formato JSON básico
                                    fallback_info = {
                                        'error': 'ExtendedFileService not available',
                                        'media_type': media_type,
                                        'whatsapp_type': media_type,
                                        'filename': f'unavailable_{media_type}.bin',
                                        'content_type': 'application/octet-stream',
                                        'file_size': 0,
                                        'public_url': ''
                                    }
                                    message_service.save_media_message(msg, wa_id, fallback_info)

                                # Auto-reply si es fuera de horario
                                if not auto_reply_service.is_office_hours():
                                    auto_reply_service.send_auto_reply(
                                        sender_phone, whatsapp_service, message_service , company_id=company_id
                                    )

                            except Exception as e:
                                logger.exception(f"❌ Error processing {media_type} message: {e}")
                                try:
                                    # 🔧 ÚLTIMA OPCIÓN: Guardar error como JSON
                                    error_info = {
                                        'error': str(e),
                                        'media_type': media_type or 'unknown',
                                        'whatsapp_type': media_type or 'unknown',
                                        'filename': f'failed_{media_type or "unknown"}.bin',
                                        'content_type': 'application/octet-stream',
                                        'file_size': 0,
                                        'public_url': ''
                                    }
                                    message_service.save_media_message(msg, wa_id, error_info)
                                except Exception:
                                    logger.exception("Failed to save fallback message")

                        # -------- OTROS TIPOS DE MENSAJE --------
                        else:
                            logger.info(f"📱 Received unsupported message type '{msg.get('type')}' from {sender_phone}")
                            generic_msg = {
                                'from': msg.get('from'),
                                'timestamp': msg.get('timestamp'),
                                'type': 'text',
                                'text': {
                                    'body': f"📱 Mensaje de tipo '{msg.get('type')}' recibido"
                                },
                                'id': msg.get('id')
                            }
                            message_service.save_incoming_message(generic_msg, wa_id)

                # -------- ESTADOS DE MENSAJE --------
                elif 'statuses' in value:
                    handle_message_statuses_webhook(value, db_manager)
                
                # -------- LLAMADAS (CALLS) --------
                elif 'calls' in value:
                    calls = value.get('calls', [])
                    for call in calls:
                        call_event = call.get('event', 'unknown')
                        call_status = call.get('status', 'unknown')
                        call_from = call.get('from', '')
                        call_to = call.get('to', '')
                        logger.info(f"📞 Call received: {call_event} - {call_status} from {call_from} to {call_to}")
                    # Las llamadas se logean pero no se procesan más por ahora
                
                # -------- OTROS EVENTOS --------
                else:
                    logger.info(f"📱 Webhook event not processed: {list(value.keys())}")

        return 'ok', 200

    except Exception:
        logger.exception('❌ Error processing webhook')
        return 'error', 500


import json

def save_messenger_incoming_message(page_id: str, psid: str, text: str|None, mid: str|None, ts: int|None):
    phone = resolve_phone_for_psid(page_id)
    
    # Solo guardar el texto del mensaje
    message_text = text or ""
    
    # Construir last_message_uid con prefijo messenger
    last_message_uid = f"messenger.{psid}"
    if mid:
        last_message_uid = f"messenger.{mid}"

    chat_id = f"messenger:{page_id}:{psid}"
    chat_url = f"https://www.facebook.com/messages/t/{psid}"

    db_manager.execute_query("""
        INSERT INTO public.external_messages
        ( id, message, sender_phone, responsible_email, last_message_uid, last_message_timestamp,
          from_me, status, created_at, updated_at, is_deleted, chat_id, chat_url, assigned_to_id )
        VALUES
        ( gen_random_uuid(), %s, %s, NULL, %s, NOW(),
          'false', 'messenger_received', NOW(), NOW(), FALSE, %s, %s, NULL )
    """, [
        message_text,  # Solo el texto del mensaje
        phone,        # NULL si no hay phone
        last_message_uid,  # messenger.PSID o messenger.MID
        chat_id,
        chat_url
    ], fetch_one=False)


def resolve_phone_for_psid(page_id: str, psid: str) -> str|None:
    # 1) (Opcional) mapeo en custom properties de leads
    try:
        sql_map = """
        SELECT l.phone
        FROM public.leads l
        JOIN public.object_property_values opv
          ON opv.object_reference_type = 'leads'
         AND opv.object_reference_id = l.id
        WHERE opv.property_name = 'MESSENGER_PSID'
          AND opv.value = %s
          AND COALESCE(l.is_deleted, false) = false
        LIMIT 1
        """
        row = db_manager.execute_query(sql_map, [psid], fetch_one=True)
        if row and row[0]:
            return row[0]
    except Exception:
        logger.exception("[Messenger] Error checking lead mapping")

    # 2) Mensajes previos con phone conocido
    try:
        sql_prev = """
        SELECT sender_phone
        FROM public.external_messages
        WHERE chat_id = %s
          AND sender_phone IS NOT NULL
        ORDER BY created_at DESC
        LIMIT 1
        """
        chat_id = f"messenger:{page_id}:{psid}"
        row2 = db_manager.execute_query(sql_prev, [chat_id], fetch_one=True)
        if row2 and row2[0]:
            return row2[0]
    except Exception:
        logger.exception("[Messenger] Error checking previous messages")

    return None  # no encontrado

def get_messenger_token_by_page(page_id: str) -> tuple[str | None, str | None]:
    """
    Devuelve (company_id, PAGE_ACCESS_TOKEN) para un MESSENGER_PAGE_ID dado.
    Usa properties.property_name en lugar de opv.property_name.
    """
    try:
        sql = """
        WITH comp AS (
          SELECT opv.object_reference_id AS company_id
          FROM public.object_property_values opv
          JOIN public.properties p ON p.id = opv.property_id
          WHERE opv.object_reference_type = 'companies'
            AND p.property_name = 'MESSENGER_PAGE_ID'
            AND trim(opv.value) = %s
          ORDER BY opv.created_at DESC NULLS LAST
          LIMIT 1
        )
        SELECT c.company_id::text,
               opv.value AS page_access_token
        FROM comp c
        JOIN public.object_property_values opv
          ON opv.object_reference_type = 'companies'
         AND opv.object_reference_id   = c.company_id
        JOIN public.properties p ON p.id = opv.property_id
        WHERE p.property_name = 'PAGE_ACCESS_TOKEN'
        ORDER BY opv.created_at DESC NULLS LAST
        LIMIT 1;
        """
        row = db_manager.execute_query(sql, [page_id], fetch_one=True)
        if row and row[0]:
            company_id, token = row[0], row[1]
            if token:
                logger.info(f"[Messenger] Found config for page_id={page_id}: company={company_id}")
                return company_id, token
        logger.warning(f"[Messenger] No config found for page_id={page_id}")
        return None, None
    except Exception:
        logger.exception("[Messenger] Error resolving PAGE_ACCESS_TOKEN")
        return None, None


def resolve_phone_for_psid(page_id: str, psid: str) -> str | None:
    """
    Mapea PSID -> phone del lead desde object_property_values + properties.
    """
    try:
        # 1) Primero intentar mapeo directo por PSID
        sql = """
        SELECT l.phone
        FROM public.leads l
        JOIN public.object_property_values opv
          ON opv.object_reference_type = 'leads'
         AND opv.object_reference_id   = l.id
        JOIN public.properties p ON p.id = opv.property_id
        WHERE p.property_name = 'MESSENGER_PSID'
          AND trim(opv.value) = %s
          AND COALESCE(l.is_deleted, false) = false
        ORDER BY opv.created_at DESC NULLS LAST
        LIMIT 1;
        """
        row = db_manager.execute_query(sql, [psid], fetch_one=True)
        if row and row[0]:
            logger.info(f"[Messenger] Found phone {row[0]} for PSID {psid} via direct mapping")
            return row[0]

        # 2) Si no hay mapeo, buscar en mensajes previos
        chat_id = f"messenger:{page_id}:{psid}"
        sql_prev = """
        SELECT sender_phone
        FROM public.external_messages
        WHERE chat_id = %s
          AND sender_phone IS NOT NULL
        ORDER BY created_at DESC
        LIMIT 1
        """
        row2 = db_manager.execute_query(sql_prev, [chat_id], fetch_one=True)
        if row2 and row2[0]:
            logger.info(f"[Messenger] Found phone {row2[0]} for PSID {psid} via previous messages")
            return row2[0]

        logger.warning(f"[Messenger] No phone found for PSID {psid}")
        return None

    except Exception:
        logger.exception(f"[Messenger] Error resolving phone for PSID {psid}")
        return None


import requests

def send_messenger_text(page_access_token: str, psid: str, text: str):
    """
    Envía un mensaje de texto simple a un usuario de Messenger.
    """
    try:
        url = "https://graph.facebook.com/v22.0/me/messages"
        params = {"access_token": page_access_token}
        payload = {
            "recipient": {"id": psid},
            "message": {"text": text}
        }
        r = requests.post(url, params=params, json=payload, timeout=10)
        r.raise_for_status()
        logger.info(f"[Messenger] Sent echo to {psid}: {text!r}")
    except Exception:
        logger.exception("[Messenger] Error sending message")


def fetch_messenger_profile(page_access_token: str, psid: str) -> dict:
    """
    Pide a Graph API el perfil del usuario de Messenger.
    Devuelve dict con first_name, last_name, profile_pic si existen.
    """
    try:
        url = f"https://graph.facebook.com/v19.0/{psid}"
        params = {
            "fields": "first_name,last_name,profile_pic",
            "access_token": page_access_token
        }
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        data = r.json() or {}
        return {
            "first_name": data.get("first_name") or "",
            "last_name": data.get("last_name") or "",
            "profile_pic": data.get("profile_pic") or ""
        }
    except Exception:
        logger.exception("[Messenger] Error fetching profile from Graph")
        return {"first_name": "", "last_name": "", "profile_pic": ""}


from flask import request, jsonify  # por si no estaba ya importado
import requests


def fetch_messenger_profile_simple(page_access_token: str, psid: str) -> dict:
    try:
        url = f"https://graph.facebook.com/v19.0/{psid}"
        params = {
            "fields": "first_name,last_name",
            "access_token": page_access_token
        }
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        data = r.json() or {}
        return {
            "first_name": data.get("first_name") or "",
            "last_name": data.get("last_name") or ""
        }
    except Exception:
        # no rompas el flujo si falla
        return {"first_name": "", "last_name": ""}

def extract_phone_from_text(text: str | None) -> str | None:
    """
    Saca un teléfono si el usuario lo ha escrito en el mensaje.
    Soporta formatos: 600123123, 600 123 123, +34600123123, 0034600123123, etc.
    """
    if not text:
        return None

    # 1) quitar separadores comunes
    cleaned = re.sub(r"[().\-]", " ", text)

    # 2) buscar candidatos con o sin prefijo internacional
    #    - España: 9 dígitos empezando por 6/7/8/9
    #    - con prefijo +34 o 0034
    pat = re.compile(
        r"(?:\+34|0034)?\s*([6789]\s*\d\s*\d\s*\d\s*\d\s*\d\s*\d\s*\d\s*\d)",
        flags=re.IGNORECASE
    )
    m = pat.search(cleaned)
    if not m:
        # plan B: números largos genéricos (8-13 dígitos) por si mandan otro país
        m2 = re.search(r"(\+?\d[\d\s]{7,14}\d)", cleaned)
        if not m2:
            return None
        cand = re.sub(r"\s+", "", m2.group(1))
        return cand

    # normalizar a sólo dígitos y prefijo +34 si procede
    cand = re.sub(r"\s+", "", m.group(0))
    # homogeneizar prefijos 0034 -> +34
    cand = re.sub(r"^0034", "+34", cand)
    if cand.startswith("+34"):
        # quitar +34 para almacenar “limpio” y ya decidir luego
        only_digits = re.sub(r"\D", "", cand)
        # only_digits = 34XXXXXXXXX -> deja los 9 últimos
        return only_digits[-9:]
    else:
        # sin prefijo -> deja sólo dígitos
        return re.sub(r"\D", "", cand)

def send_messenger_text_simple(page_access_token: str, psid: str, text: str) -> None:
    try:
        url = "https://graph.facebook.com/v22.0/me/messages"
        params = {"access_token": page_access_token}
        payload = {
            "messaging_type": "RESPONSE",
            "recipient": {"id": psid},
            "message": {"text": text}
        }
        r = requests.post(url, params=params, json=payload, timeout=10)
        # no frenamos el flujo por un 400, sólo lo dejamos en logs
        print("[Messenger] send reply status:", r.status_code, r.text)
    except Exception as e:
        print("[Messenger] error sending reply:", e)


# =================================================================================
# AÑADIR ESTAS IMPORTACIONES AL INICIO DE TU ARCHIVO (después de las existentes)
# =================================================================================
from enum import Enum
from typing import Dict, Optional, Tuple

# =================================================================================
# AÑADIR ESTAS CLASES Y CONFIGURACIÓN (después de tus imports, antes de las rutas)
# =================================================================================

# Estados del flujo conversacional
class ConversationState(Enum):
    INITIAL = "initial"
    WAITING_NAME = "waiting_name"
    WAITING_PHONE = "waiting_phone" 
    WAITING_EMAIL = "waiting_email"
    COMPLETED = "completed"
    ERROR = "error"

# Configuración del flujo (EDITABLE)
MESSENGER_CONVERSATION_CONFIG = {
    "enabled": True,  # Cambiar a False para desactivar el flujo
    "timeout_hours": 24,
    "source": "Messenger",
    "company_name": "sheets"  # Cambiar según necesites
}

# =================================================================================
# AÑADIR ESTAS FUNCIONES ANTES DE TUS RUTAS EXISTENTES
# =================================================================================

class MessengerConversationManager:
    """Maneja el estado y flujo de conversaciones de Messenger"""
    
    def __init__(self, db_manager):
        self.db_manager = db_manager
    
    def get_conversation_state(self, psid: str, page_id: str) -> Tuple[ConversationState, Dict]:
        """Obtiene el estado actual de la conversación y datos recolectados"""
        try:
            query = """
                SELECT message, created_at
                FROM public.external_messages
                WHERE chat_id = %s
                  AND status = 'messenger_conversation_state'
                ORDER BY created_at DESC
                LIMIT 1
            """
            chat_id = f"messenger:{page_id}:{psid}"
            row = self.db_manager.execute_query(query, [chat_id], fetch_one=True)
            
            if not row:
                return ConversationState.INITIAL, {}
            
            try:
                state_data = json.loads(row[0])
                state = ConversationState(state_data.get('state', 'initial'))
                user_data = state_data.get('user_data', {})
                created_at = row[1]
                
                # Verificar timeout
                if created_at and isinstance(created_at, datetime):
                    if datetime.now() - created_at > timedelta(hours=MESSENGER_CONVERSATION_CONFIG["timeout_hours"]):
                        logger.warning(f"[Messenger] Conversation timeout for {psid}")
                        return ConversationState.INITIAL, {}
                
                return state, user_data
                
            except (json.JSONDecodeError, ValueError) as e:
                logger.error(f"[Messenger] Error parsing conversation state: {e}")
                return ConversationState.INITIAL, {}
                
        except Exception as e:
            logger.exception(f"[Messenger] Error getting conversation state for {psid}")
            return ConversationState.INITIAL, {}
    
    def save_conversation_state(self, psid: str, page_id: str, state: ConversationState, user_data: Dict):
        """Guarda el estado actual de la conversación"""
        try:
            chat_id = f"messenger:{page_id}:{psid}"
            state_data = {
                'state': state.value,
                'user_data': user_data,
                'updated_at': datetime.now().isoformat()
            }
            
            query = """
                INSERT INTO public.external_messages (
                    id, message, sender_phone, responsible_email, last_message_uid, last_message_timestamp,
                    from_me, status, created_at, updated_at, is_deleted, chat_id, chat_url, assigned_to_id
                ) VALUES (
                    gen_random_uuid(), %s, %s, NULL, %s, NOW(),
                    'true', 'messenger_conversation_state', NOW(), NOW(), FALSE, %s, %s, NULL
                )
            """
            self.db_manager.execute_query(query, [
                json.dumps(state_data),
                None,
                f"state_{psid}_{int(datetime.now().timestamp())}",
                chat_id,
                f"https://www.facebook.com/messages/t/{psid}"
            ])
            
            logger.info(f"[Messenger] Saved conversation state {state.value} for {psid}")
            
        except Exception as e:
            logger.exception(f"[Messenger] Error saving conversation state for {psid}")

# Instancia global del manager
messenger_conversation_manager = None

def get_messenger_conversation_manager():
    """Obtiene la instancia del conversation manager"""
    global messenger_conversation_manager
    if messenger_conversation_manager is None:
        messenger_conversation_manager = MessengerConversationManager(db_manager)
    return messenger_conversation_manager

def validate_messenger_name(name: str) -> Tuple[bool, str]:
    """Valida que el nombre sea válido"""
    name = name.strip()
    if len(name) < 2:
        return False, "Por favor, introduce un nombre válido (mínimo 2 caracteres)."
    if len(name) > 100:
        return False, "El nombre es demasiado largo. Por favor, usa máximo 100 caracteres."
    if not re.match(r'^[a-zA-ZáéíóúàèìòùäëïöüâêîôûñçÁÉÍÓÚÀÈÌÒÙÄËÏÖÜÂÊÎÔÛÑÇ\s\-\'\.]+$', name):
        return False, "Por favor, introduce solo letras y espacios en el nombre."
    return True, ""

def validate_messenger_phone(phone: str) -> Tuple[bool, str, str]:
    """Valida y normaliza el teléfono español"""
    # Limpiar el teléfono - solo dígitos
    clean_phone = re.sub(r'[^\d]', '', phone.strip())
    
    try:
        # Si empieza con 34, quitarlo
        if clean_phone.startswith('34') and len(clean_phone) == 11:
            clean_phone = clean_phone[2:]
        
        # Verificar que sea exactamente 9 dígitos
        if len(clean_phone) != 9 or not clean_phone.isdigit():
            return False, "Por favor, introduce un número de teléfono español válido (9 dígitos).", ""
        
        # Verificar que empiece con 6, 7, 8 o 9 (móviles y fijos españoles)
        if not clean_phone.startswith(('6', '7', '8', '9')):
            return False, "Por favor, introduce un número de teléfono español válido (debe empezar por 6, 7, 8 o 9).", ""
        
        logger.info(f"[Messenger] Teléfono validado correctamente: {phone} -> {clean_phone}")
        return True, "", clean_phone
        
    except Exception as e:
        logger.error(f"[Messenger] Error validando teléfono {phone}: {e}")
        return False, "Por favor, introduce un número de teléfono válido.", ""

def validate_messenger_email(email: str) -> Tuple[bool, str]:
    """Valida el email"""
    email = email.strip().lower()
    if len(email) < 5:
        return False, "Por favor, introduce un email válido."
    
    # Regex básico para email
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    if not re.match(email_pattern, email):
        return False, "Por favor, introduce un email válido (ejemplo: nombre@dominio.com)."
    
    return True, ""

def process_messenger_conversation_flow(psid: str, page_id: str, user_message: str, page_token: str) -> bool:
    """
    Procesa el flujo conversacional de Messenger.
    Retorna True si manejó el mensaje, False si debe usar el comportamiento por defecto.
    """
    if not MESSENGER_CONVERSATION_CONFIG["enabled"]:
        return False
    
    try:
        manager = get_messenger_conversation_manager()
        current_state, user_data = manager.get_conversation_state(psid, page_id)
        
        logger.info(f"[Messenger Flow] PSID {psid} - State: {current_state.value}, Message: '{user_message[:50]}...'")
        
        # Procesar según el estado actual
        if current_state == ConversationState.INITIAL:
            return handle_messenger_initial_state(psid, page_id, user_message, page_token, manager, user_data)
            
        elif current_state == ConversationState.WAITING_NAME:
            return handle_messenger_waiting_name(psid, page_id, user_message, page_token, manager, user_data)
            
        elif current_state == ConversationState.WAITING_PHONE:
            return handle_messenger_waiting_phone(psid, page_id, user_message, page_token, manager, user_data)
            
        elif current_state == ConversationState.WAITING_EMAIL:
            return handle_messenger_waiting_email(psid, page_id, user_message, page_token, manager, user_data)
            
        elif current_state == ConversationState.COMPLETED:
            # Usuario ya completó el flujo, comportamiento normal
            return False
            
        else:
            # Estado no reconocido, reiniciar
            logger.warning(f"[Messenger Flow] Unknown state {current_state}, resetting to INITIAL")
            return handle_messenger_initial_state(psid, page_id, user_message, page_token, manager, {})
            
    except Exception as e:
        logger.exception(f"[Messenger Flow] Error processing conversation flow for {psid}")
        send_messenger_text(page_token, psid, 
            "¡Ups! Hubo un error temporal. Por favor, escribe cualquier mensaje para empezar de nuevo.")
        return True

def handle_messenger_initial_state(psid: str, page_id: str, user_message: str, page_token: str, manager, user_data: Dict) -> bool:
    """Maneja el estado inicial - envía bienvenida y pide nombre"""
    try:
        welcome_message = """¡Hola! 👋 Bienvenido/a a nuestro servicio.

Para poder ayudarte mejor, necesito recopilar algunos datos básicos.

Por favor, dime tu **nombre completo**:"""
        
        send_messenger_text(page_token, psid, welcome_message)
        
        # Cambiar estado a esperar nombre
        manager.save_conversation_state(psid, page_id, ConversationState.WAITING_NAME, user_data)
        
        return True
        
    except Exception as e:
        logger.exception(f"[Messenger Flow] Error in handle_messenger_initial_state for {psid}")
        return False

def handle_messenger_waiting_name(psid: str, page_id: str, user_message: str, page_token: str, manager, user_data: Dict) -> bool:
    """Maneja el estado esperando nombre"""
    try:
        # Validar nombre
        is_valid, error_msg = validate_messenger_name(user_message)
        
        if not is_valid:
            send_messenger_text(page_token, psid, error_msg)
            return True
        
        # Guardar nombre y pedir teléfono
        user_data['nombre_y_apellidos'] = user_message.strip()
        
        phone_message = f"""Perfecto, {user_message.split()[0]} 👍

Ahora necesito tu **número de teléfono**.
Por favor, escríbelo sin espacios (ejemplo: 612345678):"""
        
        send_messenger_text(page_token, psid, phone_message)
        
        # Cambiar estado
        manager.save_conversation_state(psid, page_id, ConversationState.WAITING_PHONE, user_data)
        
        return True
        
    except Exception as e:
        logger.exception(f"[Messenger Flow] Error in handle_messenger_waiting_name for {psid}")
        return False

def handle_messenger_waiting_phone(psid: str, page_id: str, user_message: str, page_token: str, manager, user_data: Dict) -> bool:
    """Maneja el estado esperando teléfono"""
    try:
        # Validar teléfono
        is_valid, error_msg, clean_phone = validate_messenger_phone(user_message)
        
        if not is_valid:
            retry_message = f"""{error_msg}

Recuerda: debe ser un número español de 9 dígitos.
Ejemplo: 612345678 o +34612345678"""
            send_messenger_text(page_token, psid, retry_message)
            return True
        
        # Guardar teléfono y pedir email
        user_data['número_de_teléfono'] = clean_phone
        
        email_message = f"""¡Excelente! ✅

Por último, necesito tu **correo electrónico**.
Por favor, escríbelo completo (ejemplo: tu@email.com):"""
        
        send_messenger_text(page_token, psid, email_message)
        
        # Cambiar estado
        manager.save_conversation_state(psid, page_id, ConversationState.WAITING_EMAIL, user_data)
        
        return True
        
    except Exception as e:
        logger.exception(f"[Messenger Flow] Error in handle_messenger_waiting_phone for {psid}")
        return False

def handle_messenger_waiting_email(psid: str, page_id: str, user_message: str, page_token: str, manager, user_data: Dict) -> bool:
    """Maneja el estado esperando email - último paso"""
    try:
        # Validar email
        is_valid, error_msg = validate_messenger_email(user_message)
        
        if not is_valid:
            retry_message = f"""{error_msg}

Ejemplo de formato correcto: nombre@dominio.com"""
            send_messenger_text(page_token, psid, retry_message)
            return True
        
        # Guardar email y crear usuario
        user_data['correo_electrónico'] = user_message.strip().lower()
        
        # Intentar crear el usuario en el portal usando TU función existente
        success, result_message = create_messenger_portal_user(user_data, psid)
        
        if success:
            success_message = f"""🎉 ¡Perfecto! He registrado tus datos correctamente.

📝 **Resumen:**
• Nombre: {user_data.get('nombre_y_apellidos')}
• Teléfono: {user_data.get('número_de_teléfono')}
• Email: {user_data.get('correo_electrónico')}

¡Ya puedes escribirme cualquier consulta que tengas!"""
            
            send_messenger_text(page_token, psid, success_message)
            
            # Marcar como completado
            manager.save_conversation_state(psid, page_id, ConversationState.COMPLETED, user_data)
            
        else:
            error_message = f"""❌ Hubo un problema al registrar tus datos: {result_message}

Por favor, inténtalo de nuevo más tarde o contacta con nuestro soporte.
Mientras tanto, puedes escribirme cualquier consulta."""
            
            send_messenger_text(page_token, psid, error_message)
            
            # Marcar como completado aunque haya fallado (para no repetir el flujo)
            manager.save_conversation_state(psid, page_id, ConversationState.COMPLETED, user_data)
        
        return True
        
    except Exception as e:
        logger.exception(f"[Messenger Flow] Error in handle_messenger_waiting_email for {psid}")
        send_messenger_text(page_token, psid, 
            "Hubo un error procesando tus datos. Por favor, contacta con soporte.")
        return True

def create_portal_user(data, source=None, config_obj=None):
    """
    Crea un usuario en el portal Solvify Leads.
    Args:
        data: dict con los campos requeridos.
        source: fuente del lead (opcional).
        config_obj: configuración adicional (opcional).
    Returns:
        dict si éxito, None si error.
    """
    try:
        # Use global config if no config object provided
        global config
        if config_obj is None:
            config_obj = config

        if not config_obj or not hasattr(config_obj, 'api_token'):
            logger.error("No valid config object with api_token found")
            return None

        # Determinar category_id y base_url según config file [APP]
        use_test = getattr(config_obj, "use_test", None)
        base_url = getattr(config_obj, "base_url", None)
        api_token = getattr(config_obj, "api_token", None)

        # Category IDs
        category_id = "bcb1ae3e-4c23-4461-9dae-30ed137d53e2"
        url = f"{base_url}/leads/{category_id}/"

        print("Creating portal user with data:", data)

        # Extraer nombre y apellidos
        first_name = data.get("first_name") or data.get("nombre_y_apellidos", "").split()[0]
        last_name = data.get("last_name") or " ".join(data.get("nombre_y_apellidos", "").split()[1:])

        # Email y teléfono
        email = data.get("email") or data.get("correo_electrónico", "")
        phone = data.get("phone_number") or data.get("número_de_teléfono", "")
        phone = strip_country_code(phone)

        # Campaña y formulario
        campaign = data.get("campaign_name") or data.get("campaign") or data.get("company_name") or "Default"
        form_name = data.get("form_name") or "Messenger Conversation"

        # Token de autenticación desde config
        HEADERS = {
            'Authorization': f'Bearer {config.api_token}',
            'Content-Type': 'application/json'
        }

        payload = {
            "first_name": first_name,
            "last_name": last_name,
            "email": email,
            "phone": phone,
            "channel": source or "Messenger",
            "campaign": campaign,
            "form_name": form_name
        }
        try:
            response = requests.post(url, data=json.dumps(payload), headers=HEADERS)
            if response.status_code in [200, 201]:
                return response.json()
            else:
                logging.error("Error al crear el usuario en el portal: %s", response.text)
                return None
        except Exception as e:
            logging.error("Excepción al crear el usuario en el portal: %s", str(e))
            return None
    except Exception as e:
        logger.exception("Error in create_portal_user: %s", str(e))
        return None

    # Extraer nombre y apellidos
    first_name = data.get("first_name") or data.get("nombre_y_apellidos", "").split()[0]
    last_name = data.get("last_name") or " ".join(data.get("nombre_y_apellidos", "").split()[1:])

    # Email y teléfono
    email = data.get("email") or data.get("correo_electrónico", "")
    phone = data.get("phone_number") or data.get("número_de_teléfono", "")
    phone = strip_country_code(phone)

    # Campaña y formulario
    campaign = data.get("campaign_name") or data.get("campaign") or (config.get("company_name") if config else "")
    form_name = data.get("form_name") or "Messenger Conversation"

    # Token de autenticación desde config
    HEADERS = {
        'Authorization': f'Bearer {config.api_token}',
        'Content-Type': 'application/json'
    }

    payload = {
        "first_name": first_name,
        "last_name": last_name,
        "email": email,
        "phone": phone,
        "channel": source or "Messenger",
        "campaign": campaign,
        "form_name": form_name
    }
    try:
        response = requests.post(url, data=json.dumps(payload), headers=HEADERS)
        if response.status_code in [200, 201]:
            return response.json()
        else:
            logging.error("Error al crear el usuario en el portal: %s", response.text)
            return None
    except Exception as e:
        logging.error("Excepción al crear el usuario en el portal: %s", str(e))
        return None

def strip_country_code(phone):
    phone = str(phone).strip().replace("+", "")
    if phone.startswith("34") and len(phone) > 9:
        return phone[2:]
    if phone.startswith("0034") and len(phone) > 9:
        return phone[4:]
    return phone

def create_messenger_portal_user(user_data: Dict, psid: str) -> Tuple[bool, str]:
    """
    Crea un usuario en el portal usando TU función create_portal_user existente.
    Retorna (success: bool, message: str)
    """
    try:
        logger.info(f"[Messenger] Creating portal user from conversation data: {user_data}")
        
        # Preparar datos en el formato que espera TU función create_portal_user
        portal_data = {
            'nombre_y_apellidos': user_data.get('nombre_y_apellidos', ''),
            'número_de_teléfono': user_data.get('número_de_teléfono', ''),
            'correo_electrónico': user_data.get('correo_electrónico', ''),
            'form_name': 'Messenger Conversation',
            'campaign_name': 'Messenger Lead Generation',
            'leadgen_id': f'messenger_{psid}_{int(datetime.now().timestamp())}'
        }
        
        # Usar la configuración específica para Messenger
        messenger_config = {
            "company_name": MESSENGER_CONVERSATION_CONFIG["company_name"]
        }
        
        # LLAMAR A TU FUNCIÓN EXISTENTE create_portal_user
        result = create_portal_user(
            data=portal_data, 
            source=MESSENGER_CONVERSATION_CONFIG["source"],
            config=messenger_config
        )
        
        if result:
            logger.info(f"[Messenger] Portal user created successfully for PSID {psid}")
            return True, "Usuario creado exitosamente"
        else:
            logger.warning(f"[Messenger] Failed to create portal user for PSID {psid}")
            return False, "No se pudo crear el usuario (posible duplicado o error de validación)"
            
    except Exception as e:
        logger.exception(f"[Messenger] Exception creating portal user for PSID {psid}")
        return False, f"Error interno: {str(e)}"

# =================================================================================
# REEMPLAZAR TU FUNCIÓN webhook_messenger EXISTENTE POR ESTA VERSIÓN
# =================================================================================
@app.route('/webhook/messenger', methods=['GET', 'POST'])
def webhook_messenger():
    """Webhook de Messenger con flujo conversacional para captura de datos"""
    
    logger.info("=" * 80)
    logger.info("[Messenger] 📄 Nueva petición webhook recibida")
    logger.info(f"[Messenger] Método: {request.method}")

    if request.method == 'GET':
        # Verificación del webhook (sin cambios)
        mode = request.args.get('hub.mode')
        token = request.args.get('hub.verify_token')
        challenge = request.args.get('hub.challenge')
        
        logger.info(f"[Messenger] 🔐 Verificación webhook:")
        logger.info(f"[Messenger] - Mode: {mode}")
        logger.info(f"[Messenger] - Token recibido: {token}")
        logger.info(f"[Messenger] - Token esperado: {VERIFY_TOKEN}")

        if mode == 'subscribe' and token == VERIFY_TOKEN:
            logger.info("[Messenger] ✅ Webhook verificado correctamente")
            return challenge, 200
        else:
            logger.error("[Messenger] ❌ Verificación webhook fallida")
            return 'Forbidden', 403

    # Manejo de POST - MEJORADO con flujo conversacional
    try:
        logger.info("[Messenger] 🔥 Procesando webhook POST")
        data = request.get_json(force=True)
        logger.info(f"[Messenger] Payload recibido: {json.dumps(data, indent=2)}")

        if not data or 'entry' not in data:
            logger.warning("[Messenger] ⚠️ Payload vacío o sin 'entry'")
            return 'ok', 200

        for entry in data.get('entry', []):
            entry_page_id = entry.get('id')
            logger.info(f"[Messenger] 🔑 Procesando entry para page_id={entry_page_id}")

            for messaging in entry.get('messaging', []):
                logger.info(f"[Messenger] 💬 Procesando messaging: {json.dumps(messaging, indent=2)}")
                
                msg = messaging.get('message', {})
                is_echo = bool(msg.get('is_echo'))
                
                if is_echo:
                    logger.info("[Messenger] 🔄 Echo recibido - omitiendo")
                    continue

                # Obtener datos básicos del mensaje
                sender_id = messaging.get('sender', {}).get('id')
                page_id = messaging.get('recipient', {}).get('id') or entry_page_id
                mid = msg.get('mid')
                text = (msg.get('text') or '').strip()

                logger.info(f"[Messenger] 📋 Mensaje entrante:")
                logger.info(f"[Messenger] - PSID: {sender_id}")
                logger.info(f"[Messenger] - Page ID: {page_id}")
                logger.info(f"[Messenger] - Message ID: {mid}")
                logger.info(f"[Messenger] - Texto: '{text}'")

                if not text:
                    logger.info("[Messenger] ⚠️ Mensaje sin texto - omitiendo")
                    continue

                # Obtener token de página
                company_id, page_token = get_messenger_token_by_page(page_id)
                if not page_token:
                    logger.error(f"[Messenger] ❌ No hay token para page_id={page_id}")
                    continue

                # Definir chat_id una sola vez (mover antes del try para evitar errores)
                chat_id = f"messenger:{page_id}:{sender_id}"
                chat_url = f"https://www.facebook.com/messages/t/{sender_id}"

                # Verificar si ya tenemos un lead usando el teléfono
                existing_lead = False
                clean_phone = PhoneUtils.strip_34(resolve_phone_for_psid(page_id, sender_id) or '')
                try:
                    sql = """
                    SELECT id 
                    FROM public.leads 
                    WHERE phone = %s
                    AND COALESCE(is_deleted, false) = false
                    LIMIT 1
                    """
                    row = db_manager.execute_query(sql, [clean_phone], fetch_one=True)
                    existing_lead = bool(row)
                    
                    if clean_phone:
                        # Actualizar sender_phone en los mensajes existentes del chat
                        update_sql = """
                        UPDATE public.external_messages
                        SET sender_phone = %s,
                            updated_at = NOW()
                        WHERE chat_id = %s
                        AND sender_phone IS NULL
                        """
                        db_manager.execute_query(update_sql, [clean_phone, chat_id], fetch_one=False)
                    
                    logger.info(f"[Messenger] Lead existente: {existing_lead} (phone: {clean_phone})")
                except Exception as e:
                    logger.exception("[Messenger] Error verificando lead existente")

                if existing_lead:
                    # Si ya tenemos sus datos, enviar mensaje de espera
                    response = "En estos momentos no hay operadores disponibles. Te contactaremos lo antes posible. Gracias por tu paciencia."
                    send_messenger_text(page_token, sender_id, response)
                    
                    # Guardar mensaje y respuesta
                    save_external_message(text, chat_id, chat_url, from_me=False, status='messenger_received')
                    save_external_message(response, chat_id, chat_url, from_me=True, status='messenger_sent')
                else:
                    # Procesar según el estado de la conversación
                    try:
                        # Buscar último estado en external_messages
                        sql_state = """
                        SELECT message, status 
                        FROM public.external_messages 
                        WHERE chat_id = %s 
                        AND from_me = 'true'
                        ORDER BY created_at DESC 
                        LIMIT 1
                        """
                        row = db_manager.execute_query(sql_state, [chat_id], fetch_one=True)
                        last_message = row[0] if row else None
                        
                        response = None  # Inicializar response
                        
                        # Determinar estado actual y siguiente acción
                        if not row or "nombre completo" in last_message.lower():
                            # Primera interacción o esperando nombre
                            nombre = text.strip()
                            if len(nombre) < 3:
                                response = "Por favor, introduce un nombre válido (mínimo 3 caracteres)"
                            else:
                                # Guardar nombre y pedir email
                                save_external_message(f"NOMBRE: {nombre}", chat_id, chat_url, from_me=False, status='messenger_data')
                                response = f"Gracias {nombre.split()[0]}. ¿Podrías proporcionarme tu email?"
                        
                        elif "email" in last_message.lower():
                            # Esperando email
                            email = text.strip().lower()
                            if not re.match(r"[^@]+@[^@]+\.[^@]+", email):
                                response = "Por favor, introduce un email válido"
                            else:
                                # Guardar email y pedir teléfono
                                save_external_message(f"EMAIL: {email}", chat_id, chat_url, from_me=False, status='messenger_data')
                                response = "Perfecto. Por último, ¿podrías proporcionarme tu número de teléfono?"
                        
                        elif "teléfono" in last_message.lower():
                            # Esperando teléfono
                            phone = ''.join(filter(str.isdigit, text))
                            if not re.match(r"^[6789]\d{8}$", phone):
                                response = "Por favor, introduce un número de teléfono español válido (9 dígitos)"
                            else:
                                # Tenemos todos los datos, crear lead
                                try:
                                    # CORRECCIÓN: Obtener datos previos usando fetch_all=True
                                    sql_data = """
                                    SELECT message FROM public.external_messages 
                                    WHERE chat_id = %s AND (message LIKE 'NOMBRE:%' OR message LIKE 'EMAIL:%')
                                    ORDER BY created_at ASC
                                    """
                                    rows = db_manager.execute_query(sql_data, [chat_id], fetch_all=True)
                                    
                                    # Verificar que rows sea una lista/tupla antes de iterar
                                    if not rows:
                                        logger.error("[Messenger] No se encontraron datos previos")
                                        response = "Error: No se encontraron tus datos anteriores. Por favor, comienza de nuevo."
                                    else:
                                        datos = {}
                                        # Ahora rows debería ser iterable
                                        for r in rows:
                                            message_text = r[0] if isinstance(r, (list, tuple)) else r
                                            if message_text.startswith("NOMBRE: "):
                                                datos['nombre'] = message_text.replace("NOMBRE: ", "")
                                            elif message_text.startswith("EMAIL: "):
                                                datos['email'] = message_text.replace("EMAIL: ", "")
                                        
                                        # Verificar que tenemos todos los datos
                                        if 'nombre' not in datos or 'email' not in datos:
                                            logger.error(f"[Messenger] Datos incompletos: {datos}")
                                            response = "Error: Datos incompletos. Por favor, comienza de nuevo."
                                        else:
                                            # Guardar teléfono
                                            save_external_message(f"TELEFONO: {phone}", chat_id, chat_url, from_me=False, status='messenger_data')
                                            
                                            # Crear lead
                                            lead_data = {
                                                'nombre_y_apellidos': datos['nombre'],
                                                'correo_electrónico': datos['email'],
                                                'número_de_teléfono': phone,
                                                'source': 'messenger',
                                                'company_name': 'default'  # O usar company_id para determinar
                                            }
                                            
                                            lead_id = create_portal_user(lead_data)
                                            
                                            # Guardar PSID en properties del lead
                                            if lead_id:
                                                save_lead_property(lead_id, 'MESSENGER_PSID', sender_id)
                                                response = "¡Gracias por proporcionar tus datos! En estos momentos no hay operadores disponibles, pero te contactaremos lo antes posible."
                                            else:
                                                response = "Error al crear tu perfil. Por favor, inténtalo de nuevo más tarde."
                                
                                except Exception as e:
                                    logger.exception("[Messenger] Error creando lead")
                                    response = "Lo siento, ha ocurrido un error procesando tus datos. Por favor, inténtalo de nuevo más tarde."
                        
                        else:
                            # Estado inicial o no reconocido
                            response = "¡Hola! Para poder ayudarte mejor, ¿podrías proporcionarme tu nombre completo?"

                        # Enviar respuesta
                        if response:
                            send_messenger_text(page_token, sender_id, response)
                            
                            # Guardar mensaje del usuario y respuesta del bot
                            save_external_message(text, chat_id, chat_url, from_me=False, status='messenger_received')
                            save_external_message(response, chat_id, chat_url, from_me=True, status='messenger_sent')

                    except Exception as e:
                        logger.exception("[Messenger] Error en flujo conversacional")
                        response = "Lo siento, ha ocurrido un error. Por favor, inténtalo de nuevo."
                        send_messenger_text(page_token, sender_id, response)
                        
                        # Guardar error
                        save_external_message(text, chat_id, chat_url, from_me=False, status='messenger_received')
                        save_external_message(response, chat_id, chat_url, from_me=True, status='messenger_error')

        logger.info("[Messenger] ✅ Procesamiento de webhook completado")
        return 'ok', 200
        
    except Exception as e:
        logger.exception(f"[Messenger] 💥 Error general en webhook: {str(e)}")
        return 'ok', 200


def save_external_message(message, chat_id, chat_url, from_me=False, status='received'):
    """Función auxiliar para guardar mensajes en external_messages"""
    try:
        db_manager.execute_query("""
            INSERT INTO public.external_messages (
                id, message, sender_phone, chat_id, chat_url, from_me, status, created_at, updated_at, is_deleted
            ) VALUES (
                gen_random_uuid(), %s, NULL, %s, %s, %s, %s, NOW(), NOW(), FALSE
            )
        """, [message, chat_id, chat_url, from_me, status], fetch_one=False)
        logger.info(f"[Messenger] ✅ Mensaje guardado: {status}")
    except Exception as e:
        logger.exception(f"[Messenger] Error guardando mensaje: {status}")


def save_lead_property(lead_id, property_name, property_value):
    """Función auxiliar para guardar propiedades del lead"""
    try:
        # Buscar property_id
        sql_prop = "SELECT id FROM public.properties WHERE property_name = %s LIMIT 1"
        prop_row = db_manager.execute_query(sql_prop, [property_name], fetch_one=True)
        
        if prop_row:
            property_id = prop_row[0]
            # Insertar valor de propiedad
            db_manager.execute_query("""
                INSERT INTO public.object_property_values (
                    id, object_reference_type, object_reference_id, property_id, value, created_at, updated_at
                ) VALUES (
                    gen_random_uuid(), 'leads', %s, %s, %s, NOW(), NOW()
                )
            """, [lead_id, property_id, property_value], fetch_one=False)
            logger.info(f"[Messenger] ✅ Propiedad {property_name} guardada para lead {lead_id}")
        else:
            logger.error(f"[Messenger] Property {property_name} no encontrada")
    except Exception as e:
        logger.exception(f"[Messenger] Error guardando propiedad {property_name}")
# =================================================================================
# OPCIONAL: ENDPOINTS DE ADMINISTRACIÓN (añadir al final de tu archivo)
# =================================================================================

@app.route('/messenger/config', methods=['GET', 'POST'])
def messenger_config_admin():
    """Endpoint para ver/modificar la configuración del flujo conversacional"""
    global MESSENGER_CONVERSATION_CONFIG
    
    if request.method == 'GET':
        return jsonify({
            'status': 'success',
            'current_config': MESSENGER_CONVERSATION_CONFIG,
            'conversation_states': [state.value for state in ConversationState]
        }), 200
    
    elif request.method == 'POST':
        try:
            data = request.get_json(force=True)
            
            if 'enabled' in data:
                MESSENGER_CONVERSATION_CONFIG['enabled'] = bool(data['enabled'])
            if 'timeout_hours' in data:
                MESSENGER_CONVERSATION_CONFIG['timeout_hours'] = max(1, int(data['timeout_hours']))
            if 'source' in data:
                MESSENGER_CONVERSATION_CONFIG['source'] = str(data['source'])
            if 'company_name' in data:
                MESSENGER_CONVERSATION_CONFIG['company_name'] = str(data['company_name'])
            
            logger.info(f"[Messenger Config] Configuration updated: {MESSENGER_CONVERSATION_CONFIG}")
            
            return jsonify({
                'status': 'success',
                'message': 'Configuration updated successfully',
                'new_config': MESSENGER_CONVERSATION_CONFIG
            }), 200
            
        except Exception as e:
            logger.exception("[Messenger Config] Error updating configuration")
            return jsonify({
                'status': 'error',
                'message': f'Error updating configuration: {str(e)}'
            }), 500

@app.route('/messenger/reset/<psid>', methods=['POST'])
def reset_messenger_conversation(psid):
    """Endpoint para resetear una conversación (útil para testing)"""
    try:
        page_id = request.json.get('page_id', 'unknown') if request.json else 'unknown'
        manager = get_messenger_conversation_manager()
        
        # Resetear a estado inicial
        manager.save_conversation_state(psid, page_id, ConversationState.INITIAL, {})
        
        logger.info(f"[Messenger] Conversation reset for PSID {psid}")
        
        return jsonify({
            'status': 'success',
            'message': f'Conversation reset for PSID {psid}',
            'new_state': ConversationState.INITIAL.value
        }), 200
        
    except Exception as e:
        logger.exception(f"[Messenger] Error resetting conversation for {psid}")
        return jsonify({
            'status': 'error',
            'message': f'Error resetting conversation: {str(e)}'
        }), 500

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

WABA_WINDOW_SEC = 24 * 60 * 60
TZ = ZoneInfo("Europe/Madrid")

def _now():
    return datetime.now(TZ)

def _get_cfg_db():
    global config, db
    try:
        cfg = config
    except NameError:
        cfg = Config()
    try:
        dbm = db
    except NameError:
        dbm = DatabaseManager(cfg.db_config)
    return cfg, dbm

def _normalize_phone_candidates(phone: str | None) -> list[str]:
    if not phone:
        return []
    digits = ''.join(ch for ch in phone if ch.isdigit())
    if not digits:
        return []
    if digits.startswith('34'):
        return [digits, digits[2:]]
    else:
        return [digits, '34' + digits]

def _lead_exists(dbm, lead_id: str) -> bool:
    row = dbm.execute_query(
        "SELECT 1 FROM public.leads WHERE id = %s LIMIT 1",
        [lead_id], fetch_one=True
    )
    return bool(row)

# --- REEMPLAZA _get_lead_phone por esta versión ---
def _get_lead_phone(dbm, lead_id: str) -> str | None:
    """
    Solo consulta la tabla si el id es un UUID válido. Si no lo es, devolvemos None.
    Evita ERROR: invalid input syntax for type uuid (22P02).
    """
    if not lead_id or not _is_valid_uuid(lead_id):
        return None
    row = dbm.execute_query(
        "SELECT phone FROM public.leads WHERE id = %s LIMIT 1",
        [lead_id], fetch_one=True
    )
    return row[0] if row and row[0] else None

# --- REEMPLAZA _last_user_message_ts por esta versión ---
def _last_user_message_ts(dbm, phone: str) -> datetime | None:
    """
    En tu DDL, from_me es TEXT -> hay que comparar con 'false' (string), no boolean.
    """
    cands = _normalize_phone_candidates(phone)
    if not cands:
        return None
    row = dbm.execute_query(
        """
        SELECT last_message_timestamp
        FROM public.external_messages
        WHERE from_me = 'false'
          AND sender_phone IN (%s, %s)
        ORDER BY last_message_timestamp DESC
        LIMIT 1
        """,
        [cands[0], cands[1] if len(cands) > 1 else cands[0]],
        fetch_one=True
    )
    return row[0] if row and row[0] else None

def _can_send_message(last_user_ts: datetime | None) -> tuple[bool, int]:
    if last_user_ts is None:
        return False, 0
    if last_user_ts.tzinfo is None:
        last_user_ts = last_user_ts.replace(tzinfo=TZ)
    end_ts = last_user_ts + timedelta(seconds=WABA_WINDOW_SEC)
    now = _now()
    if now >= end_ts:
        return False, 0
    return True, max(0, int((end_ts - now).total_seconds()))
# --- NUEVO: validador de UUID ---
from uuid import UUID

def _is_valid_uuid(v: str) -> bool:
    try:
        UUID(str(v))
        return True
    except Exception:
        return False


# ==========================
# Endpoints directos con app
# ==========================


# =========================================================================
# ENDPOINTS ADICIONALES PARA MONITOREO DE ESTADOS
# =========================================================================

@app.route('/message_status_stats', methods=['GET'])
def message_status_statistics():
    """Endpoint para obtener estadísticas de estados de mensajes"""
    try:
        stats = get_status_statistics(db_manager)
        
        return jsonify({
            'status': 'success',
            'timestamp': now_madrid().isoformat(),
            'statistics': stats,
            'flows_implemented': {
                'templates': 'template_sent → template_delivered → template_read | template_failed',
                'text': 'sent → message_delivered → message_read | message_failed',
                'media': 'media_sent → media_delivered → media_read | media_failed',
                'auto_responses': 'autoresponse_delivered (unchanged)'
            }
        }), 200
        
    except Exception as e:
        logger.exception('Error getting message status statistics')
        return jsonify({
            'status': 'error',
            'message': f'Error getting statistics: {str(e)}'
        }), 500


@app.route('/migrate_message_statuses', methods=['POST'])
def migrate_statuses_endpoint():
    """
    Endpoint para migrar estados existentes al nuevo sistema.
    ⚠️ Solo usar durante la implementación inicial.
    """
    try:
        # Verificar que es una llamada autorizada (puedes agregar auth aquí)
        data = request.get_json(silent=True) or {}
        confirm_migration = data.get('confirm_migration', False)
        
        if not confirm_migration:
            return jsonify({
                'status': 'error',
                'message': 'Migration requires explicit confirmation. Send {"confirm_migration": true}'
            }), 400
        
        # Ejecutar migración
        result = migrate_existing_message_statuses(db_manager)
        
        if result['success']:
            return jsonify({
                'status': 'success',
                'message': 'Migration completed successfully',
                'results': result
            }), 200
        else:
            return jsonify({
                'status': 'error',
                'message': 'Migration failed',
                'error': result.get('error')
            }), 500
            
    except Exception as e:
        logger.exception('Error in status migration endpoint')
        return jsonify({
            'status': 'error',
            'message': f'Migration error: {str(e)}'
        }), 500


@app.route('/message_status/<message_id>', methods=['GET'])
def get_message_status(message_id):
    """Obtener el estado actual de un mensaje específico"""
    try:
        query = """
            SELECT 
                id, status, message, sender_phone, from_me,
                last_message_timestamp, created_at, updated_at
            FROM public.external_messages 
            WHERE last_message_uid = %s
            LIMIT 1
        """
        
        result = db_manager.execute_query(query, [message_id], fetch_one=True)
        
        if not result:
            return jsonify({
                'status': 'error',
                'message': f'Message not found: {message_id}'
            }), 404
        
        msg_info = {
            'message_id': message_id,
            'internal_id': str(result[0]),
            'current_status': result[1],
            'content_preview': (result[2] or '')[:100] + '...' if len(result[2] or '') > 100 else result[2],
            'sender_phone': result[3],
            'from_me': result[4],
            'whatsapp_timestamp': result[5].isoformat() if result[5] else None,
            'created_at': result[6].isoformat() if result[6] else None,
            'updated_at': result[7].isoformat() if result[7] else None
        }
        
        # Determinar posibles próximos estados
        current_status = result[1]
        possible_next = []
        
        if current_status == 'template_sent':
            possible_next = ['template_delivered', 'template_failed']
        elif current_status == 'template_delivered':
            possible_next = ['template_read', 'template_failed']
        elif current_status == 'sent':
            possible_next = ['message_delivered', 'message_failed']
        elif current_status == 'message_delivered':
            possible_next = ['message_read', 'message_failed']
        elif current_status == 'media_sent':
            possible_next = ['media_delivered', 'media_failed']
        elif current_status == 'media_delivered':
            possible_next = ['media_read', 'media_failed']
        
        msg_info['possible_next_statuses'] = possible_next
        
        return jsonify({
            'status': 'success',
            'message_info': msg_info
        }), 200
        
    except Exception as e:
        logger.exception(f'Error getting message status for {message_id}')
        return jsonify({
            'status': 'error',
            'message': f'Error retrieving message status: {str(e)}'
        }), 500


@app.route('/test_status_update', methods=['POST'])
def test_status_update():
    """
    Endpoint para probar manualmente la actualización de estados.
    Útil para debugging y testing.
    """
    try:
        data = request.get_json(force=True)
        message_id = data.get('message_id')
        whatsapp_status = data.get('whatsapp_status')
        
        if not message_id or not whatsapp_status:
            return jsonify({
                'status': 'error',
                'message': 'message_id and whatsapp_status are required'
            }), 400
        
        if not is_valid_whatsapp_status(whatsapp_status):
            return jsonify({
                'status': 'error',
                'message': f'Invalid whatsapp_status. Valid values: delivered, read, failed, sent'
            }), 400
        
        # Obtener estado actual antes de la actualización
        get_sql = "SELECT status FROM public.external_messages WHERE last_message_uid = %s LIMIT 1"
        before = db_manager.execute_query(get_sql, [message_id], fetch_one=True)
        
        if not before:
            return jsonify({
                'status': 'error',
                'message': f'Message not found: {message_id}'
            }), 404
        
        status_before = before[0]
        
        # Realizar actualización
        success = message_service.update_outgoing_status(message_id, whatsapp_status)
        
        # Obtener estado después de la actualización
        after = db_manager.execute_query(get_sql, [message_id], fetch_one=True)
        status_after = after[0] if after else None
        
        return jsonify({
            'status': 'success',
            'message_id': message_id,
            'test_input': {
                'whatsapp_status': whatsapp_status
            },
            'results': {
                'update_successful': success,
                'status_before': status_before,
                'status_after': status_after,
                'status_changed': status_before != status_after
            }
        }), 200
        
    except Exception as e:
        logger.exception('Error in test status update')
        return jsonify({
            'status': 'error',
            'message': f'Test error: {str(e)}'
        }), 500

###WARM UP SYSTEM

@app.route('/send_template_direct', methods=['POST'])
def send_template_direct():
    """
    Envía un template directamente a WhatsApp sin guardar nada en base de datos.
    
    Payload esperado:
    {
        "phone": "679609016",
        "template_name": "agendar_llamada_inicial", 
        "template_data": {
            "first_name": "Juan",
            "deal_id": "123",
            "responsible_first_name": "Ana",
            "company_name": "Mi Empresa"
        }
    }
    """
    try:
        data = request.get_json(force=True)
        
        # Validar parámetros requeridos
        phone = data.get('phone')
        template_name = data.get('template_name')
        template_data = data.get('template_data', {})
        
        if not phone:
            return jsonify({
                'status': 'error',
                'message': 'phone es requerido'
            }), 400
            
        if not template_name:
            return jsonify({
                'status': 'error', 
                'message': 'template_name es requerido'
            }), 400

        # Normalizar teléfono a formato internacional
        destination = PhoneUtils.add_34(phone)
        
        logger.info(f"[DIRECT TEMPLATE] Enviando '{template_name}' a {destination}")
        logger.info(f"[DIRECT TEMPLATE] Template data: {template_data}")
        
        # Construir payload del template
        payload = _build_template_payload_direct(template_name, template_data, destination)
        
        # Enviar directamente a WhatsApp
        response = requests.post(
            config.whatsapp_config['base_url'],
            headers=config.whatsapp_config['headers'],
            json=payload,
            timeout=15
        )
        
        logger.info(f"[DIRECT TEMPLATE] WhatsApp response: {response.status_code}")
        
        if response.ok:
            result = response.json()
            message_id = result.get('messages', [{}])[0].get('id')
            
            logger.info(f"[DIRECT TEMPLATE] ✅ Enviado exitosamente. ID: {message_id}")
            
            return jsonify({
                'status': 'success',
                'message_id': message_id,
                'sent_to': destination,
                'template_name': template_name,
                'whatsapp_response': result
            }), 200
        else:
            error_detail = response.text
            logger.error(f"[DIRECT TEMPLATE] ❌ Error {response.status_code}: {error_detail}")
            
            return jsonify({
                'status': 'error',
                'message': f'WhatsApp API error: {response.status_code}',
                'details': error_detail,
                'sent_to': destination,
                'template_name': template_name
            }), response.status_code

    except ValueError as e:
        logger.error(f"[DIRECT TEMPLATE] Template validation error: {e}")
        return jsonify({
            'status': 'error',
            'message': f'Template error: {str(e)}'
        }), 400
        
    except requests.exceptions.RequestException as e:
        logger.error(f"[DIRECT TEMPLATE] Request error: {e}")
        return jsonify({
            'status': 'error',
            'message': f'Network error: {str(e)}'
        }), 500
        
    except Exception as e:
        logger.exception(f"[DIRECT TEMPLATE] Unexpected error: {e}")
        return jsonify({
            'status': 'error',
            'message': f'Internal error: {str(e)}'
        }), 500


@app.route('/send_text_direct', methods=['POST'])
def send_text_direct():
    """
    Envía un mensaje de texto directamente sin guardar en BD.
    
    Payload:
    {
        "phone": "679609016",
        "message": "Hola, este es un mensaje directo"
    }
    """
    try:
        data = request.get_json(force=True)
        
        phone = data.get('phone')
        message = data.get('message')
        
        if not phone or not message:
            return jsonify({
                'status': 'error',
                'message': 'phone y message son requeridos'
            }), 400

        destination = PhoneUtils.add_34(phone)
        
        payload = {
            "messaging_product": "whatsapp",
            "to": destination,
            "type": "text",
            "text": {"body": message}
        }
        
        logger.info(f"[DIRECT TEXT] Enviando texto a {destination}: {message[:50]}...")
        
        response = requests.post(
            config.whatsapp_config['base_url'],
            headers=config.whatsapp_config['headers'],
            json=payload,
            timeout=15
        )
        
        if response.ok:
            result = response.json()
            message_id = result.get('messages', [{}])[0].get('id')
            
            logger.info(f"[DIRECT TEXT] ✅ Texto enviado. ID: {message_id}")
            
            return jsonify({
                'status': 'success',
                'message_id': message_id,
                'sent_to': destination,
                'message_preview': message[:100]
            }), 200
        else:
            error_detail = response.text
            logger.error(f"[DIRECT TEXT] ❌ Error {response.status_code}: {error_detail}")
            
            return jsonify({
                'status': 'error',
                'message': f'WhatsApp API error: {response.status_code}',
                'details': error_detail
            }), response.status_code

    except Exception as e:
        logger.exception(f"[DIRECT TEXT] Error: {e}")
        return jsonify({
            'status': 'error',
            'message': f'Internal error: {str(e)}'
        }), 500


@app.route('/templates_direct', methods=['GET'])
def get_available_templates_direct():
    """
    Lista los templates disponibles para envío directo.
    """
    templates = [
        {
            "name": "agendar_llamada_inicial",
            "description": "Template para agendar llamada inicial",
            "required_params": ["first_name"],
            "optional_params": ["deal_id"]
        },
        {
            "name": "nuevo_numero",
            "description": "Template para nuevo número de contacto",
            "required_params": ["first_name"],
            "optional_params": ["deal_id", "responsible_first_name", "responsible_name"]
        },
        {
            "name": "followup_missed_calls", 
            "description": "Template para seguimiento de llamadas perdidas",
            "required_params": ["first_name"],
            "optional_params": ["deal_id"]
        },
        {
            "name": "recordatorio_llamada_agendada",
            "description": "Template recordatorio de llamada agendada",
            "required_params": ["first_name"],
            "optional_params": ["responsible_first_name", "responsible_name", "company_name"]
        },
        {
            "name": "retomar_contacto",
            "description": "Template para retomar contacto",
            "required_params": ["first_name"],
            "optional_params": ["responsible_first_name", "responsible_name", "company_name"]
        }
    ]
    
    return jsonify({
        'status': 'success',
        'available_templates': templates,
        'total_templates': len(templates),
        'note': 'Estos templates no requieren datos de BD y se envían directamente'
    }), 200

###WARM UP SYSTEM


# Función para migrar estados existentes (si es necesario)
def migrate_existing_message_statuses(db_manager) -> dict:
    """
    Migra mensajes existentes que tienen estados antiguos a los nuevos flujos.
    ⚠️ Usar con cuidado - solo para migración inicial.
    
    Returns:
        Dict con resultados de la migración
    """
    try:
        # Mapeo de estados antiguos a nuevos
        migration_map = {
            'delivered': {
                'template_sent': 'template_delivered',
                'sent': 'message_delivered',
                'media_sent': 'media_delivered'
            },
            'read': {
                'template_delivered': 'template_read',
                'message_delivered': 'message_read', 
                'media_delivered': 'media_read'
            },
            'failed': {
                'template_sent': 'template_failed',
                'sent': 'message_failed',
                'media_sent': 'media_failed'
            }
        }
        
        migrated_count = 0
        errors = []
        
        # Obtener mensajes con estados antiguos
        old_statuses_sql = """
            SELECT id, status, message, from_me
            FROM public.external_messages 
            WHERE status IN ('delivered', 'read', 'failed')
              AND from_me = 'true'
        """
        
        old_messages = db_manager.execute_query(old_statuses_sql, fetch_all=True)
        
        for msg_id, old_status, message_content, from_me in old_messages:
            try:
                # Determinar el tipo de mensaje original
                is_template = 'template' in (message_content or '').lower()
                is_media = any(media_word in (message_content or '').lower() 
                             for media_word in ['📎', 'media', 'image', 'video', 'audio', 'document'])
                
                # Determinar estado base
                if is_template:
                    base_status = 'template_sent'
                elif is_media:
                    base_status = 'media_sent'
                else:
                    base_status = 'sent'
                
                # Obtener nuevo estado
                new_status = migration_map.get(old_status, {}).get(base_status)
                
                if new_status:
                    update_sql = """
                        UPDATE public.external_messages 
                        SET status = %s, updated_at = NOW()
                        WHERE id = %s
                    """
                    db_manager.execute_query(update_sql, [new_status, msg_id])
                    migrated_count += 1
                    
            except Exception as e:
                errors.append(f"Error migrando mensaje {msg_id}: {e}")
        
        return {
            'success': True,
            'migrated_count': migrated_count,
            'errors': errors
        }
        
    except Exception as e:
        logger.exception("Error en migración de estados")
        return {
            'success': False,
            'error': str(e)
        }

def _last_template_sent_ts(dbm, phone: str) -> datetime | None:
    """
    Obtiene el timestamp del último template enviado a un teléfono específico.
    Busca CUALQUIER status de template (template_sent, template_delivered, template_read, template_failed).
    """
    cands = _normalize_phone_candidates(phone)
    logger.info(f"[TEMPLATE CHECK] Phone: {phone} -> Candidates: {cands}")
    
    if not cands:
        logger.warning(f"[TEMPLATE CHECK] No valid phone candidates for: {phone}")
        return None
    
    # Primero, veamos qué templates hay en la BD para este teléfono
    debug_query = """
        SELECT status, from_me, sender_phone, last_message_timestamp, message
        FROM public.external_messages
        WHERE sender_phone IN (%s, %s)
          AND (status LIKE '%template%' OR from_me = 'true')
        ORDER BY last_message_timestamp DESC
        LIMIT 5
    """
    debug_rows = dbm.execute_query(
        debug_query,
        [cands[0], cands[1] if len(cands) > 1 else cands[0]],
        fetch_all=True
    )
    
    logger.info(f"[TEMPLATE CHECK] Debug - Recent messages for {phone}:")
    if debug_rows:
        for i, row in enumerate(debug_rows):
            logger.info(f"  [{i+1}] Status: {row[0]}, From_me: {row[1]}, Phone: {row[2]}, Time: {row[3]}, Message: {str(row[4])[:100]}...")
    else:
        logger.info("  No messages found")
    
    # 🔧 CAMBIO PRINCIPAL: Buscar cualquier template, no solo template_sent
    row = dbm.execute_query(
        """
        SELECT last_message_timestamp, message, status
        FROM public.external_messages
        WHERE from_me = 'true'
          AND message LIKE '%template%'
          AND sender_phone IN (%s, %s)
        ORDER BY last_message_timestamp DESC
        LIMIT 1
        """,
        [cands[0], cands[1] if len(cands) > 1 else cands[0]],
        fetch_one=True
    )
    
    if row and row[0]:
        logger.info(f"[TEMPLATE CHECK] Last template found: {row[0]} (status: {row[2]}) - Message: {str(row[1])[:100]}...")
        return row[0]
    else:
        logger.info(f"[TEMPLATE CHECK] No templates found for candidates: {cands}")
        return None

def _can_send_template(last_template_ts: datetime | None, phone: str, dbm) -> tuple[bool, int, str]:
    """
    Verifica si se puede enviar un template con lógica de support window.
    
    Reglas:
    1. Si el cliente escribió hace menos de 24h -> Support window activa -> SÍ se puede enviar
    2. Si no hay support window activa -> Solo 1 template cada 24h
    
    Returns: (can_send, seconds_remaining, reason)
    """
    now = _now()
    logger.info(f"[TEMPLATE CHECK] Current time: {now.isoformat()}")
    
    # 1. Verificar support window (último mensaje del cliente)
    last_user_message_ts = _last_user_message_ts(dbm, phone)
    logger.info(f"[TEMPLATE CHECK] Last user message: {last_user_message_ts.isoformat() if last_user_message_ts else 'None'}")
    
    if last_user_message_ts:
        if last_user_message_ts.tzinfo is None:
            last_user_message_ts = last_user_message_ts.replace(tzinfo=TZ)
        
        support_window_end = last_user_message_ts + timedelta(seconds=WABA_WINDOW_SEC)
        time_since_user_msg = (now - last_user_message_ts).total_seconds()
        
        logger.info(f"[TEMPLATE CHECK] Support window end: {support_window_end.isoformat()}")
        logger.info(f"[TEMPLATE CHECK] Time since user message: {time_since_user_msg/3600:.1f} hours")
        
        if now < support_window_end:
            # Support window activa - se puede enviar template
            remaining_window = max(0, int((support_window_end - now).total_seconds()))
            logger.info(f"[TEMPLATE CHECK] SUPPORT WINDOW ACTIVE - CAN SEND (window expires in {remaining_window/3600:.1f}h)")
            return True, 0, f"Support window active (expires in {remaining_window/3600:.1f}h)"
    
    # 2. No hay support window - aplicar restricción de 1 template/24h
    logger.info("[TEMPLATE CHECK] No active support window - checking 24h template restriction")
    
    if last_template_ts is None:
        logger.info("[TEMPLATE CHECK] No previous template found - CAN SEND")
        return True, 0, "No previous templates sent"
    
    if last_template_ts.tzinfo is None:
        last_template_ts = last_template_ts.replace(tzinfo=TZ)
    
    next_allowed_ts = last_template_ts + timedelta(seconds=WABA_WINDOW_SEC)
    time_diff = (now - last_template_ts).total_seconds()
    
    logger.info(f"[TEMPLATE CHECK] Last template: {last_template_ts.isoformat()}")
    logger.info(f"[TEMPLATE CHECK] Next allowed: {next_allowed_ts.isoformat()}")
    logger.info(f"[TEMPLATE CHECK] Time since last template: {time_diff/3600:.1f} hours")
    
    if now >= next_allowed_ts:
        logger.info("[TEMPLATE CHECK] 24h elapsed since last template - CAN SEND")
        return True, 0, f"24h elapsed since last template ({time_diff/3600:.1f}h ago)"
    
    seconds_remaining = max(0, int((next_allowed_ts - now).total_seconds()))
    logger.info(f"[TEMPLATE CHECK] Template restriction active - CANNOT SEND ({seconds_remaining/3600:.1f}h remaining)")
    return False, seconds_remaining, f"24h template limit active (last sent {time_diff/3600:.1f}h ago)"

@app.route("/canSendTemplate", methods=["POST"])
def can_send_template():
    """
    Verifica si se puede enviar un template con lógica de support window.
    
    Reglas:
    1. Si el cliente escribió hace menos de 24h -> Support window activa -> SÍ se puede enviar
    2. Si no hay support window activa -> Solo 1 template cada 24h
    """
    logger.info("=" * 50)
    logger.info("[TEMPLATE CHECK] Iniciando verificación de template con support window")
    
    _, dbm = _get_cfg_db()
    data = request.get_json(silent=True) or {}
    id_ = data.get("id")
    phone_override = data.get("phone")
    
    logger.info(f"[TEMPLATE CHECK] Request data - ID: {id_}, Phone: {phone_override}")

    if not id_ and not phone_override:
        logger.error("[TEMPLATE CHECK] Error: Falta id o phone")
        return jsonify({"ok": False, "error": "Falta id o phone"}), 400

    phone = None
    if phone_override:
        phone = phone_override
        logger.info(f"[TEMPLATE CHECK] Using phone override: {phone}")
    elif _is_valid_uuid(id_):
        phone = _get_lead_phone(dbm, id_)
        logger.info(f"[TEMPLATE CHECK] Retrieved phone from lead {id_}: {phone}")
    else:
        logger.error(f"[TEMPLATE CHECK] Error: id '{id_}' no es UUID válido y no se proporcionó phone")
        return jsonify({"ok": False, "error": "id no es UUID válido y no se proporcionó phone"}), 400

    if not phone:
        logger.error(f"[TEMPLATE CHECK] Error: No se encontró teléfono para el id {id_}")
        return jsonify({"ok": False, "error": "No se encontró teléfono para el id"}), 404

    # Verificar último template enviado
    logger.info(f"[TEMPLATE CHECK] Verificando último template para teléfono: {phone}")
    last_template_ts = _last_template_sent_ts(dbm, phone)
    
    if last_template_ts and last_template_ts.tzinfo is None:
        last_template_ts = last_template_ts.replace(tzinfo=TZ)
    
    # Verificar si se puede enviar (con lógica de support window)
    can_send, seconds_remaining, reason = _can_send_template(last_template_ts, phone, dbm)

    result = {
        "ok": True,
        "id": id_,
        "phone": phone,
        "canSendTemplate": can_send,
        "reason": reason,
        "debug_info": {
            "phone_candidates": _normalize_phone_candidates(phone),
            "last_template_timestamp": last_template_ts.isoformat() if last_template_ts else None,
            "current_time": _now().isoformat(),
            "waba_window_hours": WABA_WINDOW_SEC / 3600
        }
    }

    # Agregar información adicional según el resultado
    if not can_send:
        result["secondsUntilNextTemplate"] = seconds_remaining
        result["hoursUntilNextTemplate"] = round(seconds_remaining / 3600, 1)
        
        if last_template_ts:
            result["lastTemplateSentAt"] = last_template_ts.isoformat()
            
        logger.info(f"[TEMPLATE CHECK] RESULTADO: NO SE PUEDE ENVIAR - {reason}")
    else:
        result["secondsUntilNextTemplate"] = 0
        
        if last_template_ts:
            result["lastTemplateSentAt"] = last_template_ts.isoformat()
            hours_since_last = round(((_now() - last_template_ts).total_seconds()) / 3600, 1)
            result["hoursSinceLastTemplate"] = hours_since_last
            logger.info(f"[TEMPLATE CHECK] RESULTADO: SÍ SE PUEDE ENVIAR - {reason}")
        else:
            logger.info(f"[TEMPLATE CHECK] RESULTADO: SÍ SE PUEDE ENVIAR - {reason}")

    # Agregar información del support window si está activo
    last_user_msg_ts = _last_user_message_ts(dbm, phone)
    if last_user_msg_ts:
        if last_user_msg_ts.tzinfo is None:
            last_user_msg_ts = last_user_msg_ts.replace(tzinfo=TZ)
        
        support_window_end = last_user_msg_ts + timedelta(seconds=WABA_WINDOW_SEC)
        now = _now()
        
        result["support_window"] = {
            "last_user_message_at": last_user_msg_ts.isoformat(),
            "support_window_active": now < support_window_end,
            "support_window_expires_at": support_window_end.isoformat(),
            "hours_since_user_message": round((now - last_user_msg_ts).total_seconds() / 3600, 1)
        }

    logger.info(f"[TEMPLATE CHECK] Respuesta final: {result}")
    logger.info("=" * 50)
    
    return jsonify(result), 200

@app.route("/canSendMessage", methods=["POST"])
def can_send_message():
    _, dbm = _get_cfg_db()
    data = request.get_json(silent=True) or {}
    id_ = data.get("id")
    phone_override = data.get("phone")

    if not id_ and not phone_override:
        return jsonify({"ok": False, "error": "Falta id o phone"}), 400

    phone = None
    if phone_override:
        phone = phone_override
    elif _is_valid_uuid(id_):
        phone = _get_lead_phone(dbm, id_)
    else:
        return jsonify({"ok": False, "error": "id no es UUID válido y no se proporcionó phone"}), 400

    if not phone:
        return jsonify({"ok": False, "error": "No se encontró teléfono para el id"}), 404

    last_ts = _last_user_message_ts(dbm, phone)
    is_open, secs = _can_send_message(last_ts)

    return jsonify({
        "ok": True,
        "id": id_,
        "phone": phone,
        "canSendMessage": is_open,
        "secondsUntilTemplateOnly": secs
    }), 200

@app.route("/timeToTemplate", methods=["POST"])
def time_to_template():
    _, dbm = _get_cfg_db()
    data = request.get_json(silent=True) or {}
    id_ = data.get("id")
    phone_override = data.get("phone")

    if not id_ and not phone_override:
        return jsonify({"ok": False, "error": "Falta id o phone"}), 400

    phone = None
    if phone_override:
        phone = phone_override
    elif _is_valid_uuid(id_):
        phone = _get_lead_phone(dbm, id_)
    else:
        return jsonify({"ok": False, "error": "id no es UUID válido y no se proporcionó phone"}), 400

    if not phone:
        return jsonify({"ok": False, "error": "No se encontró teléfono para el id"}), 404

    last_ts = _last_user_message_ts(dbm, phone)
    is_open, secs = _can_send_message(last_ts)

    return jsonify({
        "ok": True,
        "id": id_,
        "phone": phone,
        "secondsToTemplate": 0 if not is_open else secs
    }), 200



@app.route('/test_whatsapp_curl', methods=['POST'])
def test_whatsapp_curl():
    """Test usando exactamente la misma configuración que funciona en curl"""
    try:
        data = request.get_json(force=True)
        phone = data.get('phone', '34608684495')
        
        # Exactamente los mismos valores que tu curl exitoso
        url = 'https://graph.facebook.com/v22.0/734206063117516/messages'
        headers = {
            'Authorization': 'Bearer EAASXAvD0atABPbrErdkYRVwH2LLjfp9fTh6VqpZCodZADLb6SHJnoEiG5mwn3CKBs6yk2nO8ZB7C87mYFIiLMkI0QdZB230rCwxJfn2bXzcPE6HikaTkrZBphil4X4wkXi2g0ZB8KQZBZAuoeRG8imWaxmgyGKZCvK7g7OGHGarwEWlOmvJzTekqOjysZCEzm41ZAZCV0gfEn6ahjtqFuioHPt8qdrxhoEJntPren3P5Anab',
            'Content-Type': 'application/json'
        }
        
        payload = {
            "messaging_product": "whatsapp",
            "to": phone,
            "type": "template",
            "template": {
                "name": "hello_world",
                "language": {"code": "en_US"}
            }
        }
        
        logger.info(f"[CURL TEST] URL: {url}")
        logger.info(f"[CURL TEST] Headers: {headers}")
        logger.info(f"[CURL TEST] Payload: {payload}")
        
        response = requests.post(url, headers=headers, json=payload, timeout=15)
        
        return jsonify({
            'status': 'success' if response.ok else 'error',
            'status_code': response.status_code,
            'response': response.json() if response.ok else response.text
        }), 200 if response.ok else 500
        
    except Exception as e:
        logger.exception("[CURL TEST] Error")
        return jsonify({'status': 'error', 'message': str(e)}), 500
@app.route('/debug_tokens', methods=['GET'])
def debug_tokens():
    return jsonify({
        'access_token_from_config': f"{config.whatsapp_config['access_token'][:20]}...{config.whatsapp_config['access_token'][-10:]}",
        'access_token_global': f"{ACCESS_TOKEN[:20]}...{ACCESS_TOKEN[-10:]}",
        'phone_number_id': PHONE_NUMBER_ID,
        'base_url': WHATSAPP_BASE_URL,
        'tokens_match': config.whatsapp_config['access_token'] == ACCESS_TOKEN
    })    
##
@app.route('/', methods=['GET'])
def root():
    return jsonify({
        'service': 'WhatsApp Webhook Service',
        'status': 'running',
        'version': '2.0-extended',
        'waba_id_configured': WABA_ID,
        'configuration_status': 'ready' if WABA_ID else 'incomplete',
        'extended_mime_support': True,
        'features': [
            'Extended MIME type support (any valid type up to 100MB for documents)',
            'Auto-detection of WhatsApp message types',
            'Supabase file storage integration',
            'Flow exit management',
            'Auto-reply for office hours',
            'Template message support',
            'Media message processing'
        ]
    }), 200

def start_http_server():
    app.run(host=config.server_config['host'], port=config.server_config['http_port'], debug=False)

def start_https_server():
    ssl_context = None
    if config.server_config['ssl_cert'] and config.server_config['ssl_key']:
        ssl_context = (config.server_config['ssl_cert'], config.server_config['ssl_key'])
    app.run(host=config.server_config['host'], port=config.server_config['https_port'], ssl_context=ssl_context, debug=False)

if __name__ == '__main__':
    logger.info("Starting WhatsApp Webhook Service v2.0 with Extended MIME Type Support")
    if WABA_ID:
        logger.info(f"🎯 WABA_ID configurado correctamente: {WABA_ID}")
        logger.info("✅ ¡Listo para obtener templates!")
    else:
        logger.warning("❌ WABA_ID no configurado. El endpoint /get_templates no funcionará.")

    # Test ExtendedFileService on startup
    try:
        fs = get_file_service()
        support_info = fs.get_supported_types_info()
        logger.info(f"🎉 ExtendedFileService initialized successfully!")
        logger.info(f"📎 Total MIME types supported: {support_info['total_mime_types_supported']}")
        logger.info(f"📄 Documents accept any MIME type: {support_info['document_accepts_any_mime']}")
        logger.info(f"💾 Max sizes: {support_info['max_sizes']}")
    except Exception as e:
        logger.error(f"❌ Error initializing ExtendedFileService: {e}")

    # Start HTTP server in separate thread
    http_thread = Thread(target=start_http_server, daemon=True)
    http_thread.start()
    logger.info(f"HTTP server started on port {config.server_config['http_port']}")

    # Start HTTPS server in main thread
    logger.info(f"HTTPS server starting on port {config.server_config['https_port']}")
    start_https_server()