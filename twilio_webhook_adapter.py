#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Twilio to Meta Cloud API Webhook Adapter

This module provides a webhook endpoint that receives Twilio WhatsApp webhook events,
validates them, and then transforms them into Meta Cloud API format to be processed
by the existing WhatsApp Business Cloud API handling code.

The adapter acts as a bridge, making Twilio WhatsApp messages appear as if they came
directly from Meta's Cloud API, allowing for a unified processing pipeline.

Environment Variables:
    TWILIO_AUTH_TOKEN (required): Your Twilio Auth Token for signature validation
    TWILIO_ACCOUNT_SID (optional): Your Twilio Account SID (required for media proxy)
    TWILIO_USE_MEDIA_PROXY (optional): Set to 'true' to enable media URL proxying
    TWILIO_MEDIA_PROXY_URL (optional): Base URL for the media proxy service
    INTERNAL_HMAC_SECRET (optional): Secret key for internal HMAC signing

Features:
    - Validates Twilio webhook signatures
    - Normalizes phone numbers to E.164 format
    - Transforms all message types (text, media, location, contacts, interactive)
    - Handles message status callbacks
    - Proxies media URLs if configured
    - Forwards transformed data to existing Meta Cloud API webhook handler
"""

import os
import re
import time
import json
import uuid
import hashlib
import hmac
import base64
import logging
import urllib.parse
import traceback
from typing import Dict, List, Optional, Tuple, Union, Any
from flask import Flask, request, jsonify, Response
import requests
import vobject
from CloudAPIWebhook import app, db_manager, logger, config
# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Add a file handler for Twilio webhook debugging
if not logger.handlers:
    handler = logging.FileHandler('twilio_webhook.log')
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# Configuration (load from environment variables)
# Required variables:
TWILIO_AUTH_TOKEN = os.environ.get('TWILIO_AUTH_TOKEN')
if not TWILIO_AUTH_TOKEN:
    logger.warning("TWILIO_AUTH_TOKEN environment variable is not set. Webhook signature validation will fail!")

# Media proxy configuration
USE_MEDIA_PROXY = os.environ.get('TWILIO_USE_MEDIA_PROXY', 'false').lower() == 'true'
MEDIA_PROXY_BASE_URL = os.environ.get('TWILIO_MEDIA_PROXY_URL', '')

# Optional variables with defaults:
# Account SID is needed for media proxy authentication
TWILIO_ACCOUNT_SID = os.environ.get('TWILIO_ACCOUNT_SID', '')
if USE_MEDIA_PROXY and not TWILIO_ACCOUNT_SID:
    logger.warning("Media proxy is enabled but TWILIO_ACCOUNT_SID is not set")

# Optional: HMAC secret for internal service-to-service signing
INTERNAL_HMAC_SECRET = os.environ.get('INTERNAL_HMAC_SECRET', '')
if INTERNAL_HMAC_SECRET:
    logger.info("Internal HMAC signing is enabled")

# Log configuration status
logger.info(f"Twilio Webhook Adapter initialized. Media proxy: {'Enabled' if USE_MEDIA_PROXY else 'Disabled'}")
logger.info(f"Webhook endpoint will be available at: /webhookT")

# Utility functions
def log_and_capture_exception(message: str) -> str:
    """
    Log an exception and return the formatted traceback
    
    Args:
        message: The error message to log
        
    Returns:
        str: The formatted traceback
    """
    import traceback
    tb = traceback.format_exc()
    logger.exception(f"{message}\n{tb}")
    return tb

def validate_twilio_signature(request_url: str, form_data: Dict[str, str], signature: str) -> bool:
    """
    Validates the X-Twilio-Signature header against the request URL and form data.
    
    Args:
        request_url: The full URL of the request (without query parameters)
        form_data: The form data from the request
        signature: The X-Twilio-Signature header value
        
    Returns:
        bool: True if the signature is valid, False otherwise
    """
    # Sort the form data by key
    sorted_data = dict(sorted(form_data.items()))
    
    # Concatenate the URL with all key/value pairs from the request alphabetically
    validation_string = request_url
    
    # Append form parameters
    for key, value in sorted_data.items():
        validation_string += key + value
        
    # Create the HMAC-SHA1 hash of the validation string with the Twilio auth token
    mac = hmac.new(
        key=TWILIO_AUTH_TOKEN.encode('utf-8'),
        msg=validation_string.encode('utf-8'),
        digestmod=hashlib.sha1
    )
    
    # Base64 encode the hash
    expected = base64.b64encode(mac.digest()).decode('utf-8')
    
    # Compare the expected signature with the provided signature
    return hmac.compare_digest(expected, signature)

def normalize_msisdn(phone: str) -> str:
    """
    Normalize a phone number to E.164 format without the + prefix
    
    Args:
        phone: Phone number to normalize, may include "whatsapp:" prefix, +, spaces, etc.
        
    Returns:
        str: Normalized phone number (only digits)
    """
    # Return empty string for None or empty
    if not phone:
        return ""
        
    # Remove "whatsapp:" prefix if present
    phone = phone.replace("whatsapp:", "")
    
    # Remove any non-digit characters (spaces, parentheses, dashes, plus signs)
    digits_only = re.sub(r"[^0-9]", "", phone)
    
    # If it starts with 00, convert to international format
    if digits_only.startswith("00"):
        digits_only = digits_only[2:]
        
    return digits_only

def proxy_twilio_media_url(media_url: str) -> str:
    """
    Proxy a Twilio media URL if configured to do so
    
    This can be used to handle authentication for Twilio media URLs that
    require the Twilio Account SID and Auth Token to access.
    
    Args:
        media_url: The original Twilio media URL
        
    Returns:
        str: The proxied URL or the original URL if proxying is not configured
    """
    if not USE_MEDIA_PROXY:
        # Media proxying is disabled
        return media_url
        
    if not MEDIA_PROXY_BASE_URL:
        # Media proxy URL is not configured
        logger.warning("Twilio media proxy is enabled (TWILIO_USE_MEDIA_PROXY=true) but TWILIO_MEDIA_PROXY_URL is not set")
        return media_url
        
    if not TWILIO_ACCOUNT_SID:
        # Twilio account SID is not configured
        logger.warning("Twilio media proxy is enabled but TWILIO_ACCOUNT_SID is not set")
        return media_url
    
    try:
        # Create a proxy URL that your internal systems can use
        # This assumes you have a separate service that can fetch and serve Twilio media
        encoded_url = urllib.parse.quote_plus(media_url)
        proxy_url = f"{MEDIA_PROXY_BASE_URL.rstrip('/')}?url={encoded_url}&sid={TWILIO_ACCOUNT_SID}"
        
        logger.debug(f"Proxying Twilio media URL: {media_url} -> {proxy_url}")
        return proxy_url
    except Exception as e:
        tb = log_and_capture_exception(f"Error proxying Twilio media URL: {e}")
        # Return the original URL if there's an error
        return media_url

def get_receiver_context(db_manager, to_value: str) -> Tuple[str, str, str]:
    """
    Find the receiver in profiles table and generate metadata values
    
    Args:
        db_manager: Database manager to execute queries
        to_value: The 'To' value from Twilio payload (WhatsApp number that received the message)
    
    Returns:
        Tuple[str, str, str]: (display_phone_number, phone_number_id, entry_id)
    """
    # Normalize the receiver phone number
    receiver = normalize_msisdn(to_value)
    
    try:
        # Try to find the receiver in profiles table
        row = db_manager.execute_query(
            """
            SELECT id, phone
            FROM public.profiles
            WHERE regexp_replace(phone, '[^0-9]', '', 'g') = %s
            LIMIT 1
            """,
            [receiver],
            fetch_one=True
        )
        
        # Log whether we found a match or not
        if row:
            logger.info(f"Found matching profile for receiver {receiver}")
        else:
            logger.warning(f"No matching profile found for receiver {receiver}, using normalized value")
        
        # Calculate deterministic values based on the receiver
        display_phone_number = receiver
        phone_number_id = f"twilio:{display_phone_number}"
        entry_id = phone_number_id
        
        return display_phone_number, phone_number_id, entry_id
        
    except Exception as e:
        tb = log_and_capture_exception(f"Error getting receiver context: {e}")
        # Fallback to using normalized value directly
        display_phone_number = receiver
        phone_number_id = f"twilio:{display_phone_number}"
        entry_id = phone_number_id
        
        return display_phone_number, phone_number_id, entry_id

def build_meta_message(form_data: Dict[str, str], now_epoch: int, db_manager) -> Dict[str, Any]:
    """
    Build a Meta Cloud API format message from Twilio form data
    
    Args:
        form_data: The form data from the Twilio webhook
        now_epoch: Current timestamp in epoch seconds
        db_manager: Database manager for lookups
        
    Returns:
        dict: Meta Cloud API format message
    """
    # Log the raw form data for debugging
    logger.debug(f"Raw Twilio form data: {json.dumps(form_data)}")
    
    # Extract common fields
    from_phone = form_data.get('From', '')  # whatsapp:+34600000000
    wa_id = form_data.get('WaId', '')       # 34600000000 (no whatsapp: prefix)
    to_phone = form_data.get('To', '')      # whatsapp:+34999999999
    profile_name = form_data.get('ProfileName', wa_id)
    message_sid = form_data.get('MessageSid', '')
    
    # Normalize the sender's phone number if WaId is not provided
    if not wa_id and from_phone:
        wa_id = normalize_msisdn(from_phone)
    
    # Get receiver context for metadata
    display_phone_number, phone_number_id, entry_id = get_receiver_context(db_manager, to_phone)
    
    # Create basic message structure
    meta_payload = {
        "object": "whatsapp_business_account",
        "entry": [{
            "id": entry_id,
            "changes": [{
                "field": "messages",
                "value": {
                    "messaging_product": "whatsapp",
                    "metadata": {
                        "display_phone_number": display_phone_number,
                        "phone_number_id": phone_number_id
                    },
                    "contacts": [{
                        "profile": {"name": profile_name},
                        "wa_id": wa_id
                    }],
                    "messages": []
                }
            }]
        }]
    }
    
    # Detect message type and add appropriate data
    message_type = detect_message_type(form_data)
    logger.info(f"Detected message type: {message_type}")
    
    # Add message based on detected type
    if message_type == "text":
        add_text_message(meta_payload, form_data, wa_id, message_sid, now_epoch)
    elif message_type.startswith("media_"):
        add_media_message(meta_payload, form_data, wa_id, message_sid, now_epoch)
    elif message_type == "location":
        add_location_message(meta_payload, form_data, wa_id, message_sid, now_epoch)
    elif message_type == "vcard":
        add_vcard_message(meta_payload, form_data, wa_id, message_sid, now_epoch)
    elif message_type == "button":
        add_button_message(meta_payload, form_data, wa_id, message_sid, now_epoch)
    elif message_type == "list":
        add_list_message(meta_payload, form_data, wa_id, message_sid, now_epoch)
    else:
        # Default to text with a placeholder
        logger.warning(f"Unsupported message type: {message_type}, defaulting to text")
        form_data['Body'] = f"[Unsupported message type: {message_type}]"
        add_text_message(meta_payload, form_data, wa_id, message_sid, now_epoch)
    
    # Log the generated Meta payload for debugging
    logger.debug(f"Generated Meta payload: {json.dumps(meta_payload)}")
    
    return meta_payload

def detect_message_type(form_data: Dict[str, str]) -> str:
    """
    Detect the message type from Twilio form data
    
    Args:
        form_data: The form data from the Twilio webhook
        
    Returns:
        str: Detected message type (text, media_image, media_audio, media_video, 
             media_document, location, vcard, button, list)
    """
    # Check for button interactive message
    if 'ButtonText' in form_data and 'ButtonPayload' in form_data:
        return "button"
    
    # Check for list interactive message
    if 'ListId' in form_data and 'ListTitle' in form_data:
        return "list"
    
    # Check for location
    if 'Latitude' in form_data and 'Longitude' in form_data:
        return "location"
    
    # Check for media
    num_media = int(form_data.get('NumMedia', '0'))
    if num_media > 0:
        content_type = form_data.get('MediaContentType0', '')
        
        # Check if it's a vCard
        if 'vcard' in content_type.lower():
            return "vcard"
            
        # Determine media type based on content type
        if content_type.startswith('image/'):
            return "media_image"
        elif content_type.startswith('audio/'):
            return "media_audio"
        elif content_type.startswith('video/'):
            return "media_video"
        else:
            return "media_document"
    
    # Default to text
    return "text"

def add_text_message(meta_payload: Dict, form_data: Dict[str, str], wa_id: str, message_sid: str, timestamp: int) -> None:
    """Add a text message to the Meta payload"""
    body = form_data.get('Body', '')
    
    # Skip if empty body and not a special case
    if not body:
        logger.info("Empty message body received, skipping")
        return
        
    message = {
        "from": wa_id,
        "id": message_sid,
        "timestamp": str(timestamp),
        "type": "text",
        "text": {
            "body": body
        }
    }
    
    meta_payload["entry"][0]["changes"][0]["value"]["messages"].append(message)

def add_media_message(meta_payload: Dict, form_data: Dict[str, str], wa_id: str, message_sid: str, timestamp: int) -> None:
    """Add a media message to the Meta payload"""
    num_media = int(form_data.get('NumMedia', '0'))
    
    # Process each media attachment
    for i in range(num_media):
        media_url = form_data.get(f'MediaUrl{i}', '')
        content_type = form_data.get(f'MediaContentType{i}', '')
        
        # Proxy the media URL if configured
        if USE_MEDIA_PROXY:
            media_url = proxy_twilio_media_url(media_url)
        
        # Determine media type
        media_type = "document"  # Default
        if content_type.startswith('image/'):
            media_type = "image"
        elif content_type.startswith('audio/'):
            media_type = "audio"
        elif content_type.startswith('video/'):
            media_type = "video"
        
        # Create media message
        message = {
            "from": wa_id,
            "id": f"{message_sid}-{i}" if i > 0 else message_sid,
            "timestamp": str(timestamp),
            "type": media_type,
            media_type: {
                "mime_type": content_type,
                "link": media_url
            }
        }
        
        # Add filename for documents
        if media_type == "document":
            # Try to extract filename from URL or use a default
            url_parts = media_url.split('/')
            filename = url_parts[-1] if url_parts else f"document-{i}.bin"
            message[media_type]["filename"] = filename
        
        meta_payload["entry"][0]["changes"][0]["value"]["messages"].append(message)
        
        # If there's also a body text and this is the first media, add it as a separate message
        if i == 0 and form_data.get('Body'):
            body_message = {
                "from": wa_id,
                "id": f"{message_sid}-text",
                "timestamp": str(timestamp),
                "type": "text",
                "text": {
                    "body": form_data.get('Body', '')
                }
            }
            meta_payload["entry"][0]["changes"][0]["value"]["messages"].append(body_message)

def add_location_message(meta_payload: Dict, form_data: Dict[str, str], wa_id: str, message_sid: str, timestamp: int) -> None:
    """Add a location message to the Meta payload"""
    latitude = form_data.get('Latitude', '')
    longitude = form_data.get('Longitude', '')
    address = form_data.get('Address', '')
    label = form_data.get('Label', '')
    
    message = {
        "from": wa_id,
        "id": message_sid,
        "timestamp": str(timestamp),
        "type": "location",
        "location": {
            "latitude": latitude,
            "longitude": longitude,
            "name": label or '',
            "address": address or ''
        }
    }
    
    meta_payload["entry"][0]["changes"][0]["value"]["messages"].append(message)

def add_vcard_message(meta_payload: Dict, form_data: Dict[str, str], wa_id: str, message_sid: str, timestamp: int) -> None:
    """Add a contacts/vCard message to the Meta payload"""
    vcard_data = None
    profile_name = form_data.get('ProfileName', wa_id)
    
    # Try to get vCard from media
    num_media = int(form_data.get('NumMedia', '0'))
    if num_media > 0:
        media_url = form_data.get('MediaUrl0', '')
        content_type = form_data.get('MediaContentType0', '')
        
        if 'vcard' in content_type.lower():
            # Fetch vCard content
            try:
                response = requests.get(media_url)
                if response.status_code == 200:
                    vcard_data = response.text
            except Exception as e:
                logger.error(f"Error fetching vCard from URL: {e}")
    
    # If no vCard from media, try Body
    if not vcard_data and form_data.get('Body', '').strip().startswith('BEGIN:VCARD'):
        vcard_data = form_data.get('Body', '')
    
    # If we have vCard data, parse it
    contacts = []
    if vcard_data:
        try:
            vobj = vobject.readOne(vcard_data)
            
            # Extract name
            formatted_name = ''
            if hasattr(vobj, 'fn') and vobj.fn.value:
                formatted_name = vobj.fn.value
            elif hasattr(vobj, 'n') and vobj.n.value:
                n_parts = vobj.n.value
                if isinstance(n_parts, list) and len(n_parts) >= 2:
                    formatted_name = f"{n_parts[0]} {n_parts[1]}"
                else:
                    formatted_name = str(n_parts)
            
            # Extract phone
            phones = []
            if hasattr(vobj, 'tel'):
                for tel in vobj.tel_list:
                    phones.append({
                        "phone": tel.value,
                        "type": "CELL"
                    })
            
            # Create contact
            if formatted_name or phones:
                contact = {
                    "name": {"formatted_name": formatted_name or profile_name}
                }
                if phones:
                    contact["phones"] = phones
                contacts.append(contact)
                
        except Exception as e:
            logger.error(f"Error parsing vCard data: {e}")
    
    # If no contacts could be parsed, create a minimal one
    if not contacts:
        contacts = [{
            "name": {"formatted_name": profile_name},
            "phones": [{
                "phone": wa_id,
                "type": "CELL"
            }]
        }]
    
    # Create message
    message = {
        "from": wa_id,
        "id": message_sid,
        "timestamp": str(timestamp),
        "type": "contacts",
        "contacts": contacts
    }
    
    meta_payload["entry"][0]["changes"][0]["value"]["messages"].append(message)

def add_button_message(meta_payload: Dict, form_data: Dict[str, str], wa_id: str, message_sid: str, timestamp: int) -> None:
    """Add a button interactive message to the Meta payload"""
    button_text = form_data.get('ButtonText', '')
    button_payload = form_data.get('ButtonPayload', '')
    
    message = {
        "from": wa_id,
        "id": message_sid,
        "timestamp": str(timestamp),
        "type": "interactive",
        "interactive": {
            "type": "button_reply",
            "button_reply": {
                "id": button_payload,
                "title": button_text
            }
        }
    }
    
    meta_payload["entry"][0]["changes"][0]["value"]["messages"].append(message)
    
    # If there's also body text, add it as a separate message
    if form_data.get('Body'):
        body_message = {
            "from": wa_id,
            "id": f"{message_sid}-text",
            "timestamp": str(timestamp),
            "type": "text",
            "text": {
                "body": form_data.get('Body', '')
            }
        }
        meta_payload["entry"][0]["changes"][0]["value"]["messages"].append(body_message)

def add_list_message(meta_payload: Dict, form_data: Dict[str, str], wa_id: str, message_sid: str, timestamp: int) -> None:
    """Add a list interactive message to the Meta payload"""
    list_id = form_data.get('ListId', '')
    list_title = form_data.get('ListTitle', '')
    list_value = form_data.get('ListValue', '')
    
    message = {
        "from": wa_id,
        "id": message_sid,
        "timestamp": str(timestamp),
        "type": "interactive",
        "interactive": {
            "type": "list_reply",
            "list_reply": {
                "id": list_id,
                "title": list_title,
                "description": list_value
            }
        }
    }
    
    meta_payload["entry"][0]["changes"][0]["value"]["messages"].append(message)
    
    # If there's also body text, add it as a separate message
    if form_data.get('Body'):
        body_message = {
            "from": wa_id,
            "id": f"{message_sid}-text",
            "timestamp": str(timestamp),
            "type": "text",
            "text": {
                "body": form_data.get('Body', '')
            }
        }
        meta_payload["entry"][0]["changes"][0]["value"]["messages"].append(body_message)

def build_meta_status(form_data: Dict[str, str], now_epoch: int, db_manager) -> Dict[str, Any]:
    """
    Build a Meta Cloud API format status message from Twilio form data
    
    Args:
        form_data: The form data from the Twilio webhook
        now_epoch: Current timestamp in epoch seconds
        db_manager: Database manager for lookups
        
    Returns:
        dict: Meta Cloud API format status message
    """
    # Extract fields
    message_sid = form_data.get('MessageSid', '')
    message_status = form_data.get('MessageStatus', '')
    error_code = form_data.get('ErrorCode', '0')
    error_message = form_data.get('ErrorMessage', '')
    timestamp = form_data.get('Timestamp', str(now_epoch))
    to_phone = form_data.get('To', '')
    from_phone = form_data.get('From', '')
    
    # Convert timestamp if in Twilio format
    try:
        if timestamp and not timestamp.isdigit():
            # Try to parse Twilio timestamp format
            import dateutil.parser
            dt = dateutil.parser.parse(timestamp)
            timestamp = str(int(dt.timestamp()))
        elif not timestamp:
            timestamp = str(now_epoch)
    except Exception as e:
        logger.error(f"Error parsing timestamp: {e}")
        timestamp = str(now_epoch)
    
    # Map Twilio status to Meta status
    meta_status = map_twilio_status(message_status)
    
    # Determine recipient ID (the customer's phone number)
    recipient_id = normalize_msisdn(to_phone)
    
    # If this is an outbound message status, the recipient is the 'To' number
    # If inbound, we can't tell from the callback, so default to the normalized 'To'
    
    # Get receiver context for metadata (this is our WhatsApp number)
    display_phone_number, phone_number_id, entry_id = get_receiver_context(db_manager, from_phone)
    
    # Create status payload
    meta_payload = {
        "object": "whatsapp_business_account",
        "entry": [{
            "id": entry_id,
            "changes": [{
                "field": "messages",
                "value": {
                    "messaging_product": "whatsapp",
                    "metadata": {
                        "display_phone_number": display_phone_number,
                        "phone_number_id": phone_number_id
                    },
                    "statuses": [{
                        "id": message_sid,
                        "status": meta_status,
                        "timestamp": timestamp,
                        "recipient_id": recipient_id
                    }]
                }
            }]
        }]
    }
    
    # Add errors if present
    if meta_status == "failed" and (error_code or error_message):
        meta_payload["entry"][0]["changes"][0]["value"]["statuses"][0]["errors"] = [{
            "code": error_code or "0",
            "title": error_message or "Unknown error"
        }]
    
    return meta_payload

def map_twilio_status(twilio_status: str) -> str:
    """
    Map Twilio message status to Meta status
    
    Args:
        twilio_status: Twilio status (queued, sending, sent, delivered, read, undelivered, failed)
        
    Returns:
        str: Meta status (sent, delivered, read, failed)
    """
    status_map = {
        "queued": "sent",
        "sending": "sent",
        "sent": "sent",
        "delivered": "delivered",
        "read": "read",
        "undelivered": "failed",
        "failed": "failed"
    }
    
    return status_map.get(twilio_status.lower(), "sent")

def sign_payload(payload: Dict, secret: str) -> str:
    """
    Sign a payload with HMAC-SHA256 for internal service-to-service validation
    
    Args:
        payload: The payload to sign
        secret: The secret key for signing
        
    Returns:
        str: The signature
    """
    if not secret:
        return ""
        
    # Convert payload to JSON string
    payload_str = json.dumps(payload, separators=(',', ':'), sort_keys=True)
    
    # Create HMAC-SHA256 signature
    mac = hmac.new(
        key=secret.encode('utf-8'),
        msg=payload_str.encode('utf-8'),
        digestmod=hashlib.sha256
    )
    
    # Return hex digest
    return mac.hexdigest()

# Configure your Flask app
# We assume that the main application instance 'app' is imported from CloudAPIWebhook.py
from CloudAPIWebhook import app, db_manager, logger
import traceback

# We need to create a wrapper for the Meta webhook handling since there's no direct function

def handle_meta_webhook_payload(payload: Dict) -> bool:
    """
    Process a Meta-format webhook payload
    
    This function simulates a direct POST to the Meta webhook endpoint
    by creating a request-like object and calling the webhook handler directly.
    
    Args:
        payload: The Meta Cloud API format payload
        
    Returns:
        bool: True if successful, False otherwise
    """
    # Generate a unique ID for this request
    request_id = f"meta-fwd-{int(time.time())}-{hash(str(payload))}"
    
    try:
        # Log the forwarding attempt
        logger.info(f"[{request_id}] Forwarding normalized payload to Meta webhook handler")
        
        # Import the webhook handler function from CloudAPIWebhook
        from CloudAPIWebhook import webhook
        
        # Create a mock request object with our payload
        class MockRequest:
            def __init__(self, json_data):
                self._json_data = json_data
                self.headers = {}  # Add empty headers for compatibility
                
            def get_json(self, force=False):
                return self._json_data
        
        # Create a mock context for the webhook function
        import flask
        from flask import _request_ctx_stack
        
        if _request_ctx_stack.top is None:
            # If there's no request context, we create one
            logger.debug(f"[{request_id}] Creating new request context")
            with app.test_request_context():
                # Save the original request
                old_request = flask.request
                
                try:
                    # Replace the request with our mock
                    flask.request = MockRequest(payload)
                    
                    # Call the webhook function
                    logger.debug(f"[{request_id}] Calling webhook handler in new context")
                    response = webhook()
                    
                    # Check if successful
                    success = isinstance(response, tuple) and len(response) > 1 and response[1] == 200
                    if success:
                        logger.info(f"[{request_id}] Meta webhook handler processed successfully with status 200")
                    else:
                        status = response[1] if isinstance(response, tuple) and len(response) > 1 else "unknown"
                        logger.warning(f"[{request_id}] Meta webhook handler returned non-200 status: {status}")
                    
                    return success
                except Exception as e:
                    tb = log_and_capture_exception(f"[{request_id}] Error in webhook call (new context): {e}")
                    return False
                finally:
                    # Restore the original request
                    flask.request = old_request
        else:
            # If there's already a request context, we just replace the request
            logger.debug(f"[{request_id}] Using existing request context")
            old_request = flask.request
            
            try:
                # Replace the request with our mock
                flask.request = MockRequest(payload)
                
                # Call the webhook function
                logger.debug(f"[{request_id}] Calling webhook handler in existing context")
                response = webhook()
                
                # Check if successful
                success = isinstance(response, tuple) and len(response) > 1 and response[1] == 200
                if success:
                    logger.info(f"[{request_id}] Meta webhook handler processed successfully with status 200")
                else:
                    status = response[1] if isinstance(response, tuple) and len(response) > 1 else "unknown"
                    logger.warning(f"[{request_id}] Meta webhook handler returned non-200 status: {status}")
                
                return success
            except Exception as e:
                tb = log_and_capture_exception(f"[{request_id}] Error in webhook call (existing context): {e}")
                return False
            finally:
                # Restore the original request
                flask.request = old_request
    except Exception as e:
        tb = log_and_capture_exception(f"[{request_id}] Error setting up Meta webhook forwarding: {e}")
        return False

@app.route('/webhookT', methods=['POST'])
def webhook_twilio():
    """
    Webhook endpoint for receiving Twilio WhatsApp events
    
    Receives Twilio webhook payloads, validates them, transforms them to Meta Cloud API format,
    and forwards them to the existing handler for Meta Cloud API webhooks.
    """
    # Get request ID for tracking
    request_id = f"twilio-{int(time.time())}-{hash(str(request.headers))}"
    
    try:
        # Log initial request info
        logger.info(f"[{request_id}] Received Twilio webhook from {request.remote_addr}")
        
        # Get full URL for signature validation
        request_url = request.url_root.rstrip('/') + request.path
        
        # Validate Twilio signature
        twilio_signature = request.headers.get('X-Twilio-Signature', '')
        if not validate_twilio_signature(request_url, request.form, twilio_signature):
            logger.warning(f"[{request_id}] Invalid Twilio signature from {request.remote_addr}")
            return "Forbidden", 403
        
        # Get current time
        now_epoch = int(time.time())
        
        # Extract form data
        form_data = dict(request.form)
        
        # Log incoming webhook data
        logger.info(f"[{request_id}] Received Twilio webhook with keys: {list(form_data.keys())}")
        
        # Add some diagnostic info to help with debugging
        message_sid = form_data.get('MessageSid', 'unknown')
        sender = form_data.get('From', 'unknown')
        receiver = form_data.get('To', 'unknown')
        
        try:
            # Determine if this is a status callback or message
            if "MessageStatus" in form_data:
                # This is a status callback
                status = form_data.get('MessageStatus', 'unknown')
                logger.info(f"[{request_id}] Processing Twilio status callback: {status} for message {message_sid}")
                meta_payload = build_meta_status(form_data, now_epoch, db_manager)
                logger.info(f"[{request_id}] Processed Twilio status callback: {status} for {message_sid}")
            else:
                # This is a message
                logger.info(f"[{request_id}] Processing Twilio message from {sender} to {receiver}, SID: {message_sid}")
                meta_payload = build_meta_message(form_data, now_epoch, db_manager)
                msg_type = form_data.get('SmsMessageSid') and 'text' or 'media/other'
                logger.info(f"[{request_id}] Processed Twilio {msg_type} message from {sender} to {receiver}")
        
            # Sign the payload if configured
            if INTERNAL_HMAC_SECRET:
                signature = sign_payload(meta_payload, INTERNAL_HMAC_SECRET)
                logger.debug(f"[{request_id}] Signed payload with HMAC: {signature[:10]}...")
            
            # Forward the transformed payload to the Meta webhook handler
            logger.debug(f"[{request_id}] Forwarding to Meta webhook handler")
            result = handle_meta_webhook_payload(meta_payload)
            
            if result:
                logger.info(f"[{request_id}] Successfully processed and forwarded Twilio webhook")
            else:
                logger.warning(f"[{request_id}] Meta webhook handler returned failure")
            
            # Return success regardless of internal processing to avoid Twilio retries
            # (Twilio will retry on non-2xx responses which could cause duplicate processing)
            return "OK", 200
            
        except Exception as inner_e:
            # Log the error with the request ID
            tb = log_and_capture_exception(f"[{request_id}] Error processing Twilio webhook data: {inner_e}")
            # Return a 200 to avoid Twilio retries
            return "Processed with errors", 200
        
    except Exception as e:
        # Log the error but don't expose details in the response
        tb = log_and_capture_exception(f"[{request_id}] Error in Twilio webhook endpoint: {e}")
        # Return a 200 to avoid Twilio retries that might flood our logs
        return "Processed with errors", 200
    
if __name__ == '__main__':
    # Configuración para ejecutar el servidor Flask directamente
    import ssl

    # Obtener configuración del puerto desde el objeto config
    http_port = getattr(config, 'http_port', 5041) + 1
    # El puerto HTTPS siempre será el puerto HTTP + 2
    https_port = http_port + 2
    host = getattr(config, 'webhook_host', '0.0.0.0')

    # Certificados SSL
    ssl_cert = getattr(config, 'ssl_cert_path', None)
    ssl_key = getattr(config, 'ssl_key_path', None)

    logger.info(f"🚀 Iniciando servidor Twilio Webhook Adapter")
    logger.info(f"   • Endpoint disponible en: /webhookT")
    logger.info(f"   • Puerto HTTP: {http_port}")
    logger.info(f"   • Puerto HTTPS: {https_port} (HTTP + 2)")
    
    # Registrar información de los certificados SSL para diagnóstico
    logger.info(f"   • Certificado SSL: {ssl_cert}")
    logger.info(f"   • Clave SSL: {ssl_key}")

    # Decidir si usar HTTPS o HTTP
    if ssl_cert and ssl_key and os.path.exists(ssl_cert) and os.path.exists(ssl_key):
        try:
            logger.info(f"🔒 Iniciando servidor HTTPS en puerto {https_port}")
            context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
            logger.info(f"   • Contexto SSL creado correctamente")
            context.load_cert_chain(ssl_cert, ssl_key)
            logger.info(f"   • Certificados cargados correctamente")
            app.run(host=host, port=https_port, ssl_context=context, debug=False)
        except Exception as e:
            logger.error(f"❌ Error al iniciar el servidor HTTPS: {e}")
            logger.error(traceback.format_exc())
            logger.warning("⚠️ Intentando iniciar en modo HTTP como fallback")
            app.run(host=host, port=http_port, debug=False)
    else:
        # Explicar por qué no se usa HTTPS
        if not ssl_cert:
            logger.warning("⚠️ No se encontró la ruta del certificado SSL (ssl_cert_path)")
        elif not ssl_key:
            logger.warning("⚠️ No se encontró la ruta de la clave SSL (ssl_key_path)")
        elif not os.path.exists(ssl_cert):
            logger.warning(f"⚠️ El archivo de certificado no existe en la ruta: {ssl_cert}")
        elif not os.path.exists(ssl_key):
            logger.warning(f"⚠️ El archivo de clave no existe en la ruta: {ssl_key}")
        
        logger.warning("⚠️ Certificados SSL no encontrados o no válidos, usando HTTP")
        logger.info(f"🌐 Iniciando servidor HTTP en puerto {http_port}")
        app.run(host=host, port=http_port, debug=False)