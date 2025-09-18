# Twilio WhatsApp Webhook Adapter

This module provides a webhook adapter that converts Twilio WhatsApp webhook events to Meta Cloud API format, allowing a seamless integration with existing Cloud API webhook handlers.

## Overview

The Twilio Webhook Adapter:

1. Receives WhatsApp events from Twilio in form-data format
2. Validates the Twilio signature for security
3. Normalizes phone numbers to a standard format
4. Looks up receiver information in the database
5. Transforms the data into Meta Cloud API format
6. Handles different message types (text, media, location, contacts, interactive)
7. Processes message status callbacks
8. Forwards the transformed data to the existing Meta Cloud API webhook handler

## Configuration

The adapter is configured using environment variables:

### Required Environment Variables

- `TWILIO_AUTH_TOKEN`: Your Twilio Auth Token (required for signature validation)

### Optional Environment Variables

- `TWILIO_ACCOUNT_SID`: Your Twilio Account SID (required for media proxy)
- `TWILIO_USE_MEDIA_PROXY`: Set to 'true' to enable media URL proxying (default: 'false')
- `TWILIO_MEDIA_PROXY_URL`: Base URL for the media proxy service
- `INTERNAL_HMAC_SECRET`: Secret key for internal HMAC signing (optional)

## Setting Up in Twilio

1. In your Twilio WhatsApp console, set the webhook URL to:
   ```
   https://your-domain.com/webhookT
   ```

2. Make sure to enable both message and status callback webhooks

3. If handling media messages, ensure your server can handle media URLs from Twilio (which may require authentication)

## Message Types Supported

The adapter supports the following message types:

- Text messages
- Media messages (images, audio, video, documents)
- Location messages
- Contact messages (vCards)
- Interactive messages

## Media URL Handling

Twilio media URLs require authentication with your Account SID and Auth Token. The adapter provides two options:

1. **Direct access**: The Meta Cloud API handler must handle Twilio authentication
2. **Media proxy**: Enable `TWILIO_USE_MEDIA_PROXY=true` and set `TWILIO_MEDIA_PROXY_URL` to use a proxy service that can fetch and serve the media without requiring Twilio authentication

## Status Callbacks

The adapter maps Twilio status callbacks to Meta Cloud API status format:

- `sent` → `sent`
- `delivered` → `delivered`
- `read` → `read`
- `failed` → `failed`

## Security

- The adapter validates the X-Twilio-Signature header to ensure requests come from Twilio
- Optionally signs forwarded payloads with HMAC-SHA256 for internal service-to-service authentication

## Error Handling

The adapter includes comprehensive error handling and logging:

- Detailed request logging with unique request IDs
- Exception capturing with full tracebacks
- Graceful fallbacks for database errors
- Returns 200 OK to Twilio even on internal errors (to prevent unnecessary retries)

## Dependencies

- Flask web framework
- requests for HTTP operations
- vobject for vCard parsing

## Testing

To test the adapter:

1. Set up the required environment variables
2. Send test messages through Twilio WhatsApp
3. Check the `twilio_webhook.log` file for detailed logs