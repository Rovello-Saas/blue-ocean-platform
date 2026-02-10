"""
Google Imagen 4 Image Generator backend via Vertex AI.

Supports:
- imagen-4.0-generate-001 (standard quality)
- imagen-4.0-fast-generate-001 (faster, lower cost)

Requires a Google Cloud project with Vertex AI enabled and a service account.
Set GOOGLE_CLOUD_PROJECT in .env.

Usage:
    gen = GoogleImagenGenerator(model="standard")
    images = gen.generate(prompt="...", num_images=1)
"""

from __future__ import annotations

import base64
import json
import logging
import os
from typing import Optional

import requests

from src.core.config import get_env, get_service_account_credentials

logger = logging.getLogger(__name__)

# Model variants
IMAGEN_MODELS = {
    "google-imagen-4": "imagen-4.0-generate-001",
    "google-imagen-4-fast": "imagen-4.0-fast-generate-001",
}


class GoogleImagenGenerator:
    """
    Image generator using Google Imagen 4 via Vertex AI REST API.

    Requires:
    - GOOGLE_CLOUD_PROJECT env var
    - A service account with Vertex AI permissions
    """

    def __init__(self, model_key: str = "google-imagen-4"):
        self.project_id = get_env("GOOGLE_CLOUD_PROJECT", "")
        self.location = get_env("GOOGLE_CLOUD_LOCATION", "us-central1")
        self.model_id = IMAGEN_MODELS.get(model_key, IMAGEN_MODELS["google-imagen-4"])
        self.model_key = model_key

    def _get_access_token(self) -> str:
        """Get OAuth2 access token from service account credentials."""
        scopes = ["https://www.googleapis.com/auth/cloud-platform"]
        creds = get_service_account_credentials(scopes=scopes)
        creds.refresh(requests.Request() if hasattr(requests, "Request") else _google_auth_request())
        return creds.token

    def generate(
        self,
        prompt: str,
        negative_prompt: str = "",
        num_images: int = 1,
        aspect_ratio: str = "1:1",
        reference_image_bytes: Optional[bytes] = None,
    ) -> list[bytes]:
        """
        Generate images using Google Imagen 4.

        Returns list of image bytes (PNG).
        """
        if not self.project_id:
            raise ValueError(
                "GOOGLE_CLOUD_PROJECT environment variable not set. "
                "Set it to your Google Cloud project ID."
            )

        url = (
            f"https://{self.location}-aiplatform.googleapis.com/v1/"
            f"projects/{self.project_id}/locations/{self.location}/"
            f"publishers/google/models/{self.model_id}:predict"
        )

        # Build request payload
        instance = {
            "prompt": prompt,
        }

        if negative_prompt:
            instance["negativePrompt"] = negative_prompt

        if reference_image_bytes:
            instance["image"] = {
                "bytesBase64Encoded": base64.b64encode(reference_image_bytes).decode()
            }

        parameters = {
            "sampleCount": min(num_images, 4),  # Max 4 per request
            "aspectRatio": aspect_ratio,
            "outputOptions": {
                "mimeType": "image/png",
            },
        }

        payload = {
            "instances": [instance],
            "parameters": parameters,
        }

        try:
            access_token = self._get_access_token()
        except Exception as e:
            logger.error("Failed to get Google Cloud access token: %s", e)
            raise ValueError(
                "Could not authenticate with Google Cloud. "
                "Ensure your service account has Vertex AI permissions."
            ) from e

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }

        logger.info(
            "Generating %d image(s) with Imagen 4 (%s) — prompt: %.80s...",
            num_images, self.model_key, prompt,
        )

        try:
            response = requests.post(url, json=payload, headers=headers, timeout=120)
            response.raise_for_status()
            result = response.json()

            images = []
            for prediction in result.get("predictions", []):
                b64_data = prediction.get("bytesBase64Encoded", "")
                if b64_data:
                    images.append(base64.b64decode(b64_data))

            logger.info("Generated %d image(s) successfully with Imagen 4", len(images))
            return images

        except requests.exceptions.RequestException as e:
            logger.error("Vertex AI API error: %s", e)
            if hasattr(e, "response") and e.response is not None:
                logger.error("Response: %s", e.response.text[:500])
            raise

    def estimate_cost(self, num_images: int = 1) -> float:
        """Estimate cost in USD."""
        costs = {
            "google-imagen-4": 0.04,
            "google-imagen-4-fast": 0.02,
        }
        per_image = costs.get(self.model_key, 0.04)
        return per_image * num_images


def _google_auth_request():
    """Create a google.auth.transport.requests.Request for token refresh."""
    try:
        from google.auth.transport.requests import Request
        return Request()
    except ImportError:
        raise ImportError(
            "google-auth is required for Google Imagen. "
            "Install with: pip install google-auth"
        )
