"""
Adobe Firefly Image Generator backend.

Uses Adobe's Firefly API for commercial-safe image generation.
All images are generated from content trained on licensed Adobe Stock,
making them the safest choice for commercial use and marketplace compliance.

Requires:
- ADOBE_FIREFLY_CLIENT_ID
- ADOBE_FIREFLY_CLIENT_SECRET

Usage:
    gen = AdobeFireflyGenerator()
    images = gen.generate(prompt="...", num_images=1)
"""

from __future__ import annotations

import base64
import logging
import os
from typing import Optional

import requests

from src.core.config import get_env

logger = logging.getLogger(__name__)

# API endpoints
TOKEN_URL = "https://ims-na1.adobelogin.com/ims/token/v3"
FIREFLY_URL = "https://firefly-api.adobe.io/v3/images/generate"


class AdobeFireflyGenerator:
    """
    Image generator using Adobe Firefly API.

    Requires:
    - ADOBE_FIREFLY_CLIENT_ID env var
    - ADOBE_FIREFLY_CLIENT_SECRET env var
    """

    def __init__(self):
        self.client_id = get_env("ADOBE_FIREFLY_CLIENT_ID", "")
        self.client_secret = get_env("ADOBE_FIREFLY_CLIENT_SECRET", "")
        self._access_token: Optional[str] = None

    def _authenticate(self) -> str:
        """Get an access token using client credentials."""
        if self._access_token:
            return self._access_token

        if not self.client_id or not self.client_secret:
            raise ValueError(
                "Adobe Firefly credentials not set. "
                "Set ADOBE_FIREFLY_CLIENT_ID and ADOBE_FIREFLY_CLIENT_SECRET in .env"
            )

        payload = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "scope": "openid,AdobeID,firefly_enterprise,firefly_api,ff_apis",
        }

        try:
            response = requests.post(TOKEN_URL, data=payload, timeout=30)
            response.raise_for_status()
            data = response.json()
            self._access_token = data.get("access_token", "")
            return self._access_token
        except requests.exceptions.RequestException as e:
            logger.error("Adobe authentication failed: %s", e)
            raise

    def generate(
        self,
        prompt: str,
        negative_prompt: str = "",
        num_images: int = 1,
        width: int = 1024,
        height: int = 1024,
        content_class: str = "photo",
        reference_image_bytes: Optional[bytes] = None,
    ) -> list[bytes]:
        """
        Generate images using Adobe Firefly.

        Args:
            prompt: Text description of the desired image
            negative_prompt: What to avoid (not all Firefly versions support this)
            num_images: Number of images (1-4)
            width: Image width
            height: Image height
            content_class: "photo" or "art"
            reference_image_bytes: Optional reference image

        Returns list of image bytes (PNG/JPEG).
        """
        access_token = self._authenticate()

        headers = {
            "Authorization": f"Bearer {access_token}",
            "x-api-key": self.client_id,
            "Content-Type": "application/json",
        }

        payload = {
            "prompt": prompt,
            "n": min(num_images, 4),
            "size": {
                "width": width,
                "height": height,
            },
            "contentClass": content_class,
        }

        if negative_prompt:
            payload["negativePrompt"] = negative_prompt

        # Reference image support (style reference)
        if reference_image_bytes:
            payload["styles"] = {
                "referenceImage": {
                    "source": {
                        "uploadId": self._upload_reference(reference_image_bytes, access_token),
                    }
                }
            }

        logger.info(
            "Generating %d image(s) with Adobe Firefly — prompt: %.80s...",
            num_images, prompt,
        )

        try:
            response = requests.post(
                FIREFLY_URL, json=payload, headers=headers, timeout=120
            )
            response.raise_for_status()
            result = response.json()

            images = []
            for output in result.get("outputs", []):
                img_url = output.get("image", {}).get("url", "")
                if img_url:
                    img_response = requests.get(img_url, timeout=60)
                    img_response.raise_for_status()
                    images.append(img_response.content)

            logger.info("Generated %d image(s) successfully with Firefly", len(images))
            return images

        except requests.exceptions.RequestException as e:
            logger.error("Adobe Firefly API error: %s", e)
            if hasattr(e, "response") and e.response is not None:
                logger.error("Response: %s", e.response.text[:500])
            raise

    def _upload_reference(self, image_bytes: bytes, access_token: str) -> str:
        """Upload a reference image and return its upload ID."""
        upload_url = "https://firefly-api.adobe.io/v2/storage/image"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "x-api-key": self.client_id,
            "Content-Type": "image/png",
        }

        try:
            response = requests.post(
                upload_url, data=image_bytes, headers=headers, timeout=60
            )
            response.raise_for_status()
            data = response.json()
            return data.get("images", [{}])[0].get("id", "")
        except Exception as e:
            logger.warning("Reference image upload failed: %s", e)
            return ""

    def estimate_cost(self, num_images: int = 1) -> float:
        """Estimate cost in USD (credit-based)."""
        return 0.04 * num_images
