"""
Nano Banana (Gemini) Image Generator backend via Google Gemini API.

Two model variants:
- Nano Banana      (gemini-2.5-flash-image)  — fast, high-volume
- Nano Banana Pro  (gemini-3-pro-image-preview) — best quality, advanced reasoning

Uses the google-genai SDK with a GEMINI_API_KEY (from .env or AI Studio).

Usage:
    gen = NanoBananaGenerator(model_key="nano-banana-pro")
    images = gen.generate(prompt="...", reference_image_bytes=None)
    pipeline = gen.generate_product_images(ref_urls, description, ...)
"""

from __future__ import annotations

import base64
import io
import logging
import os
from typing import Optional

import requests as http_requests

from src.core.config import get_env

logger = logging.getLogger(__name__)

# Model variants
NANO_BANANA_MODELS = {
    "nano-banana": "gemini-2.5-flash-image",
    "nano-banana-pro": "gemini-3-pro-image-preview",
}

LANGUAGE_NAMES = {
    "de": "German", "nl": "Dutch", "fr": "French",
    "es": "Spanish", "it": "Italian", "pl": "Polish", "en": "English",
}


def _download_image(url: str) -> Optional[bytes]:
    """Download an image from a URL."""
    try:
        resp = http_requests.get(url, timeout=30)
        resp.raise_for_status()
        return resp.content
    except Exception as e:
        logger.error("Failed to download image: %s", e)
        return None


class NanoBananaGenerator:
    """
    Image generator using Google Nano Banana (Gemini native image generation).

    Requires GEMINI_API_KEY in .env (get one at https://aistudio.google.com/apikey).
    """

    def __init__(self, model_key: str = "nano-banana-pro"):
        self.api_key = get_env("GEMINI_API_KEY", "")
        self.model_id = NANO_BANANA_MODELS.get(model_key, NANO_BANANA_MODELS["nano-banana-pro"])
        self.model_key = model_key
        self._client = None

    def _get_client(self):
        """Lazy-init the google-genai client."""
        if self._client is not None:
            return self._client
        if not self.api_key or self.api_key.startswith("your_"):
            raise ValueError(
                "GEMINI_API_KEY not configured. "
                "Get one at https://aistudio.google.com/apikey and add it to .env"
            )
        try:
            from google import genai
        except ImportError:
            raise ImportError(
                "google-genai package not installed. "
                "Install it with: pip install google-genai"
            )
        self._client = genai.Client(api_key=self.api_key)
        return self._client

    # ── Single image generation ──────────────────────────────────

    def generate(
        self,
        prompt: str,
        reference_image_bytes: Optional[bytes] = None,
        aspect_ratio: str = "1:1",
        resolution: str = "1K",
    ) -> list[bytes]:
        """
        Generate one or more images from a text prompt.
        Optionally pass a reference image for editing / style transfer.

        Returns list of image bytes (PNG).
        """
        from google.genai import types

        client = self._get_client()

        config = types.GenerateContentConfig(
            response_modalities=["TEXT", "IMAGE"],
        )

        # Build contents
        contents = []
        if reference_image_bytes:
            from PIL import Image as PILImage
            img = PILImage.open(io.BytesIO(reference_image_bytes))
            contents.append(img)
        contents.append(prompt)

        try:
            response = client.models.generate_content(
                model=self.model_id,
                contents=contents,
                config=config,
            )
        except Exception as e:
            logger.error("Nano Banana generation failed: %s", e)
            raise

        # Extract images from response
        images = []
        for part in response.parts:
            if part.inline_data is not None:
                img_bytes = part.inline_data.data
                if isinstance(img_bytes, str):
                    img_bytes = base64.b64decode(img_bytes)
                images.append(img_bytes)
            elif part.text is not None:
                logger.info("Nano Banana text response: %s", part.text[:200])

        if not images:
            logger.warning("Nano Banana returned no images for prompt: %s", prompt[:100])

        return images

    # ── Full product image pipeline (multi-turn conversation) ────

    def generate_product_images(
        self,
        reference_image_urls: list[str],
        product_description: str,
        target_language: str = "de",
        num_images: int = 5,
        product_instructions: str = "",
    ) -> list[dict]:
        """
        Generate a set of product images using multi-turn conversation.
        Same output format as AIImageGenerator: list of dicts with
        'image_data' (bytes) and 'image_type' (str).
        """
        from google.genai import types

        if not reference_image_urls:
            logger.warning("No reference images provided")
            return []

        client = self._get_client()
        lang_name = LANGUAGE_NAMES.get(target_language, "German")

        # Download reference image
        ref_url = reference_image_urls[0]
        logger.info("Downloading reference image for Nano Banana pipeline...")
        ref_bytes = _download_image(ref_url)
        if not ref_bytes:
            logger.error("Failed to download reference image")
            return []

        # Build prompts (same structure as OpenAI pipeline)
        prompts = _build_product_prompts(
            product_description, lang_name, product_instructions, num_images
        )

        # Use multi-turn chat for consistency across images
        config = types.GenerateContentConfig(
            response_modalities=["TEXT", "IMAGE"],
        )
        chat = client.chats.create(model=self.model_id, config=config)

        generated_images: list[dict] = []
        spec_block = f"\nUSER INSTRUCTIONS: {product_instructions}" if product_instructions else ""

        for i, spec in enumerate(prompts):
            image_type = spec["image_type"]
            prompt = spec["prompt"]

            logger.info(
                "Generating image %d/%d (%s) via Nano Banana %s...",
                i + 1, num_images, image_type,
                "Pro" if "pro" in self.model_key else "",
            )

            try:
                if i == 0:
                    # First turn: include reference image + context
                    from PIL import Image as PILImage
                    ref_img = PILImage.open(io.BytesIO(ref_bytes))
                    message = [
                        ref_img,
                        (
                            f"This is a product image for: {product_description}. "
                            f"Study this image in extreme detail — the product shape, "
                            f"color, fabric texture, stitching pattern, and any accessories. "
                            f"You will generate multiple product images based on this reference. "
                            f"Keep the product looking EXACTLY the same across ALL images. "
                            f"Remove any logos, watermarks, or brand names. "
                            f"{spec_block}\n\n"
                            f"Now generate the first image:\n{prompt}"
                        ),
                    ]
                else:
                    # Subsequent turns: model remembers the conversation
                    message = (
                        f"Now generate the next product image. Keep the EXACT same "
                        f"product appearance — same color, same texture, same details "
                        f"as in all the previous images you generated. "
                        f"Remove any logos, watermarks, or brand names.\n\n"
                        f"{prompt}"
                    )

                response = chat.send_message(message)

                # Extract image from response
                image_data = None
                for part in response.parts:
                    if part.inline_data is not None:
                        img_bytes = part.inline_data.data
                        if isinstance(img_bytes, str):
                            img_bytes = base64.b64decode(img_bytes)
                        image_data = img_bytes
                        break
                    elif part.text is not None:
                        logger.info("  Nano Banana text: %s", part.text[:150])

                if image_data:
                    generated_images.append({
                        "image_data": image_data,
                        "image_type": image_type,
                    })
                    logger.info("Image %d (%s) generated", i + 1, image_type)
                else:
                    logger.warning("Image %d (%s): no image in response", i + 1, image_type)

            except Exception as e:
                logger.error("Image %d (%s) failed: %s", i + 1, image_type, e)

        logger.info(
            "Nano Banana pipeline complete: %d/%d images",
            len(generated_images), num_images,
        )
        return generated_images


def _build_product_prompts(
    description: str,
    lang_name: str,
    product_instructions: str,
    num_images: int,
) -> list[dict]:
    """Build ordered image prompts for the product pipeline."""
    prompts = []

    if num_images >= 1:
        prompts.append({
            "image_type": "main_white_bg",
            "prompt": (
                f"Recreate this product ({description}) on a clean white "
                f"background. Professional e-commerce product photography with "
                f"soft natural shadows. Keep the EXACT same product — same color, "
                f"fabric texture, pattern, and accessories. "
                f"No text overlays, no background elements."
            ),
        })

    if num_images >= 2:
        prompts.append({
            "image_type": "lifestyle",
            "prompt": (
                f"Place the EXACT same product on a comfortable sofa in a cozy "
                f"modern living room. Warm natural lighting from a window, soft "
                f"cushions. Keep the exact same color, texture, and details. "
                f"Professional lifestyle product photography, editorial quality. "
                f"No text, no watermarks."
            ),
        })

    if num_images >= 3:
        prompts.append({
            "image_type": "feature_infographic",
            "prompt": (
                f"Create a professional e-commerce product infographic. "
                f"Show the EXACT same product on a clean white background. "
                f"Add curved arrows pointing from feature labels to the "
                f"specific part of the product. All labels must be in {lang_name}. "
                f"Include 4-6 key product features as text labels. "
                f"Clean, modern typography. All text must be clearly readable "
                f"and correctly spelled in {lang_name}."
            ),
        })

    if num_images >= 4:
        prompts.append({
            "image_type": "detail_closeup",
            "prompt": (
                f"Create a close-up detail shot of the product. Focus on textures, "
                f"materials, and fine details. The product MUST match exactly what "
                f"you created in the previous images. "
                f"Macro product photography, shallow depth of field, soft "
                f"natural lighting. No text overlays."
            ),
        })

    if num_images >= 5:
        prompts.append({
            "image_type": "creative_scene",
            "prompt": (
                f"Show the EXACT same product being used by a person in a cozy "
                f"setting. The person is comfortably using the product. "
                f"Soft lighting, modern interior, neutral tones. Keep the same "
                f"product color, texture, and pattern. "
                f"Professional lifestyle photography. No text, no watermarks."
            ),
        })

    return prompts
