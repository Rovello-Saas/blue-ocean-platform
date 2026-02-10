"""
Fal.ai Image Generator backend.

Supports multiple Flux models via the Fal.ai API:
- flux-pro: Highest quality, slower
- flux-dev: Good balance of quality and speed
- flux-schnell: Fastest, good for drafts

Usage:
    gen = FalImageGenerator(model="fal-ai/flux-pro/v1.1")
    images = gen.generate(prompt="...", num_images=1)
"""

from __future__ import annotations

import io
import logging
import os
from typing import Optional

import requests

from src.core.config import get_env

logger = logging.getLogger(__name__)

# Model ID mapping
FAL_MODELS = {
    "fal-flux-pro": "fal-ai/flux-pro/v1.1",
    "fal-flux-dev": "fal-ai/flux/dev",
    "fal-flux-schnell": "fal-ai/flux/schnell",
}

# Default image sizes
IMAGE_SIZES = {
    "square": {"width": 1024, "height": 1024},
    "landscape": {"width": 1344, "height": 768},
    "portrait": {"width": 768, "height": 1344},
    "shopify": {"width": 1024, "height": 1024},
}


class FalImageGenerator:
    """
    Image generator using Fal.ai's Flux models.
    
    Requires FAL_KEY environment variable.
    """

    def __init__(self, model_key: str = "fal-flux-pro"):
        self.api_key = get_env("FAL_KEY", "")
        self.model_id = FAL_MODELS.get(model_key, FAL_MODELS["fal-flux-pro"])
        self.model_key = model_key
        self.base_url = "https://queue.fal.run"

    def generate(
        self,
        prompt: str,
        negative_prompt: str = "",
        num_images: int = 1,
        image_size: str = "square",
        reference_image_url: str = "",
        guidance_scale: float = 3.5,
        num_inference_steps: int = 28,
        seed: Optional[int] = None,
    ) -> list[bytes]:
        """
        Generate images using Fal.ai Flux model.
        
        Returns list of image bytes (PNG).
        """
        if not self.api_key:
            raise ValueError(
                "FAL_KEY environment variable not set. "
                "Get your API key at https://fal.ai/dashboard/keys"
            )

        size = IMAGE_SIZES.get(image_size, IMAGE_SIZES["square"])

        payload = {
            "prompt": prompt,
            "image_size": size,
            "num_images": num_images,
            "guidance_scale": guidance_scale,
            "num_inference_steps": num_inference_steps,
            "enable_safety_checker": True,
            "output_format": "png",
        }

        if negative_prompt:
            payload["negative_prompt"] = negative_prompt

        if seed is not None:
            payload["seed"] = seed

        # If reference image provided, use image-to-image endpoint
        if reference_image_url:
            payload["image_url"] = reference_image_url
            payload["strength"] = 0.75  # How much to deviate from reference

        headers = {
            "Authorization": f"Key {self.api_key}",
            "Content-Type": "application/json",
        }

        url = f"{self.base_url}/{self.model_id}"
        logger.info(
            "Generating %d image(s) with %s — prompt: %.80s...",
            num_images, self.model_key, prompt,
        )

        try:
            response = requests.post(url, json=payload, headers=headers, timeout=120)
            response.raise_for_status()
            result = response.json()

            images = []
            for img_data in result.get("images", []):
                img_url = img_data.get("url", "")
                if img_url:
                    # Download the image
                    img_response = requests.get(img_url, timeout=60)
                    img_response.raise_for_status()
                    images.append(img_response.content)

            logger.info("Generated %d image(s) successfully", len(images))
            return images

        except requests.exceptions.RequestException as e:
            logger.error("Fal.ai API error: %s", e)
            if hasattr(e, 'response') and e.response is not None:
                logger.error("Response: %s", e.response.text[:500])
            raise

    def estimate_cost(self, num_images: int = 1) -> float:
        """Estimate cost in USD for generating images."""
        # Approximate pricing (as of 2025)
        costs = {
            "fal-flux-pro": 0.05,      # per image
            "fal-flux-dev": 0.025,      # per image
            "fal-flux-schnell": 0.003,  # per image
        }
        per_image = costs.get(self.model_key, 0.05)
        return per_image * num_images
