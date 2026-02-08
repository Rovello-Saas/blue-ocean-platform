"""
AI Image Generation Pipeline.
Creates unique, Google-compliant product images from AliExpress reference images.
Handles text translation to target language (e.g., German).
"""

from __future__ import annotations

import base64
import io
import logging
from typing import Optional

import requests
from openai import OpenAI

from src.core.config import AppConfig, OPENAI_API_KEY

logger = logging.getLogger(__name__)

LANGUAGE_NAMES = {
    "de": "German",
    "nl": "Dutch",
    "fr": "French",
    "es": "Spanish",
    "it": "Italian",
    "pl": "Polish",
    "en": "English",
}


class AIImageGenerator:
    """
    Generates unique product images using AI.
    Uses reference images from AliExpress as input.
    """

    def __init__(self, config: AppConfig = None):
        self.config = config or AppConfig()
        self.client = OpenAI(api_key=OPENAI_API_KEY)

    def generate_product_images(
        self,
        reference_image_urls: list[str],
        product_description: str,
        target_language: str = "de",
        num_images: int = 4,
    ) -> list[dict]:
        """
        Generate unique product images from reference images.

        Args:
            reference_image_urls: URLs of reference images (from AliExpress)
            product_description: Description of the product
            target_language: Target language for any text in images
            num_images: Number of images to generate

        Returns:
            List of dicts with 'image_data' (bytes) and 'image_type' (str)
        """
        if not reference_image_urls:
            logger.warning("No reference images provided")
            return []

        generated_images = []
        lang_name = LANGUAGE_NAMES.get(target_language, "German")

        # Step 1: Analyze reference image
        analysis = self._analyze_reference_image(
            reference_image_urls[0], product_description
        )

        if not analysis:
            logger.error("Failed to analyze reference image")
            return []

        # Step 2: Generate images
        image_prompts = self._build_image_prompts(
            analysis, product_description, lang_name, num_images
        )

        for i, prompt in enumerate(image_prompts):
            try:
                image_data = self._generate_single_image(prompt)
                if image_data:
                    generated_images.append({
                        "image_data": image_data,
                        "image_type": self._get_image_type(i),
                        "prompt_used": prompt,
                    })
                    logger.info("Generated image %d/%d", i + 1, num_images)
            except Exception as e:
                logger.error("Failed to generate image %d: %s", i + 1, e)

        logger.info(
            "Generated %d/%d images for: %s",
            len(generated_images), num_images, product_description[:50]
        )
        return generated_images

    def _analyze_reference_image(
        self, image_url: str, product_description: str
    ) -> Optional[dict]:
        """
        Use GPT-4o vision to analyze a reference image.
        Returns structured analysis of the product.
        """
        try:
            response = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": f"""Analyze this product image in detail. The product is: {product_description}

Provide a structured analysis:
1. Product type and name
2. Key visual features (color, material, shape, size indicators)
3. Any text visible in the image (and what language it's in)
4. Background type (white, lifestyle, colored)
5. Number of items shown
6. Any branding or logos visible
7. Overall composition style

Be specific and detailed as this will be used to generate new, unique product images.""",
                            },
                            {
                                "type": "image_url",
                                "image_url": {"url": image_url},
                            },
                        ],
                    }
                ],
                max_tokens=1000,
            )

            analysis_text = response.choices[0].message.content
            return {"analysis": analysis_text, "original_url": image_url}

        except Exception as e:
            logger.error("Image analysis failed: %s", e)
            return None

    def _build_image_prompts(
        self,
        analysis: dict,
        product_description: str,
        language_name: str,
        num_images: int,
    ) -> list[str]:
        """Build specific prompts for each image type."""
        base_analysis = analysis.get("analysis", "")
        prompts = []

        # Image 1: Clean white background product shot (main image)
        prompts.append(
            f"Professional e-commerce product photography of {product_description}. "
            f"Clean white background, studio lighting, sharp focus, high resolution. "
            f"Based on this product description: {base_analysis[:300]}. "
            f"Style: Amazon/Google Shopping main product image. "
            f"NO text, NO watermarks, NO logos. Product centered, well-lit, professional."
        )

        if num_images >= 2:
            # Image 2: Alternative angle
            prompts.append(
                f"Professional product photo of {product_description} from a 45-degree angle. "
                f"White background, showing product details and texture. "
                f"Based on: {base_analysis[:300]}. "
                f"Sharp focus, professional studio photography. NO text, NO watermarks."
            )

        if num_images >= 3:
            # Image 3: Lifestyle/context image
            prompts.append(
                f"Lifestyle product photography of {product_description} in use. "
                f"Modern, bright, natural setting. Shows the product being used in its "
                f"intended context. Warm, inviting atmosphere. "
                f"Based on: {base_analysis[:300]}. "
                f"Professional quality, editorial style. NO text overlays."
            )

        if num_images >= 4:
            # Image 4: Feature highlight with language-specific text
            prompts.append(
                f"Product feature infographic for {product_description}. "
                f"Clean, modern design on white background. "
                f"Show 3-4 key features with small {language_name} labels pointing to product details. "
                f"All text MUST be in {language_name}. Professional, minimalist design. "
                f"Based on: {base_analysis[:300]}."
            )

        if num_images >= 5:
            # Image 5: Detail/close-up
            prompts.append(
                f"Close-up detail shot of {product_description}. "
                f"Macro photography showing material quality, texture, and craftsmanship. "
                f"White or soft gradient background. Professional studio lighting. "
                f"Based on: {base_analysis[:300]}. NO text, NO watermarks."
            )

        return prompts[:num_images]

    def _generate_single_image(self, prompt: str) -> Optional[bytes]:
        """Generate a single image using OpenAI's image generation."""
        try:
            response = self.client.images.generate(
                model="dall-e-3",
                prompt=prompt,
                size="1024x1024",
                quality="standard",
                n=1,
                response_format="b64_json",
            )

            b64_data = response.data[0].b64_json
            return base64.b64decode(b64_data)

        except Exception as e:
            logger.error("Image generation failed: %s", e)
            return None

    @staticmethod
    def _get_image_type(index: int) -> str:
        """Get image type label by index."""
        types = [
            "main_white_bg",
            "angle_shot",
            "lifestyle",
            "feature_infographic",
            "detail_closeup",
        ]
        return types[index] if index < len(types) else f"extra_{index}"


def download_image(url: str) -> Optional[bytes]:
    """Download an image from a URL."""
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return response.content
    except Exception as e:
        logger.error("Failed to download image from %s: %s", url, e)
        return None
