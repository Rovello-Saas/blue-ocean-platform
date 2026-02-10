"""
AI Image Generation Pipeline (v7 — Conversational / Responses API).

Two modes of operation:

A) CONVERSATIONAL MODE (Responses API) — when org is verified:
   Replicates the ChatGPT experience. Each image is a turn in a
   multi-turn conversation, so the model remembers the reference image
   and every image it has already created. This produces the best
   consistency across all 5 images.

B) CHAINED MODE (Images API fallback) — if Responses API is blocked:
   Falls back to the previous approach: Image 1 is generated from the
   competitor reference, all subsequent images use Image 1 as input,
   each with a detailed product spec + instructions in the prompt.

Pipeline order (both modes):
1. Main product       — from competitor ref → becomes "canonical" product
2. Lifestyle (sofa)   — keep exact same product
3. Infographic        — feature labels in target language
4. Detail close-up    — controller & fabric macro
5. Creative (bedroom) — person using the product
"""

from __future__ import annotations

import base64
import io
import json
import logging
import os
import tempfile
from typing import Optional

import requests
from openai import OpenAI
from PIL import Image

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

MAX_QUALITY_RETRIES = 2


class AIImageGenerator:
    """
    Generates product images using either the conversational Responses API
    (best consistency) or the chained Images API (fallback).
    """

    OUTPUT_SIZE = (1024, 1024)

    def __init__(self, config: AppConfig = None):
        self.config = config or AppConfig()
        self.client = OpenAI(api_key=OPENAI_API_KEY)
        self._responses_api_available: Optional[bool] = None

    # ─── Public API ──────────────────────────────────────────────────

    def generate_product_images(
        self,
        reference_image_urls: list[str],
        product_description: str,
        target_language: str = "de",
        num_images: int = 5,
        product_instructions: str = "",
    ) -> list[dict]:
        """
        Generate product images. Automatically uses the conversational
        Responses API if available, otherwise falls back to chained mode.

        Returns:
            List of dicts with 'image_data' (bytes) and 'image_type' (str).
        """
        if not reference_image_urls:
            logger.warning("No reference images provided")
            return []

        lang_name = LANGUAGE_NAMES.get(target_language, "German")
        ref_url = reference_image_urls[0]

        # Download reference image (needed by both modes)
        logger.info("Downloading reference image …")
        ref_bytes = download_image(ref_url)
        if not ref_bytes:
            logger.error("Failed to download reference image")
            return []
        ref_bytes = self._ensure_png(ref_bytes)

        # Extract features for infographic (shared by both modes)
        logger.info("Extracting features for infographic …")
        features = self._extract_features(ref_url, product_description, lang_name)
        logger.info("Extracted %d features", len(features))

        # Try conversational mode first, fall back to chained mode
        if self._check_responses_api():
            logger.info("=== Using CONVERSATIONAL mode (Responses API) ===")
            return self._generate_conversational(
                ref_bytes, ref_url, product_description, lang_name,
                features, product_instructions, num_images,
            )
        else:
            logger.info("=== Using CHAINED mode (Images API fallback) ===")
            return self._generate_chained(
                ref_bytes, ref_url, product_description, lang_name,
                features, product_instructions, num_images,
            )

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  MODE A: CONVERSATIONAL (Responses API)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def _generate_conversational(
        self,
        ref_bytes: bytes,
        ref_url: str,
        product_description: str,
        lang_name: str,
        features: list[str],
        product_instructions: str,
        num_images: int,
    ) -> list[dict]:
        """Generate images via multi-turn Responses API conversation."""

        # Upload reference image to OpenAI Files API
        logger.info("Uploading reference image to OpenAI …")
        file_id = self._upload_file(ref_bytes)
        if not file_id:
            logger.warning("File upload failed, falling back to chained mode")
            return self._generate_chained(
                ref_bytes, ref_url, product_description, lang_name,
                features, product_instructions, num_images,
            )
        logger.info("Reference uploaded: file_id=%s", file_id)

        # Build conversation prompts
        spec_block = ""
        if product_instructions:
            spec_block = f"\n\nUSER INSTRUCTIONS: {product_instructions}"

        image_specs = self._build_conversation_prompts(
            product_description, lang_name, features, spec_block, num_images
        )

        # Generate images via multi-turn conversation
        generated_images: list[dict] = []
        previous_response_id = None

        for i, spec in enumerate(image_specs):
            image_type = spec["image_type"]
            prompt = spec["prompt"]

            logger.info(
                "Generating image %d/%d (%s) via conversation …",
                i + 1, num_images, image_type,
            )

            # First turn includes the reference image
            if i == 0:
                input_content = [
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "input_image",
                                "file_id": file_id,
                            },
                            {
                                "type": "input_text",
                                "text": (
                                    f"This is a product image for: {product_description}. "
                                    f"Study this image in extreme detail — the product shape, "
                                    f"color, fabric texture, stitching pattern, and especially "
                                    f"any accessories like remote controls (exact button count, "
                                    f"button layout, display type, housing shape, cable). "
                                    f"You will be generating multiple product images based on "
                                    f"this reference. You MUST keep the product looking EXACTLY "
                                    f"the same across ALL images — same color, same texture, "
                                    f"same controller design, same details. "
                                    f"Remove any logos, watermarks, or brand names. "
                                    f"{spec_block}\n\n"
                                    f"Now generate the first image:\n{prompt}"
                                ),
                            },
                        ],
                    }
                ]
            else:
                # Subsequent turns reference all previous context automatically
                input_content = (
                    f"Now generate the next product image. Keep the EXACT same "
                    f"product appearance — same color, same texture, same controller "
                    f"design as in all the previous images you generated. "
                    f"Remove any logos, watermarks, or brand names.\n\n"
                    f"{prompt}"
                )

            try:
                response = self.client.responses.create(
                    model="gpt-4o",
                    input=input_content,
                    tools=[{
                        "type": "image_generation",
                        "quality": "high",
                        "size": "1024x1024",
                    }],
                    previous_response_id=previous_response_id,
                )

                # Extract generated image
                image_data = None
                for output in response.output:
                    if output.type == "image_generation_call":
                        image_b64 = output.result
                        image_data = base64.b64decode(image_b64)
                        break

                if image_data:
                    generated_images.append({
                        "image_data": image_data,
                        "image_type": image_type,
                    })
                    logger.info("✓ Image %d (%s) generated", i + 1, image_type)
                else:
                    logger.warning(
                        "  Image %d (%s): no image in response", i + 1, image_type
                    )

                # Chain: next turn inherits full conversation context
                previous_response_id = response.id

            except Exception as e:
                logger.error("  Image %d (%s) failed: %s", i + 1, image_type, e)
                # Don't break the chain — if one fails, try the rest
                # But keep the previous_response_id from the last success

        logger.info(
            "Conversational pipeline complete: %d/%d images",
            len(generated_images), num_images,
        )
        return generated_images

    @staticmethod
    def _build_conversation_prompts(
        description: str,
        lang_name: str,
        features: list[str],
        spec_block: str,
        num_images: int,
    ) -> list[dict]:
        """Build the ordered list of image prompts for the conversation."""
        feature_text = "\n".join(f"- {f}" for f in features[:6])
        prompts = []

        if num_images >= 1:
            prompts.append({
                "image_type": "main_white_bg",
                "prompt": (
                    f"Recreate this product ({description}) on a clean white "
                    f"background. Show it neatly folded from a slightly different "
                    f"angle. Keep the EXACT same product — same color, fabric "
                    f"texture, stitching pattern, and controller design. "
                    f"Professional e-commerce product photography with soft "
                    f"natural shadows. No text overlays, no background elements."
                ),
            })

        if num_images >= 2:
            prompts.append({
                "image_type": "lifestyle",
                "prompt": (
                    f"Take the EXACT same product you just created and place it "
                    f"draped on a comfortable sofa in a cozy modern living room. "
                    f"Warm natural lighting from a window, a cup of tea on a side "
                    f"table, soft cushions. Keep the exact same color, texture, "
                    f"and controller as the previous image. "
                    f"Professional lifestyle product photography, editorial quality. "
                    f"No text, no watermarks."
                ),
            })

        if num_images >= 3:
            prompts.append({
                "image_type": "feature_infographic",
                "prompt": (
                    f"Create a professional e-commerce product infographic. "
                    f"Show the EXACT same product neatly folded on a clean white "
                    f"background. Add curved arrows pointing FROM feature labels "
                    f"TO the specific part of the product each feature refers to. "
                    f"The labels must be in {lang_name}:\n"
                    f"{feature_text}\n\n"
                    f"Layout: product centered, labels distributed around it "
                    f"with thin curved arrows. Clean, modern typography. "
                    f"All text must be clearly readable and correctly spelled."
                ),
            })

        if num_images >= 4:
            prompts.append({
                "image_type": "detail_closeup",
                "prompt": (
                    f"Create a close-up detail shot. Focus on the controller/"
                    f"remote and the fabric texture. Show the controller "
                    f"clearly with its display and buttons, lying on the soft "
                    f"fabric. The controller MUST match exactly what you created "
                    f"in the previous images — same button layout, same display, "
                    f"same housing shape. "
                    f"Macro product photography, shallow depth of field, soft "
                    f"natural lighting. No text overlays."
                ),
            })

        if num_images >= 5:
            prompts.append({
                "image_type": "creative_scene",
                "prompt": (
                    f"Show the EXACT same product being used by a person relaxing "
                    f"on a bed in a cozy bedroom. The person is comfortably "
                    f"wrapped in the product, reading a book, looking warm and "
                    f"content. Soft evening lighting, modern bedroom, neutral "
                    f"tones. Keep the same product color, texture, and pattern. "
                    f"Professional lifestyle photography. No text, no watermarks."
                ),
            })

        return prompts

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  MODE B: CHAINED (Images API fallback)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def _generate_chained(
        self,
        ref_bytes: bytes,
        ref_url: str,
        product_description: str,
        lang_name: str,
        features: list[str],
        product_instructions: str,
        num_images: int,
    ) -> list[dict]:
        """Generate images via chained images.edit calls (fallback)."""

        # Analyze reference → JSON product spec
        logger.info("Analyzing reference image with GPT-4o Vision …")
        product_spec = self._analyze_reference_image(ref_url, product_description)
        if product_spec:
            logger.info("Product spec extracted: %s", product_spec[:200])
        else:
            logger.warning("Could not extract product spec — continuing without")
            product_spec = ""

        self._product_spec = product_spec
        self._product_instructions = product_instructions

        generated_images: list[dict] = []

        # Image 1 — Main product (from competitor ref)
        if num_images >= 1:
            main_prompt = self._prompt_main(product_description)
            main_checks = self._checks_main()
            logger.info("Generating image 1/%d (main_white_bg) …", num_images)

            main_data = self._generate_with_quality_loop(
                ref_bytes, main_prompt, main_checks,
                "main_white_bg", product_description,
            )
            if main_data:
                generated_images.append({
                    "image_data": main_data,
                    "image_type": "main_white_bg",
                })
                logger.info("✓ Image 1 (main_white_bg) accepted")
            else:
                logger.error("Failed to generate main product image")
                return generated_images

        canonical = main_data

        # Image 2 — Lifestyle (from Image 1)
        if num_images >= 2:
            lifestyle_prompt = self._prompt_lifestyle(product_description)
            lifestyle_checks = self._checks_lifestyle()
            logger.info("Generating image 2/%d (lifestyle) …", num_images)

            lifestyle_data = self._generate_with_quality_loop(
                canonical, lifestyle_prompt, lifestyle_checks,
                "lifestyle", product_description,
            )
            if lifestyle_data:
                generated_images.append({
                    "image_data": lifestyle_data,
                    "image_type": "lifestyle",
                })
                logger.info("✓ Image 2 (lifestyle) accepted")

        # Image 3 — Infographic (from Image 1)
        if num_images >= 3:
            infographic_prompt = self._prompt_infographic(
                product_description, lang_name, features
            )
            infographic_checks = self._checks_infographic(lang_name)
            logger.info("Generating image 3/%d (feature_infographic) …", num_images)

            infographic_data = self._generate_with_quality_loop(
                canonical, infographic_prompt, infographic_checks,
                "feature_infographic", product_description,
            )
            if infographic_data:
                generated_images.append({
                    "image_data": infographic_data,
                    "image_type": "feature_infographic",
                })
                logger.info("✓ Image 3 (feature_infographic) accepted")

        # Image 4 — Detail close-up (from Image 1)
        if num_images >= 4:
            detail_prompt = self._prompt_detail(product_description)
            detail_checks = self._checks_detail()
            logger.info("Generating image 4/%d (detail_closeup) …", num_images)

            detail_data = self._generate_with_quality_loop(
                canonical, detail_prompt, detail_checks,
                "detail_closeup", product_description,
            )
            if detail_data:
                generated_images.append({
                    "image_data": detail_data,
                    "image_type": "detail_closeup",
                })
                logger.info("✓ Image 4 (detail_closeup) accepted")

        # Image 5 — Creative (from Image 2 / lifestyle)
        if num_images >= 5:
            chain_source = lifestyle_data if lifestyle_data else canonical
            creative_prompt = self._prompt_creative(product_description)
            creative_checks = self._checks_creative()
            logger.info("Generating image 5/%d (creative_scene) …", num_images)

            creative_data = self._generate_with_quality_loop(
                chain_source, creative_prompt, creative_checks,
                "creative_scene", product_description,
            )
            if creative_data:
                generated_images.append({
                    "image_data": creative_data,
                    "image_type": "creative_scene",
                })
                logger.info("✓ Image 5 (creative_scene) accepted")

        logger.info(
            "Chained pipeline complete: %d/%d images",
            len(generated_images), num_images,
        )
        return generated_images

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  QUALITY LOOP (chained mode only)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def _generate_with_quality_loop(
        self,
        input_bytes: bytes,
        prompt: str,
        quality_checks: str,
        image_type: str,
        product_description: str,
    ) -> Optional[bytes]:
        """Generate an image with quality inspection and auto-retry."""
        current_prompt = prompt
        best_image: Optional[bytes] = None

        for attempt in range(1, MAX_QUALITY_RETRIES + 2):
            image_data = self._call_image_edit(input_bytes, current_prompt)
            if not image_data:
                logger.warning(
                    "  Attempt %d/%d: generation failed, retrying …",
                    attempt, MAX_QUALITY_RETRIES + 1,
                )
                continue

            best_image = image_data

            if attempt > MAX_QUALITY_RETRIES:
                logger.info("  Attempt %d: max retries, using best result", attempt)
                break

            logger.info("  Attempt %d: inspecting quality …", attempt)
            passed, issues = self._inspect_quality(
                image_data, quality_checks, product_description,
            )

            if passed:
                logger.info("  Attempt %d: PASSED ✓", attempt)
                break
            else:
                logger.info("  Attempt %d: FAILED — %s", attempt, issues[:150])
                current_prompt = (
                    f"{prompt}\n\n"
                    f"IMPORTANT CORRECTIONS:\n{issues}"
                )

        return best_image

    def _inspect_quality(
        self,
        image_data: bytes,
        quality_checks: str,
        product_description: str,
    ) -> tuple[bool, str]:
        """Use GPT-4o Vision to inspect quality."""
        try:
            b64_generated = base64.b64encode(image_data).decode("utf-8")
            generated_url = f"data:image/png;base64,{b64_generated}"

            response = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": [
                    {
                        "type": "text",
                        "text": (
                            f"You are a quality inspector for e-commerce product "
                            f"images. The product is: {product_description}\n\n"
                            f"Inspect this image:\n{quality_checks}\n\n"
                            f"Respond with ONLY valid JSON:\n"
                            f'{{"pass": true}} or {{"pass": false, "issues": "..."}}\n'
                            f"Be strict but reasonable."
                        ),
                    },
                    {
                        "type": "image_url",
                        "image_url": {"url": generated_url, "detail": "low"},
                    },
                ]}],
                max_tokens=250,
            )

            text = response.choices[0].message.content.strip()
            if text.startswith("```"):
                text = text.split("\n", 1)[-1]
                text = text.rsplit("```", 1)[0]

            result = json.loads(text)
            return result.get("pass", False), result.get("issues", "")

        except Exception as e:
            logger.error("Quality inspection failed: %s", e)
            return True, ""

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  REFERENCE ANALYSIS (chained mode)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def _analyze_reference_image(
        self, image_url: str, product_description: str
    ) -> str:
        """Analyze reference image → detailed JSON product specification."""
        try:
            response = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[{
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": (
                                f"You are a product photography analyst. The product is: "
                                f"{product_description}\n\n"
                                f"Analyze this product image in EXTREME detail:\n"
                                f"- Main product: shape, color, material, texture, pattern, "
                                f"  folding style\n"
                                f"- Accessories (remote control, cable, etc.): EXACT shape, "
                                f"  color, button count, button layout, display type, "
                                f"  housing shape, cable type\n"
                                f"- Labels, tags, or markings\n"
                                f"- Any other distinctive details\n\n"
                                f"Return ONLY valid JSON."
                            ),
                        },
                        {
                            "type": "image_url",
                            "image_url": {"url": image_url, "detail": "high"},
                        },
                    ],
                }],
                max_tokens=800,
            )

            text = response.choices[0].message.content.strip()
            if text.startswith("```"):
                text = text.split("\n", 1)[-1]
                text = text.rsplit("```", 1)[0]

            parsed = json.loads(text)
            return json.dumps(parsed, indent=2)

        except Exception as e:
            logger.error("Reference image analysis failed: %s", e)
            return ""

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  PROMPTS (chained mode)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def _spec_block(self) -> str:
        parts = []
        spec = getattr(self, "_product_spec", "")
        instructions = getattr(self, "_product_instructions", "")
        if spec:
            parts.append(
                f"\n\nPRODUCT SPECIFICATION (match these details precisely):\n{spec}"
            )
        if instructions:
            parts.append(f"\n\nUSER INSTRUCTIONS:\n{instructions}")
        return "".join(parts)

    def _prompt_main(self, description: str) -> str:
        return (
            f"Recreate this product ({description}) on a clean white background. "
            f"Show it neatly folded from a slightly different angle. "
            f"Keep the exact same product — same color, fabric texture, stitching "
            f"pattern, and controller design. "
            f"Remove any logos, watermarks, brand names, or text. "
            f"Professional e-commerce product photography with soft natural shadows. "
            f"No text overlays, no background elements."
            + self._spec_block()
        )

    def _prompt_lifestyle(self, description: str) -> str:
        return (
            f"Take this product ({description}) and place it draped on a "
            f"comfortable sofa in a cozy modern living room. Warm natural "
            f"lighting from a window, a cup of tea on a side table, soft "
            f"cushions. The product should look exactly like the input image — "
            f"same color, texture, and controller design. "
            f"Remove any logos, watermarks, or brand names. "
            f"Professional lifestyle product photography, editorial quality. "
            f"No text, no watermarks, no overlays."
            + self._spec_block()
        )

    def _prompt_infographic(
        self, description: str, lang_name: str, features: list[str]
    ) -> str:
        feature_text = "\n".join(f"- {f}" for f in features[:6])
        return (
            f"Create a professional e-commerce product infographic for this "
            f"product ({description}). Show the product neatly folded on a "
            f"clean white background. "
            f"Add curved arrows pointing FROM feature labels TO the relevant "
            f"product area. Labels must be in {lang_name}:\n"
            f"{feature_text}\n\n"
            f"Product centered, labels around it with thin curved arrows. "
            f"Clean, modern typography. All text clearly readable. "
            f"Remove any logos, watermarks, or brand names."
            + self._spec_block()
        )

    def _prompt_detail(self, description: str) -> str:
        return (
            f"Create a close-up detail shot of this product ({description}). "
            f"Focus on the controller/remote and the fabric texture. "
            f"Show the controller clearly with its display and buttons, "
            f"lying on the soft fabric. Controller and fabric must match "
            f"the input image exactly. "
            f"Remove any logos, watermarks, or brand names. "
            f"Macro product photography, shallow depth of field, soft lighting."
            + self._spec_block()
        )

    def _prompt_creative(self, description: str) -> str:
        return (
            f"Take this product ({description}) and show it being used by a "
            f"person relaxing on a bed in a cozy bedroom. Person wrapped in "
            f"the product, reading a book, looking warm and content. "
            f"Soft evening lighting, modern bedroom, neutral tones. "
            f"Product should look exactly like the input — same color, "
            f"texture, and pattern. "
            f"Remove any logos, watermarks, or brand names. "
            f"Professional lifestyle photography. No text, no watermarks."
            + self._spec_block()
        )

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  QUALITY CHECKS (chained mode)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    @staticmethod
    def _checks_main() -> str:
        return (
            "1. Product clearly visible and neatly folded\n"
            "2. Clean white background\n"
            "3. No logos, watermarks, or brand names\n"
            "4. Controller visible with display and buttons\n"
            "5. Only ONE controller (not two)\n"
            "6. Professional quality, no artifacts"
        )

    @staticmethod
    def _checks_lifestyle() -> str:
        return (
            "1. Product draped on sofa in living room\n"
            "2. No logos, watermarks, or brand names\n"
            "3. Controller visible\n"
            "4. Only ONE controller\n"
            "5. Warm, cozy lighting, editorial quality\n"
            "6. No text overlays"
        )

    @staticmethod
    def _checks_infographic(lang_name: str) -> str:
        return (
            f"1. Product visible and centered on white background\n"
            f"2. Feature labels present and readable\n"
            f"3. Labels in {lang_name}\n"
            f"4. Arrows point from labels to product areas\n"
            f"5. No logos, watermarks, or brand names\n"
            f"6. Text cleanly rendered and correctly spelled"
        )

    @staticmethod
    def _checks_detail() -> str:
        return (
            "1. Close-up shows controller clearly\n"
            "2. Display and buttons visible\n"
            "3. Fabric texture visible\n"
            "4. No logos, watermarks, or brand names\n"
            "5. Shallow depth of field, professional quality\n"
            "6. No text overlays"
        )

    @staticmethod
    def _checks_creative() -> str:
        return (
            "1. Person using/wrapped in product in bedroom\n"
            "2. Product color and texture match\n"
            "3. No logos, watermarks, or brand names\n"
            "4. Warm, cozy lighting, professional quality\n"
            "5. No text overlays\n"
            "6. Scene looks natural"
        )

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  FEATURE EXTRACTION (GPT-4o Vision)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def _extract_features(
        self, image_url: str, product_description: str, language_name: str
    ) -> list[str]:
        """Extract key selling features via GPT-4o Vision."""
        try:
            response = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[{
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": (
                                f"Analyze this product image. Product: {product_description}\n"
                                f"Extract 5-6 key selling features for an infographic.\n"
                                f"Short punchy labels in {language_name} (max 3 words).\n"
                                f"Return ONLY a JSON array of strings."
                            ),
                        },
                        {
                            "type": "image_url",
                            "image_url": {"url": image_url},
                        },
                    ],
                }],
                max_tokens=300,
            )

            text = response.choices[0].message.content.strip()
            if text.startswith("```"):
                text = text.split("\n", 1)[-1]
                text = text.rsplit("```", 1)[0]

            parsed = json.loads(text)
            labels = []
            for item in parsed:
                if isinstance(item, str):
                    labels.append(item)
                elif isinstance(item, dict):
                    labels.append(item.get("label", str(item)))
            return labels

        except Exception as e:
            logger.error("Feature extraction failed: %s", e)
            return [
                "Premium Qualität",
                "Einfache Bedienung",
                "Sicher & Geprüft",
                "Pflegeleicht",
                "Schnelle Lieferung",
            ]

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  FILE UPLOAD (Responses API)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def _upload_file(self, image_bytes: bytes) -> Optional[str]:
        """Upload an image to OpenAI Files API, return file ID."""
        try:
            with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
                tmp.write(image_bytes)
                tmp_path = tmp.name

            try:
                with open(tmp_path, "rb") as f:
                    file_obj = self.client.files.create(
                        file=f,
                        purpose="vision",
                    )
                return file_obj.id
            finally:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass

        except Exception as e:
            logger.error("File upload failed: %s", e)
            return None

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  RESPONSES API AVAILABILITY CHECK
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def _check_responses_api(self) -> bool:
        """
        Check if the Responses API with image_generation tool is available.
        Caches the result for the lifetime of this generator instance.
        """
        if self._responses_api_available is not None:
            return self._responses_api_available

        try:
            # Quick probe: try creating a minimal response with image tool
            response = self.client.responses.create(
                model="gpt-4o",
                input="Generate a 1x1 white pixel. Minimal.",
                tools=[{
                    "type": "image_generation",
                    "quality": "low",
                    "size": "1024x1024",
                }],
            )
            self._responses_api_available = True
            logger.info("✓ Responses API with image_generation is available")
            return True

        except Exception as e:
            error_msg = str(e)
            if "verified" in error_msg.lower() or "403" in error_msg:
                logger.warning(
                    "Responses API image_generation not available "
                    "(org verification required). Using chained fallback."
                )
            else:
                logger.warning(
                    "Responses API image_generation probe failed: %s. "
                    "Using chained fallback.", e
                )
            self._responses_api_available = False
            return False

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  CORE API CALLS (Images API — used by chained mode + regeneration)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def _call_image_edit(self, image_bytes: bytes, prompt: str) -> Optional[bytes]:
        """Call gpt-image-1 images.edit with one image."""
        try:
            with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
                tmp.write(image_bytes)
                tmp_path = tmp.name

            try:
                with open(tmp_path, "rb") as f:
                    result = self.client.images.edit(
                        model="gpt-image-1",
                        image=[f],
                        prompt=prompt,
                        size="1024x1024",
                    )
                b64_data = result.data[0].b64_json
                return base64.b64decode(b64_data)
            finally:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass

        except Exception as e:
            logger.error("gpt-image-1 API call failed: %s", e)
            return None

    def _call_image_edit_multi(self, image_bytes_list: list, prompt: str) -> Optional[bytes]:
        """Call gpt-image-1 images.edit with multiple input images."""
        try:
            logger.info(
                "Calling gpt-image-1 with %d images (sizes: %s)",
                len(image_bytes_list),
                [len(b) for b in image_bytes_list],
            )
            tmp_paths = []
            for img_bytes in image_bytes_list:
                if not img_bytes or len(img_bytes) == 0:
                    logger.warning("Skipping empty image bytes")
                    continue
                tmp = tempfile.NamedTemporaryFile(suffix=".png", delete=False)
                tmp.write(img_bytes)
                tmp.close()
                tmp_paths.append(tmp.name)

            try:
                file_handles = [open(p, "rb") for p in tmp_paths]
                result = self.client.images.edit(
                    model="gpt-image-1",
                    image=file_handles,
                    prompt=prompt,
                    size="1024x1024",
                )
                for fh in file_handles:
                    fh.close()
                b64_data = result.data[0].b64_json
                return base64.b64decode(b64_data)
            finally:
                for p in tmp_paths:
                    try:
                        os.unlink(p)
                    except OSError:
                        pass

        except Exception as e:
            logger.error("gpt-image-1 multi-image API call failed: %s", e)
            return None

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  HELPERS
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    @staticmethod
    def _ensure_png(image_bytes: bytes) -> bytes:
        """Convert any image format to PNG bytes."""
        try:
            img = Image.open(io.BytesIO(image_bytes))
            buf = io.BytesIO()
            img.save(buf, format="PNG")
            return buf.getvalue()
        except Exception:
            return image_bytes

    @staticmethod
    def _image_to_bytes(image: Image.Image, fmt: str = "PNG") -> bytes:
        """Convert PIL Image to bytes."""
        buf = io.BytesIO()
        if fmt.upper() == "JPEG":
            image = image.convert("RGB")
        image.save(buf, format=fmt, quality=95)
        return buf.getvalue()


def download_image(url: str) -> Optional[bytes]:
    """Download an image from a URL."""
    try:
        response = requests.get(
            url, timeout=30,
            headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"},
        )
        response.raise_for_status()
        return response.content
    except Exception as e:
        logger.error("Failed to download image from %s: %s", url, e)
        return None
