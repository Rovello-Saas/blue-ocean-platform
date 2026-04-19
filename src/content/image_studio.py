"""
Image Studio service — approval-first image generation workflow.

Flow:
1. User picks a product → system auto-generates 5 images using the
   existing AIImageGenerator pipeline (chained, quality-looped)
2. All 5 images appear in the Studio for review
3. User approves or rejects each image
4. Rejected images are regenerated using the user's feedback prompt
5. Once all images are approved → push to Shopify / Google Drive
"""

from __future__ import annotations

import base64
import io
import json
import logging
import os
import tempfile
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional

from src.core.config import AppConfig, OPENAI_API_KEY, get_env
from src.core.models import (
    ImageJob, ImageRequest, ImageJobStatus, ImageGeneratorType,
)

logger = logging.getLogger(__name__)

# Where to cache generated images locally
CACHE_DIR = Path(tempfile.gettempdir()) / "blue_ocean_image_studio"
CACHE_DIR.mkdir(exist_ok=True)

# Human-friendly image type labels
IMAGE_TYPE_LABELS = {
    "main_white_bg": "Main Product Shot",
    "lifestyle": "Lifestyle Scene",
    "feature_infographic": "Feature Infographic",
    "detail_closeup": "Detail Close-up",
    "creative_scene": "Creative / Mood Shot",
}

# Generator display names and cost estimates
GENERATOR_INFO = {
    ImageGeneratorType.OPENAI_GPT_IMAGE.value: {
        "name": "OpenAI GPT-Image-1",
        "description": "Best for complex instructions, text in images, infographics",
        "speed": "Medium",
        "cost_per_image": 0.06,
        "cost_label": "~$0.06/image",
    },
    ImageGeneratorType.GOOGLE_IMAGEN_4.value: {
        "name": "Google Imagen 4",
        "description": "High-quality photorealism from Google Vertex AI, great for GMC compliance",
        "speed": "Medium",
        "cost_per_image": 0.04,
        "cost_label": "~$0.04/image",
    },
    ImageGeneratorType.GOOGLE_IMAGEN_4_FAST.value: {
        "name": "Google Imagen 4 Fast",
        "description": "Faster variant of Imagen 4, good for quick iterations",
        "speed": "Fast",
        "cost_per_image": 0.02,
        "cost_label": "~$0.02/image",
    },
    ImageGeneratorType.NANO_BANANA.value: {
        "name": "Nano Banana (Gemini Flash)",
        "description": "Fast image generation via Gemini 2.5 Flash — good for quick iterations",
        "speed": "Fast",
        "cost_per_image": 0.02,
        "cost_label": "~$0.02/image",
    },
    ImageGeneratorType.NANO_BANANA_PRO.value: {
        "name": "Nano Banana Pro (Gemini 3 Pro)",
        "description": "Best quality: photorealistic, accurate text rendering, 4K, advanced reasoning — recommended",
        "speed": "Medium",
        "cost_per_image": 0.04,
        "cost_label": "~$0.04/image",
    },
    ImageGeneratorType.FAL_FLUX_PRO.value: {
        "name": "Flux Pro (Fal.ai)",
        "description": "Highest quality photorealism, excellent for product shots",
        "speed": "Medium",
        "cost_per_image": 0.05,
        "cost_label": "~$0.05/image",
    },
    ImageGeneratorType.FAL_FLUX_DEV.value: {
        "name": "Flux Dev (Fal.ai)",
        "description": "Good balance of quality and cost",
        "speed": "Fast",
        "cost_per_image": 0.025,
        "cost_label": "~$0.025/image",
    },
    ImageGeneratorType.FAL_FLUX_SCHNELL.value: {
        "name": "Flux Schnell (Fal.ai)",
        "description": "Ultra-fast drafts, great for iteration",
        "speed": "Very Fast",
        "cost_per_image": 0.003,
        "cost_label": "~$0.003/image",
    },
}


def get_cost_per_image(generator: str = None) -> float:
    """Get cost per image for the given generator, or the configured default."""
    if generator and generator in GENERATOR_INFO:
        return GENERATOR_INFO[generator]["cost_per_image"]
    config = AppConfig()
    default_gen = config.get(
        "image_studio.default_generator",
        ImageGeneratorType.OPENAI_GPT_IMAGE.value,
    )
    info = GENERATOR_INFO.get(default_gen)
    return info["cost_per_image"] if info else 0.06


class ImageStudioService:
    """Orchestrates auto-generation, review, and publishing of product images."""

    def __init__(self):
        self._jobs: dict[str, ImageJob] = {}
        self._image_cache: dict[str, bytes] = {}
        self._load_cache()

    # ── Auto-generate from existing pipeline ────────────────────

    def auto_generate_job(
        self,
        product_id: str,
        product_keyword: str,
        reference_image_urls: list,
        product_description: str,
        target_language: str = "en",
        num_images: int = 5,
        progress_callback=None,
        product_instructions: str = "",
        generator_key: str = "",
    ) -> ImageJob:
        """
        Auto-generate product images using the selected generator pipeline.

        Args:
            product_id: Product ID from the pipeline
            product_keyword: Human-readable product name
            reference_image_urls: Competitor reference image URLs
            product_description: Product description for prompts
            target_language: Language for infographic labels
            num_images: Number of images (1-5)
            progress_callback: Optional fn(current, total, message)
            product_instructions: Optional user-provided instructions
            generator_key: Which generator to use (default: OpenAI GPT-Image-1)
        """
        generator_key = generator_key or ImageGeneratorType.OPENAI_GPT_IMAGE.value

        # Create the job
        job = ImageJob(
            product_id=product_id,
            product_keyword=product_keyword,
            num_images=num_images,
            default_generator=generator_key,
            status=ImageJobStatus.GENERATING.value,
            notes=json.dumps({
                "reference_url": reference_image_urls[0] if reference_image_urls else "",
                "cost_initial": 0.0,
                "cost_regenerations": 0.0,
                "product_instructions": product_instructions,
                "target_language": target_language,
            }),
        )

        if progress_callback:
            progress_callback(0, num_images, "Analyzing reference image...")

        # Dispatch to the correct generator pipeline
        try:
            results = self._run_generation_pipeline(
                generator_key=generator_key,
                reference_image_urls=reference_image_urls,
                product_description=product_description,
                target_language=target_language,
                num_images=num_images,
                product_instructions=product_instructions,
            )
        except Exception as e:
            logger.error("Image generation pipeline failed: %s", e)
            job.status = ImageJobStatus.FAILED.value
            job.notes = str(e)
            self._jobs[job.job_id] = job
            self._save_cache()
            return job

        # Convert pipeline output to ImageRequest dicts
        images = []
        for i, result in enumerate(results):
            image_data = result["image_data"]
            image_type = result["image_type"]

            req = ImageRequest(
                image_index=i + 1,
                prompt=IMAGE_TYPE_LABELS.get(image_type, image_type),
                generator=generator_key,
                status=ImageJobStatus.REVIEW.value,
            )
            req_dict = req.to_dict()
            req_dict["image_type"] = image_type
            images.append(req_dict)

            # Cache the image
            self._image_cache[req.request_id] = image_data
            cache_path = CACHE_DIR / f"{req.request_id}.png"
            cache_path.write_bytes(image_data)

            if progress_callback:
                progress_callback(i + 1, num_images, f"Generated {image_type}")

        job.images = images
        job.status = ImageJobStatus.REVIEW.value

        # Track initial generation cost based on configured generator
        cost_per_image = get_cost_per_image(job.default_generator)
        initial_cost = len(images) * cost_per_image
        try:
            meta = json.loads(job.notes) if job.notes else {}
        except (json.JSONDecodeError, TypeError):
            meta = {}
        meta["cost_initial"] = round(initial_cost, 3)
        job.notes = json.dumps(meta)

        self._jobs[job.job_id] = job
        self._save_cache()

        logger.info(
            "Auto-generated %d images for '%s' (job %s)",
            len(images), product_keyword, job.job_id,
        )
        return job

    # ── Generator dispatch ─────────────────────────────────────

    @staticmethod
    def _run_generation_pipeline(
        generator_key: str,
        reference_image_urls: list,
        product_description: str,
        target_language: str,
        num_images: int,
        product_instructions: str,
    ) -> list[dict]:
        """Dispatch image generation to the selected backend."""
        if generator_key in (
            ImageGeneratorType.NANO_BANANA.value,
            ImageGeneratorType.NANO_BANANA_PRO.value,
        ):
            from src.content.nano_banana_generator import NanoBananaGenerator
            gen = NanoBananaGenerator(model_key=generator_key)
            return gen.generate_product_images(
                reference_image_urls=reference_image_urls,
                product_description=product_description,
                target_language=target_language,
                num_images=num_images,
                product_instructions=product_instructions,
            )
        else:
            # Default: OpenAI GPT-Image-1 (or any non-Nano-Banana key)
            from src.content.image_generator import AIImageGenerator
            gen = AIImageGenerator()
            return gen.generate_product_images(
                reference_image_urls=reference_image_urls,
                product_description=product_description,
                target_language=target_language,
                num_images=num_images,
                product_instructions=product_instructions,
            )

    # ── Regenerate a single rejected image with feedback ────────

    def regenerate_with_feedback(
        self,
        job_id: str,
        request_id: str,
        feedback: str,
        reference_image_bytes: Optional[bytes] = None,
        generator_override: str = "",
    ) -> bytes:
        """
        Regenerate a rejected image incorporating the user's feedback.

        Uses gpt-image-1 with the canonical (Image 1) as input,
        plus the original prompt enhanced with the user's corrections.
        Optionally accepts an additional reference image (e.g. a screenshot
        of a specific detail the user wants reproduced).
        generator_override — if set, use this generator instead of the job default.
        """
        job = self._jobs.get(job_id)
        if not job:
            raise ValueError(f"Job {job_id} not found")

        # Find the image request
        img_req = None
        img_idx = None
        for i, img in enumerate(job.images):
            if img.get("request_id") == request_id:
                img_req = img
                img_idx = i
                break

        if img_req is None:
            raise ValueError(f"Image request {request_id} not found")

        # Get canonical image (Image 1) as the base for regeneration
        canonical_bytes = None
        for img in job.images:
            if img.get("image_index") == 1:
                canonical_bytes = self.get_image_bytes(img["request_id"])
                break

        if not canonical_bytes:
            raise ValueError("Canonical image (Image 1) not found for regeneration")

        # Build the regeneration prompt
        image_type = img_req.get("image_type", "product")
        original_label = IMAGE_TYPE_LABELS.get(image_type, image_type)

        if reference_image_bytes:
            logger.info(
                "Regenerating %s with reference image (%d bytes)",
                request_id, len(reference_image_bytes),
            )
            regen_prompt = (
                f"Regenerate this {original_label} image for the product: "
                f"{job.product_keyword}.\n\n"
                f"USER FEEDBACK — apply these changes:\n{feedback}\n\n"
                f"CRITICAL INSTRUCTION: You are receiving TWO input images.\n"
                f"- Image 1 (first input): The current product image to modify.\n"
                f"- Image 2 (second input): A REFERENCE photo the user uploaded.\n\n"
                f"You MUST carefully study Image 2 and reproduce the specific element(s) "
                f"the user describes in their feedback. Copy the EXACT shape, proportions, "
                f"buttons, layout, color, and details from Image 2. Do NOT invent or "
                f"hallucinate details — match the reference precisely.\n\n"
                f"Keep the rest of the product image consistent with Image 1. "
                f"Remove any logos, watermarks, or brand names."
            )
        else:
            regen_prompt = (
                f"Regenerate this {original_label} image for the product: "
                f"{job.product_keyword}.\n\n"
                f"USER FEEDBACK — apply these changes:\n{feedback}\n\n"
                f"Keep the product appearance consistent with the input image. "
                f"Remove any logos, watermarks, or brand names."
            )

        # Save the current version before overwriting (with its generator info)
        current_version = img_req.get("retry_count", 0)
        current_gen = img_req.get("generator", job.default_generator)
        self._save_version(request_id, current_version, generator=current_gen)

        # Determine which generator to use for this regeneration
        active_gen = generator_override or job.default_generator

        # Update status and store generator on the image request
        job.images[img_idx]["status"] = ImageJobStatus.GENERATING.value
        job.images[img_idx]["feedback"] = feedback
        job.images[img_idx]["retry_count"] = current_version + 1
        job.images[img_idx]["generator"] = active_gen
        self._save_cache()

        try:
            if active_gen in (
                ImageGeneratorType.NANO_BANANA.value,
                ImageGeneratorType.NANO_BANANA_PRO.value,
            ):
                from src.content.nano_banana_generator import NanoBananaGenerator
                nb_gen = NanoBananaGenerator(model_key=active_gen)
                # Combine canonical + optional reference into the prompt
                combined_ref = canonical_bytes
                if reference_image_bytes:
                    regen_prompt += (
                        "\n\nYou are receiving a reference image alongside. "
                        "Carefully study it and reproduce the specific details."
                    )
                    combined_ref = reference_image_bytes  # Use user-provided ref
                images = nb_gen.generate(
                    prompt=regen_prompt,
                    reference_image_bytes=combined_ref,
                )
                image_data = images[0] if images else None
            else:
                from src.content.image_generator import AIImageGenerator
                generator = AIImageGenerator()
                if reference_image_bytes:
                    image_data = generator._call_image_edit_multi(
                        [canonical_bytes, reference_image_bytes], regen_prompt
                    )
                else:
                    image_data = generator._call_image_edit(canonical_bytes, regen_prompt)

            if not image_data:
                raise RuntimeError("Image generation returned no data")

            # Store result (overwrites current version, old one is saved as _vN)
            self._image_cache[request_id] = image_data
            cache_path = CACHE_DIR / f"{request_id}.png"
            cache_path.write_bytes(image_data)

            job.images[img_idx]["status"] = ImageJobStatus.REVIEW.value
            job.images[img_idx]["updated_at"] = datetime.utcnow().isoformat()

            # Track regeneration cost based on the generator used
            regen_cost = get_cost_per_image(active_gen)
            try:
                meta = json.loads(job.notes) if job.notes else {}
            except (json.JSONDecodeError, TypeError):
                meta = {}
            meta["cost_regenerations"] = round(
                meta.get("cost_regenerations", 0.0) + regen_cost, 3
            )
            job.notes = json.dumps(meta)

            self._update_job_status(job)
            self._save_cache()

            logger.info("Regenerated image %s with feedback", request_id)
            return image_data

        except Exception as e:
            job.images[img_idx]["status"] = ImageJobStatus.FAILED.value
            job.images[img_idx]["feedback"] = str(e)
            self._update_job_status(job)
            self._save_cache()
            logger.error("Regeneration failed: %s", e)
            raise

    # ── Review Actions ──────────────────────────────────────────

    def approve_image(self, job_id: str, request_id: str) -> None:
        """Approve a generated image."""
        job = self._jobs.get(job_id)
        if not job:
            raise ValueError(f"Job {job_id} not found")

        for img in job.images:
            if img.get("request_id") == request_id:
                img["status"] = ImageJobStatus.APPROVED.value
                img["updated_at"] = datetime.utcnow().isoformat()
                break

        self._update_job_status(job)
        self._save_cache()

    def reject_image(self, job_id: str, request_id: str, feedback: str = "") -> None:
        """Reject an image with feedback for regeneration."""
        job = self._jobs.get(job_id)
        if not job:
            raise ValueError(f"Job {job_id} not found")

        for img in job.images:
            if img.get("request_id") == request_id:
                img["status"] = ImageJobStatus.REJECTED.value
                img["feedback"] = feedback
                img["updated_at"] = datetime.utcnow().isoformat()
                break

        self._update_job_status(job)
        self._save_cache()

    def approve_all(self, job_id: str) -> None:
        """Approve all images in a job at once."""
        job = self._jobs.get(job_id)
        if not job:
            return
        for img in job.images:
            if img["status"] == ImageJobStatus.REVIEW.value:
                img["status"] = ImageJobStatus.APPROVED.value
                img["updated_at"] = datetime.utcnow().isoformat()
        self._update_job_status(job)
        self._save_cache()

    # ── Job / Image Retrieval ───────────────────────────────────

    def refresh(self) -> None:
        """Reload jobs from disk cache (picks up external changes)."""
        self._load_cache()

    def get_job(self, job_id: str) -> Optional[ImageJob]:
        return self._jobs.get(job_id)

    def get_jobs(self, product_id: str = None, include_archived: bool = False) -> list:
        # Always reload from disk to pick up jobs created externally
        self._load_cache()
        jobs = list(self._jobs.values())
        if product_id:
            jobs = [j for j in jobs if j.product_id == product_id]
        if not include_archived:
            jobs = [j for j in jobs if j.status != ImageJobStatus.ARCHIVED.value]
        return sorted(jobs, key=lambda j: j.created_at, reverse=True)

    def get_image_bytes(self, request_id: str) -> Optional[bytes]:
        if request_id in self._image_cache:
            return self._image_cache[request_id]
        cache_path = CACHE_DIR / f"{request_id}.png"
        if cache_path.exists():
            data = cache_path.read_bytes()
            self._image_cache[request_id] = data
            return data
        return None

    def get_approved_images(self, job_id: str) -> list:
        """Get all approved images as (request_id, bytes) tuples."""
        job = self._jobs.get(job_id)
        if not job:
            return []
        result = []
        for img in job.images:
            if img["status"] == ImageJobStatus.APPROVED.value:
                data = self.get_image_bytes(img["request_id"])
                if data:
                    result.append((img["request_id"], data))
        return result

    MAX_ARCHIVED_PER_PRODUCT = 5

    def archive_job(self, job_id: str) -> Optional[dict]:
        """
        Archive a job instead of deleting it.
        Returns the job's settings (reference_url, generator, etc.)
        so the caller can auto-create a new job with the same config.
        """
        job = self._jobs.get(job_id)
        if not job:
            return None

        # Extract settings before archiving
        meta = {}
        try:
            meta = json.loads(job.notes) if job.notes else {}
        except (json.JSONDecodeError, TypeError):
            pass

        settings = {
            "reference_url": meta.get("reference_url", ""),
            "product_id": job.product_id,
            "product_keyword": job.product_keyword,
            "num_images": job.num_images,
            "default_generator": job.default_generator,
            "product_instructions": meta.get("product_instructions", ""),
        }

        # Mark as archived
        job.status = ImageJobStatus.ARCHIVED.value
        job.updated_at = datetime.utcnow().isoformat()
        self._save_cache()

        # Purge oldest archived if over the limit
        self._purge_old_archives(job.product_id)

        logger.info("Archived job %s for product '%s'", job_id, job.product_keyword)
        return settings

    def restore_job(self, job_id: str) -> Optional[ImageJob]:
        """
        Restore an archived job back to active.
        Archives the current active job for the same product first.
        """
        job = self._jobs.get(job_id)
        if not job or job.status != ImageJobStatus.ARCHIVED.value:
            return None

        # Archive the currently-active job for this product (if any)
        active = self._get_active_job(job.product_id)
        if active:
            active.status = ImageJobStatus.ARCHIVED.value
            active.updated_at = datetime.utcnow().isoformat()

        # Restore
        job.status = ImageJobStatus.REVIEW.value
        job.updated_at = datetime.utcnow().isoformat()
        self._save_cache()

        logger.info("Restored job %s", job_id)
        return job

    def get_archived_jobs(self, product_id: str) -> list:
        """Get all archived jobs for a product, newest first."""
        return sorted(
            [j for j in self._jobs.values()
             if j.product_id == product_id and j.status == ImageJobStatus.ARCHIVED.value],
            key=lambda j: j.updated_at,
            reverse=True,
        )

    def delete_job(self, job_id: str) -> None:
        """Permanently delete a job and its images from disk."""
        job = self._jobs.pop(job_id, None)
        if job:
            for img in job.images:
                rid = img.get("request_id", "")
                self._image_cache.pop(rid, None)
                # Delete current image
                cache_path = CACHE_DIR / f"{rid}.png"
                if cache_path.exists():
                    cache_path.unlink()
                # Delete version files
                for v in range(100):
                    vp = CACHE_DIR / f"{rid}_v{v}.png"
                    vm = CACHE_DIR / f"{rid}_v{v}.json"
                    if vp.exists():
                        vp.unlink()
                    if vm.exists():
                        vm.unlink()
                    elif v > 0:
                        break
            self._save_cache()

    def _get_active_job(self, product_id: str) -> Optional[ImageJob]:
        """Get the currently active (non-archived, non-failed) job for a product."""
        for j in self._jobs.values():
            if (j.product_id == product_id
                    and j.status not in (ImageJobStatus.ARCHIVED.value, ImageJobStatus.FAILED.value)):
                return j
        return None

    def _purge_old_archives(self, product_id: str) -> None:
        """Keep only the N most recent archived jobs per product; delete the rest."""
        archived = self.get_archived_jobs(product_id)
        if len(archived) <= self.MAX_ARCHIVED_PER_PRODUCT:
            return
        to_remove = archived[self.MAX_ARCHIVED_PER_PRODUCT:]
        for old_job in to_remove:
            logger.info("Purging old archive %s", old_job.job_id)
            self.delete_job(old_job.job_id)

    # ── Version History ─────────────────────────────────────────

    def _save_version(self, request_id: str, version: int, generator: str = "") -> None:
        """Save the current image + metadata as a numbered version before overwriting."""
        current_path = CACHE_DIR / f"{request_id}.png"
        if current_path.exists():
            version_path = CACHE_DIR / f"{request_id}_v{version}.png"
            version_path.write_bytes(current_path.read_bytes())
            # Save metadata for this version
            meta_path = CACHE_DIR / f"{request_id}_v{version}.json"
            meta_path.write_text(json.dumps({
                "version": version,
                "generator": generator,
                "timestamp": datetime.utcnow().isoformat(),
            }))
            logger.info("Saved version %d for image %s", version, request_id)

    def get_version_bytes(self, request_id: str, version: int) -> Optional[bytes]:
        """Get a specific version of an image."""
        version_path = CACHE_DIR / f"{request_id}_v{version}.png"
        if version_path.exists():
            return version_path.read_bytes()
        return None

    def get_version_meta(self, request_id: str, version: int) -> dict:
        """Get metadata for a specific version (generator, timestamp)."""
        meta_path = CACHE_DIR / f"{request_id}_v{version}.json"
        if meta_path.exists():
            try:
                return json.loads(meta_path.read_text())
            except (json.JSONDecodeError, TypeError):
                pass
        return {}

    def get_all_versions(self, request_id: str, current_generator: str = "") -> list:
        """
        Get all versions for an image as a list of dicts:
        [{"version": 0, "bytes": ..., "generator": "...", "is_current": False}, ...]
        """
        versions = []
        for v in range(100):
            vb = self.get_version_bytes(request_id, v)
            if vb:
                meta = self.get_version_meta(request_id, v)
                gen_name = meta.get("generator", "")
                # Look up display name
                info = GENERATOR_INFO.get(gen_name, {})
                versions.append({
                    "version": v,
                    "bytes": vb,
                    "generator": info.get("name", gen_name) if gen_name else "Unknown",
                    "is_current": False,
                })
            elif v > 0:
                break
        # Add current version
        current_bytes = self.get_image_bytes(request_id)
        if current_bytes:
            info = GENERATOR_INFO.get(current_generator, {})
            versions.append({
                "version": len(versions),
                "bytes": current_bytes,
                "generator": info.get("name", current_generator) if current_generator else "Current",
                "is_current": True,
            })
        return versions

    # ── Internal ────────────────────────────────────────────────

    def _update_job_status(self, job: ImageJob) -> None:
        statuses = [img["status"] for img in job.images]
        if all(s == ImageJobStatus.APPROVED.value for s in statuses):
            job.status = ImageJobStatus.APPROVED.value
        elif all(s == ImageJobStatus.UPLOADED.value for s in statuses):
            job.status = ImageJobStatus.UPLOADED.value
        elif any(s == ImageJobStatus.GENERATING.value for s in statuses):
            job.status = ImageJobStatus.GENERATING.value
        elif any(s == ImageJobStatus.REVIEW.value for s in statuses):
            job.status = ImageJobStatus.REVIEW.value
        elif any(s == ImageJobStatus.FAILED.value for s in statuses):
            job.status = ImageJobStatus.FAILED.value
        else:
            job.status = ImageJobStatus.PENDING.value
        job.updated_at = datetime.utcnow().isoformat()

    def _save_cache(self) -> None:
        cache_file = CACHE_DIR / "jobs.json"
        data = {jid: j.to_dict() for jid, j in self._jobs.items()}
        cache_file.write_text(json.dumps(data, indent=2, default=str))

    def _load_cache(self) -> None:
        cache_file = CACHE_DIR / "jobs.json"
        if cache_file.exists():
            try:
                data = json.loads(cache_file.read_text())
                # Replace in-memory jobs with what's on disk (source of truth)
                self._jobs = {}
                for job_id, job_data in data.items():
                    self._jobs[job_id] = ImageJob.from_dict(job_data)
                logger.info("Loaded %d image jobs from cache", len(self._jobs))
            except Exception as e:
                logger.warning("Failed to load image job cache: %s", e)
