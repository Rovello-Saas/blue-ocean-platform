"""
Google Drive image storage for product images.

Creates a folder per product in a configurable parent folder, uploads
the generated images, and adds a _README.txt with the Shopify product
URL so the user can easily identify which listing the images belong to.
"""

from __future__ import annotations

import io
import logging
from typing import Optional

from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

from src.core.config import get_service_account_credentials, get_env

logger = logging.getLogger(__name__)

DRIVE_SCOPES = ["https://www.googleapis.com/auth/drive"]

# Configurable via .env — the ID of the parent folder in Google Drive
# where all product image folders will be created.
PARENT_FOLDER_ID = get_env("GOOGLE_DRIVE_PARENT_FOLDER_ID", "")


class DriveImageStore:
    """
    Manages per-product image folders in Google Drive.

    Usage:
        store = DriveImageStore()
        folder_url = store.upload_product_images(
            product_keyword="Electric Heated Blanket",
            shopify_handle="premium-heated-throw-blanket-timer-180x130",
            shopify_url="https://shop.example.com/products/...",
            images=[
                {"image_data": b"...", "image_type": "main_white_bg"},
                ...
            ],
        )
    """

    def __init__(self, parent_folder_id: str = ""):
        self.parent_folder_id = parent_folder_id or PARENT_FOLDER_ID
        self._service = None

    @property
    def service(self):
        """Lazy-init the Drive API service."""
        if self._service is None:
            creds = get_service_account_credentials(scopes=DRIVE_SCOPES)
            self._service = build("drive", "v3", credentials=creds)
        return self._service

    # ── Public API ────────────────────────────────────────────

    def upload_product_images(
        self,
        product_keyword: str,
        shopify_handle: str,
        shopify_url: str,
        images: list[dict],
    ) -> Optional[str]:
        """
        Create a Drive folder for this product and upload all images.

        Args:
            product_keyword: Human-readable product name
            shopify_handle: URL slug from Shopify
            shopify_url: Full Shopify product URL
            images: List of dicts with 'image_data' (bytes) and 'image_type' (str)

        Returns:
            Google Drive folder URL (shareable link), or None on failure.
        """
        if not images:
            logger.warning("No images to upload")
            return None

        try:
            # 1. Create the product folder
            folder_name = f"{product_keyword} — {shopify_handle}"
            folder_id = self._create_folder(folder_name)
            if not folder_id:
                return None

            # 2. Upload each image
            for i, img in enumerate(images):
                image_type = img.get("image_type", f"image_{i}")
                filename = f"{i + 1}_{image_type}.png"
                self._upload_file(
                    folder_id=folder_id,
                    filename=filename,
                    data=img["image_data"],
                    mime_type="image/png",
                )
                logger.info("  Uploaded %s to Drive folder", filename)

            # 3. Add a _README.txt with the Shopify link
            readme_content = (
                f"Product: {product_keyword}\n"
                f"Shopify URL: {shopify_url}\n"
                f"Handle: {shopify_handle}\n\n"
                f"This folder contains {len(images)} AI-generated product images.\n"
                f"You can edit these images in ChatGPT or any image editor,\n"
                f"then re-upload to Shopify via the product admin page.\n"
            )
            self._upload_file(
                folder_id=folder_id,
                filename="_README.txt",
                data=readme_content.encode("utf-8"),
                mime_type="text/plain",
            )

            # 4. Make the folder viewable via link
            folder_url = self._make_shareable(folder_id)

            logger.info(
                "Uploaded %d images to Drive folder: %s",
                len(images), folder_url,
            )
            return folder_url

        except Exception as e:
            logger.error("Failed to upload images to Drive: %s", e)
            return None

    # ── Internal helpers ──────────────────────────────────────

    def _create_folder(self, name: str) -> Optional[str]:
        """Create a folder in Google Drive, return its ID."""
        try:
            metadata = {
                "name": name,
                "mimeType": "application/vnd.google-apps.folder",
            }
            if self.parent_folder_id:
                metadata["parents"] = [self.parent_folder_id]

            folder = (
                self.service.files()
                .create(body=metadata, fields="id")
                .execute()
            )
            folder_id = folder.get("id")
            logger.info("Created Drive folder: %s (id: %s)", name, folder_id)
            return folder_id

        except Exception as e:
            logger.error("Failed to create Drive folder '%s': %s", name, e)
            return None

    def _upload_file(
        self,
        folder_id: str,
        filename: str,
        data: bytes,
        mime_type: str,
    ) -> Optional[str]:
        """Upload a file to a specific Drive folder, return its ID."""
        try:
            metadata = {
                "name": filename,
                "parents": [folder_id],
            }
            media = MediaIoBaseUpload(
                io.BytesIO(data),
                mimetype=mime_type,
                resumable=True,
            )
            result = (
                self.service.files()
                .create(body=metadata, media_body=media, fields="id")
                .execute()
            )
            return result.get("id")

        except Exception as e:
            logger.error("Failed to upload '%s': %s", filename, e)
            return None

    def _make_shareable(self, folder_id: str) -> str:
        """
        Set the folder to 'anyone with the link can view' and return the URL.
        """
        try:
            self.service.permissions().create(
                fileId=folder_id,
                body={"role": "reader", "type": "anyone"},
            ).execute()
        except Exception as e:
            logger.warning("Could not set sharing permissions: %s", e)

        return f"https://drive.google.com/drive/folders/{folder_id}"
