"""
Page cloner client — thin HTTP wrapper around the Node page-cloner service.

The page cloner is a separate Node/Express process that handles the
scrape → AI generate → upload images → push template → publish pipeline.
We keep it out-of-process because (a) it relies on Puppeteer and Sharp,
(b) it's already working and under active use for Merivalo, and (c) a
clean service boundary lets us evolve the Python platform without risking
the cloning pipeline.

Typical use:

    from src.page_cloner import PageClonerClient

    client = PageClonerClient()
    result = client.clone(
        source_url="https://mellowsleep.com/products/cloud-alignment-pillow",
        store="merivalo",
        target_language="de",
    )
    print(result["productUrl"])
"""

from src.page_cloner.client import (
    PageClonerClient,
    PageClonerError,
    PageClonerUnavailable,
    PageClonerJobFailed,
    PageClonerTimeout,
)

__all__ = [
    "PageClonerClient",
    "PageClonerError",
    "PageClonerUnavailable",
    "PageClonerJobFailed",
    "PageClonerTimeout",
]
