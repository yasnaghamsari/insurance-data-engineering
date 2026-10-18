"""Save one PNG per Metabase dashboard (built by setup_metabase.py) to images/.

    pip install playwright && playwright install chromium
    python scripts/screenshot_dashboards.py

Logs in through the API, opens each dashboard in headless Chromium at a
1600 px wide viewport, waits until every card has finished loading, fails
loudly if any card shows an error, and saves a screenshot cropped to the
dashboard itself. Set PLAYWRIGHT_CHANNEL=chrome to drive an installed Google
Chrome instead of Playwright's bundled Chromium. Credentials come from
MB_ADMIN_EMAIL / MB_ADMIN_PASSWORD (environment or .env), as for
setup_metabase.py.
"""

from __future__ import annotations

import os
import re
import sys

from playwright.sync_api import sync_playwright
from setup_metabase import QUERIES_DIR, REPO_ROOT, Metabase, load_dotenv, title_from_name

IMAGES_DIR = REPO_ROOT / "images"
VIEWPORT = {"width": 1600, "height": 1000}
LOADING = '[data-testid="loading-indicator"], [data-testid="loading-spinner"], .LoadingSpinner'
ERROR_TEXT = re.compile(r"There was a problem|Something.s gone wrong|error", re.IGNORECASE)


def slug(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def dashboard_ids(mb: Metabase) -> dict[str, int]:
    wanted = [title_from_name(p.name) for p in sorted(QUERIES_DIR.iterdir()) if p.is_dir()]
    found = {d["name"]: d["id"] for d in mb.request("GET", "/search", params={"models": "dashboard"})["data"]}
    missing = [name for name in wanted if name not in found]
    if missing:
        raise RuntimeError(f"Dashboards not found (run setup_metabase.py first): {missing}")
    return {name: found[name] for name in wanted}


def main() -> int:
    load_dotenv(REPO_ROOT / ".env")
    mb = Metabase(os.environ.get("METABASE_URL", "http://localhost:3000"))
    mb.wait_until_healthy()
    mb.login(os.environ["MB_ADMIN_EMAIL"], os.environ["MB_ADMIN_PASSWORD"])
    session_id = mb.session.headers["X-Metabase-Session"]

    failures = []
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True, channel=os.environ.get("PLAYWRIGHT_CHANNEL") or None)
        context = browser.new_context(viewport=VIEWPORT, device_scale_factor=1)
        context.add_cookies([{"name": "metabase.SESSION", "value": session_id, "url": mb.url}])
        page = context.new_page()

        for name, dashboard_id in dashboard_ids(mb).items():
            page.goto(f"{mb.url}/dashboard/{dashboard_id}", wait_until="networkidle")
            page.wait_for_selector('[data-testid="dashcard"]', timeout=60_000)
            page.wait_for_function(
                f"document.querySelectorAll('{LOADING}').length === 0", timeout=120_000, polling=500
            )
            page.wait_for_load_state("networkidle")
            page.wait_for_timeout(1500)  # chart enter animations

            cards = page.locator('[data-testid="dashcard"]')
            errored = [
                i for i in range(cards.count()) if cards.nth(i).locator('[data-testid*="error"]').count()
            ]
            if errored:
                failures.append(f"{name}: {len(errored)} card(s) show an error")

            # Grow the viewport to the dashboard's full height so nothing is cut off.
            target = page.locator('[data-testid="dashboard-grid"]').first
            box = target.bounding_box()
            page.set_viewport_size({"width": VIEWPORT["width"], "height": int(box["y"] + box["height"]) + 40})
            page.wait_for_timeout(1500)

            header = page.locator('[data-testid="dashboard-header"]').first
            top = header.bounding_box()["y"] if header.count() else box["y"]
            box = target.bounding_box()
            height = box["y"] + box["height"] - top
            out = IMAGES_DIR / f"{slug(name)}.png"
            page.screenshot(
                path=str(out), clip={"x": box["x"], "y": top, "width": box["width"], "height": height}
            )
            page.set_viewport_size(VIEWPORT)
            print(f"{name}: {cards.count()} cards -> {out.relative_to(REPO_ROOT)}")

        browser.close()

    for failure in failures:
        print(f"FAILED: {failure}", file=sys.stderr)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
