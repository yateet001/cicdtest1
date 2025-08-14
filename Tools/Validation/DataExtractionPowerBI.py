import sys
import os
import asyncio
import logging
import re
import json
from typing import Dict, List, Set, Tuple, Optional
from playwright.async_api import async_playwright, Page, Browser, BrowserContext, Download

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def _resolve_edge_user_data_and_profile():
    """
    Returns (edge_user_data_dir, edge_profile_dir_name)
    edge_profile_dir_name is typically 'Default', 'Profile 1', 'Profile 2', ...
    You can override via env EDGE_PROFILE_DIR.
    """
    # Detect base Edge user-data dir by OS
    if sys.platform.startswith("win"):
        base = os.path.join(os.environ.get("LOCALAPPDATA", ""), "Microsoft", "Edge", "User Data")
    elif sys.platform == "darwin":
        base = os.path.expanduser("~/Library/Application Support/Microsoft Edge")
    else:
        # Linux
        base = os.path.expanduser("~/.config/microsoft-edge")

    profile = os.environ.get("EDGE_PROFILE_DIR", "Default")  # e.g., "Default", "Profile 1"
    return base, profile


# -------------------- UTIL -------------------- #
def _sanitize_filename(s: str) -> str:
    return (
        (s or "")
        .replace("/", "_")
        .replace("\\", "_")
        .replace(":", "_")
        .replace("?", "_")
        .replace("*", "_")
        .replace("|", "_")
        .replace('"', "_")
        .replace("<", "_")
        .replace(">", "_")
        .replace(" ", "_")
        .strip("_")
    )

def _norm(s: str) -> str:
    """Normalize for matching (case-insensitive, collapse whitespace)."""
    return re.sub(r"\s+", " ", (s or "").strip()).lower()

# -------------------- RUNNER (MULTI-REPORT) -------------------- #
class ReportsRunner:
    def __init__(self):
        cfg_path = os.environ.get("REPORT_CONFIG_FILE", "reports.json")
        if not os.path.isfile(cfg_path):
            raise FileNotFoundError(
                f"Config file '{cfg_path}' not found. Set REPORT_CONFIG_FILE or create reports.json."
            )
        with open(cfg_path, "r", encoding="utf-8") as f:
            raw = json.load(f)

        all_reports = raw.get("reports", [])
        if not all_reports:
            raise ValueError("Config has no 'reports' entries.")

        desired = os.environ.get("REPORT_NAME")
        if desired:
            self.reports = [r for r in all_reports if _norm(r.get("name")) == _norm(desired)]
            if not self.reports:
                raise ValueError(f"REPORT_NAME='{desired}' not found in config.")
        else:
            self.reports = all_reports  # iterate all

    async def run(self):
        async with async_playwright() as p:
            edge_user_data_dir, edge_profile = _resolve_edge_user_data_and_profile()

            # Safety: ensure Edge user-data exists
            if not os.path.isdir(edge_user_data_dir):
                raise RuntimeError(
                    f"Edge user-data directory not found: {edge_user_data_dir}\n"
                    "Open Microsoft Edge once to initialize the profile, or set EDGE_PROFILE_DIR/. "
                )

            try:
                # IMPORTANT: This uses your real Edge profile.
                # Close all Edge windows that use this profile before running, or you'll get a 'profile is in use' error.
                context = await p.chromium.launch_persistent_context(
                    user_data_dir=edge_user_data_dir,
                    channel="msedge",            # force Microsoft Edge
                    headless=False,
                    viewport={"width": 1920, "height": 1080},
                    accept_downloads=True,
                    ignore_https_errors=True,
                    args=[
                        f"--profile-directory={edge_profile}",  # use your existing profile (cookies, extensions, SSO)
                        "--disable-dev-shm-usage",
                        "--no-first-run",
                        "--no-default-browser-check",
                    ],
                )
                logger.info(f"Launched Edge with profile '{edge_profile}' at '{edge_user_data_dir}'")
            except Exception as e:
                # Most common: profile is already in use. Close Edge (all windows) and try again.
                raise RuntimeError(
                    "Could not launch Microsoft Edge with your existing profile.\n"
                    "Tip: Close all Edge windows that use this profile and rerun.\n"
                    f"Underlying error: {e}"
                )

            # In a persistent context, 'context' is already the BrowserContext
            page = context.pages[0] if context.pages else await context.new_page()

            for idx, report in enumerate(self.reports):
                name = report.get("name") or f"report_{idx+1}"
                url = report.get("url")
                pages = report.get("pages", [])

                if not url:
                    logger.warning(f"Report '{name}' missing URL. Skipping.")
                    continue

                logger.info(f"=== [{idx+1}/{len(self.reports)}] Opening report: {name} ===")
                await page.goto(url, wait_until="domcontentloaded")

                # With your real profile, you should already be signed in.
                # Keep this guard just in case first run still needs a one-time login.
                if idx == 0:
                    try:
                        await page.wait_for_selector("[data-testid='artifact-info-title'], #pvExplorationHost", timeout=30000)
                    except:
                        print("\nIf you see a login or MFA prompt, complete it in the Edge window.")
                        input("Press Enter after the report is fully loaded (you should see report visuals)... ")

                # Build plan (same as before)
                pages_order = [p.get("name", f"page_{i+1}") for i, p in enumerate(pages)]
                page_visuals = {
                    _norm(p.get("name", f"page_{i+1}")): {_norm(v) for v in p.get("visuals", [])}
                    for i, p in enumerate(pages)
                }

                worker = SingleReportWorker(
                    page=page,
                    config_report_name=name,
                    pages_order=pages_order,
                    page_visuals=page_visuals
                )
                await worker.run_for_current_report(url)

            await context.close()


# -------------------- SINGLE REPORT WORKER -------------------- #
class SingleReportWorker:
    def __init__(self, page: Page, config_report_name: str, pages_order: List[str], page_visuals: Dict[str, Set[str]]):
        self.page = page
        self.config_report_name = config_report_name
        self.pages_order = pages_order or []
        self.page_visuals = page_visuals or {}
        self.report_name: Optional[str] = None
        self.download_dir: Optional[str] = None

    async def run_for_current_report(self, url: str):
        await self._setup_report_folder()  # uses config name
        # Try to get pages nav
        try:
            await self.page.wait_for_selector('[data-testid="pages-navigation-list"]', timeout=30000)
            pages_pane = await self.page.query_selector('[data-testid="pages-navigation-list"]')
            page_items = await pages_pane.query_selector_all('[data-testid="pages-navigation-list-items"]')
        except Exception as e:
            logger.warning(f"Could not find pages navigation list: {e}. Will operate on current page.")
            page_items = []

        if self.pages_order and page_items:
            # Map normalized aria-label -> element
            tabs: List[Tuple[object, str]] = []
            for it in page_items:
                label = await it.get_attribute("aria-label") or ""
                label_clean = re.sub(r'\s*selected\s*$', '', label, flags=re.IGNORECASE).strip()
                tabs.append((it, label_clean))

            for cfg_page_name in self.pages_order:
                norm_cfg = _norm(cfg_page_name)
                safe_page_name = _sanitize_filename(cfg_page_name)

                # find matching tab
                match_el = None
                for it, lab in tabs:
                    if _norm(lab) == norm_cfg:
                        match_el = it
                        break
                if not match_el:
                    logger.warning(f"Page '{cfg_page_name}' from config not found in the report; skipping.")
                    continue

                logger.info(f"[{self.config_report_name}] Switching to page: {cfg_page_name}")
                try:
                    await match_el.click()
                    await self.page.wait_for_timeout(1500)
                except Exception as e:
                    logger.warning(f"Failed to click page '{cfg_page_name}': {e}")
                    continue

                await self._capture_page_screenshot(safe_page_name)
                allowed_visuals = self.page_visuals.get(norm_cfg, set())
                await self._export_visuals_on_current_page(safe_page_name, allowed_visuals)

        else:
            # Single-page or tabs not available—use union of visuals across config
            logger.info(f"[{self.config_report_name}] Operating on current page (no tabs or no pages in config).")
            await self._capture_page_screenshot("current_page")
            allowed = set()
            for vset in self.page_visuals.values():
                allowed |= vset
            await self._export_visuals_on_current_page("current_page", allowed)

    async def _setup_report_folder(self):
        # Use the name from config to ensure stable, per-report folder
        safe_report_name = _sanitize_filename(self.config_report_name) or "PowerBI_Report"
        self.report_name = safe_report_name
        self.download_dir = os.path.join(os.path.abspath(os.getcwd()), self.report_name)
        os.makedirs(self.download_dir, exist_ok=True)

    async def _capture_page_screenshot(self, safe_page_name: str):
        screenshot_dir = os.path.join(self.download_dir, safe_page_name, "screenshot")
        os.makedirs(screenshot_dir, exist_ok=True)
        screenshot_path = os.path.join(screenshot_dir, f"{safe_page_name}.png")
        try:
            # OVERWRITE behavior: direct save (Playwright overwrites if file exists)
            await self.page.screenshot(path=screenshot_path, full_page=True)
            logger.info(f"Screenshot saved: {screenshot_path}")
        except Exception as e:
            logger.error(f"Failed to save screenshot for {safe_page_name}: {e}")

    async def _get_visual_title_for_matching(self, container) -> str:
        # Try header title attribute
        try:
            title_el = await container.query_selector("div[data-testid='visual-title']")
            if title_el:
                t = await title_el.get_attribute("title")
                if t and t.strip():
                    return t.strip()
        except:
            pass

        # Fallback aria-label
        try:
            al = await container.get_attribute("aria-label")
            if al and al.strip():
                return al.strip()
        except:
            pass

        return ""

    async def _get_visual_name_for_files(self, container, index: int) -> str:
        name = ""
        try:
            title = await container.query_selector("div[data-testid='visual-title']")
            if title:
                t = await title.get_attribute("title")
                if t:
                    name = t.strip()
        except:
            pass

        if not name:
            name = (await container.get_attribute("aria-label") or "").strip()

        if not name:
            rd = (await container.get_attribute("aria-roledescription") or "visual").strip()
            name = f"{rd}_{index+1}"

        return _sanitize_filename(name)

    async def _open_menu_for_container(self, container, retries=3):
        for _ in range(retries):
            try:
                await container.scroll_into_view_if_needed()
                await container.hover()
                await self.page.wait_for_timeout(200)

                more_btn = await container.query_selector(
                    "button[aria-label*='More options'], "
                    "button[data-testid='visual-more-options-btn'], "
                    ".vcMenuBtn"
                )
                if not more_btn:
                    await self.page.wait_for_timeout(300)
                    more_btn = await container.query_selector(
                        "button[aria-label*='More options'], "
                        "button[data-testid='visual-more-options-btn'], "
                        ".vcMenuBtn"
                    )
                if not more_btn:
                    await self.page.mouse.wheel(0, 200)
                    await self.page.wait_for_timeout(150)
                    continue

                try:
                    await self.page.evaluate("(b) => b.click()", more_btn)
                except:
                    await more_btn.click()

                await self.page.wait_for_selector("[role='menu'], .pbi-menu", timeout=3000)
                menus = await self.page.query_selector_all("[role='menu'], .pbi-menu")
                for m in menus[::-1]:
                    if await m.is_visible():
                        return m
            except:
                pass

            await self.page.mouse.wheel(0, 200)
            await self.page.wait_for_timeout(200)

        return None

    async def _export_visuals_on_current_page(self, safe_page_name: str, allowed_visuals: Set[str]):
        allowed_visuals = allowed_visuals or set()
        if not allowed_visuals:
            logger.info("No allowed visuals configured for this page; skipping.")
            return

        await self.page.wait_for_selector(".visualContainer[role='group']", timeout=60000)

        all_containers = await self.page.query_selector_all(".visualContainer[role='group']")
        containers = []
        for c in all_containers:
            try:
                if await c.is_visible():
                    containers.append(c)
            except:
                pass

        logger.info(f"Visual containers found: {len(all_containers)} | Visible containers: {len(containers)}")
        if not containers:
            logger.warning("No visible visuals found on this page.")
            return
        
        page_dir = os.path.join(self.download_dir, safe_page_name)
        visuals_dir = os.path.join(page_dir, "visuals")
        data_dir    = os.path.join(page_dir, "data")
        os.makedirs(visuals_dir, exist_ok=True)
        os.makedirs(data_dir, exist_ok=True)

        processed_any = False
        for i, container in enumerate(containers):
            try:
                human_title = await self._get_visual_title_for_matching(container)
                if _norm(human_title) not in allowed_visuals:
                    continue  # not in whitelist

                processed_any = True
                visual_name = await self._get_visual_name_for_files(container, i)

                # Crop target: inner 'visualWrapper' preferred
                wrapper = await container.query_selector("[data-testid='visual-style'].visualWrapper, .visualWrapper")
                target_for_shot = wrapper or container

                # ---- Screenshot (OVERWRITE) ----
                img_path = os.path.join(visuals_dir, f"{visual_name}.png")
                await target_for_shot.screenshot(path=img_path)
                logger.info(f"[{self.config_report_name} | {safe_page_name} | {human_title}] Screenshot → {img_path}")

                # ---- Export data ----
                menu = await self._open_menu_for_container(container, retries=3)
                if not menu:
                    logger.warning(f"[{human_title}] Could not open menu; skipping export.")
                    continue

                clicked = False
                items = await menu.query_selector_all(".pbi-menu-item-text-container, [role='menuitem']")
                for it in items:
                    txt = (await it.inner_text() or "").strip().lower()
                    if "export data" in txt:
                        await it.click()
                        clicked = True
                        break

                if not clicked:
                    logger.warning(f"[{human_title}] 'Export data' not found; skipping export.")
                    try:
                        await self.page.keyboard.press("Escape")
                    except:
                        pass
                    continue

                try:
                    await self.page.wait_for_selector(".pbi-modern-button", timeout=6000)
                    buttons = await self.page.query_selector_all(".pbi-modern-button")
                    did_export = False
                    for b in buttons:
                        bt = (await b.inner_text() or "").lower()
                        if "export" in bt:
                            async with self.page.expect_download() as dl_info:
                                await b.click()
                            dl: Download = await dl_info.value
                            filename = dl.suggested_filename
                            out_path = os.path.join(data_dir, filename)
                            # ---- OVERWRITE behavior ----
                            await dl.save_as(out_path)
                            logger.info(f"[{self.config_report_name} | {safe_page_name} | {human_title}] Export → {out_path}")
                            did_export = True
                            break

                    if not did_export:
                        logger.warning(f"[{human_title}] Export dialog buttons not found/clicked.")
                except Exception as e:
                    logger.warning(f"[{human_title}] Export failed: {e}")
                    try:
                        await self.page.keyboard.press("Escape")
                    except:
                        pass
                    continue

                await self.page.wait_for_timeout(150)

            except Exception as e:
                logger.error(f"Failed to process a visual: {e}")
                continue

        if not processed_any:
            logger.info("No visuals matched the whitelist on this page.")

# -------------------- ENTRY -------------------- #
if __name__ == "__main__":
    runner = ReportsRunner()
    asyncio.run(runner.run())