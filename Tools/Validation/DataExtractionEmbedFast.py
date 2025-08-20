import sys
import os
import asyncio
import logging
import re
import json
from typing import Dict, List, Set, Tuple, Optional
from pathlib import Path
from playwright.async_api import async_playwright, Page, Browser, BrowserContext, Download, Frame, TimeoutError as PWTimeout          

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def _resolve_edge_profile_dir():
    """
    Returns the path for Edge profile directory
    """
    PROFILE_DIR = ".edge-user-data"  # persistent profile (reuses your SSO)
    profile_path = str(Path(PROFILE_DIR).resolve())
    Path(profile_path).mkdir(parents=True, exist_ok=True)
    return profile_path

def likely_auth_url(u: str) -> bool:
    """Check if URL is likely an authentication endpoint"""
    u = (u or "").lower()
    return any(s in u for s in [
        "login.microsoftonline.com", "login.microsoft.com",
        "sts.", "adfs.", "sso.", "auth."
    ])

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
            profile_path = _resolve_edge_profile_dir()
            logger.info(f"Using persistent profile at: {profile_path}")

            try:
                # Launch Edge with persistent profile but allowing new tabs
                context = await p.chromium.launch_persistent_context(
                    user_data_dir=profile_path,
                    channel="msedge",
                    headless=False,
                    viewport={"width": 1920, "height": 1080},
                    accept_downloads=True,
                    ignore_https_errors=True,
                    args=[
                        "--no-first-run",
                        "--no-default-browser-check",
                    ],
                )
                logger.info(f"Launched Edge with profile at '{profile_path}'")
            except Exception as e:
                raise RuntimeError(
                    "Could not launch Microsoft Edge with persistent profile.\n"
                    f"Underlying error: {e}"
                )

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

                # Handle SSO if needed
                try:
                    await page.wait_for_load_state("domcontentloaded", timeout=20000)
                except PWTimeout:
                    pass

                if likely_auth_url(page.url):
                    logger.info("Sign-in detected. Waiting for auth completion...")
                    try:
                        not_auth_pattern = re.compile(
                            r"^(?!.*(login\.microsoftonline\.com|login\.microsoft\.com|sts\.|adfs\.|sso\.|auth\.)).*",
                            re.I
                        )
                        await page.wait_for_url(not_auth_pattern, timeout=240000)
                        logger.info("Authentication completed.")
                    except PWTimeout:
                        logger.warning("Authentication timeout. Please complete login manually.")
                        continue

                # Wait for report to load and handle iframes
                try:
                    await page.wait_for_selector(
                        "iframe, #pvExplorationHost, [data-testid='artifact-info-title']",
                        timeout=30000
                    )
                    
                    # Build plan for pages and visuals
                    pages_order = [p.get("name", f"page_{i+1}") for i, p in enumerate(pages)]
                    page_visuals = {
                        _norm(p.get("name", f"page_{i+1}")): {_norm(v) for v in p.get("visuals", [])}
                        for i, p in enumerate(pages)
                    }

                    # Check if report is in iframe and switch to it if needed
                    iframe = await page.query_selector("iframe")
                    if iframe:
                        logger.info("Report detected in iframe, switching context...")
                        frame = await iframe.content_frame()
                        if frame:
                            # Store both page and frame - we need both
                            worker = SingleReportWorker(
                                page=page,
                                frame=frame,  # Pass the frame separately
                                config_report_name=name,
                                pages_order=pages_order,
                                page_visuals=page_visuals
                            )
                            await worker.run_for_current_report(url)
                            continue  # Skip the normal flow since we handled it in iframe
                        else:
                            logger.warning("Failed to switch to iframe context")
                            continue
                    
                except PWTimeout:
                    logger.warning("Report surface not detected. Please check if report loaded correctly.")
                    continue

                # Build plan for pages and visuals
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
    def __init__(self, page: Page, config_report_name: str, pages_order: List[str], 
                 page_visuals: Dict[str, Set[str]], frame: Frame = None):
        self.page = page
        self.frame = frame  # Store the frame if we're working with an iframe
        self.context = frame if frame else page  # Use frame for element operations if available
        self.config_report_name = config_report_name
        self.pages_order = pages_order or []
        self.page_visuals = page_visuals or {}
        self.report_name: Optional[str] = None
        self.download_dir: Optional[str] = None

    async def run_for_current_report(self, url: str):
        await self._setup_report_folder()  # uses config name
        # Try to get pages nav
        try:
            await self.context.wait_for_selector('[data-testid="pages-navigation-list"]', timeout=30000)
            pages_pane = await self.context.query_selector('[data-testid="pages-navigation-list"]')
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
                    await self.context.wait_for_timeout(1500)
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
            # Always use self.page for screenshots, even when working with frames
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
                await self.context.wait_for_timeout(200)

                # Try different button selectors
                button_selectors = [
                    "button[aria-label*='More options']",
                    "button[data-testid='visual-more-options-btn']",
                    ".vcMenuBtn",
                    "[aria-label*='More options']",
                    "[title*='More options']",
                    "[class*='menu-btn']",
                    "[class*='more-options']"
                ]

                more_btn = None
                for selector in button_selectors:
                    more_btn = await container.query_selector(selector)
                    if more_btn and await more_btn.is_visible():
                        break

                if not more_btn:
                    await self.context.wait_for_timeout(300)
                    # Try moving viewport slightly
                    try:
                        await self.page.mouse.wheel(0, 100)
                    except:
                        # If mouse wheel fails, try scrolling the container
                        await container.evaluate("el => el.scrollIntoView({behavior: 'smooth', block: 'center'})")
                    await self.context.wait_for_timeout(200)
                    continue

                # Try different click methods
                try:
                    await more_btn.click()
                except:
                    try:
                        await self.context.evaluate("(b) => b.click()", more_btn)
                    except:
                        try:
                            # Force click using JavaScript
                            await self.context.evaluate("""(element) => {
                                const clickEvent = new MouseEvent('click', {
                                    bubbles: true,
                                    cancelable: true,
                                    view: window
                                });
                                element.dispatchEvent(clickEvent);
                            }""", more_btn)
                        except:
                            continue

                # Check for menu using multiple selectors
                menu_selectors = [
                    "[role='menu']", 
                    ".pbi-menu",
                    "[class*='menu-container']",
                    "[class*='context-menu']"
                ]

                menu = None
                for selector in menu_selectors:
                    try:
                        await self.context.wait_for_selector(selector, timeout=3000)
                        menus = await self.context.query_selector_all(selector)
                        for m in menus[::-1]:
                            if await m.is_visible():
                                menu = m
                                break
                        if menu:
                            break
                    except:
                        continue

                if menu:
                    return menu

            except Exception as e:
                logger.debug(f"Menu interaction attempt failed: {e}")
                pass

            try:
                await self.page.mouse.wheel(0, 200)
            except:
                pass
            await self.context.wait_for_timeout(200)

        return None

    async def _export_visuals_on_current_page(self, safe_page_name: str, allowed_visuals: Set[str]):
        allowed_visuals = allowed_visuals or set()
        if not allowed_visuals:
            logger.info("No allowed visuals configured for this page; skipping.")
            return

        # Try different selectors for visual containers
        selectors = [
            ".visualContainer[role='group']",  # Standard Power BI
            ".visual-container",               # EmbedFast common
            "[class*='visual'][role='group']", # Generic visual container
            "[class*='visual-container']",     # Another common pattern
        ]

        containers = []
        for selector in selectors:
            try:
                await self.context.wait_for_selector(selector, timeout=5000)
                all_containers = await self.context.query_selector_all(selector)
                
                # Check visibility
                for c in all_containers:
                    try:
                        if await c.is_visible():
                            containers.append(c)
                    except:
                        continue
                
                if containers:
                    logger.info(f"Found visuals using selector: {selector}")
                    break
            except Exception as e:
                logger.debug(f"Selector '{selector}' failed: {e}")
                continue

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
                    await self.context.wait_for_selector(".pbi-modern-button", timeout=6000)
                    buttons = await self.context.query_selector_all(".pbi-modern-button")
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


# give exact loaction in code where I need to update it
# Keep the other code parts as it is