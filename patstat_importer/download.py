"""
Download PATSTAT zip files from the EPO BDDS portal.

Setup:
    pip install "patstat_importer[download]"
    playwright install chromium

Usage (CMD):
    set EPO_USERNAME=your@email.com
    set EPO_PASSWORD=yourpassword
    patstat-download --edition "PATSTAT Global 2025 Autumn" ^
                     --output-dir "D:/Download/Patstat/"

    # Show the browser window (useful for first run / debugging):
    patstat-download --edition "PATSTAT Global 2025 Autumn" ^
                     --output-dir "D:/Download/Patstat/" --visible
"""

import argparse
import os
import sys
import urllib.request
import urllib.error
from pathlib import Path

PORTAL_URL = "https://publication-bdds.apps.epo.org/raw-data/products"


def parse_args():
    p = argparse.ArgumentParser(
        description="Download PATSTAT zip files from the EPO BDDS portal",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument(
        "--edition",
        required=True,
        help='Edition name as shown on the portal, e.g. "PATSTAT Global 2025 Autumn"',
    )
    p.add_argument(
        "--output-dir",
        required=True,
        help="Directory where downloaded zip files will be saved",
    )
    p.add_argument(
        "--visible",
        action="store_true",
        default=False,
        help="Show the browser window instead of running headless",
    )
    return p.parse_args()


def get_credentials():
    username = os.environ.get("EPO_USERNAME", "").strip()
    password = os.environ.get("EPO_PASSWORD", "").strip()
    if not username:
        sys.exit("Error: EPO_USERNAME environment variable is not set.")
    if not password:
        sys.exit("Error: EPO_PASSWORD environment variable is not set.")
    return username, password


def _fmt_bytes(n):
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def _dump_page(page, limit=2000):
    print("  URL :", page.url)
    try:
        text = page.inner_text("body")
    except Exception:
        text = page.content()
    if limit is None or len(text) <= limit:
        print(text)
    else:
        print(text[:limit])
        print(f"  ... (truncated; full length {len(text)} chars)")


def _scroll_to_load_all(page, max_steps=10, pause_ms=400):
    """Scroll down until either the page stops growing or max_steps is reached.

    Many BDDS product cards render lazily as you scroll, so the subscription
    products (incl. PATSTAT Global) only appear once you've scrolled past the
    Public Area.
    """
    prev_height = -1
    for _ in range(max_steps):
        height = page.evaluate("document.body.scrollHeight")
        if height == prev_height:
            break
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(pause_ms)
        prev_height = height
    page.evaluate("window.scrollTo(0, 0)")


def _dismiss_cookie_banner(page):
    """Best-effort: click any cookie consent button so it doesn't block other clicks."""
    selectors = (
        'button:has-text("Ok")',
        'button:has-text("OK")',
        'button:has-text("Accept")',
        'button:has-text("essential cookies only")',
        'button:has-text("No, thanks")',
        '[aria-label="Accept all"]',
        '[id*="cookie"] button',
    )
    for sel in selectors:
        try:
            loc = page.locator(sel).first
            if loc.count() > 0 and loc.is_visible():
                loc.click(timeout=2_000)
                print(f"  Dismissed cookie banner via '{sel}'.")
                page.wait_for_timeout(300)
                return
        except Exception:
            continue


_USERNAME_SEL = (
    'input[name="username"], input[id="username"], '
    'input[name="email"], input[id="email"], '
    'input[name="identifier"], '
    'input[autocomplete="username"], input[autocomplete="email"], '
    'input[type="email"]'
)


def _find_login_page(context):
    """Return the first page in the browser context whose DOM shows a username field."""
    for p in context.pages:
        try:
            if p.locator(_USERNAME_SEL).count() > 0:
                return p
        except Exception:
            continue
    return None


def _click_signin_if_needed(page):
    """Walk the BDDS multi-step sign-in flow until the Keycloak login form appears.

    The portal often shows multiple 'Sign in' elements (a section header AND
    one or more action buttons). The actionable one is usually the last visible
    candidate, and clicking it may open a popup window. This helper loops:
    each iteration it picks the last visible 'Sign in'/'Log in' clickable, clicks
    it, then checks every page in the browser context for the username field.

    Returns the page that holds the login form (may differ from the input page
    if a popup was opened). Returns the input page unchanged if it gives up.
    """
    found = _find_login_page(page.context)
    if found is not None:
        return found

    print("Looking for 'Sign in' button (multi-step flow)...")

    prev_url, prev_html_len = None, 0
    for step in range(1, 7):
        cands = page.locator(
            'a[href*="oidc/login"], a[href*="/login"], a[href*="auth/login"], '
            'a:has-text("Sign in"), button:has-text("Sign in"), '
            'a:has-text("Log in"), button:has-text("Log in"), '
            'input[type="submit"][value*="Sign in" i], '
            'input[type="submit"][value*="Log in" i]'
        )
        visible = []
        for i in range(cands.count()):
            try:
                el = cands.nth(i)
                if el.is_visible():
                    visible.append(el)
            except Exception:
                continue

        if not visible:
            print(f"  Step {step}: no visible 'Sign in' clickables.")
            return page

        cur_url, cur_html_len = page.url, len(page.content())
        if step > 1 and cur_url == prev_url and cur_html_len == prev_html_len:
            print(f"  Step {step}: page did not change after previous click; aborting.")
            return page
        prev_url, prev_html_len = cur_url, cur_html_len

        target = visible[-1]
        try:
            tag = target.evaluate('el => el.tagName')
            txt = (target.text_content() or '').strip().replace('\n', ' ')[:60]
        except Exception:
            tag, txt = '?', '?'

        pages_before = set(page.context.pages)
        print(f"  Step {step}: clicking <{tag}> '{txt}'...")
        try:
            target.click(timeout=5_000)
        except Exception as exc:
            print(f"    click error: {exc}")
            return page

        try:
            page.wait_for_load_state("networkidle", timeout=15_000)
        except Exception:
            pass

        new_pages = [p for p in page.context.pages if p not in pages_before]
        if new_pages:
            popup = new_pages[-1]
            print(f"    → Popup opened: {popup.url}")
            try:
                popup.wait_for_load_state("networkidle", timeout=15_000)
            except Exception:
                pass
            page = popup

        found = _find_login_page(page.context)
        if found is not None:
            print(f"  → Login form reached after step {step}: {found.url}")
            return found

    print("  Exhausted sign-in steps without finding login form.")
    return page


_POPUP_CONFIRM_SELECTORS = (
    '[data-testid="download-item-button"]',
    '[data-testid="download-confirm-button"]',
    '[data-testid*="download"][role="button"]',
    'button:has-text("Download item")',
    'button:has-text("Download")',
    'a:has-text("Download item")',
    'a:has-text("Download")',
    'a[href$=".zip"]',
)


def _capture_download_handle(context, page, button, timeout_s=30):
    """Click ``button`` and return the Playwright ``Download`` handle so the
    caller can save it via the authenticated browser session.

    The BDDS UI requires two clicks on a 'Download item' element: one on the
    row in the main page (opens a popup window), and a second one inside the
    popup (actually triggers the download). The download URL itself requires
    more than session cookies to authenticate — probably a Bearer token held
    in the React app's memory — so urllib calls return an HTML rejection
    page instead of the zip. We therefore let Playwright (which already has
    the right auth) perform the actual byte transfer via ``download.save_as``.

    Returns (Download | None, list-of-popups-to-close).
    """
    captured = {'download': None}

    def on_download(d):
        if captured['download'] is None:
            captured['download'] = d
            # NB: no d.cancel() — we want the browser to stream the file
            # so save_as() can move the temp file to its final destination.

    page.on("download", on_download)
    pages_before = set(context.pages)

    # Use context.expect_page to catch the popup that the row click opens.
    # If the click doesn't open a popup (e.g., it shows an in-page modal
    # instead), expect_page raises; we catch and fall through.
    popup = None
    try:
        with context.expect_page(timeout=10_000) as popup_info:
            button.click()
        popup = popup_info.value
    except Exception:
        # Last-resort: check whether a new page appeared anyway.
        for p in context.pages:
            if p is not page and p not in pages_before:
                popup = p
                break

    if popup is not None:
        try:
            popup.on("download", on_download)
        except Exception:
            pass
        try:
            popup.wait_for_load_state("domcontentloaded", timeout=10_000)
        except Exception:
            pass
        try:
            print(f"  Popup opened at: {popup.url}")
        except Exception:
            print("  Popup opened (URL unavailable).")

    # If no direct download fired yet, click the confirmation element in
    # the popup (preferred) or in an in-page modal (fallback).
    if captured['download'] is None:
        if popup is not None:
            target = popup
            scope_prefixes = ['']  # the whole popup is the modal context
            target_label = 'popup'
        else:
            target = page
            # On the main page, restrict to dialog/modal containers so we
            # don't accidentally re-click the row's own Download item.
            scope_prefixes = [
                '[role="dialog"] ',
                '[role="alertdialog"] ',
                '[class*="modal" i] ',
                '[class*="dialog" i] ',
            ]
            target_label = 'in-page modal'

        clicked = False
        for scope in scope_prefixes:
            for sel in _POPUP_CONFIRM_SELECTORS:
                full_sel = scope + sel
                try:
                    el = target.locator(full_sel).first
                    if el.count() == 0:
                        continue
                    if not el.is_visible():
                        continue
                    el.click(timeout=5_000)
                    print(f"  Clicked '{full_sel}' in {target_label} to trigger download.")
                    clicked = True
                    break
                except Exception:
                    continue
            if clicked:
                break
        if not clicked and captured['download'] is None:
            print(
                f"  Warning: no confirmation element matched "
                f"(popup_present={popup is not None})."
            )

    # Final poll for a Download event.
    deadline_ticks = max(1, int(timeout_s * 1000 / 250))
    for _ in range(deadline_ticks):
        if captured['download'] is not None:
            break
        page.wait_for_timeout(250)

    # Listener cleanup (we still need the popup alive so save_as can finish
    # streaming; caller closes popup pages after save_as returns).
    try:
        page.remove_listener("download", on_download)
    except Exception:
        pass
    if popup is not None:
        try:
            popup.remove_listener("download", on_download)
        except Exception:
            pass

    # Collect popup pages so the caller can close them after the save.
    popups_to_close = [
        p for p in context.pages
        if p is not page and p not in pages_before
    ]

    return captured['download'], popups_to_close


def collect_and_download(edition, username, password, output_dir, headless=True):
    """Log in, navigate to the PATSTAT product details page, and download all
    files in the requested edition serially.

    The Download item buttons in the React UI have no static URL — the URL is
    constructed at click time. So for each row we click inside
    ``page.expect_download(...)`` to capture the runtime URL, cancel the
    Playwright transfer, and hand the URL to ``download_zip()`` so we keep
    Range/resume support and Content-Length verification."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        sys.exit(
            "playwright is not installed.\n"
            'Run:  pip install "patstat_importer[download]" && playwright install chromium'
        )

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=headless)
        context = browser.new_context()
        page = context.new_page()

        # ── Login ─────────────────────────────────────────────────────────────
        print(f"Opening {PORTAL_URL} ...")
        page.goto(PORTAL_URL, wait_until="networkidle", timeout=30_000)

        # Some BDDS portal versions land on a public page with a cookie banner
        # and a "Sign in" link that must be clicked before Keycloak shows up.
        # The flow may also open the login form in a popup window.
        _dismiss_cookie_banner(page)
        page = _click_signin_if_needed(page)

        print("Waiting for login form...")
        try:
            page.wait_for_selector(_USERNAME_SEL, timeout=20_000)
        except Exception:
            print("Could not find login form.")
            _dump_page(page)
            browser.close()
            sys.exit(1)

        print("  Entering credentials...")
        page.locator(_USERNAME_SEL).first.fill(username)

        # Some Keycloak setups show a "Next" step before revealing the password field
        pwd_sel = 'input[name="password"], input[type="password"]'
        if not page.locator(pwd_sel).count():
            page.locator(
                'button[type="submit"], input[type="submit"]'
            ).first.click()
            page.wait_for_selector(pwd_sel, timeout=10_000)

        page.locator(pwd_sel).first.fill(password)
        page.locator(
            'button[id="kc-login"], input[id="kc-login"],'
            ' button[type="submit"], input[type="submit"]'
        ).first.click()

        # Wait for the FULL OIDC redirect chain to complete:
        #   login.epo.org/... → bdds.../raw-data/oidc/callback?code=... → bdds.../raw-data/products
        # `networkidle` can fire mid-chain (at the callback hop), so we wait for
        # a BDDS URL that is NOT the oidc-callback step. If we don't, a
        # subsequent page.goto(PORTAL_URL) would interrupt the callback and
        # leave us anonymous.
        try:
            page.wait_for_url(
                lambda url: (
                    "publication-bdds.apps.epo.org/raw-data/" in url
                    and "/oidc/" not in url
                ),
                timeout=60_000,
            )
        except Exception:
            pass  # auth-success check below will surface the real failure

        # If the login happened in a popup that closed, fall back to the
        # original main page (still alive in the same browser context).
        if page.is_closed():
            page = next((p for p in context.pages if not p.is_closed()), None)
            if page is None:
                print("All pages closed after login; cannot continue.")
                browser.close()
                sys.exit(1)
            try:
                page.wait_for_url(
                    lambda url: (
                        "publication-bdds.apps.epo.org/raw-data/" in url
                        and "/oidc/" not in url
                    ),
                    timeout=30_000,
                )
            except Exception:
                pass
        print(f"  Authenticated. URL: {page.url}")

        # Only navigate to the products page if we're already on the BDDS
        # domain (session established) AND not already there. This guard
        # avoids the previous bug where a goto fired during the OIDC callback
        # interrupted the session-establishment redirect.
        if ("publication-bdds.apps.epo.org" in page.url
                and "/raw-data/products" not in page.url):
            page.goto(PORTAL_URL, wait_until="domcontentloaded", timeout=30_000)

        # Explicit auth-success check: the products page shows "Sign out" (top
        # nav) and a "Subscriptions" heading only when authenticated. If we
        # see neither within 15s, dump the page and exit with a clear message
        # instead of silently proceeding into a confused products-page search.
        signed_in = False
        for sel in ('text=Sign out', 'text=Subscriptions'):
            try:
                page.wait_for_selector(sel, timeout=15_000)
                signed_in = True
                break
            except Exception:
                continue

        if not signed_in:
            print("Authentication did not complete (page is still anonymous).")
            print("  The Okta → BDDS redirect did not finish, or the submit")
            print("  click on the password page was not registered.")
            _dump_page(page, limit=None)
            browser.close()
            sys.exit(1)

        # ── Find PATSTAT Global subscription card ─────────────────────────────
        # The BDDS portal was migrated to a React UI: "See details" is now a
        # <button> that expands a collapsible panel inline (no navigation).
        # The card uses stable data-testid attributes; the obfuscated CSS
        # classes are not usable as selectors.
        print("Looking for 'PATSTAT Global' subscription card...")
        _scroll_to_load_all(page)

        product_heading = page.locator(
            '[data-testid="productName"]'
        ).filter(has_text="PATSTAT Global").first

        try:
            product_heading.wait_for(state="visible", timeout=20_000)
            heading_text = (product_heading.text_content() or "").strip()
            print(f"  Found product: {heading_text}")
        except Exception:
            print("Could not find 'PATSTAT Global' product card.")
            _dump_page(page, limit=None)
            browser.close()
            sys.exit(1)

        # Click 'See details' — this NAVIGATES the page to the product
        # details URL (/raw-data/products/subscription/product/<id>), it does
        # not expand an inline panel. The previous in-page-expand assumption
        # was wrong; the URL change after click is the definitive signal.
        panel_header = product_heading.locator(
            'xpath=ancestor::*[@role="button" and @aria-expanded][1]'
        )
        details_btn = panel_header.locator('[data-testid="detailsButton"]').first

        print("Clicking 'See details' to open the product details page...")
        details_btn.click()

        # Wait for the navigation to the product details page to settle.
        try:
            page.wait_for_url("**/subscription/product/**", timeout=20_000)
        except Exception:
            pass
        try:
            page.wait_for_load_state("domcontentloaded", timeout=15_000)
        except Exception:
            pass
        print(f"  Now on: {page.url}")

        # ── Locate the matching edition table on the details page ─────────────
        # Each edition is a [data-testid="epo-data-table"] with a
        # <h3 data-testid="table-title"> like "14.24 PATSTAT Global 2025 Autumn"
        # (the "...NOTIFICATION" table holds just the README and must be
        # excluded). Rows are <tr role="row"> under tbody.p-datatable-tbody;
        # first <td> holds the filename, last <td> holds the Download item
        # <button> whose URL is constructed at click time by the React app.
        print(f"Looking for edition table '{edition}'...")
        try:
            page.wait_for_selector(
                '[data-testid="epo-data-table"]', state='visible', timeout=20_000
            )
        except Exception:
            print("No data table appeared on the product details page.")
            _dump_page(page, limit=None)
            browser.close()
            sys.exit(1)

        # Capture cookies and User-Agent before any per-row clicks. These are
        # stable for the rest of the session and feed urllib for the actual
        # byte transfers.
        cookie_dict = {c["name"]: c["value"] for c in context.cookies()}
        user_agent = page.evaluate("navigator.userAgent")
        headers = {"User-Agent": user_agent, "Accept": "*/*"}

        edition_table = page.locator(
            '[data-testid="epo-data-table"]'
        ).filter(
            has=page.locator(
                '[data-testid="table-title"]'
            ).filter(has_text=edition)
        ).filter(
            has_not_text='NOTIFICATION'
        ).first

        try:
            edition_table.wait_for(state='visible', timeout=15_000)
            title_loc = edition_table.locator('[data-testid="table-title"]').first
            edition_title = (title_loc.text_content() or '').strip()
        except Exception:
            print(f"Could not find edition table whose title ends with '{edition}'.")
            _dump_page(page, limit=None)
            browser.close()
            sys.exit(1)

        rows = edition_table.locator('tbody.p-datatable-tbody tr[role="row"]')
        n_rows = rows.count()
        print(f"Found edition table: '{edition_title}' ({n_rows} rows)")

        if n_rows == 0:
            print(f"Edition table for '{edition}' has no rows.")
            browser.close()
            sys.exit(1)

        # ── Per-row download loop ─────────────────────────────────────────────
        # For each row: capture the runtime URL via a click inside
        # page.expect_download(), cancel the Playwright transfer, then run the
        # actual urllib download (which gives us Range/resume and Content-Length
        # verification — neither of which Playwright downloads support).
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        for i in range(n_rows):
            row = rows.nth(i)
            try:
                filename = (
                    row.locator('td').first.inner_text(timeout=5_000).strip()
                )
            except Exception:
                print(f"[{i+1}/{n_rows}] Could not read filename; skipping.")
                continue

            dest = output_dir / filename
            # Prefer the stable data-testid; fall back to visible text in case
            # the testid disappears in a future release.
            button = row.locator(
                '[data-testid="download-item-button"], button:has-text("Download item")'
            ).first

            # Skip if a valid-looking zip is already on disk. We require the
            # zip "PK" signature and a minimum size — this lets the script
            # discard the 57 KB HTML "error" files that the old urllib path
            # may have left behind.
            if dest.exists():
                ok = False
                try:
                    if dest.stat().st_size > 1_000_000:
                        with open(dest, "rb") as fh:
                            if fh.read(2) == b"PK":
                                ok = True
                except Exception:
                    pass
                if ok:
                    print(f"[{i+1}/{n_rows}] Already complete: {filename}\n")
                    continue
                try:
                    dest.unlink()
                    print(f"[{i+1}/{n_rows}] Removed invalid prior file: {filename}")
                except Exception:
                    pass

            print(f"[{i+1}/{n_rows}] Capturing URL for: {filename}")
            dl, popups_to_close = _capture_download_handle(
                context, page, button, timeout_s=30
            )
            if dl is None:
                print(f"  ERROR: no download event after click for {filename}")
                print(f"[{i+1}/{n_rows}] FAILED — moving to next file.\n")
                # Still close any popups we left open.
                for p in popups_to_close:
                    try:
                        if not p.is_closed():
                            p.close()
                    except Exception:
                        pass
                continue
            try:
                print(f"  URL: {dl.url}")
            except Exception:
                pass

            print(f"[{i+1}/{n_rows}] Downloading: {filename}")
            try:
                # Playwright's save_as blocks until the download is fully
                # streamed to dest. It uses the browser's authenticated
                # session, which is what urllib couldn't replicate.
                dl.save_as(str(dest))
                final_size = dest.stat().st_size if dest.exists() else 0
                print(f"  Saved: {dest} ({_fmt_bytes(final_size)})")
                if final_size < 1_000_000:
                    try:
                        with open(dest, "rb") as fh:
                            sniff = fh.read(200)
                        print(
                            "  WARNING: file is suspiciously small. "
                            "First bytes (latin-1):"
                        )
                        print("  " + sniff.decode("latin-1", errors="replace")
                              .replace("\n", "\n  "))
                    except Exception:
                        pass
                print(f"[{i+1}/{n_rows}] Complete: {filename}\n")
            except Exception as exc:
                print(f"  ERROR saving {filename}: {exc}")
                print(f"[{i+1}/{n_rows}] FAILED — moving to next file.\n")
            finally:
                # Close popup tabs that opened during this row — otherwise
                # they pile up across 67 iterations.
                for p in popups_to_close:
                    try:
                        if not p.is_closed():
                            p.close()
                    except Exception:
                        pass
                # Defensive: dismiss any leftover modal on the main page.
                # When a download succeeds the React app closes its own
                # modal; when it fails the modal stays open and would block
                # the next row's locator. Esc handles both cases safely (it
                # is a no-op when no modal is open).
                try:
                    page.keyboard.press("Escape")
                    page.wait_for_timeout(300)
                except Exception:
                    pass

        print(f"Done. Files saved to: {output_dir}")
        browser.close()


def download_zip(url, dest, cookies, headers):
    """Stream a zip file to dest, resuming if a partial file exists.

    Blocks until the file is fully written (or an exception is raised). Returns
    the final size on disk in bytes; raises IOError if the local file size
    does not match the server's Content-Length after the stream ends.
    """
    dest = Path(dest)
    existing = dest.stat().st_size if dest.exists() else 0
    expected_total = None

    req = urllib.request.Request(
        url, headers={**headers, "Range": f"bytes={existing}-"}
    )
    for name, value in cookies.items():
        req.add_header("Cookie", f"{name}={value}")

    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            if resp.status == 416:
                print("  Already complete.")
                return dest.stat().st_size
            content_length = int(resp.headers.get("Content-Length", 0))
            mode = "ab" if existing and resp.status == 206 else "wb"
            if mode == "wb":
                existing = 0
            downloaded = existing
            expected_total = content_length + existing if content_length else None
            with open(dest, mode) as fh:
                while True:
                    buf = resp.read(1024 * 1024)
                    if not buf:
                        break
                    fh.write(buf)
                    fh.flush()
                    downloaded += len(buf)
                    if expected_total:
                        pct = downloaded / expected_total * 100
                        print(
                            f"\r  {_fmt_bytes(downloaded)} / {_fmt_bytes(expected_total)}"
                            f"  ({pct:.1f}%)",
                            end="", flush=True,
                        )
                    else:
                        print(f"\r  {_fmt_bytes(downloaded)}", end="", flush=True)
            print()
    except urllib.error.HTTPError as exc:
        if exc.code == 416:
            print("  Already complete.")
            return dest.stat().st_size
        raise

    final_size = dest.stat().st_size
    if expected_total and final_size != expected_total:
        raise IOError(
            f"Truncated download: {dest.name} is {final_size} bytes "
            f"but server reported {expected_total} bytes. "
            "Re-run the command to resume."
        )
    return final_size


def main():
    args = parse_args()
    username, password = get_credentials()
    output_dir = Path(args.output_dir)

    collect_and_download(
        edition=args.edition,
        username=username,
        password=password,
        output_dir=output_dir,
        headless=not args.visible,
    )


if __name__ == "__main__":
    main()
