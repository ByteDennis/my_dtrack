"""Headless-Chrome smoke check for the dtrack web UI.

Boots a real `dtrack serve` and loads every page in headless Chrome (Playwright
driving the system google-chrome), capturing uncaught JS exceptions and console
errors, then runs runtime assertions on the consolidated common.js helpers.

This is the parity check TestClient can't give: TestClient renders the template
but never EXECUTES the page JS. Run on demand:

    .venv/bin/python tests/web/browser_smoke.py

Not auto-collected by pytest (no test_ prefix) — it boots a server subprocess.
"""

import json
import os
import subprocess
import sys
import tempfile
import time
import urllib.request

PORT = 8099
BASE = f"http://127.0.0.1:{PORT}"
PAGES = ["/", "/pairs", "/load_row", "/row_compare", "/col_mapping", "/col_gen",
         "/load_col", "/col_compare", "/csv_compare", "/playground", "/s3", "/sync"]

# helpers that must exist + behave at runtime after the dedup refactor
RUNTIME_ASSERTS = """() => {
    const r = {};
    r.escHtml = typeof escHtml === 'function' && escHtml('<a&b>') === '&lt;a&amp;b&gt;';
    r.escHtmlAttr = typeof escHtmlAttr === 'function' && escHtmlAttr('"<') === '&quot;&lt;';
    r.downloadBlob = typeof downloadBlob === 'function';
    r.showSuccess = typeof showSuccess === 'function';
    r.showError = typeof showError === 'function';
    r.fmtEta = typeof _fmtEta === 'function' && _fmtEta(3725) === '1h 2m';
    r.logMessage = typeof logMessage === 'function';
    r.setQuickDate = typeof setQuickDate === 'function';
    r.clearGlobalDates = typeof clearGlobalDates === 'function';
    r.syncSelectAll = typeof syncSelectAllCheckbox === 'function';
    return r;
}"""


def _wait_ready(timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(BASE + "/", timeout=2) as resp:
                if resp.status == 200:
                    return True
        except Exception:
            time.sleep(0.4)
    return False


def main():
    from dtrack.db import init_database

    tmp = tempfile.mkdtemp()
    db = os.path.join(tmp, "smoke.db")
    cfg = os.path.join(tmp, "pairs.json")
    open(cfg, "w").write(json.dumps({"metadata": {}, "settings": {}, "pairs": {}}))
    init_database(db)

    dtrack_bin = os.path.join(os.path.dirname(sys.executable), "dtrack")
    server = subprocess.Popen(
        [dtrack_bin, "serve", db, "--config", cfg,
         "--host", "127.0.0.1", "--port", str(PORT)],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        if not _wait_ready():
            print("SERVER DID NOT START"); server.terminate(); return 1

        from playwright.sync_api import sync_playwright
        failures = []
        with sync_playwright() as p:
            browser = p.chromium.launch(channel="chrome", headless=True,
                                        args=["--no-sandbox", "--disable-dev-shm-usage"])

            # runtime-verify the extracted parseSSE: a real SSE byte stream split
            # across reads (exercises the cross-read buffer), asserting event order.
            vpage = browser.new_page()
            vpage.goto(BASE + "/pairs", wait_until="networkidle", timeout=15000)
            sse = vpage.evaluate("""async () => {
                const chunks = [
                    'event: log\\ndata: {"line":"a","level":"info"}\\n\\nevent: prog',
                    'ress\\ndata: {"done":1,"total":2}\\n\\n',
                    'event: done\\ndata: {"ok":true}\\n\\n'];
                const enc = new TextEncoder();
                const stream = new ReadableStream({start(c){
                    for (const ch of chunks) c.enqueue(enc.encode(ch));
                    c.close();
                }});
                const events = [];
                await parseSSE({body: stream}, (et, msg) => events.push([et, msg]));
                return events;
            }""")
            expected = [["log", {"line": "a", "level": "info"}],
                        ["progress", {"done": 1, "total": 2}],
                        ["done", {"ok": True}]]
            if sse == expected:
                print("OK   parseSSE runtime (cross-read buffer + event order)")
            else:
                failures.append(("parseSSE", [f"got {sse}"], [], []))
                print(f"FAIL parseSSE runtime: {sse}")
            vpage.close()
            for path in PAGES:
                page = browser.new_page()
                errors, console_errs = [], []
                page.on("pageerror", lambda e: errors.append(str(e)))
                page.on("console", lambda m: console_errs.append(m.text)
                        if m.type == "error" else None)
                page.goto(BASE + path, wait_until="networkidle", timeout=15000)
                page.wait_for_timeout(300)
                checks = page.evaluate(RUNTIME_ASSERTS)
                bad_checks = [k for k, v in checks.items() if not v]
                # uncaught JS exceptions = hard fail (a broken reference from refactor)
                js_excepts = [e for e in errors]
                if js_excepts or bad_checks:
                    failures.append((path, js_excepts, bad_checks, console_errs))
                    print(f"FAIL {path}: excepts={js_excepts} bad_helpers={bad_checks}")
                else:
                    note = f" (console-errors: {len(console_errs)})" if console_errs else ""
                    print(f"OK   {path}{note}")
                page.close()
            browser.close()
        print("\n" + ("ALL PAGES CLEAN (no uncaught JS, helpers OK)"
                      if not failures else f"{len(failures)} PAGE(S) FAILED"))
        return 1 if failures else 0
    finally:
        server.terminate()
        try:
            server.wait(timeout=5)
        except Exception:
            server.kill()


if __name__ == "__main__":
    sys.exit(main())
