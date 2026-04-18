#!/usr/bin/env python3
"""Convert Cookie-Editor JSON export to twikit's flat dict format.

Usage:
    python scripts/import_cookies.py cookie_editor_export.json
    # Writes to data/twitter_cookies.json
"""
import json
import sys
from pathlib import Path

if len(sys.argv) < 2:
    print("Usage: python scripts/import_cookies.py <cookie_editor_export.json>")
    sys.exit(1)

src = Path(sys.argv[1])
cookies = json.loads(src.read_text())

# Cookie-Editor exports a list; twikit expects a flat dict {name: value}
if isinstance(cookies, list):
    flat = {c["name"]: c["value"] for c in cookies}
else:
    flat = cookies  # already flat

out = Path("data/twitter_cookies.json")
out.parent.mkdir(exist_ok=True)
out.write_text(json.dumps(flat, indent=2))
print(f"Wrote {len(flat)} cookies to {out}")

key_cookies = [k for k in ["ct0", "auth_token"] if k in flat]
print(f"Key cookies present: {key_cookies}")

if "ct0" not in flat or "auth_token" not in flat:
    print("WARNING: ct0 or auth_token missing — cookies may not work")
else:
    print("OK — ready to run the pipeline")
