#!/usr/bin/env python3
"""Apply monkey-patches to twikit for known X API compatibility issues.

Run once after `pip install -r requirements.txt`:
    python scripts/patch_twikit.py

Patches applied:
  1. x_client_transaction/transaction.py — KEY_BYTE indices fallback
     X changed their anti-bot JavaScript format; the regex no longer matches.
     Fallback to last known-good index values [2, [12, 14, 7]] so that a
     plausible transaction ID is still generated.

  2. user.py — optional fields use .get() instead of direct key access
     X's GraphQL API omits 'urls', 'pinned_tweet_ids_str', and
     'withheld_in_countries' for some accounts. twikit 2.3.3 assumes these
     keys always exist, causing KeyError.
"""
import sys
import site
from pathlib import Path

# Locate twikit package
site_packages = [Path(p) for p in site.getsitepackages()]
twikit_root: Path | None = None
for sp in site_packages:
    candidate = sp / "twikit"
    if candidate.is_dir():
        twikit_root = candidate
        break

if twikit_root is None:
    print("ERROR: twikit not found in site-packages. Install it first.")
    sys.exit(1)

print(f"Found twikit at: {twikit_root}")

# ── Patch 1: KEY_BYTE indices fallback ──────────────────────────────────────
transaction_path = twikit_root / "x_client_transaction" / "transaction.py"
original = '        if not key_byte_indices:\n            raise Exception("Couldn\'t get KEY_BYTE indices")'
patched  = '        if not key_byte_indices:\n            # X changed their JS format; fall back to last known-good indices\n            return 2, [12, 14, 7]'

text = transaction_path.read_text()
if original in text:
    transaction_path.write_text(text.replace(original, patched))
    print("✓ Patch 1 applied: KEY_BYTE indices fallback")
elif patched in text:
    print("• Patch 1 already applied (skipping)")
else:
    print("WARNING: Patch 1 target not found — twikit may have been updated. Review manually.")

# ── Patch 2: User optional fields ───────────────────────────────────────────
user_path = twikit_root / "user.py"
replacements = [
    (
        "self.description_urls: list = legacy['entities']['description']['urls']",
        "self.description_urls: list = legacy['entities']['description'].get('urls', [])",
    ),
    (
        "self.pinned_tweet_ids: list[str] = legacy['pinned_tweet_ids_str']",
        "self.pinned_tweet_ids: list[str] = legacy.get('pinned_tweet_ids_str', [])",
    ),
    (
        "self.withheld_in_countries: list[str] = legacy['withheld_in_countries']",
        "self.withheld_in_countries: list[str] = legacy.get('withheld_in_countries', [])",
    ),
]

text = user_path.read_text()
changes = 0
for orig, repl in replacements:
    if orig in text:
        text = text.replace(orig, repl)
        changes += 1
user_path.write_text(text)

if changes:
    print(f"✓ Patch 2 applied: {changes}/3 User field(s) made optional")
else:
    print("• Patch 2 already applied (skipping)")

print("Done — twikit patches applied.")
