#!/usr/bin/env python3
"""
AIS -> TRMNL bridge, run-once version for GitHub Actions.

Each run: fetches what TRMNL currently shows (so nothing has to be stored in
the repo), listens to aisstream.io for a few minutes for the vessels you
follow, merges in anything new, and pushes the result back to TRMNL.

Env vars (set as GitHub repository secrets):
  AISSTREAM_API_KEY   your aisstream.io key
  TRMNL_WEBHOOK_URL   https://trmnl.com/api/custom_plugins/<plugin-uuid>
  VESSELS             comma list of MMSIs, optional label after a colon:
                      "310627000:Queen Mary 2,244660000"
Optional:
  LISTEN_SECONDS      how long to listen each run (default 240)
  MAX_PAYLOAD_BYTES   2048 on free TRMNL, 5120 with TRMNL+ (default 2048)
  STALE_AFTER_HOURS   mark a vessel as out of range after this (default 6)
"""
import asyncio
import json
import os
import time
import urllib.error
import urllib.request

import websockets

AIS_URL = "wss://stream.aisstream.io/v0/stream"
AIS_KEY = os.environ["AISSTREAM_API_KEY"]
WEBHOOK = os.environ["TRMNL_WEBHOOK_URL"].strip()
LISTEN = int(os.environ.get("LISTEN_SECONDS", "240"))
MAX_BYTES = int(os.environ.get("MAX_PAYLOAD_BYTES", "2048"))
STALE_AFTER = float(os.environ.get("STALE_AFTER_HOURS", "6")) * 3600
# TRMNL sits behind Cloudflare, which blocks Python's default User-Agent
# (error code 1010), so identify ourselves like a normal client.
HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; trmnl-vessels/1.0)",
    "Accept": "application/json",
}

LABELS = {}
for part in os.environ["VESSELS"].split(","):
    mmsi, _, label = part.strip().partition(":")
    if mmsi.strip():
        LABELS[mmsi.strip()] = label.strip()
MMSIS = list(LABELS)

POSITION_TYPES = (
    "PositionReport",                 # class A (ships)
    "StandardClassBPositionReport",   # class B (yachts, small craft)
    "ExtendedClassBPositionReport",
)
STATIC_TYPES = ("ShipStaticData", "StaticDataReport")

NAV_STATUS = {
    0: "Underway", 1: "At anchor", 2: "Not under command",
    3: "Restricted", 4: "Draught-limited", 5: "Moored", 6: "Aground",
    7: "Fishing", 8: "Sailing",
}


def log(*args):
    print(*args, flush=True)


def clean(text):
    return " ".join(str(text or "").replace("@", " ").split())


def fmt_pos(lat, lon):
    def dm(x, pos, neg):
        hemi = pos if x >= 0 else neg
        x = abs(x)
        d = int(x)
        return f"{d}°{(x - d) * 60:04.1f}'{hemi}"
    return f"{dm(lat, 'N', 'S')} {dm(lon, 'E', 'W')}"


def fmt_age(seconds):
    m = max(0, int(seconds // 60))
    if m < 60:
        return f"{m}m"
    if m < 48 * 60:
        return f"{m // 60}h"
    return f"{m // 1440}d"


# ---------- AIS ----------

def handle(msg, fresh):
    """Fold one AIS message into `fresh` (mmsi -> latest values this run)."""
    mtype = msg.get("MessageType")
    meta = msg.get("MetaData", {})
    mmsi = str(meta.get("MMSI", ""))
    if mmsi not in LABELS:
        return
    body = msg.get("Message", {}).get(mtype, {})
    f = fresh.setdefault(mmsi, {})

    if clean(meta.get("ShipName")):
        f["name"] = clean(meta["ShipName"])

    if mtype in POSITION_TYPES:
        lat, lon = body.get("Latitude"), body.get("Longitude")
        if lat is None or lon is None or abs(lat) > 90 or abs(lon) > 180:
            return  # 91/181 means "not available"
        f["t"] = int(time.time())
        f["p"] = fmt_pos(lat, lon)
        sog, cog = body.get("Sog"), body.get("Cog")
        f["v"] = round(sog, 1) if sog is not None and sog < 102.3 else None
        f["c"] = round(cog) if cog is not None and cog < 360 else None
        if "NavigationalStatus" in body:
            f["s"] = NAV_STATUS.get(body["NavigationalStatus"], "")

    elif mtype == "ShipStaticData":
        if clean(body.get("Name")):
            f["name"] = clean(body["Name"])
        if clean(body.get("Destination")):
            f["d"] = clean(body["Destination"])[:20]
        eta = body.get("Eta") or {}
        if eta.get("Month") and eta.get("Day"):
            hh, mm = eta.get("Hour", 24), eta.get("Minute", 60)
            t = f" {hh:02d}:{mm:02d}" if hh < 24 and mm < 60 else ""
            f["e"] = f"{eta['Day']:02d}/{eta['Month']:02d}{t}"

    elif mtype == "StaticDataReport":
        name = (body.get("ReportA") or {}).get("Name")
        if clean(name):
            f["name"] = clean(name)


async def listen(fresh):
    sub = {
        "APIKey": AIS_KEY,
        "BoundingBoxes": [[[-90, -180], [90, 180]]],
        "FiltersShipMMSI": MMSIS,
        "FilterMessageTypes": list(POSITION_TYPES + STATIC_TYPES),
    }
    deadline = time.monotonic() + LISTEN
    count = 0
    while time.monotonic() < deadline:
        try:
            async with websockets.connect(AIS_URL, ping_interval=20,
                                          ping_timeout=20) as ws:
                await ws.send(json.dumps(sub))  # must arrive within 3s
                log(f"connected, listening for {LISTEN}s "
                    f"to {len(MMSIS)} vessels")
                while (left := deadline - time.monotonic()) > 0:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=left)
                    except asyncio.TimeoutError:
                        break
                    msg = json.loads(raw)
                    if "error" in msg:
                        log(f"aisstream error: {msg['error']}")
                        return count
                    handle(msg, fresh)
                    count += 1
        except Exception as e:
            log(f"stream dropped: {e}")
            await asyncio.sleep(5)
    return count


# ---------- TRMNL ----------

def fetch_previous():
    """What TRMNL currently has, so vessels not heard this run keep data."""
    try:
        req = urllib.request.Request(WEBHOOK, headers=HTTP_HEADERS)
        with urllib.request.urlopen(req, timeout=20) as r:
            data = json.loads(r.read())
        mv = data.get("merge_variables", data) or {}
        return {str(r["m"]): r for r in mv.get("vessels", [])
                if isinstance(r, dict) and "m" in r}
    except Exception as e:
        log(f"could not read previous data (fine on first run): {e}")
        return {}


def build_payload(prev, fresh):
    now = time.time()
    rows = []
    for mmsi in MMSIS:
        row = {k: v for k, v in prev.get(mmsi, {}).items()
               if k not in ("a", "o")}
        row["m"] = mmsi
        f = fresh.get(mmsi, {})
        for key in ("t", "p", "v", "c", "s", "d", "e"):
            if key in f:
                row[key] = f[key]
        row = {k: v for k, v in row.items() if v not in (None, "")}
        row["n"] = (LABELS[mmsi] or f.get("name") or row.get("n")
                    or mmsi)[:22]
        if "t" in row:
            age = now - row["t"]
            row["a"] = fmt_age(age)
            row["o"] = age > STALE_AFTER
        rows.append(row)

    def encode():
        return json.dumps({"merge_variables": {
            "vessels": rows,
            "updated": time.strftime("%H:%M UTC", time.gmtime(now)),
        }}, separators=(",", ":"), ensure_ascii=False).encode()

    data = encode()
    for key in ("e", "p", "d"):          # too big: drop least useful fields
        if len(data) <= MAX_BYTES:
            break
        for r in rows:
            r.pop(key, None)
        data = encode()
    while len(data) > MAX_BYTES and rows:  # still too big: drop vessels
        rows.pop()
        data = encode()
    return data


def push(data):
    req = urllib.request.Request(
        WEBHOOK, data=data, method="POST",
        headers={**HTTP_HEADERS, "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            log(f"pushed {len(data)} bytes -> HTTP {r.status}")
    except urllib.error.HTTPError as e:
        log(f"push failed HTTP {e.code}: {e.read()[:300]!r}")
        raise SystemExit(1)


def main():
    prev = fetch_previous()
    fresh = {}
    n = asyncio.run(listen(fresh))
    heard = [m for m in MMSIS if "t" in fresh.get(m, {})]
    log(f"{n} messages; new positions for {len(heard)}/{len(MMSIS)} vessels")
    push(build_payload(prev, fresh))


if __name__ == "__main__":
    main()
