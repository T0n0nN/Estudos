#!/usr/bin/env python3
"""MailHunter (OSM) - generate local business leads without paid APIs.

Data sources (free/open):
- Nominatim: resolves a city query into an OSM boundary relation id
- Overpass: fetches POIs (amenity/shop/office/tourism) inside that boundary

Optional:
- If a place has a website, fetches up to a few pages from that website and
  extracts emails with a regex (best effort). Respects robots.txt.

Outputs:
- Full CSV with all collected places
- Filtered CSV with only places without a website

Important limitations:
- OSM coverage varies. Some businesses won't exist in OSM.
- Many businesses don't publish email publicly.

Usage:
  python mailhunter_osm.py --city "Limeira, SP, Brazil"
  python mailhunter_osm.py --city "Limeira, SP, Brazil" --extract-email
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import os
import re
import sys
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

import requests

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
OVERPASS_URL = "https://overpass-api.de/api/interpreter"

EMAIL_RE = re.compile(r"(?i)(?:mailto:)?([a-z0-9_.+-]+@[a-z0-9-]+\.[a-z0-9-.]+)")


@dataclass(frozen=True)
class Lead:
    name: str
    osm_type: str
    osm_id: int
    lat: float | None
    lon: float | None
    category: str
    address: str
    phone: str
    website: str
    email_osm: str
    emails_found: str


def _user_agent() -> str:
    contact = os.getenv("MAILHUNTER_CONTACT", "")
    if contact:
        return f"MailHunterOSM/1.0 ({contact})"
    return "MailHunterOSM/1.0"


def _get_json(session: requests.Session, url: str, params: dict[str, Any] | None = None, data: str | None = None, *, timeout_s: int = 60) -> Any:
    if data is None:
        resp = session.get(url, params=params, timeout=timeout_s)
    else:
        resp = session.post(url, data=data.encode("utf-8"), timeout=timeout_s)

    resp.raise_for_status()
    return resp.json()


def nominatim_resolve_city(session: requests.Session, city_query: str) -> tuple[str, int]:
    # We prefer an administrative boundary relation.
    params = {
        "q": city_query,
        "format": "json",
        "addressdetails": 1,
        "limit": 5,
    }
    results = _get_json(session, NOMINATIM_URL, params=params, timeout_s=60)
    if not results:
        raise RuntimeError(f"Nominatim returned no results for: {city_query!r}")

    # Find first relation or way that looks like a city boundary.
    for r in results:
        osm_type = str(r.get("osm_type") or "")
        osm_id = int(r.get("osm_id") or 0)
        rclass = str(r.get("class") or "")
        rtype = str(r.get("type") or "")
        if osm_type in ("relation", "way") and (rclass in ("boundary", "place") or rtype in ("administrative", "city", "town")):
            return osm_type, osm_id

    # Fallback: use the first result that is relation/way.
    for r in results:
        osm_type = str(r.get("osm_type") or "")
        osm_id = int(r.get("osm_id") or 0)
        if osm_type in ("relation", "way") and osm_id:
            return osm_type, osm_id

    raise RuntimeError(f"Could not resolve a boundary relation/way for: {city_query!r}")


def overpass_area_id(osm_type: str, osm_id: int) -> int:
    # Overpass: area id is derived from the OSM id.
    # relation: 3600000000 + id
    # way:      2400000000 + id
    # node:     1600000000 + id (not useful for city boundaries)
    if osm_type == "relation":
        return 3_600_000_000 + osm_id
    if osm_type == "way":
        return 2_400_000_000 + osm_id
    if osm_type == "node":
        return 1_600_000_000 + osm_id
    raise ValueError(f"Unsupported osm_type: {osm_type}")


def build_overpass_query(area_id: int, *, max_elements: int) -> str:
    # Collect a broad set of POIs.
    # Note: Overpass returns at most what it can within timeout/memory.
    return f"""
[out:json][timeout:180];
area({area_id})->.a;
(
  nwr[\"amenity\"](area.a);
  nwr[\"shop\"](area.a);
  nwr[\"office\"](area.a);
  nwr[\"tourism\"](area.a);
  nwr[\"leisure\"](area.a);
);
out tags center {max_elements};
""".strip()


def normalize_website(url: str) -> str:
    url = (url or "").strip()
    if not url:
        return ""
    if url.startswith("www."):
        url = "https://" + url
    if not url.startswith("http://") and not url.startswith("https://"):
        url = "https://" + url
    return url


def format_address(tags: dict[str, Any]) -> str:
    parts: list[str] = []
    for k in ("addr:street", "addr:housenumber", "addr:suburb", "addr:city", "addr:state", "addr:postcode"):
        v = tags.get(k)
        if v:
            parts.append(str(v).strip())
    return ", ".join([p for p in parts if p])


def extract_emails_from_html(html: str) -> list[str]:
    emails = {m.group(1).strip() for m in EMAIL_RE.finditer(html or "")}
    cleaned = sorted({e for e in emails if len(e) <= 254})
    return cleaned


def same_domain(a: str, b: str) -> bool:
    try:
        da = urlparse(a).netloc.lower()
        db = urlparse(b).netloc.lower()
    except Exception:
        return False
    if da.startswith("www."):
        da = da[4:]
    if db.startswith("www."):
        db = db[4:]
    return da == db and da != ""


def get_robot_parser(session: requests.Session, base_url: str, cache: dict[str, RobotFileParser]) -> RobotFileParser:
    parsed = urlparse(base_url)
    key = parsed.scheme + "://" + parsed.netloc
    if key in cache:
        return cache[key]

    rp = RobotFileParser()
    rp.set_url(urljoin(key, "/robots.txt"))
    try:
        resp = session.get(rp.url, timeout=20)
        if resp.status_code < 400:
            rp.parse(resp.text.splitlines())
        else:
            rp.parse([])
    except requests.RequestException:
        rp.parse([])

    cache[key] = rp
    return rp


def fetch_website_emails(
    session: requests.Session,
    website: str,
    *,
    max_pages: int,
    delay_s: float,
    robots_cache: dict[str, RobotFileParser],
) -> list[str]:
    website = normalize_website(website)
    if not website:
        return []

    rp = get_robot_parser(session, website, robots_cache)
    ua = _user_agent()

    to_visit: list[str] = [website]
    visited: set[str] = set()
    found: set[str] = set()

    while to_visit and len(visited) < max_pages:
        url = to_visit.pop(0)
        if url in visited:
            continue
        visited.add(url)

        if not rp.can_fetch(ua, url):
            continue

        try:
            resp = session.get(url, timeout=25, allow_redirects=True)
        except requests.RequestException:
            continue

        if resp.status_code >= 400:
            continue

        ctype = (resp.headers.get("content-type") or "").lower()
        if "html" not in ctype and "text" not in ctype:
            continue

        html = resp.text or ""
        for e in extract_emails_from_html(html):
            found.add(e)

        # Try to discover a couple of likely contact pages.
        # Keep it very small to be polite.
        if len(visited) < max_pages:
            # naive href discovery
            for m in re.finditer(r"(?i)href=\"([^\"]+)\"", html):
                href = m.group(1).strip()
                if not href or href.startswith("#") or href.startswith("mailto:"):
                    continue
                if any(x in href.lower() for x in ("contato", "contact", "sobre", "about", "fale-conosco")):
                    next_url = urljoin(resp.url, href)
                    if same_domain(resp.url, next_url) and next_url not in visited and next_url not in to_visit:
                        to_visit.append(next_url)
                        if len(to_visit) >= max_pages:
                            break

        time.sleep(max(delay_s, 0.0))

    return sorted(found)[:8]


def write_csv(path: str, leads: list[Lead]) -> None:
    fieldnames = [
        "name",
        "category",
        "address",
        "phone",
        "website",
        "email_osm",
        "emails_found",
        "osm_type",
        "osm_id",
        "lat",
        "lon",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for l in leads:
            w.writerow(
                {
                    "name": l.name,
                    "category": l.category,
                    "address": l.address,
                    "phone": l.phone,
                    "website": l.website,
                    "email_osm": l.email_osm,
                    "emails_found": l.emails_found,
                    "osm_type": l.osm_type,
                    "osm_id": l.osm_id,
                    "lat": "" if l.lat is None else l.lat,
                    "lon": "" if l.lon is None else l.lon,
                }
            )


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="MailHunter using OpenStreetMap (no paid API)")
    p.add_argument("--city", default="Limeira, SP, Brazil", help="City query for Nominatim")
    p.add_argument("--max-elements", type=int, default=30000, help="Overpass output cap (default: 30000)")
    p.add_argument("--extract-email", action="store_true", help="Fetch business websites and extract emails")
    p.add_argument("--max-sites", type=int, default=250, help="Max number of websites to crawl for emails")
    p.add_argument("--max-pages", type=int, default=3, help="Max pages per website (default: 3)")
    p.add_argument("--delay", type=float, default=0.3, help="Delay between website requests (seconds)")
    p.add_argument("--out", default="", help="Output CSV path (default uses timestamp)")
    return p.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)

    ts = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    out_path = args.out or f"leads_osm_{ts}.csv"
    out_no_site = out_path.replace(".csv", "_no_site.csv")

    session = requests.Session()
    session.headers.update({"User-Agent": _user_agent(), "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.4"})

    print(f"Resolving city boundary via Nominatim: {args.city}")
    osm_type, osm_id = nominatim_resolve_city(session, args.city)
    area_id = overpass_area_id(osm_type, osm_id)
    print(f"OSM boundary: type={osm_type} id={osm_id} -> overpass area={area_id}")

    q = build_overpass_query(area_id, max_elements=args.max_elements)
    print("Querying Overpass (this can take a bit)...")
    data = _get_json(session, OVERPASS_URL, data=q, timeout_s=180)
    elements = data.get("elements", []) or []
    print(f"Overpass returned elements: {len(elements)}")

    # Build leads
    leads: list[Lead] = []
    seen: set[tuple[str, int]] = set()

    robots_cache: dict[str, RobotFileParser] = {}
    sites_crawled = 0

    for el in elements:
        etype = str(el.get("type") or "")
        eid = int(el.get("id") or 0)
        key = (etype, eid)
        if key in seen:
            continue
        seen.add(key)

        tags = el.get("tags") or {}
        name = str(tags.get("name") or "").strip()
        if not name:
            continue

        category = str(tags.get("amenity") or tags.get("shop") or tags.get("office") or tags.get("tourism") or tags.get("leisure") or "")
        phone = str(tags.get("contact:phone") or tags.get("phone") or "").strip()
        website = normalize_website(str(tags.get("contact:website") or tags.get("website") or "").strip())
        email_osm = str(tags.get("contact:email") or tags.get("email") or "").strip()
        address = format_address(tags)

        lat = el.get("lat")
        lon = el.get("lon")
        if lat is None or lon is None:
            center = el.get("center") or {}
            lat = center.get("lat")
            lon = center.get("lon")

        emails_found = ""
        if args.extract_email and website and sites_crawled < args.max_sites:
            found = fetch_website_emails(
                session,
                website,
                max_pages=max(1, int(args.max_pages)),
                delay_s=float(args.delay),
                robots_cache=robots_cache,
            )
            sites_crawled += 1
            if found:
                emails_found = ";".join(found)

        leads.append(
            Lead(
                name=name,
                osm_type=etype,
                osm_id=eid,
                lat=float(lat) if lat is not None else None,
                lon=float(lon) if lon is not None else None,
                category=category,
                address=address,
                phone=phone,
                website=website,
                email_osm=email_osm,
                emails_found=emails_found,
            )
        )

    write_csv(out_path, leads)
    no_site = [l for l in leads if not l.website]
    write_csv(out_no_site, no_site)

    print("\nDone.")
    print(f"Full CSV: {out_path} ({len(leads)} rows)")
    print(f"No-site CSV: {out_no_site} ({len(no_site)} rows)")
    if args.extract_email:
        print(f"Websites crawled for email extraction: {sites_crawled}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
