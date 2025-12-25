import os
import sys
import json
import requests
import sqlite3
import time
import random
import shutil
import concurrent.futures
from datetime import datetime
from pathlib import Path
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --------------------------------------------------
# CONFIG
# --------------------------------------------------

UI_MAP = {
    "Movies": {"api_type": "movies", "item_type": "Movie"},
    "Shows":  {"api_type": "tvshows", "item_type": "Series"},
    "Music":  {"api_type": "music", "item_type": "MusicAlbum"},
}

BASE_DIR = os.path.dirname(sys.executable if getattr(sys, "frozen", False) else __file__)
DATA_ROOT = os.path.join(BASE_DIR, "JellyDiscover_Data")
DB_FILE = os.path.join(BASE_DIR, "jelly_data.db")

def load_json(name):
    with open(os.path.join(BASE_DIR, name), "r") as f:
        return json.load(f)

CONFIG = load_json("config.json")
LIBS = load_json("libraries.json")

# --------------------------------------------------
# NETWORK
# --------------------------------------------------

session = requests.Session()
retry = Retry(total=3, backoff_factor=1,
              status_forcelist=[429, 500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retry)
session.mount("http://", adapter)
session.mount("https://", adapter)
session.headers.update({
    "X-Emby-Token": CONFIG["API_KEY"],
    "Content-Type": "application/json"
})
TIMEOUT = 120

# --------------------------------------------------
# DB
# --------------------------------------------------

def init_db():
    conn = sqlite3.connect(DB_FILE)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS user_prefs (
            user_id TEXT PRIMARY KEY,
            prefs TEXT,
            updated TEXT
        )
    """)
    conn.commit()
    return conn

# --------------------------------------------------
# CATEGORY WEIGHTS
# --------------------------------------------------

CATEGORY_WEIGHTS = {
    "Movies": {
        "genres": 1.0,
        "actors": 1.5,
        "directors": 2.5,
        "community": 2.0,
        "collection": 5.0,
        "seen_penalty": 10.0,
        "diversity": 1.2
    },
    "Shows": {
        "genres": 1.5,
        "actors": 2.0,
        "directors": 1.0,
        "community": 1.5,
        "collection": 3.0,
        "seen_penalty": 6.0,
        "diversity": 1.0
    },
    "Music": {
        "genres": 2.0,
        "actors": 0.0,
        "directors": 0.0,
        "community": 1.0,
        "collection": 2.0,
        "seen_penalty": 4.0,
        "diversity": 0.8
    }
}

# --------------------------------------------------
# HELPERS
# --------------------------------------------------

def empty_prefs():
    return {
        "genres": {},
        "actors": {},
        "directors": {},
        "collections": set()
    }

def inc(d, k, v):
    d[k] = d.get(k, 0) + v

def normalize(d):
    if not d:
        return d
    m = max(d.values())
    return {k: v / m for k, v in d.items()} if m else d

def recency_multiplier(item):
    last = item.get("LastPlayedDate")
    if not last:
        return 0.7
    days = (datetime.utcnow() -
            datetime.fromisoformat(last.replace("Z", ""))).days
    if days < 30: return 1.5
    if days < 90: return 1.0
    if days < 365: return 0.6
    return 0.3

# --------------------------------------------------
# USER MODEL
# --------------------------------------------------

def analyze_user(user):
    params = {
        "Recursive": "true",
        "Filters": "IsPlayed",
        "Fields": "Genres,People,CollectionName,LastPlayedDate,UserData",
        "Limit": 3000
    }

    try:
        items = session.get(
            f"{CONFIG['JELLYFIN_URL']}/Users/{user['Id']}/Items",
            params=params,
            timeout=TIMEOUT
        ).json().get("Items", [])
    except Exception:
        items = []

    prefs = empty_prefs()

    for i in items:
        w = recency_multiplier(i)

        if i.get("UserData", {}).get("Played") and i.get("PlayCount", 0) == 1:
            w *= 0.5

        for g in i.get("Genres", []):
            inc(prefs["genres"], g, 1.0 * w)

        for p in i.get("People", []):
            if p["Type"] == "Director":
                inc(prefs["directors"], p["Name"], 4.0 * w)
            elif p["Type"] == "Actor":
                inc(prefs["actors"], p["Name"], 2.0 * w)

        if i.get("CollectionName"):
            prefs["collections"].add(i["CollectionName"])

    prefs["genres"] = normalize(prefs["genres"])
    prefs["actors"] = normalize(prefs["actors"])
    prefs["directors"] = normalize(prefs["directors"])

    has_history = len(items) >= 5
    return prefs, has_history

# --------------------------------------------------
# SCORING
# --------------------------------------------------

def score_item(item, prefs, weights, cold):
    score = 0.0

    cr = item.get("CommunityRating")
    if cr:
        score += max(0, cr - 6.5) * weights["community"]

    if not cold:
        for g in item.get("Genres", []):
            score += prefs["genres"].get(g, 0) * weights["genres"]

        for p in item.get("People", []):
            if p["Type"] == "Director":
                score += prefs["directors"].get(p["Name"], 0) * weights["directors"]
            elif p["Type"] == "Actor":
                score += prefs["actors"].get(p["Name"], 0) * weights["actors"]

        if item.get("CollectionName") in prefs["collections"]:
            score += weights["collection"]

        if item.get("UserData", {}).get("Played"):
            score -= weights["seen_penalty"]

    score += random.uniform(0, weights["diversity"])
    return score

# --------------------------------------------------
# USER PROCESSING
# --------------------------------------------------

def process_user(user, lib_map, index):
    prefs, has_history = analyze_user(user)
    safe = "".join(c for c in user["Name"] if c.isalnum() or c in " -_")
    suffix = "\u3164" * (index + 1)

    for cat, meta in lib_map.items():
        weights = CATEGORY_WEIGHTS.get(cat, CATEGORY_WEIGHTS["Movies"])

        params = {
            "ParentIds": ",".join(meta["source_ids"]),
            "IncludeItemTypes": meta["item_type"],
            "Recursive": "true",
            "Fields": "Path,CommunityRating,Genres,People,CollectionName,UserData",
            "Limit": 600
        }

        items = session.get(
            f"{CONFIG['JELLYFIN_URL']}/Users/{user['Id']}/Items",
            params=params,
            timeout=TIMEOUT
        ).json().get("Items", [])

        scored = []
        for i in items:
            if not i.get("Path"):
                continue
            s = score_item(i, prefs, weights, not has_history)
            if s >= meta["min_score"]:
                i["_Score"] = s
                scored.append(i)

        scored.sort(key=lambda x: x["_Score"], reverse=True)
        top = scored[:CONFIG["RECOMMENDATION_COUNT"]]

        out = Path(DATA_ROOT) / safe / cat
        if out.exists():
            shutil.rmtree(out)
        out.mkdir(parents=True, exist_ok=True)

        for i in top:
            name = "".join(c for c in i["Name"] if c.isalnum() or c in " -_")
            try:
                os.symlink(i["Path"], out / name)
            except Exception:
                pass

        session.post(
            f"{CONFIG['JELLYFIN_URL']}/Library/VirtualFolders",
            params={
                "name": f"{meta['discovery_name']}{suffix}",
                "collectionType": meta["collection_type"],
                "paths": [str(out)],
                "refreshLibrary": "true"
            },
            json={}
        )

    return user["Name"]

# --------------------------------------------------
# RUN
# --------------------------------------------------

def run():
    os.makedirs(DATA_ROOT, exist_ok=True)
    init_db()

    libs = session.get(
        f"{CONFIG['JELLYFIN_URL']}/Library/VirtualFolders",
        timeout=TIMEOUT
    ).json()

    lib_map = {}
    for cat, cfg in LIBS["CATEGORIES"].items():
        if not cfg["enabled"] or cat not in UI_MAP:
            continue
        ids = [l["ItemId"] for l in libs if l["CollectionType"] == UI_MAP[cat]["api_type"]]
        if ids:
            lib_map[cat] = {
                "source_ids": ids,
                "item_type": UI_MAP[cat]["item_type"],
                "discovery_name": cfg["discovery_name"],
                "min_score": cfg["min_community_score"],
                "collection_type": UI_MAP[cat]["api_type"]
            }

    users = session.get(f"{CONFIG['JELLYFIN_URL']}/Users", timeout=TIMEOUT).json()

    with concurrent.futures.ThreadPoolExecutor(CONFIG["MAX_THREADS"]) as ex:
        for i, u in enumerate(users):
            ex.submit(process_user, u, lib_map, i)

if __name__ == "__main__":
    run()
