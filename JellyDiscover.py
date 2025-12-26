print(">>> JellyDiscover Starting")
import os
import sys
import json
import requests
import sqlite3
import time
import random
import shutil
import subprocess
import socket
import concurrent.futures
from datetime import datetime, timedelta, timezone
from pathlib import Path
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging

# --- FORCE UTF-8 ---
if sys.platform == "win32" and hasattr(sys.stdout, "reconfigure"):
    try:
        if sys.stdout: sys.stdout.reconfigure(encoding='utf-8')
    except Exception: pass

# --------------------------------------------------
# CONFIGURATION & PATHS
# --------------------------------------------------

UI_MAP = {
    "Movies": {"api_type": "movies", "item_type": "Movie"},
    "Shows":  {"api_type": "tvshows", "item_type": "Series"},
    "Music":  {"api_type": "music", "item_type": "MusicAlbum"},
}

DATA_ROOT = os.path.join(os.getenv("PROGRAMDATA"), "JellyDiscover")
os.makedirs(DATA_ROOT, exist_ok=True)

LOG_FILE = os.path.join(DATA_ROOT, "JellyDiscover.log")
DB_FILE = os.path.join(DATA_ROOT, "jelly_data.db")
CACHE_FILE = os.path.join(DATA_ROOT, "library_cache.json")
INSTALL_MARKER = os.path.join(DATA_ROOT, ".installed")

BASE_DIR = os.path.dirname(sys.executable if getattr(sys, "frozen", False) else __file__)

def load_json(name):
    path = os.path.join(BASE_DIR, name)
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception as e:
        print(f"[!] Failed to load {name}: {e}")
        sys.exit(1)

CONFIG = load_json("config.json")
LIBS = load_json("libraries.json")

# --------------------------------------------------
# SMART LOCK
# --------------------------------------------------
_lock_socket = None

def acquire_lock():
    global _lock_socket
    _lock_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        _lock_socket.bind(('127.0.0.1', 64001))
        return True
    except socket.error:
        return False

# --------------------------------------------------
# NOTIFICATIONS
# --------------------------------------------------
def send_notification(title, message):
    try:
        ps_script = f"""
        [Windows.UI.Notifications.ToastNotificationManager, Windows.UI.Notifications, ContentType=WindowsRuntime] > $null
        $template = [Windows.UI.Notifications.ToastNotificationManager]::GetTemplateContent([Windows.UI.Notifications.ToastTemplateType]::ToastText02)
        $textNodes = $template.GetElementsByTagName("text")
        $textNodes.Item(0).AppendChild($template.CreateTextNode("{title}")) > $null
        $textNodes.Item(1).AppendChild($template.CreateTextNode("{message}")) > $null
        $notifier = [Windows.UI.Notifications.ToastNotificationManager]::CreateToastNotifier("JellyDiscover")
        $notification = [Windows.UI.Notifications.ToastNotification, Windows.UI.Notifications, ContentType=WindowsRuntime]::new($template)
        $notifier.Show($notification)
        """
        subprocess.run(["powershell", "-Command", ps_script], capture_output=True, creationflags=subprocess.CREATE_NO_WINDOW)
    except Exception as e:
        print(f"Notification failed: {e}")

# --------------------------------------------------
# LOGGER SETUP
# --------------------------------------------------
print(">>> Initializing Logger")
log_handlers = []
console_handler = logging.StreamHandler(sys.stdout)
try: 
    if sys.stdout: console_handler.setStream(sys.stdout)
except: pass
log_handlers.append(console_handler)

try:
    file_handler = logging.FileHandler(LOG_FILE, encoding='utf-8')
    log_handlers.append(file_handler)
except PermissionError:
    print("[!] Cannot write to ProgramData log file. Logging to console only.")

logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=log_handlers
)

# --------------------------------------------------
# UTILS & NETWORK
# --------------------------------------------------
def is_safe_path(path):
    try:
        return path.startswith(os.path.abspath(DATA_ROOT))
    except:
        return False

def startup_local_cleanup():
    if os.path.exists(INSTALL_MARKER):
        return

    logging.info("[*] First run detected. Performing local cleanup...")
    for item in os.listdir(DATA_ROOT):
        if item in ("JellyDiscover.log", "jelly_data.db", ".installed"):
            continue
        full_path = os.path.join(DATA_ROOT, item)
        if not is_safe_path(full_path):
            continue
        try:
            if os.path.isdir(full_path): shutil.rmtree(full_path)
            else: os.remove(full_path)
        except Exception as e:
            logging.warning(f"[!] Cleanup failed for {item}: {e}")

    with open(INSTALL_MARKER, "w") as f:
        f.write(datetime.now(timezone.utc).isoformat())

session = requests.Session()
retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retry)
session.mount("http://", adapter)
session.mount("https://", adapter)
session.headers.update({
    "X-Emby-Token": CONFIG.get("API_KEY", ""),
    "Content-Type": "application/json"
})
TIMEOUT = 120

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

def load_cache():
    try:
        if os.path.exists(CACHE_FILE):
            with open(CACHE_FILE, "r") as f: return json.load(f)
    except: pass
    return None

def save_cache(data):
    try:
        with open(CACHE_FILE, "w") as f: json.dump(data, f)
    except: pass

# --------------------------------------------------
# LOGIC
# --------------------------------------------------
CATEGORY_WEIGHTS = {
    "Movies": {"genres": 1.0, "actors": 1.5, "directors": 2.5, "community": 2.0, "collection": 5.0, "seen_penalty": 10.0, "diversity": 1.2},
    "Shows": {"genres": 1.5, "actors": 2.0, "directors": 1.0, "community": 1.5, "collection": 3.0, "seen_penalty": 6.0, "diversity": 1.0},
    "Music": {"genres": 2.0, "actors": 0.0, "directors": 0.0, "community": 1.0, "collection": 2.0, "seen_penalty": 4.0, "diversity": 0.8}
}

def empty_prefs(): return {"genres": {}, "actors": {}, "directors": {}, "collections": set()}
def normalize(d):
    m = max(d.values()) if d else 0
    return {k: v / m for k, v in d.items()} if m else d

def recency_multiplier(item):
    last = item.get("LastPlayedDate")
    if not last: return 0.7
    try:
        now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
        item_date = datetime.fromisoformat(last.replace("Z", ""))
        days = (now_utc - item_date).days
        return 1.5 if days < 30 else 1.0 if days < 90 else 0.6 if days < 365 else 0.3
    except: return 0.7

def analyze_user(user):
    params = {
        "Recursive": "true", "Filters": "IsPlayed",
        "Fields": "Genres,People,CollectionName,LastPlayedDate,UserData", "Limit": 3000
    }
    try:
        items = session.get(f"{CONFIG['JELLYFIN_URL']}/Users/{user['Id']}/Items", params=params, timeout=TIMEOUT).json().get("Items", [])
    except: return empty_prefs(), False

    prefs = empty_prefs()
    for i in items:
        w = recency_multiplier(i)
        if i.get("UserData", {}).get("Played") and i.get("PlayCount", 0) == 1: w *= 0.5
        for g in i.get("Genres", []): prefs["genres"][g] = prefs["genres"].get(g, 0) + (1.0 * w)
        for p in i.get("People", []):
            if p["Type"] == "Director": prefs["directors"][p["Name"]] = prefs["directors"].get(p["Name"], 0) + (4.0 * w)
            elif p["Type"] == "Actor": prefs["actors"][p["Name"]] = prefs["actors"].get(p["Name"], 0) + (2.0 * w)
        if i.get("CollectionName"): prefs["collections"].add(i["CollectionName"])

    prefs["genres"] = normalize(prefs["genres"])
    prefs["actors"] = normalize(prefs["actors"])
    prefs["directors"] = normalize(prefs["directors"])
    
    # Only consider it a "Warm" start if they have > 5 watched items
    return prefs, len(items) >= 5

def score_item(item, prefs, weights, cold):
    score = 0.0
    
    # 1. Base Score from Community Rating
    rating = item.get("CommunityRating", 0)
    
    if cold:
        # --- CRITICAL FIX: COLD START BOOST ---
        # If user has NO history, we trust Community Rating implicitly.
        # We boost it so it clears the 'min_score' filter easily.
        if rating > 0:
            score = rating + 2.0  # E.g., 7.0 becomes 9.0 (High Priority)
    else:
        # Standard Logic for Warm Users
        if rating: 
            score += max(0, rating - 6.5) * weights["community"]
        
        # Match Genres
        for g in item.get("Genres", []): 
            score += prefs["genres"].get(g, 0) * weights["genres"]
        
        # Match People
        for p in item.get("People", []):
            if p["Type"] == "Director": 
                score += prefs["directors"].get(p["Name"], 0) * weights["directors"]
            elif p["Type"] == "Actor": 
                score += prefs["actors"].get(p["Name"], 0) * weights["actors"]
        
        # Franchise Boost
        if item.get("CollectionName") in prefs["collections"]: 
            score += weights["collection"]
        
        # Seen Penalty
        if item.get("UserData", {}).get("Played"): 
            score -= weights["seen_penalty"]
            
    # Add Random Jitter
    score += random.uniform(0, weights["diversity"])
    return score

# --------------------------------------------------
# STRM GENERATOR (No Admin Required)
# --------------------------------------------------
def get_drive_map():
    drive_map = {}
    if os.name == 'nt':
        try:
            result = subprocess.run(['net', 'use'], capture_output=True, text=True, timeout=5)
            for line in result.stdout.splitlines():
                if ":" in line and "\\\\" in line:
                    parts = line.split()
                    drive = next((p for p in parts if ":" in p), None)
                    unc = next((p for p in parts if p.startswith("\\\\")), None)
                    if drive and unc: drive_map[drive.upper()] = unc
        except: pass
    return drive_map

DRIVE_MAP = get_drive_map()

def create_strm(source_path, target_path):
    """
    Creates a .strm file. Works for everyone (Local, Service, Admin, or User).
    """
    try:
        source_upper = source_path.upper()
        for drive, unc in DRIVE_MAP.items():
            if source_upper.startswith(drive):
                source_path = source_path.replace(source_path[:2], unc)
                break
        
        target_path = Path(target_path)
        if target_path.suffix.lower() != ".strm":
            target_path = target_path.with_suffix(".strm")

        with open(target_path, "w", encoding="utf-8") as f:
            f.write(source_path)
            
        logging.info(f"    [STRM] Created: {target_path}")
        
    except Exception as e:
        logging.error(f"    [FAIL] STRM creation failed: {e}")

# --------------------------------------------------
# LIBRARY MANAGEMENT
# --------------------------------------------------
def get_library_mapping():
    cached = load_cache()
    if cached: return cached
    
    try: 
        libs = session.get(f"{CONFIG['JELLYFIN_URL']}/Library/VirtualFolders", timeout=TIMEOUT).json()
    except: return {}

    lib_map = {}
    for cat, cfg in LIBS.get("CATEGORIES", {}).items():
        if not cfg.get("enabled", False) or cat not in UI_MAP: continue
        
        ids = [l["ItemId"] for l in libs if l["CollectionType"] == UI_MAP[cat]["api_type"]]
        if ids:
            lib_map[cat] = {
                "source_ids": ids,
                "item_type": UI_MAP[cat]["item_type"],
                "discovery_name": cfg["discovery_name"],
                "min_score": cfg["min_community_score"],
                "collection_type": UI_MAP[cat]["api_type"]
            }
            
    if lib_map: save_cache(lib_map)
    return lib_map

def cleanup_stale_libraries(lib_map):
    logging.info("[*] Scanning for stale discovery libraries...")
    try:
        current_libs = session.get(f"{CONFIG['JELLYFIN_URL']}/Library/VirtualFolders", timeout=TIMEOUT).json()
    except Exception:
        return

    ZERO_WIDTH = "\u200B"
    expected_bases = [meta["discovery_name"] for meta in lib_map.values()]

    for lib in current_libs:
        name = lib.get("Name", "")
        if ZERO_WIDTH in name and any(name.startswith(base) for base in expected_bases):
            clean_name = name.replace(ZERO_WIDTH, "").encode('ascii', 'ignore').decode()
            logging.info(f"    [CLEANUP] Deleting: {clean_name}")
            try: 
                session.delete(f"{CONFIG['JELLYFIN_URL']}/Library/VirtualFolders", 
                               params={"name": name, "refreshLibrary": "false"})
            except Exception as e: 
                logging.error(f"[!] Failed to delete library: {e}")

def process_user(user, lib_map, index):
    u_name, u_id = user['Name'], user['Id']
    conn = sqlite3.connect(DB_FILE)
    prefs, has_history = analyze_user(user)
    
    # Log status so we know why a user might get 0 items
    logging.info(f"[*] Analyzing user: {u_name} (History: {has_history})")
    
    safe_name = "".join(c for c in u_name if c.isalnum() or c in " -_").strip() or u_id
    ZERO_WIDTH = "\u200B"
    invisible_suffix = ZERO_WIDTH * (index + 1)

    for cat, meta in lib_map.items():
        params = {
            "ParentIds": ",".join(meta["source_ids"]),
            "IncludeItemTypes": meta["item_type"],
            "Recursive": "true",
            "Fields": "Path,CommunityRating,Genres,People,CollectionName,UserData",
            "Limit": 600
        }
        
        try: items = session.get(f"{CONFIG['JELLYFIN_URL']}/Users/{u_id}/Items", params=params, timeout=TIMEOUT).json().get("Items", [])
        except: items = []

        scored = []
        for i in items:
            path = i.get("Path")
            s = score_item(i, prefs, CATEGORY_WEIGHTS.get(cat, CATEGORY_WEIGHTS["Movies"]), not has_history)
            
            if path and s >= meta["min_score"]:
                i["_Score"] = s
                scored.append(i)
        
        # Sort and limit
        scored.sort(key=lambda x: x["_Score"], reverse=True)
        top = scored[:CONFIG.get("RECOMMENDATION_COUNT", 25)]

        out = Path(DATA_ROOT) / safe_name / cat
        if out.exists(): shutil.rmtree(out)
        out.mkdir(parents=True, exist_ok=True)

        for i in top:
            name = "".join(c for c in i["Name"] if c.isalnum() or c in " -_")
            create_strm(i["Path"], out / name)

        try:
            session.post(
                f"{CONFIG['JELLYFIN_URL']}/Library/VirtualFolders",
                params={
                    "name": f"{meta['discovery_name']}{invisible_suffix}",
                    "collectionType": meta["collection_type"],
                    "paths": [str(out)],
                    "refreshLibrary": "true"
                },
                json={}, timeout=TIMEOUT
            )
        except Exception: pass

    conn.close()
    return u_name

def apply_strict_privacy():
    logging.info("[*] Applying Privacy Shield...")
    try:
        users = session.get(f"{CONFIG['JELLYFIN_URL']}/Users", timeout=TIMEOUT).json()
        libs = session.get(f"{CONFIG['JELLYFIN_URL']}/Library/VirtualFolders", timeout=TIMEOUT).json()
    except Exception: return

    public_ids = []
    user_private_map = {u['Id']: [] for u in users}
    discovery_root = os.path.basename(DATA_ROOT)

    for lib in libs:
        if 'ItemId' not in lib: continue
        path = lib.get('Locations', [''])[0]
        if discovery_root in path:
            for user in users:
                safe = "".join(c for c in user['Name'] if c.isalnum() or c in " -_").strip() or user['Id']
                if f"{os.sep}{safe}{os.sep}" in path:
                    user_private_map[user['Id']].append(lib['ItemId'])
                    break
        else:
            public_ids.append(lib['ItemId'])

    for user in users:
        policy = user.get("Policy", {})
        policy["EnableAllFolders"] = False
        policy["EnabledFolders"] = public_ids + user_private_map.get(user['Id'], [])
        try: session.post(f"{CONFIG['JELLYFIN_URL']}/Users/{user['Id']}/Policy", json=policy, timeout=TIMEOUT)
        except: pass

# --------------------------------------------------
# MAIN EXECUTION
# --------------------------------------------------
def run_task():
    print(">>> Starting Run Task", flush=True)
    send_notification("JellyDiscover", "Starting update...")
    startup_local_cleanup()
    init_db()
    
    lib_map = get_library_mapping()
    if not lib_map:
        logging.error("[!] No libraries found.")
        return
        
    cleanup_stale_libraries(lib_map)
    
    try:
        users = session.get(f"{CONFIG['JELLYFIN_URL']}/Users", timeout=TIMEOUT).json()
        
        with concurrent.futures.ThreadPoolExecutor(CONFIG.get("MAX_THREADS", 4)) as ex:
            futures = {ex.submit(process_user, u, lib_map, idx): u for idx, u in enumerate(users)}
            for f in concurrent.futures.as_completed(futures):
                try: logging.info(f"    [DONE] {f.result()}")
                except: pass
        
        apply_strict_privacy()
        logging.info("[*] Run Complete.")
        send_notification("JellyDiscover", "Run Complete!")
        
    except Exception as e:
        logging.error(f"[!] Fatal: {e}")

def main():
    is_primary_service = acquire_lock()
    
    if not is_primary_service:
        logging.info("[*] Service is busy. Switching to Manual Force Run...")
        run_task()
        sys.exit(0)

    try:
        if not CONFIG.get('DAEMON_MODE', False):
            run_task()
        else:
            run_str = CONFIG.get('RUN_TIME', "04:00")
            logging.info(f"[*] DAEMON ACTIVE: Scheduled for {run_str}")
            logging.info("[*] Service Started. Running initial discovery...")
            run_task()

            while True:
                now = datetime.now()
                th, tm = map(int, run_str.split(':'))
                tgt = now.replace(hour=th, minute=tm, second=0)
                if tgt <= now: tgt += timedelta(days=1)
                
                seconds = (target - now).total_seconds()
                logging.info(f"[*] Sleeping {sec/3600:.1f} hours...")
                time.sleep(seconds)
                run_task()
    except KeyboardInterrupt: pass

if __name__ == "__main__":
    main()