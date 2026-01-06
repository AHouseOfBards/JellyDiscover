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
import ctypes
import concurrent.futures
import logging
import platform
from datetime import datetime, timedelta, timezone
from pathlib import Path
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import utils 

# --- FORCE UTF-8 ---
if sys.platform == "win32" and hasattr(sys.stdout, "reconfigure"):
    try:
        if sys.stdout: sys.stdout.reconfigure(encoding='utf-8')
    except Exception: pass

# --- AUTO-ELEVATION ---
def is_admin():
    try: return ctypes.windll.shell32.IsUserAnAdmin()
    except: return False

if not is_admin() and sys.platform == "win32":
    if len(sys.argv) == 1:
        try:
            print("[!] Requesting elevation for Drive Snapshot & Symlink Test...", flush=True)
            ctypes.windll.shell32.ShellExecuteW(None, "runas", sys.executable, " ".join(sys.argv), None, 1)
            sys.exit()
        except Exception: pass 

# --------------------------------------------------
# CONFIGURATION & FATAL ERROR HANDLING
# --------------------------------------------------
DATA_ROOT = utils.DATA_DIR
LOG_FILE = os.path.join(utils.LOG_DIR, "JellyDiscover.log")
DB_FILE = utils.DATA_DIR # Fixed: utils defines DB_FILE logic or we construct it
if not os.path.exists(utils.DATA_DIR): os.makedirs(utils.DATA_DIR, exist_ok=True)
DB_PATH = os.path.join(utils.DATA_DIR, "jelly_data.db")

# Load Config via Utils
CONFIG = utils.load_config()
LIBS = utils.load_libraries()

UI_MAP = {
    "Movies": {"api_type": "movies", "item_type": "Movie"},
    "Shows":  {"api_type": "tvshows", "item_type": "Series"},
    "Music":  {"api_type": "music", "item_type": "MusicAlbum"},
}

def fatal(msg):
    """Writes fatal error to status file and exits."""
    print(f"[FATAL] {msg}")
    logging.error(f"[FATAL] {msg}")
    try:
        with open(utils.STATUS_FILE, "w") as f:
            json.dump({"state": "fatal", "message": msg, "timestamp": datetime.now().isoformat()}, f)
    except: pass
    sys.exit(1)

# --------------------------------------------------
# LOCKING & LOGGING
# --------------------------------------------------
_lock_socket = None
def acquire_lock():
    global _lock_socket
    _lock_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        _lock_socket.bind(('127.0.0.1', 64001))
        return True
    except socket.error: return False

def send_notification(title, message):
    """
    Sends a desktop notification using native OS tools.
    Works on Windows (PowerShell) and Linux (notify-send).
    """
    # 1. Skip if Docker (Headless)
    if utils.IS_DOCKER: return

    try:
        system_os = platform.system().lower()
        
        # 2. Windows Notification (via PowerShell)
        if "windows" in system_os:
            ps_script = f"""
            [Windows.UI.Notifications.ToastNotificationManager, Windows.UI.Notifications, ContentType=WindowsRuntime] > $null
            $template = [Windows.UI.Notifications.ToastNotificationManager]::GetTemplateContent([Windows.UI.Notifications.ToastTemplateType]::ToastText02)
            $textNodes = $template.GetElementsByTagName("text")
            $textNodes.Item(0).AppendChild($template.CreateTextNode('{title}')) > $null
            $textNodes.Item(1).AppendChild($template.CreateTextNode('{message}')) > $null
            $notifier = [Windows.UI.Notifications.ToastNotificationManager]::CreateToastNotifier("JellyDiscover")
            $notification = [Windows.UI.Notifications.ToastNotification]::new($template)
            $notifier.Show($notification)
            """
            subprocess.run(["powershell", "-Command", ps_script], capture_output=True, creationflags=subprocess.CREATE_NO_WINDOW)

        # 3. Linux Notification (via notify-send)
        elif "linux" in system_os:
            # Check if notify-send exists
            if shutil.which("notify-send"):
                subprocess.run(["notify-send", title, message])
                
    except Exception as e:
        logging.warning(f"Notification failed: {e}")

from logging.handlers import RotatingFileHandler

log_handlers = [logging.StreamHandler(sys.stdout)]
try:
    # Rotate logs: Max 5MB, keep 3 backups
    rfh = RotatingFileHandler(LOG_FILE, maxBytes=5*1024*1024, backupCount=3, encoding='utf-8')
    log_handlers.append(rfh)
except: pass
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", handlers=log_handlers)

# --------------------------------------------------
# UTILS & NETWORK
# --------------------------------------------------
def is_safe_path(path):
    try: return path.startswith(os.path.abspath(DATA_ROOT))
    except: return False

def truncate_path(name, max_len=50):
    # Allow alphanumeric, spaces, hyphens, underscores
    clean = "".join(c for c in name if c.isalnum() or c in " -_")
    # ALWAYS strip spaces from both ends, then truncate, then strip again just in case
    return clean.strip()[:max_len].strip()

def safe_delete(path):
    if not os.path.exists(path): return
    for i in range(3):
        try:
            if os.path.isdir(path): shutil.rmtree(path)
            else: os.remove(path)
            return
        except: time.sleep(0.1)

def startup_local_cleanup():
    marker = os.path.join(DATA_ROOT, ".installed")
    if os.path.exists(marker): return
    logging.info("[*] First run detected. Performing local cleanup...")
    for item in os.listdir(DATA_ROOT):
        if item in ("JellyDiscover.log", "jelly_data.db", ".installed", "drive_map.json", "logs", "config.json", "libraries.json", "status.json"): continue
        full_path = os.path.join(DATA_ROOT, item)
        if not is_safe_path(full_path): continue
        safe_delete(full_path)
    try:
        with open(marker, "w") as f: f.write(datetime.now(timezone.utc).isoformat())
    except: pass

session = requests.Session()
retry = Retry(total=3, backoff_factor=0.2, status_forcelist=[429, 500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retry)
session.mount("http://", adapter)
session.mount("https://", adapter)
session.headers.update({"X-Emby-Token": CONFIG.get("API_KEY", ""), "Content-Type": "application/json"})
TIMEOUT = 60

def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("CREATE TABLE IF NOT EXISTS user_prefs (user_id TEXT PRIMARY KEY, prefs TEXT, updated TEXT)")
    conn.commit()
    return conn

# --------------------------------------------------
# DRIVE MAPPING & SYMLINK CHECK
# --------------------------------------------------
GLOBAL_DRIVE_MAP = {}
CAN_SYMLINK = False

def update_drive_mappings():
    global GLOBAL_DRIVE_MAP
    if not utils.IS_WINDOWS: return
    current_map = {}
    try:
        result = subprocess.run(['net', 'use'], capture_output=True, text=True, timeout=2)
        for line in result.stdout.splitlines():
            parts = line.split()
            drive = next((p for p in parts if len(p) == 2 and p[1] == ":"), None)
            unc = next((p for p in parts if p.startswith("\\\\")), None)
            if drive and unc: current_map[drive.upper()] = unc
    except: pass
    if current_map: GLOBAL_DRIVE_MAP = current_map

def resolve_path(path):
    if CONFIG.get("USE_NETWORK_DRIVE", False):
        subs = CONFIG.get("PATH_SUBSTITUTIONS", {})
        for remote, local in subs.items():
            if path.startswith(remote): return path.replace(remote, local, 1)
    if not GLOBAL_DRIVE_MAP: return path
    path_upper = path.upper()
    for drive, unc in GLOBAL_DRIVE_MAP.items():
        if path_upper.startswith(drive): return path.replace(path[:2], unc)
    return path

def check_symlink_rights():
    global CAN_SYMLINK
    test_link = os.path.join(DATA_ROOT, "test_link.tmp")
    test_target = os.path.join(DATA_ROOT, "test_target.tmp")
    try:
        with open(test_target, "w") as f: f.write("test")
        if os.path.exists(test_link): os.remove(test_link)
        os.symlink(test_target, test_link)
        CAN_SYMLINK = True
        os.remove(test_link)
        os.remove(test_target)
        logging.info("[*] Symlink capabilities: ENABLED")
    except OSError:
        CAN_SYMLINK = False
        if os.path.exists(test_target): os.remove(test_target)
        logging.warning("[!] Symlink capabilities: DISABLED")

# --------------------------------------------------
# SCORING ENGINE
# --------------------------------------------------
CATEGORY_WEIGHTS = CONFIG.get("SCORING", {}).get("DISCOVERY_BIAS", {}) # Loaded from config

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
    params = {"Recursive": "true", "Filters": "IsPlayed", "Fields": "Genres,People,CollectionName,LastPlayedDate,UserData", "Limit": 3000}
    try: items = session.get(f"{CONFIG['JELLYFIN_URL']}/Users/{user['Id']}/Items", params=params, timeout=TIMEOUT).json().get("Items", [])
    except: return empty_prefs(), False
    prefs = empty_prefs()
    for i in items:
        w = recency_multiplier(i)
        for g in i.get("Genres", []): prefs["genres"][g] = prefs["genres"].get(g, 0) + (1.0 * w)
        for p in i.get("People", []):
            if p["Type"] == "Director": prefs["directors"][p["Name"]] = prefs["directors"].get(p["Name"], 0) + (4.0 * w)
            elif p["Type"] == "Actor": prefs["actors"][p["Name"]] = prefs["actors"].get(p["Name"], 0) + (2.0 * w)
        if i.get("CollectionName"): prefs["collections"].add(i["CollectionName"])
    prefs["genres"] = normalize(prefs["genres"])
    prefs["actors"] = normalize(prefs["actors"])
    prefs["directors"] = normalize(prefs["directors"])
    return prefs, len(items) >= 5

def score_item(item, prefs, weights, cold):
    score = 0.0
    rating = item.get("CommunityRating", 0)
    is_music = item.get("Type") in ["MusicAlbum", "Audio"]
    if cold:
        if rating > 0: score = rating + 2.0
        elif is_music: score = random.uniform(6.5, 9.5)
    else:
        score = float(rating) if rating > 0 else (7.0 if is_music else 5.0)
        for g in item.get("Genres", []): score += prefs["genres"].get(g, 0) * weights["genres"]
        for p in item.get("People", []):
            if p["Type"] == "Director": score += prefs["directors"].get(p["Name"], 0) * weights["directors"]
            elif p["Type"] == "Actor": score += prefs["actors"].get(p["Name"], 0) * weights["actors"]
        if item.get("CollectionName") in prefs["collections"]: score += weights["collection"]
        if item.get("UserData", {}).get("Played"): score -= weights["seen_penalty"]
    score += random.uniform(0, weights["diversity"])
    return score

# --------------------------------------------------
# GENERATORS
# --------------------------------------------------
def escape_xml(text):
    if not text: return ""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;").replace("'", "&apos;")

def create_music_nfo(folder_path, artist, album):
    folder_path.mkdir(parents=True, exist_ok=True)
    safe_artist, safe_album = escape_xml(artist), escape_xml(album)
    album_nfo = folder_path / "album.nfo"
    if not album_nfo.exists():
        try:
            with open(album_nfo, "w", encoding="utf-8") as f:
                f.write(f"<album><title>{safe_album}</title><artist>{safe_artist}</artist></album>")
        except: pass
    artist_nfo = folder_path.parent / "artist.nfo"
    if not artist_nfo.exists():
        try:
            with open(artist_nfo, "w", encoding="utf-8") as f:
                f.write(f"<artist><name>{safe_artist}</name></artist>")
        except: pass

def create_content(source_path, target_folder, is_music=False):
    real_source = resolve_path(source_path)
    if os.path.isdir(real_source):
        target_folder.mkdir(parents=True, exist_ok=True)
        for root, dirs, files in os.walk(real_source):
            target_root = target_folder / os.path.relpath(root, real_source)
            target_root.mkdir(parents=True, exist_ok=True)
            for file in files:
                ext = os.path.splitext(file)[1].lower()
                src_file = os.path.join(root, file)
                if ext in ['.mp3', '.flac', '.m4a', '.wav', '.ogg', '.mkv', '.mp4', '.avi', '.m4v', '.wmv', '.ts', '.mov', '.iso']:
                    if is_music:
                        tgt_link = target_root / file
                        try:
                            if tgt_link.exists(): os.remove(tgt_link)
                            os.symlink(src_file, tgt_link)
                        except: pass
                    else:
                        tgt_strm = target_root / (os.path.splitext(file)[0] + ".strm")
                        try:
                            with open(tgt_strm, "w", encoding="utf-8") as f: f.write(src_file)
                        except: pass
                elif ext in ['.jpg', '.jpeg', '.png', '.tbn', '.nfo']:
                    tgt_file = target_root / file
                    if not tgt_file.exists():
                        try: shutil.copy2(src_file, tgt_file)
                        except: pass
    else:
        target_folder.mkdir(parents=True, exist_ok=True)
        tgt_file = target_folder / (os.path.basename(real_source) + ".strm")
        try:
            with open(tgt_file, "w", encoding="utf-8") as f: f.write(real_source)
        except: pass

# --------------------------------------------------
# LIBRARY MANAGEMENT
# --------------------------------------------------
def get_library_mapping():
    try:
        resp = session.get(f"{CONFIG['JELLYFIN_URL']}/Library/VirtualFolders", timeout=TIMEOUT)
        resp.raise_for_status()
        libs = resp.json()
    except Exception as e:
        fatal(f"Connection to Jellyfin failed: {e}")
        return {}
    
    lib_map = {}
    for cat, cfg in LIBS.get("CATEGORIES", {}).items():
        if not cfg.get("enabled", False): continue
        ids = [l["ItemId"] for l in libs if l["CollectionType"] == UI_MAP[cat]["api_type"]]
        if ids:
            lib_map[cat] = {
                "source_ids": ids, "item_type": UI_MAP[cat]["item_type"],
                "discovery_name": cfg["discovery_name"], "min_score": cfg["min_community_score"],
                "collection_type": UI_MAP[cat]["api_type"]
            }
    return lib_map

# --------------------------------------------------
# POLICY SANITIZER (Prevents Ghost Libraries)
# --------------------------------------------------
def sanitize_policies(deleted_ids):
    """Removes list of ItemIds from all users' EnabledFolders."""
    if not deleted_ids: return
    try:
        users = session.get(f"{CONFIG['JELLYFIN_URL']}/Users", timeout=TIMEOUT).json()
        for user in users:
            try:
                # Fetch full user to get current policy
                u_data = session.get(f"{CONFIG['JELLYFIN_URL']}/Users/{user['Id']}", timeout=TIMEOUT).json()
                policy = u_data.get("Policy", {})
                enabled = policy.get("EnabledFolders", [])
                
                # Check if this user has any of the deleted IDs
                if any(uid in deleted_ids for uid in enabled):
                    new_enabled = [uid for uid in enabled if uid not in deleted_ids]
                    policy["EnabledFolders"] = new_enabled
                    session.post(f"{CONFIG['JELLYFIN_URL']}/Users/{user['Id']}/Policy", json=policy, timeout=TIMEOUT)
            except: pass
    except: pass

def cleanup_stale_libraries(lib_map):
    logging.info("[*] Scanning for stale discovery libraries...")
    try: current_libs = session.get(f"{CONFIG['JELLYFIN_URL']}/Library/VirtualFolders", timeout=TIMEOUT).json()
    except: return
    
    ZW = "\u200B"
    # Libraries we EXPECT to exist based on current config
    expected_bases = [meta["discovery_name"] for meta in lib_map.values()]
    
    to_del_names = []
    to_del_ids = []
    
    for lib in current_libs:
        name = lib.get("Name", "")
        # Identify stale if it has our Invisible Marker OR acts like our library
        # but does NOT match the currently configured names
        if ZW in name or "Discover" in name or "Recommended" in name:
            # If it matches a valid current base name, keep it (unless it's a duplicate/glitch)
            is_valid = False
            for base in expected_bases:
                if name.startswith(base):
                    is_valid = True
                    break
            
            if not is_valid:
                to_del_names.append(name)
                to_del_ids.append(lib.get("ItemId"))

    if not to_del_names: return
    
    logging.info(f"[*] Found {len(to_del_names)} stale libraries to cleanup...")
    
    # 1. Delete from Jellyfin
    for name in to_del_names:
        try: session.delete(f"{CONFIG['JELLYFIN_URL']}/Library/VirtualFolders", params={"name": name, "refreshLibrary": "false"})
        except: pass
        
    # 2. Remove Ghost Icons from Users
    sanitize_policies(to_del_ids)

def optimize_library(library_name):
    try:
        all_libs = session.get(f"{CONFIG['JELLYFIN_URL']}/Library/VirtualFolders", timeout=TIMEOUT).json()
        target = next((l for l in all_libs if l.get('Name') == library_name), None)
        if not target: return
        session.post(f"{CONFIG['JELLYFIN_URL']}/Library/VirtualFolders/LibraryOptions", json={
            "Id": target['ItemId'], "LibraryOptions": {"EnableRealtimeMonitor": False, "EnableAutomaticSeriesGrouping": True}
        }, timeout=TIMEOUT)
    except: pass

def process_user(user, lib_map, index):
    u_name, u_id = user['Name'], user['Id']
    prefs, has_history = analyze_user(user)
    logging.info(f"[*] Analyzing: {u_name}")
    safe_name = truncate_path(u_name or u_id)
    invisible_suffix = "\u200B" * (index + 1)
    
    for cat, meta in lib_map.items():
        if cat == "Music" and not CAN_SYMLINK: continue
        
        # 1. Prepare Local Folders
        out = Path(DATA_ROOT) / safe_name / cat
        params = {"ParentIds": ",".join(meta["source_ids"]), "IncludeItemTypes": meta["item_type"], "Recursive": "true", "Filters": "IsUnplayed", "Fields": "Path,CommunityRating,Genres,People,CollectionName,UserData,AlbumArtist,Artists", "Limit": 600}
        
        try: items = session.get(f"{CONFIG['JELLYFIN_URL']}/Users/{u_id}/Items", params=params, timeout=TIMEOUT).json().get("Items", [])
        except: items = []
        if not items: continue
        
        weights = CATEGORY_WEIGHTS.get(cat, CONFIG.get("SCORING", {}).get("DISCOVERY_BIAS", {}).get("Movies"))
        scored = sorted([i for i in items if i.get("Path") and score_item(i, prefs, weights, not has_history) >= meta["min_score"]], key=lambda x: score_item(x, prefs, weights, not has_history), reverse=True)
        top = scored[:CONFIG.get("RECOMMENDATION_COUNT", 25)]
        
        if out.exists(): safe_delete(out)
        out.mkdir(parents=True, exist_ok=True)
        
        for i in top:
            clean = truncate_path(i["Name"])
            if cat == "Music":
                artist = truncate_path(i.get("AlbumArtist") or (i.get("Artists") or ["Unknown"])[0])
                folder = out / artist / clean
                create_music_nfo(folder, artist, clean)
                create_content(i["Path"], folder, is_music=True)
            else: create_content(i["Path"], out / clean, is_music=False)
            
        final_name = f"{meta['discovery_name']}{invisible_suffix}"
        
        # --- FIX: Pre-emptive Delete ---
        # We must delete the existing library to prevent "Discover Movies 2" 
        # and to force the database to clear out "Ghost Items".
        try:
            session.delete(f"{CONFIG['JELLYFIN_URL']}/Library/VirtualFolders", 
                           params={"name": final_name, "refreshLibrary": "false"}, 
                           timeout=TIMEOUT)
        except: pass
        # -------------------------------

        try:
            session.post(f"{CONFIG['JELLYFIN_URL']}/Library/VirtualFolders", 
                         params={"name": final_name, "collectionType": meta["collection_type"], "paths": [str(out)], "refreshLibrary": "true"}, 
                         json={}, 
                         timeout=TIMEOUT)
            optimize_library(final_name)
        except: pass
        
    return u_name

def apply_strict_privacy():
    logging.info("[*] Applying Privacy Shield...")
    try:
        # Get all users and all libraries
        users = session.get(f"{CONFIG['JELLYFIN_URL']}/Users", timeout=TIMEOUT).json()
        libs = session.get(f"{CONFIG['JELLYFIN_URL']}/Library/VirtualFolders", timeout=TIMEOUT).json()
        
        # 1. Normalize Root Path for comparison (Lower case, consistent slashes)
        # We use the script's known DATA_ROOT to identify which libraries are "Ours"
        root_marker = os.path.abspath(DATA_ROOT).lower()
        
        real_libs = []
        discovery_libs = []

        # 2. Sort libraries into "Real" (User's media) and "Discovery" (Our generated ones)
        for l in libs:
            locs = l.get('Locations', [])
            if not locs: continue
            
            # Check if ANY path in this library lives inside our DATA_ROOT
            is_generated = False
            for loc in locs:
                # We simply check if our Root Folder appears in the library path
                # This works even if network shares or drive mappings differ slightly
                if root_marker in os.path.abspath(loc).lower():
                    is_generated = True
                    break
            
            if is_generated:
                discovery_libs.append(l)
            else:
                real_libs.append(l)

        public_ids = [l['ItemId'] for l in real_libs]
        
        # 3. Assign Permissions
        for user in users:
            # Calculate the expected folder name for this user (e.g., "TrevyrPhillips")
            safe_name = truncate_path(user['Name'] or user['Id'])
            user_discovery_ids = []
            
            # Search our discovery libraries for ones containing this user's safe_name
            # We look for:  .../JellyDiscover/SafeName/Movies...
            expected_part = f"{os.sep}{safe_name}{os.sep}".lower()
            
            for dl in discovery_libs:
                locs = dl.get('Locations', [])
                for loc in locs:
                    if expected_part in os.path.abspath(loc).lower():
                        user_discovery_ids.append(dl['ItemId'])
                        break
            
            # FETCH FULL POLICY: The /Users endpoint only returns a summary. 
            # We fetch the specific user to get their complete current policy first.
            try:
                full_user = session.get(f"{CONFIG['JELLYFIN_URL']}/Users/{user['Id']}", timeout=TIMEOUT).json()
                current_policy = full_user.get("Policy", {})
            except:
                # Fallback to summary if fetch fails
                current_policy = user.get("Policy", {})

            # Construct the new policy
            new_policy = {
                **current_policy,
                "EnableAllFolders": False,
                "EnabledFolders": public_ids + user_discovery_ids
            }
            
            # Send the update
            session.post(f"{CONFIG['JELLYFIN_URL']}/Users/{user['Id']}/Policy", json=new_policy, timeout=TIMEOUT)
            
            logging.info(f"    - Secured {user['Name']}: Visible = {len(public_ids)} Real + {len(user_discovery_ids)} Discovery")
            
    except Exception as e:
        logging.error(f"[!] Privacy Shield Failed: {e}")

def run_task():
    # --- HOT RELOAD FIX: Refresh Config & Libraries ---
    # This ensures Dashboard changes apply instantly without service restart
    global CONFIG, LIBS
    CONFIG = utils.load_config()
    LIBS = utils.load_libraries()
    
    # Update Session Headers with potentially new API Key
    session.headers.update({"X-Emby-Token": CONFIG.get("API_KEY", "")})

    # FATAL ERROR CHECK 1: Missing API Key
    if not CONFIG.get("API_KEY"):
        fatal("API Key is missing in config.json. Please configure it in the dashboard.")

    send_notification("JellyDiscover", "Starting update...")
    startup_local_cleanup()
    init_db()
    update_drive_mappings()
    check_symlink_rights()
    
    # FATAL ERROR CHECK 2: Connection failure (handled inside get_library_mapping via fatal())
    lib_map = get_library_mapping()
    
    if not lib_map:
        logging.warning("[!] No libraries configured enabled in libraries.json.")
        return # Not fatal, just nothing to do this run
    
    cleanup_stale_libraries(lib_map)
    try:
        users = session.get(f"{CONFIG['JELLYFIN_URL']}/Users", timeout=TIMEOUT).json()
        
        # USE THREAD COUNT FROM CONFIG
        thread_count = CONFIG.get("MAX_THREADS", 2)
        logging.info(f"[*] Starting processing with {thread_count} threads...")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=thread_count) as ex:
            # We map process_user over the users list
            for res in ex.map(lambda u: process_user(u, lib_map, users.index(u)), users): 
                if res: logging.info(f"    [DONE] {res}")
        
        apply_strict_privacy()
        
        # Clear status file on success so dashboard knows we are healthy
        if os.path.exists(utils.STATUS_FILE):
             try: os.remove(utils.STATUS_FILE)
             except: pass
             
        logging.info("[*] Run Complete.")
        send_notification("JellyDiscover", "Run Complete!")
    except Exception as e:
        fatal(f"Unexpected error during run: {e}")

def main():
    if not acquire_lock():
        time.sleep(1)
        run_task()
        sys.exit(0)
    try:
        if not CONFIG.get('DAEMON_MODE', False): run_task()
        else:
            r_str = CONFIG.get('RUN_TIME', "04:00")
            logging.info(f"[*] DAEMON ACTIVE: {r_str}")
            run_task()
            while True:
                h, m = map(int, r_str.split(':'))
                now = datetime.now()
                t = now.replace(hour=h, minute=m, second=0)
                if t <= now: t += timedelta(days=1)
                time.sleep((t - now).total_seconds())
                run_task()
    except KeyboardInterrupt: pass

if __name__ == "__main__": main()