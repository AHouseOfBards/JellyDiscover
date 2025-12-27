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

# --- AUTO-ELEVATION ---
def is_admin():
    try: return ctypes.windll.shell32.IsUserAnAdmin()
    except: return False

if not is_admin():
    if len(sys.argv) == 1:
        try:
            print("[!] Requesting elevation for Drive Snapshot & Symlink Test...", flush=True)
            ctypes.windll.shell32.ShellExecuteW(None, "runas", sys.executable, " ".join(sys.argv), None, 1)
            sys.exit()
        except Exception: pass 

# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------
DATA_ROOT = os.path.join(os.getenv("PROGRAMDATA"), "JellyDiscover")
os.makedirs(DATA_ROOT, exist_ok=True)

LOG_FILE = os.path.join(DATA_ROOT, "JellyDiscover.log")
DB_FILE = os.path.join(DATA_ROOT, "jelly_data.db")
CACHE_FILE = os.path.join(DATA_ROOT, "library_cache.json")
MAPPING_FILE = os.path.join(DATA_ROOT, "drive_map.json")
INSTALL_MARKER = os.path.join(DATA_ROOT, ".installed")

BASE_DIR = os.path.dirname(sys.executable if getattr(sys, "frozen", False) else __file__)

def load_json(name):
    path = os.path.join(BASE_DIR, name)
    try:
        with open(path, "r") as f: return json.load(f)
    except Exception as e:
        print(f"[!] Failed to load {name}: {e}")
        sys.exit(1)

CONFIG = load_json("config.json")
LIBS = load_json("libraries.json")

UI_MAP = {
    "Movies": {"api_type": "movies", "item_type": "Movie"},
    "Shows":  {"api_type": "tvshows", "item_type": "Series"},
    "Music":  {"api_type": "music", "item_type": "MusicAlbum"},
}

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
    except socket.error:
        return False

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
    except: pass

log_handlers = []
console_handler = logging.StreamHandler(sys.stdout)
try: 
    if sys.stdout: console_handler.setStream(sys.stdout)
except: pass
log_handlers.append(console_handler)
try:
    file_handler = logging.FileHandler(LOG_FILE, encoding='utf-8')
    log_handlers.append(file_handler)
except PermissionError: pass

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", handlers=log_handlers)

# --------------------------------------------------
# UTILS & NETWORK
# --------------------------------------------------
def is_safe_path(path):
    try: return path.startswith(os.path.abspath(DATA_ROOT))
    except: return False

def truncate_path(name, max_len=50):
    """Truncates long folder names to prevent path limit errors."""
    clean = "".join(c for c in name if c.isalnum() or c in " -_")
    return clean[:max_len].strip() if len(clean) > max_len else clean

def safe_delete(path):
    if not os.path.exists(path): return
    for i in range(3):
        try:
            if os.path.isdir(path): shutil.rmtree(path)
            else: os.remove(path)
            return
        except: 
            time.sleep(0.1) # Fast retry

def startup_local_cleanup():
    if os.path.exists(INSTALL_MARKER): return
    logging.info("[*] First run detected. Performing local cleanup...")
    for item in os.listdir(DATA_ROOT):
        if item in ("JellyDiscover.log", "jelly_data.db", ".installed", "drive_map.json"): continue
        full_path = os.path.join(DATA_ROOT, item)
        if not is_safe_path(full_path): continue
        safe_delete(full_path)
    with open(INSTALL_MARKER, "w") as f: f.write(datetime.now(timezone.utc).isoformat())

session = requests.Session()
# FAST RETRY CONFIG:
retry = Retry(total=3, backoff_factor=0.2, status_forcelist=[429, 500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retry)
session.mount("http://", adapter)
session.mount("https://", adapter)
session.headers.update({"X-Emby-Token": CONFIG.get("API_KEY", ""), "Content-Type": "application/json"})
TIMEOUT = 60 # Reduced from 120

def init_db():
    conn = sqlite3.connect(DB_FILE)
    conn.execute("CREATE TABLE IF NOT EXISTS user_prefs (user_id TEXT PRIMARY KEY, prefs TEXT, updated TEXT)")
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
# DRIVE MAPPING & SYMLINK CHECK
# --------------------------------------------------
GLOBAL_DRIVE_MAP = {}
CAN_SYMLINK = False

def update_drive_mappings():
    global GLOBAL_DRIVE_MAP
    current_map = {}
    
    # 1. Try to detect (User Session) - FAST TIMEOUT
    try:
        result = subprocess.run(['net', 'use'], capture_output=True, text=True, timeout=2)
        for line in result.stdout.splitlines():
            parts = line.split()
            drive = next((p for p in parts if len(p) == 2 and p[1] == ":"), None)
            unc = next((p for p in parts if p.startswith("\\\\")), None)
            if drive and unc:
                current_map[drive.upper()] = unc
    except: pass

    # 2. Save or Load
    if current_map:
        GLOBAL_DRIVE_MAP = current_map
        try:
            with open(MAPPING_FILE, "w") as f: json.dump(current_map, f)
            logging.info(f"[*] Mappings detected & saved: {len(current_map)} found")
        except: pass
    else:
        if os.path.exists(MAPPING_FILE):
            try:
                with open(MAPPING_FILE, "r") as f:
                    GLOBAL_DRIVE_MAP = json.load(f)
                logging.info(f"[*] Service loaded cached mappings.")
            except: pass
        else:
            logging.warning("[!] No drive mappings found and no cache file exists!")

def resolve_path(path):
    path_upper = path.upper()
    for drive, unc in GLOBAL_DRIVE_MAP.items():
        if path_upper.startswith(drive):
            return path.replace(path[:2], unc)
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
        logging.warning("[!] Symlink capabilities: DISABLED (Music skipped)")
    except Exception as e:
        CAN_SYMLINK = False
        logging.error(f"[!] Symlink check failed unexpected: {e}")

# --------------------------------------------------
# SCORING ENGINE
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
    params = {"Recursive": "true", "Filters": "IsPlayed", "Fields": "Genres,People,CollectionName,LastPlayedDate,UserData", "Limit": 3000}
    try: items = session.get(f"{CONFIG['JELLYFIN_URL']}/Users/{user['Id']}/Items", params=params, timeout=TIMEOUT).json().get("Items", [])
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
    return prefs, len(items) >= 5

def score_item(item, prefs, weights, cold):
    score = 0.0
    rating = item.get("CommunityRating", 0)
    is_music = item.get("Type") in ["MusicAlbum", "Audio"]

    if cold:
        if rating > 0: score = rating + 2.0
        elif is_music: score = random.uniform(6.5, 9.5)
    else:
        if rating > 0: score = float(rating)
        elif is_music: score = 7.0 
        else: score = 5.0

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
    return (str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;").replace("'", "&apos;"))

def create_music_nfo(folder_path, artist, album):
    if not folder_path.exists(): folder_path.mkdir(parents=True, exist_ok=True)
    safe_artist = escape_xml(artist)
    safe_album = escape_xml(album)
    
    album_nfo = folder_path / "album.nfo"
    if not album_nfo.exists():
        try:
            with open(album_nfo, "w", encoding="utf-8") as f:
                f.write(f"<album><title>{safe_album}</title><artist>{safe_artist}</artist></album>")
        except: pass

    artist_folder = folder_path.parent
    artist_nfo = artist_folder / "artist.nfo"
    if not artist_nfo.exists():
        try:
            with open(artist_nfo, "w", encoding="utf-8") as f:
                f.write(f"<artist><name>{safe_artist}</name></artist>")
        except: pass

def create_content(source_path, target_folder, is_music=False):
    real_source = resolve_path(source_path)
    
    if os.path.isdir(real_source):
        if not target_folder.exists(): target_folder.mkdir(parents=True, exist_ok=True)
        for root, dirs, files in os.walk(real_source):
            rel_path = os.path.relpath(root, real_source)
            target_root = target_folder / rel_path
            if not target_root.exists(): target_root.mkdir(parents=True, exist_ok=True)
            
            for file in files:
                ext = os.path.splitext(file)[1].lower()
                src_file = os.path.join(root, file)
                
                # Media
                if ext in ['.mp3', '.flac', '.m4a', '.wav', '.ogg', '.mkv', '.mp4', '.avi', '.m4v', '.wmv', '.ts', '.mov', '.iso']:
                    if is_music:
                        # STRICT SYMLINK ONLY for Music
                        tgt_link = target_root / file
                        try:
                            if tgt_link.exists(): os.remove(tgt_link)
                            os.symlink(src_file, tgt_link)
                        except: pass # Skip if failed
                    else:
                        # Standard STRM for Movies/TV
                        tgt_strm = target_root / (os.path.splitext(file)[0] + ".strm")
                        try:
                            with open(tgt_strm, "w", encoding="utf-8") as f: f.write(src_file)
                        except: pass
                
                # Art
                elif ext in ['.jpg', '.jpeg', '.png', '.tbn', '.nfo']:
                    tgt_file = target_root / file
                    if not tgt_file.exists():
                        try: shutil.copy2(src_file, tgt_file)
                        except: pass
    else:
        tgt_file = target_folder / (os.path.basename(real_source) + ".strm")
        try:
            if not target_folder.exists(): target_folder.mkdir(parents=True, exist_ok=True)
            with open(tgt_file, "w", encoding="utf-8") as f: f.write(real_source)
        except: pass

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
    try: current_libs = session.get(f"{CONFIG['JELLYFIN_URL']}/Library/VirtualFolders", timeout=TIMEOUT).json()
    except: return
    ZERO_WIDTH = "\u200B"
    expected_bases = [meta["discovery_name"] for meta in lib_map.values()]
    to_delete = []
    for lib in current_libs:
        name = lib.get("Name", "")
        if ZERO_WIDTH in name and any(name.startswith(base) for base in expected_bases):
            to_delete.append(name)
    if not to_delete: return
    logging.info(f"[*] Found {len(to_delete)} libraries to cleanup. executing parallel delete...")
    def delete_single(name):
        try: session.delete(f"{CONFIG['JELLYFIN_URL']}/Library/VirtualFolders", params={"name": name, "refreshLibrary": "false"})
        except: pass
    with concurrent.futures.ThreadPoolExecutor(max_workers=CONFIG.get("MAX_THREADS", 4)) as executor:
        executor.map(delete_single, to_delete)

def optimize_library(library_name):
    try:
        all_libs = session.get(f"{CONFIG['JELLYFIN_URL']}/Library/VirtualFolders", timeout=TIMEOUT).json()
        target_lib = next((l for l in all_libs if l.get('Name') == library_name), None)
        if not target_lib: return
        lib_id = target_lib['ItemId']
        optimized_options = {
            "Id": lib_id,
            "LibraryOptions": {
                "EnableRealtimeMonitor": False,
                "EnableChapterImageExtraction": False,
                "EnableTrickplayImageExtraction": False,
                "ExtractChapterImagesDuringLibraryScan": False,
                "EnableIntroDetection": False,
                "SaveLocalMetadata": False,
                "EnableAutomaticSeriesGrouping": True,
                "EnableEmbeddedTitles": False
            }
        }
        session.post(f"{CONFIG['JELLYFIN_URL']}/Library/VirtualFolders/LibraryOptions", json=optimized_options, timeout=TIMEOUT)
    except Exception as e:
        logging.warning(f"    [WARN] Could not optimize library settings: {e}")

def force_refresh_library(library_id):
    try:
        url = f"{CONFIG['JELLYFIN_URL']}/Items/{library_id}/Refresh"
        params = {"Recursive": "true", "ImageRefreshMode": "Default", "MetadataRefreshMode": "Default"}
        session.post(url, params=params, timeout=TIMEOUT)
    except Exception as e:
        logging.warning(f"    [WARN] Failed to trigger scan: {e}")

def verify_results(path):
    try: return sum(1 for _ in Path(path).rglob('*') if _.suffix in ['.strm', '.mp3', '.flac', '.m4a', '.wav'])
    except: return 0

# --------------------------------------------------
# MAIN PROCESS
# --------------------------------------------------
def process_user(user, lib_map, index):
    u_name, u_id = user['Name'], user['Id']
    conn = sqlite3.connect(DB_FILE)
    prefs, has_history = analyze_user(user)
    logging.info(f"[*] Analyzing user: {u_name} (History: {has_history})")
    
    safe_name = truncate_path("".join(c for c in u_name if c.isalnum() or c in " -_").strip() or u_id)
    ZERO_WIDTH = "\u200B"
    invisible_suffix = ZERO_WIDTH * (index + 1)

    for cat, meta in lib_map.items():
        # STRICT MUSIC CHECK:
        if cat == "Music" and not CAN_SYMLINK:
            logging.warning("    [SKIP] Music generation disabled for this session (No Symlink Permissions).")
            continue

        MAX_RETRIES = 3
        count = 0
        out = Path(DATA_ROOT) / safe_name / cat
        
        for attempt in range(MAX_RETRIES):
            params = {
                "ParentIds": ",".join(meta["source_ids"]), 
                "IncludeItemTypes": meta["item_type"], 
                "Recursive": "true", 
                "Filters": "IsUnplayed", 
                "Fields": "Path,CommunityRating,Genres,People,CollectionName,UserData,AlbumArtist,Artists", 
                "Limit": 600
            }
            try: items = session.get(f"{CONFIG['JELLYFIN_URL']}/Users/{u_id}/Items", params=params, timeout=TIMEOUT).json().get("Items", [])
            except: items = []
            
            if not items:
                logging.warning(f"    [WARN] API returned 0 items for {cat}. (Check User Permissions)")
                time.sleep(0.5)
                continue

            scored = []
            for i in items:
                path = i.get("Path")
                s = score_item(i, prefs, CATEGORY_WEIGHTS.get(cat, CATEGORY_WEIGHTS["Movies"]), not has_history)
                if path and s >= meta["min_score"]:
                    i["_Score"] = s
                    scored.append(i)
            
            scored.sort(key=lambda x: x["_Score"], reverse=True)
            top = scored[:CONFIG.get("RECOMMENDATION_COUNT", 25)]

            if out.exists(): safe_delete(out)
            out.mkdir(parents=True, exist_ok=True)

            for i in top:
                clean_name = truncate_path("".join(c for c in i["Name"] if c.isalnum() or c in " -_"))
                
                if cat == "Music":
                    raw_artist = i.get("AlbumArtist") or (i.get("Artists") or ["Unknown"])[0]
                    clean_artist = truncate_path("".join(c for c in raw_artist if c.isalnum() or c in " -_"))
                    folder_path = out / clean_artist / clean_name
                    create_music_nfo(folder_path, clean_artist, clean_name)
                    create_content(i["Path"], folder_path, is_music=True)
                else:
                    folder_path = out / clean_name
                    create_content(i["Path"], folder_path, is_music=False)

            count = verify_results(out)
            if count > 0:
                logging.info(f"    [VERIFY] Success: {count} items created for {cat}")
                break
            else:
                logging.warning(f"    [WARN] {cat} is EMPTY (Attempt {attempt+1}/{MAX_RETRIES}).")
                time.sleep(0.2)

        if count == 0:
            logging.error(f"    [SKIP] Failed to generate content for {cat}")
            continue

        final_lib_name = f"{meta['discovery_name']}{invisible_suffix}"
        try:
            session.post(
                f"{CONFIG['JELLYFIN_URL']}/Library/VirtualFolders",
                params={
                    "name": final_lib_name,
                    "collectionType": meta["collection_type"],
                    "paths": [str(out)],
                    "refreshLibrary": "false"
                },
                json={}, timeout=TIMEOUT
            )
            
            all_libs = session.get(f"{CONFIG['JELLYFIN_URL']}/Library/VirtualFolders", timeout=TIMEOUT).json()
            target_lib = next((l for l in all_libs if l.get('Name') == final_lib_name), None)
            
            if target_lib:
                optimize_library(final_lib_name)
                time.sleep(1) # Small pause before scan
                force_refresh_library(target_lib['ItemId'])

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
                if f"{os.sep}{truncate_path(safe)}{os.sep}" in path:
                    user_private_map[user['Id']].append(lib['ItemId'])
                    break
        else:
            public_ids.append(lib['ItemId'])
    for user in users:
        p = user.get("Policy", {})
        p["EnableAllFolders"] = False
        p["EnabledFolders"] = public_ids + user_private_map.get(user['Id'], [])
        try: session.post(f"{CONFIG['JELLYFIN_URL']}/Users/{user['Id']}/Policy", json=p, timeout=TIMEOUT)
        except: pass

def run_task():
    print(">>> Starting Run Task", flush=True)
    send_notification("JellyDiscover", "Starting update...")
    startup_local_cleanup()
    init_db()
    
    update_drive_mappings()
    check_symlink_rights()

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
    except Exception as e: logging.error(f"[!] Fatal: {e}")

def main():
    if not acquire_lock():
        logging.info("[*] Service Locked. Waiting 1s before forcing...")
        time.sleep(1)
        run_task()
        sys.exit(0)
    try:
        if not CONFIG.get('DAEMON_MODE', False): run_task()
        else:
            run_str = CONFIG.get('RUN_TIME', "04:00")
            logging.info(f"[*] DAEMON ACTIVE: Scheduled for {run_str}")
            run_task()
            while True:
                now = datetime.now()
                th, tm = map(int, run_str.split(':'))
                tgt = now.replace(hour=th, minute=tm, second=0)
                if tgt <= now: tgt += timedelta(days=1)
                sec = (tgt - now).total_seconds()
                time.sleep(sec)
                run_task()
    except KeyboardInterrupt: pass

if __name__ == "__main__":
    main()