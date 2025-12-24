import os
import sys
import json
import subprocess
import requests
import sqlite3
import collections
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import concurrent.futures
import shutil
from pathlib import Path

# --- CONFIG & MAPPING ---
UI_MAP = {
    "Movies": {"api_type": "movies", "item_type": "Movie"},
    "4K":     {"api_type": "movies", "item_type": "Movie"},
    "Docs":   {"api_type": "movies", "item_type": "Movie"},
    "Shows":  {"api_type": "tvshows", "item_type": "Series"},
    "Music":  {"api_type": "music", "item_type": "MusicAlbum"}
}

# --- DYNAMIC PATH SETUP ---
if getattr(sys, 'frozen', False):
    BASE_DIR = os.path.dirname(sys.executable)
else:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DATA_ROOT = os.path.join(BASE_DIR, "JellyDiscover_Data")

def load_json(filename):
    path = os.path.join(BASE_DIR, filename)
    try:
        with open(path, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"[!] Error loading {filename}: {e}")
        sys.exit(1)

CONFIG = load_json('config.json')
LIBS = load_json('libraries.json')

# --- DB SETUP ---
DB_FILE = os.path.join(BASE_DIR, "jelly_data.db")

def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS user_tastes 
                 (user_id TEXT PRIMARY KEY, genre_weights TEXT, last_updated TEXT)''')
    conn.commit()
    return conn

# --- CONNECTION ---
session = requests.Session()
retry_strategy = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("http://", adapter)
session.mount("https://", adapter)
session.headers.update({"X-Emby-Token": CONFIG['API_KEY'], "Content-Type": "application/json"})
TIMEOUT = 120 

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
                    if drive and unc:
                        drive_map[drive.upper()] = unc
        except Exception:
            pass 
    return drive_map

DRIVE_MAP = get_drive_map()

# --- ENGINE ---
def analyze_user_taste(user_id, user_name, conn):
    print(f"    > Analyzing taste for {user_name}...", flush=True)
    params = {"IncludeItemTypes": "Movie,Series", "Recursive": "true", "Filters": "IsPlayed", "Fields": "Genres", "Limit": 2000}
    try:
        res = session.get(f"{CONFIG['JELLYFIN_URL']}/Users/{user_id}/Items", params=params, timeout=TIMEOUT)
        items = res.json().get("Items", [])
    except Exception: return {}

    if not items: return {}

    genre_counts = collections.Counter()
    for i in items:
        for g in i.get("Genres", []):
            genre_counts[g] += 1
    
    if not genre_counts: return {}

    most_common_count = genre_counts.most_common(1)[0][1]
    weights = {}
    for genre, count in genre_counts.items():
        weights[genre] = count / most_common_count
        
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO user_tastes VALUES (?, ?, ?)", (user_id, json.dumps(weights), datetime.now().isoformat()))
    conn.commit()
    return weights

def get_library_mapping():
    try:
        response = session.get(f"{CONFIG['JELLYFIN_URL']}/Library/VirtualFolders", timeout=TIMEOUT)
        all_libs = response.json()
    except Exception as e:
        print(f"[!] Connection Failed: {e}")
        sys.exit(1)
    
    active_map = {}
    for cat, settings in LIBS['CATEGORIES'].items():
        if not settings['enabled']: continue
        if cat not in UI_MAP: continue 

        internal_type = UI_MAP[cat]["api_type"]
        target_names = settings.get('source_names', [])
        
        if target_names:
            matched_ids = [l["ItemId"] for l in all_libs if l["CollectionType"] == internal_type and l["Name"] in target_names]
        else:
            matched_ids = [l["ItemId"] for l in all_libs if l["CollectionType"] == internal_type]

        if matched_ids:
            active_map[cat] = {
                "source_ids": matched_ids,
                "item_type": UI_MAP[cat]["item_type"],
                "discovery_name": settings['discovery_name'],
                "min_score": settings['min_community_score'],
                "collection_type": internal_type
            }
    return active_map

def create_symlink(source, target):
    source_upper = source.upper()
    for drive, unc in DRIVE_MAP.items():
        if source_upper.startswith(drive):
            source = source.replace(source[:2], unc)
            break
            
    if not os.path.isdir(source) and "." in source:
        ext = os.path.splitext(source)[1]
        if not str(target).endswith(ext):
            target = Path(str(target) + ext)

    try:
        if CONFIG['OS_TYPE'] == "windows":
            flag = "/D" if os.path.isdir(source) else ""
            subprocess.run(f'mklink {flag} "{target}" "{source}"', shell=True, capture_output=True, timeout=5)
        else:
            os.symlink(source, target, target_is_directory=os.path.isdir(source))
    except Exception:
        pass

def process_user(user, lib_map, index):
    u_name, u_id = user['Name'], user['Id']
    conn = sqlite3.connect(DB_FILE)
    weights = analyze_user_taste(u_id, u_name, conn)
    
    safe_fs_name = "".join(c for c in u_name if c.isalnum() or c in " -_").strip()
    if not safe_fs_name: safe_fs_name = u_id
    invisible_suffix = "\u3164" * (index + 1)

    for cat_name, meta in lib_map.items():
        params = {"ParentIds": ",".join(meta["source_ids"]), "IncludeItemTypes": meta["item_type"], "Recursive": "true", "Fields": "Path,CommunityRating,Genres", "Limit": 500}
        
        items = session.get(f"{CONFIG['JELLYFIN_URL']}/Users/{u_id}/Items", params=params, timeout=TIMEOUT).json().get("Items", [])
        scored = []
        bias_strength = CONFIG.get("BIAS_STRENGTH", 0.0)

        for i in items:
            path = i.get("Path")
            base_score = i.get("CommunityRating", 0)
            
            bonus = 0.0
            if weights and bias_strength > 0:
                item_genres = i.get("Genres", [])
                for g in item_genres:
                    if g in weights: bonus += weights[g] * bias_strength
                if len(item_genres) > 0: bonus = bonus / len(item_genres)

            final_score = base_score + bonus

            if not path or final_score < meta["min_score"]: continue
            i['_FinalScore'] = final_score
            scored.append(i)
        
        scored.sort(key=lambda x: x['_FinalScore'], reverse=True)
        top_recs = scored[:CONFIG['RECOMMENDATION_COUNT']]

        user_cat_path = Path(DATA_ROOT) / safe_fs_name / cat_name
        
        if user_cat_path.exists(): shutil.rmtree(user_cat_path)
        user_cat_path.mkdir(parents=True, exist_ok=True)

        for item in top_recs:
            clean_name = "".join(c for c in item['Name'] if c.isalnum() or c in " -_")
            create_symlink(item['Path'], user_cat_path / clean_name)

        clean_display_name = f"{meta['discovery_name']}{invisible_suffix}"
        v_folders = session.get(f"{CONFIG['JELLYFIN_URL']}/Library/VirtualFolders", timeout=TIMEOUT).json()
        
        if not any(f["Name"] == clean_display_name for f in v_folders):
            api_params = {"name": clean_display_name, "collectionType": meta['collection_type'], "paths": [str(user_cat_path)], "refreshLibrary": "true"}
            session.post(f"{CONFIG['JELLYFIN_URL']}/Library/VirtualFolders", params=api_params, json={}, timeout=TIMEOUT)
            
    conn.close()
    return u_name

def apply_strict_privacy():
    print("\n[*] Applying Privacy Shield...")
    users = session.get(f"{CONFIG['JELLYFIN_URL']}/Users", timeout=TIMEOUT).json()
    libs = session.get(f"{CONFIG['JELLYFIN_URL']}/Library/VirtualFolders", timeout=TIMEOUT).json()
    public_ids = []
    user_private_map = {u['Id']: [] for u in users}
    discovery_root_name = os.path.basename(DATA_ROOT) 

    for lib in libs:
        locs = lib.get('Locations', [])
        if not locs: 
            public_ids.append(lib['ItemId'])
            continue
        path = locs[0]
        
        if discovery_root_name in path:
            matched_owner = None
            for user in users:
                u_name = user['Name']
                safe_fs_name = "".join(c for c in u_name if c.isalnum() or c in " -_").strip()
                if not safe_fs_name: safe_fs_name = user['Id']
                if f"{os.sep}{safe_fs_name}{os.sep}" in path:
                    matched_owner = user['Id']
                    break
            if matched_owner: user_private_map[matched_owner].append(lib['ItemId'])
        else:
            public_ids.append(lib['ItemId'])

    for user in users:
        u_id = user['Id']
        u_name = user['Name']
        policy = user.get("Policy", {})
        allowed = public_ids.copy()
        if u_id in user_private_map: allowed.extend(user_private_map[u_id])
        policy["EnableAllFolders"] = False
        policy["EnabledFolders"] = allowed
        session.post(f"{CONFIG['JELLYFIN_URL']}/Users/{u_id}/Policy", json=policy, timeout=TIMEOUT)
        print(f"    [SECURE] {u_name}", flush=True)

def main():
    if not os.path.exists(DATA_ROOT): os.makedirs(DATA_ROOT)
    init_db() 
    lib_map = get_library_mapping()
    users = session.get(f"{CONFIG['JELLYFIN_URL']}/Users", timeout=TIMEOUT).json()
    print(f"[*] Starting AI Recommendation Engine...")
    print(f"[*] Data Location: {DATA_ROOT}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=CONFIG['MAX_THREADS']) as executor:
        futures = {}
        for idx, user in enumerate(users):
            futures[executor.submit(process_user, user, lib_map, idx)] = user

        for future in concurrent.futures.as_completed(futures):
            try: print(f"[+] Finished: {future.result()}", flush=True)
            except Exception as e: print(f"[!] ERROR: {e}", flush=True)

    apply_strict_privacy()
    print("[!] Run Complete.")

if __name__ == "__main__":
    main()