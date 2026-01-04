import os
import sys
import shutil
import requests
import time
import logging
import socket
import concurrent.futures
import subprocess
import platform
from logging.handlers import RotatingFileHandler
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Import Shared Brain
import utils

# --- CONFIG & LOGGING ---
LOG_FILE = os.path.join(utils.LOG_DIR, "cleaner.log")
log_handlers = [logging.StreamHandler(sys.stdout)]

try:
    rfh = RotatingFileHandler(LOG_FILE, maxBytes=5*1024*1024, backupCount=3, encoding='utf-8')
    log_handlers.append(rfh)
except Exception as e:
    print(f"Warning: Could not set up file logging: {e}")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=log_handlers
)

CONFIG = utils.load_config()

# TIMEOUT: 300s (5 Minutes) to handle massive database locks
TIMEOUT = 300 

HEADERS = {
    "X-Emby-Token": CONFIG.get("API_KEY", ""),
    "Content-Type": "application/json"
}

# --- NOTIFICATION SYSTEM (OS AGNOSTIC) ---
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

# --- SESSION SETUP (With Retries) ---
def get_session():
    """Creates a session with retry logic for connection stability."""
    s = requests.Session()
    retries = Retry(total=3, backoff_factor=2, status_forcelist=[500, 502, 503, 504])
    s.mount('http://', HTTPAdapter(max_retries=retries))
    s.headers.update(HEADERS)
    return s

session = get_session()

# --- LOCKING MECHANISM ---
_lock_socket = None

def acquire_lock():
    """Prevents Cleaner from running if Engine is active."""
    global _lock_socket
    _lock_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        _lock_socket.bind(('127.0.0.1', 64001))
        return True
    except socket.error:
        return False

# ==========================================
# WORKER FUNCTIONS (For Threading)
# ==========================================

def delete_library_worker(name):
    """Worker: Deletes a single Library Config."""
    try:
        # refreshLibrary=true triggers a DB event on the server
        url = f"{CONFIG.get('JELLYFIN_URL')}/Library/VirtualFolders?name={name}&refreshLibrary=true"
        logging.info(f"      [BUSY] Deleting Config: '{name}'")
        
        # DELETE Request
        res = session.delete(url, timeout=TIMEOUT)
        
        if res.status_code in [200, 204]:
            logging.info(f"      [DONE] Deleted Config: '{name}'")
        else:
            logging.warning(f"      [FAIL] Could not delete '{name}': {res.status_code}")

    except requests.exceptions.ReadTimeout:
        logging.error(f"      [TIMEOUT] Server took too long to delete '{name}'. It might still be processing in the background.")
    except Exception as e:
        logging.error(f"      [ERR] Error deleting '{name}': {e}")

def delete_item_worker(item_data):
    """Worker: Deletes a single Database Item."""
    name, item_id = item_data
    try:
        url = f"{CONFIG.get('JELLYFIN_URL')}/Items/{item_id}"
        logging.info(f"      [BUSY] Nuking DB Item: '{name}'")
        
        # DELETE Request
        res = session.delete(url, timeout=TIMEOUT)
        
        if res.status_code in [200, 204]:
            logging.info(f"      [DONE] Nuked Item: '{name}'")
        else:
            logging.warning(f"      [FAIL] Could not nuke '{name}': {res.status_code}")

    except requests.exceptions.ReadTimeout:
        logging.error(f"      [TIMEOUT] Server took too long to nuke '{name}'. Skipping to prevent lock-up.")
    except Exception as e:
        logging.error(f"      [ERR] Error nuking '{name}': {e}")

def prune_policy_worker(user, real_ids):
    """Worker: Syncs a single user's policy."""
    try:
        u_res = session.get(f"{CONFIG.get('JELLYFIN_URL')}/Users/{user['Id']}", timeout=TIMEOUT)
        if u_res.status_code != 200: return
        
        full_user = u_res.json()
        policy = full_user.get("Policy", {})
        enabled_folders = policy.get("EnabledFolders", [])
        
        # Filter: Keep only IDs that exist in the real world
        clean_folders = [fid for fid in enabled_folders if fid in real_ids]
        
        if len(clean_folders) < len(enabled_folders):
            diff = len(enabled_folders) - len(clean_folders)
            policy["EnabledFolders"] = clean_folders
            session.post(f"{CONFIG.get('JELLYFIN_URL')}/Users/{user['Id']}/Policy", 
                         json=policy, timeout=TIMEOUT)
            logging.info(f"      [DONE] Cleaned {diff} ghosts for user: {user['Name']}")
    except Exception as e:
        logging.error(f"      [ERR] Error pruning user {user.get('Name')}: {e}")

# ==========================================
# MAIN STAGES
# ==========================================

def remove_active_libraries():
    """Stage 1: Concurrent deletion of Library Configs."""
    logging.info("[1/4] Scanning for active Library Configurations...")
    if not CONFIG.get("JELLYFIN_URL") or not CONFIG.get("API_KEY"): return

    try:
        res = session.get(f"{CONFIG.get('JELLYFIN_URL')}/Library/VirtualFolders", timeout=TIMEOUT)
        if res.status_code != 200: return

        libraries = res.json()
        to_delete = []
        KEYWORDS = ["Discover Movies", "Discover Shows", "Discover Music", "Recommended"]
        
        for lib in libraries:
            name = lib.get("Name", "")
            if any(k in name for k in KEYWORDS):
                to_delete.append(name)

        if not to_delete:
            logging.info("      - No active configurations found.")
            return

        thread_count = CONFIG.get("MAX_THREADS", 2)
        logging.info(f"      - Found {len(to_delete)} configs. Deleting with {thread_count} threads...")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=thread_count) as executor:
            executor.map(delete_library_worker, to_delete)

    except Exception as e:
        logging.error(f"[!] Stage 1 Failed: {e}")

def remove_database_garbage():
    """Stage 2: Concurrent deletion of Orphaned Database Items."""
    logging.info("[2/4] Scanning Database for Garbage Items...")
    try:
        url = f"{CONFIG.get('JELLYFIN_URL')}/Items?Recursive=true&IncludeItemTypes=CollectionFolder,UserView&Fields=Id,Name"
        res = session.get(url, timeout=TIMEOUT)
        if res.status_code != 200: return
        
        items = res.json().get('Items', [])
        KEYWORDS = ["Discover Movies", "Discover Shows", "Discover Music", "Recommended"]
        
        to_nuke = []
        for item in items:
            name = item.get("Name", "")
            item_id = item.get("Id")
            if any(k in name for k in KEYWORDS):
                to_nuke.append((name, item_id))
        
        if not to_nuke: 
            logging.info("      - No garbage items found.")
            return

        thread_count = CONFIG.get("MAX_THREADS", 2)
        logging.info(f"      - Found {len(to_nuke)} garbage items. Nuking with {thread_count} threads...")

        with concurrent.futures.ThreadPoolExecutor(max_workers=thread_count) as executor:
            executor.map(delete_item_worker, to_nuke)
            
    except Exception as e:
        logging.error(f"[!] Stage 2 Failed: {e}")

def clean_local_files():
    """Stage 3: Disk Cleanup (Fast, so sync is fine)."""
    logging.info("[3/4] Cleaning local disk...")
    files_to_remove = [
        os.path.join(utils.DATA_DIR, "jelly_data.db"),
        os.path.join(utils.DATA_DIR, "library_cache.json"),
        os.path.join(utils.DATA_DIR, "drive_map.json"),
        os.path.join(utils.DATA_DIR, ".installed"),
        utils.STATUS_FILE
    ]
    for file_path in files_to_remove:
        if os.path.exists(file_path):
            try: os.remove(file_path)
            except: pass

    if os.path.exists(utils.DATA_DIR):
        for item in os.listdir(utils.DATA_DIR):
            item_path = os.path.join(utils.DATA_DIR, item)
            if item.lower() in ["config.json", "libraries.json", "logs", "jellydiscover.log", "cleaner.log"]: continue
            if os.path.isdir(item_path):
                try: shutil.rmtree(item_path)
                except: pass

def prune_ghost_policies():
    """Stage 4: Concurrent Audit of User Policies."""
    logging.info("[4/4] Auditing User Policies...")
    
    real_ids = []
    try:
        res = session.get(f"{CONFIG.get('JELLYFIN_URL')}/Library/VirtualFolders", timeout=TIMEOUT)
        if res.status_code == 200:
            real_ids = [lib.get("ItemId") for lib in res.json()]
    except:
        logging.warning("[!] Could not fetch library list. Skipping audit to be safe.")
        return

    try:
        users = session.get(f"{CONFIG.get('JELLYFIN_URL')}/Users", timeout=TIMEOUT).json()
        thread_count = CONFIG.get("MAX_THREADS", 2)
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=thread_count) as executor:
            futures = [executor.submit(prune_policy_worker, user, real_ids) for user in users]
            concurrent.futures.wait(futures)
                
    except Exception as e:
        logging.error(f"[!] Stage 4 Failed: {e}")

def main():
    if not acquire_lock():
        logging.error("CRITICAL: Cannot start Cleaner. Engine is running.")
        sys.exit(1)

    # NOTIFY START
    send_notification("JellyDiscover", "Cleanup Utility Started")
    
    logging.info(">>> STARTING CONCURRENT OMNIBUS CLEANER (TIMEOUT: 300s)")
    
    remove_active_libraries()    # 1. Configs
    remove_database_garbage()    # 2. Database Items
    clean_local_files()          # 3. Disk
    prune_ghost_policies()       # 4. User Profiles
    
    # NOTIFY END
    send_notification("JellyDiscover", "Cleanup Complete")
    logging.info(">>> CLEANUP COMPLETE")

if __name__ == "__main__":
    main()