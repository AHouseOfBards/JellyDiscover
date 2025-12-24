import json
import requests
import sys
import os

if getattr(sys, 'frozen', False):
    BASE_DIR = os.path.dirname(sys.executable)
else:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def load_json(filename):
    try:
        with open(os.path.join(BASE_DIR, filename), 'r') as f: return json.load(f)
    except: sys.exit(1)

CONFIG = load_json('config.json')
session = requests.Session()
session.headers.update({"X-Emby-Token": CONFIG['API_KEY'], "Content-Type": "application/json"})

def clean_stale_libraries():
    print("[*] Connecting to Jellyfin...")
    try:
        libs = session.get(f"{CONFIG['JELLYFIN_URL']}/Library/VirtualFolders").json()
    except: return

    targets = [lib for lib in libs if lib['Name'].startswith("Recommended")]

    if not targets:
        print("[!] No 'Recommended' libraries found.")
        return

    print(f"\n[!] Found {len(targets)} 'Recommended' libraries.")
    if input(f"Type 'DELETE' to destroy them: ").strip() == "DELETE":
        for i, lib in enumerate(targets):
            item_id = lib['ItemId']
            trash_name = f"TRASH_{i:03d}"
            print(f"    [{i+1}/{len(targets)}] Processing: {lib['Name']}...")
            
            # Rename via Item ID
            rename_res = session.post(f"{CONFIG['JELLYFIN_URL']}/Items/{item_id}", json={"Name": trash_name, "Id": item_id})
            target_name = trash_name if rename_res.status_code == 200 else lib['Name']

            # Delete
            del_res = session.post(f"{CONFIG['JELLYFIN_URL']}/Library/VirtualFolders/Delete", params={'name': target_name, 'refreshLibrary': 'true'})
            if del_res.status_code not in [200, 204]:
                session.delete(f"{CONFIG['JELLYFIN_URL']}/Items/{item_id}")

        print("\n[!] Cleanup Complete.")

if __name__ == "__main__":
    clean_stale_libraries()