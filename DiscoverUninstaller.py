import os
import sys
import json
import shutil
import requests

if getattr(sys, 'frozen', False):
    BASE_DIR = os.path.dirname(sys.executable)
else:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def load_json(filename):
    path = os.path.join(BASE_DIR, filename)
    try:
        with open(path, 'r') as f: return json.load(f)
    except Exception as e: sys.exit(1)

CONFIG = load_json('config.json')
LIBS = load_json('libraries.json')
SESSION = requests.Session()
SESSION.headers.update({"X-Emby-Token": CONFIG['API_KEY'], "Content-Type": "application/json"})

def delete_jellyfin_libraries():
    print("\n[1/3] Removing Libraries from Jellyfin...")
    try:
        res = SESSION.get(f"{CONFIG['JELLYFIN_URL']}/Library/VirtualFolders")
        if res.status_code != 200: return
        all_libs = res.json()
        target_names = [meta['discovery_name'] for meta in LIBS['CATEGORIES'].values()]
        to_delete = []
        for lib in all_libs:
            for t_name in target_names:
                if lib['Name'].startswith(t_name):
                    to_delete.append(lib)
                    break
        
        for lib in to_delete:
            print(f"    [-] Deleting: {lib['Name']}...")
            del_res = SESSION.post(f"{CONFIG['JELLYFIN_URL']}/Library/VirtualFolders/Delete", params={'name': lib['Name']})
            if del_res.status_code not in [200, 204]:
                 SESSION.delete(f"{CONFIG['JELLYFIN_URL']}/Items/{lib['ItemId']}")
    except Exception as e: print(f"    [!] Error: {e}")

def reset_user_permissions():
    print("\n[?] Do you want to UNLOCK all libraries for all users?")
    choice = input("    Type 'YES' to unlock, or press Enter to skip: ")
    if choice.strip().upper() == "YES":
        try:
            users = SESSION.get(f"{CONFIG['JELLYFIN_URL']}/Users").json()
            for user in users:
                policy = user.get("Policy", {})
                policy["EnableAllFolders"] = True
                policy["EnabledFolders"] = []
                SESSION.post(f"{CONFIG['JELLYFIN_URL']}/Users/{user['Id']}/Policy", json=policy)
                print(f"    [+] Unlocked: {user['Name']}")
        except Exception: pass

def clean_disk_files():
    print("\n[2/3] Cleaning Disk Files...")
    data_path = os.path.join(BASE_DIR, "JellyDiscover_Data")
    if os.path.exists(data_path): shutil.rmtree(data_path)
    db_path = os.path.join(BASE_DIR, "jelly_data.db")
    if os.path.exists(db_path): os.remove(db_path)

def main():
    print("!!! DISCOVER UNINSTALLER !!!")
    if input("Type 'UNINSTALL' to continue: ") == "UNINSTALL":
        delete_jellyfin_libraries()
        clean_disk_files()
        reset_user_permissions()
        print("\n[OK] Uninstallation Complete.")
    else: print("Cancelled.")

if __name__ == "__main__":
    main()