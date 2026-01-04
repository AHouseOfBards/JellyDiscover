import os
import sys
import json
import platform
import datetime
import glob
import shutil

# ==========================================
# 1. CORE PATH & PLATFORM LOGIC
# ==========================================

IS_WINDOWS = platform.system() == "Windows"
IS_DOCKER = os.path.exists('/.dockerenv') or os.environ.get('IS_DOCKER', '').lower() == 'true'

if getattr(sys, 'frozen', False):
    BASE_DIR = os.path.dirname(sys.executable)
else:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# PLATFORM PATH SELECTION
if IS_WINDOWS:
    # Windows: Force ProgramData to avoid PermissionErrors in Program Files
    DATA_DIR = os.path.join(os.environ.get('ProgramData', 'C:\\ProgramData'), 'JellyDiscover')
elif IS_DOCKER:
    # Docker: Check for standard /config volume first, fallback to /app/data
    if os.path.exists('/config'):
        DATA_DIR = '/config'
    else:
        DATA_DIR = os.path.join(BASE_DIR, 'data')
else:
    # Linux Native / Dev: Use local data folder
    DATA_DIR = os.path.join(BASE_DIR, 'data')

CONFIG_PATH = os.path.join(DATA_DIR, 'config.json')
LIBRARIES_PATH = os.path.join(DATA_DIR, 'libraries.json')
LOG_DIR = os.path.join(DATA_DIR, 'logs')
STATUS_FILE = os.path.join(DATA_DIR, 'status.json')

# Ensure directories exist immediately
try:
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(LOG_DIR, exist_ok=True)
except Exception as e:
    print(f"CRITICAL: Failed to create directories: {e}")

# ==========================================
# 2. CONFIGURATION MANAGEMENT
# ==========================================

def load_config():
    default_config = {
        "JELLYFIN_URL": "http://localhost:8096",
        "API_KEY": "",
        "MAX_THREADS": 2,
        "RUN_TIME": "04:00",
        "SCHEDULE_FREQ": 24,
        "DASHBOARD_PORT": 5000,
        "PATH_SUBSTITUTIONS": {},
        "USE_NETWORK_DRIVE": False,
        "SCORING": {
            "DISCOVERY_BIAS": {
                "Movies": {"genres": 1.0, "actors": 1.5, "directors": 2.5, "community": 2.0, "collection": 5.0, "seen_penalty": 10.0, "diversity": 1.2},
                "Shows": {"genres": 1.5, "actors": 2.0, "directors": 1.0, "community": 1.5, "collection": 3.0, "seen_penalty": 6.0, "diversity": 1.0},
                "Music": {"genres": 2.0, "actors": 0.0, "directors": 0.0, "community": 1.0, "collection": 2.0, "seen_penalty": 4.0, "diversity": 0.8}
            }
        }
    }

    if not os.path.exists(CONFIG_PATH):
        try:
            save_config(default_config)
        except: pass
        return default_config

    try:
        with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
            content = f.read().strip()
            if not content: return default_config
            user_config = json.loads(content)
            
            # Merge defaults for any missing keys
            for key, val in default_config.items():
                if key not in user_config:
                    user_config[key] = val
            return user_config
    except Exception:
        return default_config

def save_config(config_data):
    try:
        with open(CONFIG_PATH, 'w', encoding='utf-8') as f:
            json.dump(config_data, f, indent=4)
        return True
    except Exception as e:
        print(f"Error saving config: {e}")
        return False

def load_libraries():
    """Loads the libraries.json file safely."""
    if not os.path.exists(LIBRARIES_PATH):
        return {}
    try:
        with open(LIBRARIES_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"[!] Error loading libraries.json: {e}")
        return {}

# ==========================================
# 3. DASHBOARD HELPERS
# ==========================================

def get_platform_info():
    return {
        "os": f"{platform.system()} {platform.release()}",
        "python": platform.python_version(),
        "is_docker": IS_DOCKER,
        "base_dir": BASE_DIR,
        "logs_dir": LOG_DIR
    }

def get_last_status():
    """
    Reads the status.json file for critical errors, 
    falling back to log parsing if no status file exists.
    """
    # 1. Check for explicit fatal error status first
    if os.path.exists(STATUS_FILE):
        try:
            with open(STATUS_FILE, 'r', encoding='utf-8') as f:
                status = json.load(f)
                if status.get("state") == "fatal":
                    return {
                        "success": False, 
                        "last_run": status.get("timestamp", "Unknown"), 
                        "errors": [status.get("message", "Unknown Fatal Error")], 
                        "log_path": ""
                    }
        except: pass

    # 2. Fallback to existing log parsing logic
    try:
        if not os.path.exists(LOG_DIR):
             return {"success": True, "last_run": "Never", "errors": [], "log_path": ""}

        list_of_files = glob.glob(os.path.join(LOG_DIR, '*.log'))
        if not list_of_files:
            return {"success": True, "last_run": "Never", "errors": [], "log_path": ""}

        latest_file = max(list_of_files, key=os.path.getctime)
        status = {"success": True, "last_run": "Unknown", "errors": [], "log_path": os.path.basename(latest_file)}

        try:
            mod_time = os.path.getmtime(latest_file)
            dt = datetime.datetime.fromtimestamp(mod_time)
            status["last_run"] = dt.strftime("%Y-%m-%d %H:%M:%S")
        except: pass

        with open(latest_file, 'r', encoding='utf-8', errors='ignore') as f:
            lines = f.readlines()
            for line in lines:
                if "ERROR" in line or "CRITICAL" in line or "Traceback" in line:
                    status["success"] = False
                    clean_err = line.split("ERROR")[-1].strip() if "ERROR" in line else line.strip()
                    if clean_err not in status["errors"]:
                        status["errors"].append(clean_err)

        status["errors"] = status["errors"][:3] 
        return status
    except Exception as e:
        return {"success": False, "last_run": "Error reading logs", "errors": [str(e)], "log_path": ""}