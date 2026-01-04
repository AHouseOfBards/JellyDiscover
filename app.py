import os
import sys
import json
import time
import subprocess
import threading
import webbrowser
import psutil
import logging
from flask import Flask, render_template, request, redirect, url_for, flash

# ==========================================
# 0. CRITICAL BOOTSTRAP LOGGING
# ==========================================
# This ensures we catch errors even if 'import utils' fails
LOG_DIR = os.path.join(os.environ.get('ProgramData', 'C:\\ProgramData'), 'JellyDiscover', 'logs')
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(LOG_DIR, "dashboard.log"),
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s'
)

try:
    import utils
except Exception as e:
    with open(os.path.join(LOG_DIR, "app_startup_crash.log"), "w") as f:
        import traceback
        f.write(traceback.format_exc())
    sys.exit(1)

# ==========================================
# 1. PATH SETUP (The 500 Error Fix)
# ==========================================
if getattr(sys, 'frozen', False):
    BASE_DIR = os.path.dirname(sys.executable)
else:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

template_dir = os.path.join(BASE_DIR, 'templates')
app = Flask(__name__, template_folder=template_dir)
app.secret_key = "jellydiscover_secure_key_v1"

# ==========================================
# 2. HELPER FUNCTIONS
# ==========================================

def get_service_status():
    """
    Checks for Engine AND Cleaner processes.
    Trusts the ACTUAL process list over files or service managers.
    """
    engine_alive = False
    cleaner_alive = False
    service_status = "Stopped"
    
    # 1. SCAN PROCESSES
    try:
        for proc in psutil.process_iter(['name', 'cmdline']):
            try:
                name = (proc.info['name'] or "").lower()
                cmd = (str(proc.info['cmdline']) or "").lower()
                
                # Check for Engine
                if "engine.exe" in name: engine_alive = True
                if "engine.py" in cmd and ("python" in name or "py" in name): engine_alive = True

                # Check for Cleaner
                if "cleaner.exe" in name: cleaner_alive = True
                if "cleaner.py" in cmd and ("python" in name or "py" in name): cleaner_alive = True
                
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass
    except Exception:
        pass

    # 2. PRIORITY RETURN (Cleaner takes precedence for UI Feedback)
    if cleaner_alive:
        return "Running (Cleaner Active)"
        
    if engine_alive:
        # Check if it's the Service (Just for labeling)
        if utils.IS_WINDOWS and not utils.IS_DOCKER:
            try:
                output = subprocess.check_output("sc query JellyDiscover", shell=True).decode().lower()
                if "state" in output and "running" in output:
                    service_status = "Service"
            except: pass
        return f"Running ({service_status if service_status != 'Stopped' else 'Manual'})"

    # 3. CLEAN GHOST FILES (Crash Recovery)
    if not engine_alive and os.path.exists(utils.STATUS_FILE):
        try:
            with open(utils.STATUS_FILE, "r") as f:
                data = json.load(f)
            
            if data.get("state") == "running":
                # Nuke the ghost file
                try: 
                    f.close()
                    os.remove(utils.STATUS_FILE) 
                except: pass
                return "Stopped (Crash Detected)"
        except: pass

    return "Stopped"

# ==========================================
# 3. ROUTES
# ==========================================

@app.route('/')
def index():
    try:
        config = utils.load_config()
        status = get_service_status() # Now checks cleaner too
        platform_info = utils.get_platform_info()
        last_run_status = utils.get_last_status()
        
        # RESTORED: Full Scoring Defaults Logic
        # Without this, a fresh install shows a blank configuration page
        if "SCORING" not in config:
            config["SCORING"] = {
                "DISCOVERY_BIAS": {
                    "Movies": {"genres": 1.0, "actors": 1.5, "directors": 2.5, "community": 2.0, "collection": 5.0, "seen_penalty": 10.0, "diversity": 1.2},
                    "Shows": {"genres": 1.5, "actors": 2.0, "directors": 1.0, "community": 1.5, "collection": 3.0, "seen_penalty": 6.0, "diversity": 1.0},
                    "Music": {"genres": 2.0, "actors": 0.0, "directors": 0.0, "community": 1.0, "collection": 2.0, "seen_penalty": 4.0, "diversity": 0.8}
                }
            }
        # Ensure deep structure exists if config is partial
        elif "DISCOVERY_BIAS" not in config["SCORING"]:
             config["SCORING"]["DISCOVERY_BIAS"] = {
                    "Movies": {"genres": 1.0, "actors": 1.5, "directors": 2.5, "community": 2.0, "collection": 5.0, "seen_penalty": 10.0, "diversity": 1.2},
                    "Shows": {"genres": 1.5, "actors": 2.0, "directors": 1.0, "community": 1.5, "collection": 3.0, "seen_penalty": 6.0, "diversity": 1.0},
                    "Music": {"genres": 2.0, "actors": 0.0, "directors": 0.0, "community": 1.0, "collection": 2.0, "seen_penalty": 4.0, "diversity": 0.8}
             }

        # Restore Path Substitutions Logic
        if "PATH_SUBSTITUTIONS" not in config:
            config["PATH_SUBSTITUTIONS"] = {}

        return render_template('index.html', 
                               config=config, 
                               status=status, 
                               info=platform_info,
                               last_run=last_run_status,
                               is_docker=utils.IS_DOCKER)
    except Exception as e:
        logging.error(f"Index Route Crash: {e}", exc_info=True)
        return f"Internal Server Error (Check C:\\ProgramData\\JellyDiscover\\logs\\dashboard.log): {e}", 500

@app.route('/save_config', methods=['POST'])
def save_config():
    try:
        current_conf = utils.load_config()
        form = request.form
        restart_needed = False
        
        # 1. General Settings
        current_conf['JELLYFIN_URL'] = form.get('jellyfin_url')
        current_conf['API_KEY'] = form.get('api_key')
        current_conf['RUN_TIME'] = form.get('run_time', '04:00')
        
        # New: Thread Count (with safe integer conversion)
        try: current_conf['MAX_THREADS'] = int(form.get('max_threads', 2))
        except: current_conf['MAX_THREADS'] = 2

        try: current_conf['SCHEDULE_FREQ'] = int(form.get('schedule_freq', 24))
        except: current_conf['SCHEDULE_FREQ'] = 24
        
        # 2. Port Change Check
        if not utils.IS_DOCKER:
            old_port = current_conf.get('DASHBOARD_PORT', 5000)
            try: new_port = int(form.get('dashboard_port', 5000))
            except: new_port = 5000
            
            current_conf['DASHBOARD_PORT'] = new_port
            if old_port != new_port: restart_needed = True
        
        # 3. Path Mappings
        if "PATH_SUBSTITUTIONS" not in current_conf: current_conf["PATH_SUBSTITUTIONS"] = {}
        
        path_to_remove = form.get('remove_path')
        if path_to_remove and path_to_remove in current_conf['PATH_SUBSTITUTIONS']:
            del current_conf['PATH_SUBSTITUTIONS'][path_to_remove]
            
        new_remote = form.get('new_remote_path')
        new_local = form.get('new_local_path')
        if new_remote and new_local:
            new_remote = new_remote.replace('\\', '/')
            new_local = new_local.replace('\\', '/')
            current_conf['PATH_SUBSTITUTIONS'][new_remote] = new_local

        # 4. Scoring Logic
        if "SCORING" not in current_conf: current_conf["SCORING"] = {"DISCOVERY_BIAS": {}}
        bias_map = current_conf["SCORING"]["DISCOVERY_BIAS"]
        
        for category in ["Movies", "Shows", "Music"]:
            if category not in bias_map: bias_map[category] = {}
            for factor in ["genres", "actors", "directors", "community", "collection", "seen_penalty", "diversity"]:
                input_key = f"{category}_{factor}"
                if input_key in form:
                    try: bias_map[category][factor] = float(form[input_key])
                    except ValueError: pass

        utils.save_config(current_conf)
        
        if restart_needed: flash("RESTART_REQUIRED") 
        else: flash("Configuration Saved Successfully!")
            
        return redirect(url_for('index'))
    except Exception as e:
        logging.error(f"Save Config Crash: {e}", exc_info=True)
        return f"Error Saving Config: {e}", 500

@app.route('/action', methods=['POST'])
def action():
    try:
        cmd = request.form.get('cmd')
        current_status = get_service_status()

        # LOCK CHECK: Prevent running if ANYTHING is active (Engine or Cleaner)
        # This prevents database corruption from two processes writing at once.
        if "Running" in current_status:
            flash(f"Error: {current_status}. Please wait for it to finish.")
            return redirect(url_for('index'))

        if cmd == "run_now":
            engine_exe = os.path.join(BASE_DIR, "engine.exe")
            engine_py = os.path.join(BASE_DIR, "engine.py")
            
            if os.path.exists(engine_exe):
                subprocess.Popen([engine_exe])
                flash("Discovery Engine (EXE) started.")
            elif os.path.exists(engine_py):
                subprocess.Popen([sys.executable, engine_py])
                flash("Discovery Engine (PY) started.")
            else:
                flash("Error: engine executable not found.")
                
        elif cmd == "clean":
            cleaner_exe = os.path.join(BASE_DIR, "cleaner.exe")
            cleaner_py = os.path.join(BASE_DIR, "cleaner.py")
            
            if os.path.exists(cleaner_exe):
                subprocess.Popen([cleaner_exe])
                flash("Cleanup Utility started.")
            elif os.path.exists(cleaner_py):
                subprocess.Popen([sys.executable, cleaner_py])
                flash("Cleanup Utility started.")
            else: flash("Error: cleaner not found.")

        elif cmd == "restart_service":
            if utils.IS_WINDOWS and not utils.IS_DOCKER:
                 # Windows NSSM Logic (Existing)
                 nssm_path = os.path.join(BASE_DIR, "nssm.exe")
                 if os.path.exists(nssm_path):
                     subprocess.run(f'"{nssm_path}" restart JellyDiscover', shell=True)
                 else:
                     subprocess.run("nssm restart JellyDiscover", shell=True)
                 flash("Service Restart Command Sent.")
                 
            elif utils.IS_DOCKER:
                 # Docker Logic (Existing)
                 sys.exit(1)
                 
            else: 
                 # NEW: Linux Systemd Logic
                 # Try to restart via systemctl (requires sudoers permission or root)
                 try:
                     subprocess.run(["sudo", "systemctl", "restart", "jellydiscover"], check=True)
                     flash("Service Restarted via Systemd.")
                 except:
                     flash("Could not auto-restart. Run 'sudo systemctl restart jellydiscover' manually.")

        return redirect(url_for('index'))
    except Exception as e:
        logging.error(f"Action Crash: {e}", exc_info=True)
        return redirect(url_for('index'))

@app.route('/logs')
def view_logs():
    # Define both log paths
    engine_log = os.path.join(utils.LOG_DIR, "JellyDiscover.log")
    cleaner_log = os.path.join(utils.LOG_DIR, "cleaner.log")
    
    target_log = engine_log
    log_name = "Engine Log"

    # LOGIC: If Cleaner is running OR cleaner log is newer, show that instead.
    status = get_service_status()
    if "Cleaner" in status:
        target_log = cleaner_log
        log_name = "Cleaner Log"
    elif os.path.exists(cleaner_log) and os.path.exists(engine_log):
        # If cleaner ran more recently than engine, show cleaner
        if os.path.getmtime(cleaner_log) > os.path.getmtime(engine_log):
            target_log = cleaner_log
            log_name = "Cleaner Log"
            
    content = ""
    try:
        if os.path.exists(target_log):
            with open(target_log, "r", encoding="utf-8", errors="ignore") as f:
                lines = f.readlines()[-100:] 
                content = "".join(lines)
        else:
            content = f"Log file ({log_name}) not found."
    except Exception as e:
        content = f"Error reading logs: {e}"
        
    return f"""
    <html>
        <head>
            <title>JellyDiscover Logs</title>
            <meta http-equiv="refresh" content="5"> 
        </head>
        <body style="background:#121212; color:#e0e0e0; font-family:monospace; padding:20px;">
            <h2 style="border-bottom:1px solid #333; padding-bottom:10px; display:flex; justify-content:space-between;">
                <span>Live Log Viewer: <span style="color:#a964da">{log_name}</span></span>
                <span style="font-size:0.6em; opacity:0.7">Auto-refreshing (5s)</span>
            </h2>
            <pre style="white-space: pre-wrap; font-size: 13px;">{content}</pre>
            <script>window.scrollTo(0, document.body.scrollHeight);</script>
        </body>
    </html>
    """

def open_browser():
    try:
        cfg = utils.load_config()
        port = cfg.get("DASHBOARD_PORT", 5000)
        time.sleep(1.5)
        webbrowser.open(f"http://localhost:{port}")
    except: pass

if __name__ == '__main__':
    try:
        cfg = utils.load_config()
        port = cfg.get("DASHBOARD_PORT", 5000)
        
        if not utils.IS_DOCKER:
            threading.Thread(target=open_browser).start()
            
        logging.info(f"Starting Dashboard on port {port}")
        app.run(host='0.0.0.0', port=port, debug=False)
    except Exception as e:
        logging.critical(f"Main Loop Crash: {e}", exc_info=True)