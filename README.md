# JellyDiscover
**Automated, Personalized Recommendations for Jellyfin.**

JellyDiscover is a standalone recommendation engine that analyzes watch history to create tailored "Recommended" libraries for every user on your server. It runs in the background, calculates scores based on Genres, Actors, and Directors, and seamlessly integrates these lists back into Jellyfin.

## Features
* **Personalized:** Analyzes user history to recommend unwatched content they will actually like.
* **Zero-Copy:** Uses `.strm` files and Symlinks to link to your media. No extra storage space is consumed.
* **Self-Healing:** Automatically refreshes libraries and cleans up stale entries to keep Jellyfin in sync.
* **Universal:** Works on Windows, Linux, and Docker.
* **Dashboard:** A web-based GUI to manage settings, schedules, and path mappings.
<img width="2256" height="1072" alt="JDP1" src="https://github.com/user-attachments/assets/07fb779c-cd15-4d3c-871e-cc19f91590f9" />
<img width="2227" height="852" alt="JDP2" src="https://github.com/user-attachments/assets/a6aa4695-65ef-4ab5-bdf6-242757af8f01" />
<img width="2254" height="1058" alt="JDP3" src="https://github.com/user-attachments/assets/1d0fbfb1-5ac3-4fd5-9ce6-c99c585c5dba" />

---

## 📦 Installation

### Option 1: Windows (Installer)
**Best for:** Standard Windows users.
1.  Download the latest `JellyDiscover_Setup_v1.1.0.exe` from [Releases].
2.  Run the installer.
3.  **Drive Configuration Step:**
    * **Local Drives:** Select this if your media is on internal drives (`C:\`, `D:\`).
    * **Network Drives:** Select this if your media is on a NAS (`Z:\`, `\\NAS\Share`).
    * *Note:* If you select Network Drives, you will be prompted to enter your Windows Username and Password. This allows the background service to access your network shares securely.
4.  Once installed, open `http://localhost:5000` to finish the setup.

### Option 2: Docker (Compose)
**Best for:** Unraid, Synology, TrueNAS Scale.
1.  Download `JellyDiscover_Docker_v1.1.0.zip` from [Releases].
2.  Extract it to a folder on your server.
3.  Open `docker-compose.yml` and **edit the volume mounts** to match your media folders:
    ```yaml
    volumes:
      - ./data:/app/data
      # UPDATE THESE LINES TO MATCH YOUR FOLDERS:
      - /mnt/media/movies:/media/movies:ro
      - /mnt/media/tv:/media/tv:ro
    ```
4.  Run `docker-compose up -d --build`.
5.  Open `http://localhost:5000`. You **must** configure the **Path Substitutions** table (see below).

### Option 3: Linux (Manual)
**Best for:** Ubuntu, Debian, Fedora (Running natively).
1.  **Download & Extract:**
    Download `JellyDiscover_Linux_v1.1.0.zip` from [Releases].
    ```bash
    unzip JellyDiscover_Linux_v1.1.0.zip -d /opt/
    cd /opt/JellyDiscover
    ```
2.  **Install Dependencies:**
    ```bash
    pip3 install -r requirements.txt
    ```
3.  **Setup Dashboard Service (Systemd):**
    ```bash
    # The zip includes the service file pre-configured for /opt/JellyDiscover
    sudo cp linux/jellydiscover.service /etc/systemd/system/
    sudo systemctl enable jellydiscover
    sudo systemctl start jellydiscover
    ```
4.  **Setup Daily Schedule (Cron):**
    Open your crontab (`crontab -e`) and add the following line to run the engine daily at 4:00 AM:
    ```bash
    0 4 * * * /usr/bin/python3 /opt/JellyDiscover/src/engine.py >> /var/log/jellydiscover.log 2>&1
    ```

---

## ⚙️ The Dashboard
Access the web interface at `http://localhost:5000`.

### 1. Connection
* **Jellyfin URL:** The address of your server (e.g., `http://192.168.1.5:8096`).
* **API Key:** Generate this in Jellyfin Dashboard > API Keys.

### 2. Path Substitutions (Crucial for Docker)
This table maps the file paths Jellyfin sees to the file paths JellyDiscover sees.

* **Windows Users:** Usually **NOT** needed. The installer handles permissions for you.
* **Linux Users:** Usually **NOT** needed. Paths usually match (e.g., `/mnt/media`).
* **Docker Users:** **REQUIRED.**
    * *Example:* Jellyfin sees `/data/movies`. Docker sees `/media/movies`.
    * *Action:* Add a rule mapping Remote `/data/movies` -> Local `/media/movies`.

### 3. Scoring Bias
Adjust how the recommendation algorithm weighs different factors.
* **Seen Penalty:** How much to penalize content the user has already watched (Prevents repeats).
* **Collection Boost:** Bonus points if the item belongs to a collection the user likes (e.g., suggesting "Harry Potter 2" if they watched "Harry Potter 1").

### 4. Maintenance Tab
* **Logs:** View live logs to troubleshoot connection or scanning issues.
* **Cleaner Utility:** If your libraries ever look "glitched" (e.g., duplicate entries or items that won't play), run this tool to wipe the database and start fresh.

---

## 🛠 Troubleshooting & Errors

### Common Log Errors
Logs are located in `C:\ProgramData\JellyDiscover\logs\JellyDiscover.log` (Windows) or `./data/logs/JellyDiscover.log` (Linux/Docker).

| Error Message | Meaning | Solution |
| :--- | :--- | :--- |
| `HTTP 401 Unauthorized` | The API Key is invalid. | Generate a new API Key in Jellyfin and update the Dashboard. |
| `Connection Refused` | Cannot reach Jellyfin. | Check the Jellyfin URL. Ensure the port (8096) is correct. |
| `FileNotFoundError` | The engine cannot see your media files. | **Windows:** Re-install and ensure you entered your Windows Credentials.<br>**Docker:** Check your Path Substitutions table. |
| `0 items found` | Scoring is too strict. | Lower the `Min Score` in `libraries.json` or check if the user has enough watch history. |
| `Playback Error / Unsupported` | Database out of sync ("Ghost Items"). | Go to the **Maintenance** tab and click **Run Cleaner Utility**, then **Run Discovery**. |

### How to Uninstall
* **Windows:** Use the Uninstaller in Control Panel. It will ask if you want to run the Cleanup Tool to remove the Virtual Libraries from Jellyfin.

* **Linux/Docker:** Run `python3 src/cleaner.py` manually, then delete the folder.
