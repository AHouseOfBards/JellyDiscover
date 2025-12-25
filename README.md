# JellyDiscover

JellyDiscover is a Python application that generates personalized "Recommended" libraries for Jellyfin. It queries user watch history to identify preferred genres and combines this data with community ratings to create individual recommendation lists for each user.

## Features

* **User Isolation:** Creates separate recommendation libraries for each user.
* **Genre Weighting:** Applies a bias to content scores based on the user's most-watched genres.
* **Privacy Management:** Automatically adjusts Jellyfin permissions so users only see their own recommendation libraries.
* **Portable:** Can be run as a standalone executable on Windows or as a Python script on Linux.
* **Daemon Mode:** Supports running as a background service or a scheduled task.

---

## Installation

### Windows
1.  Download the **Windows** zip file from the [Releases](../../releases) page.
2.  Extract the contents to a directory (e.g., `C:\JellyDiscover`).
3.  Edit `config.json` before running the application (see Configuration below).

### Linux
1.  Download the **Linux** zip file from the [Releases](../../releases) page.
2.  Extract to a directory (e.g., `/opt/JellyDiscover`).
3.  Install the required dependencies:
    ```bash
    pip3 install -r requirements.txt
    ```
4.  Edit `config.json` before running the application.

---

## Configuration

Edit the `config.json` file in the installation directory:

```json
{
  "JELLYFIN_URL": "http://localhost:8096",
  "API_KEY": "YOUR_API_KEY",
  "RECOMMENDATION_COUNT": 50,
  "MAX_THREADS": 2,
  "OS_TYPE": "windows",
  "BIAS_STRENGTH": 2.0,
  "DAEMON_MODE": false,
  "RUN_TIME": "04:00"
}
```

---

## Automation

### Method 1: Scheduled Task (Recommended)
*Ensure `DAEMON_MODE` is set to `false` in config.json.*

**Windows (Command Line)**
Run this command in Command Prompt (cmd.exe) to automatically create the daily task. *Replace `C:\Path\To\JellyDiscover` with your actual path.*

```cmd
schtasks /create /tn "JellyDiscover" /tr "'C:\Path\To\JellyDiscover\JellyDiscover.exe'" /sc daily /st 04:00 /rl highest /f
```

**Linux (Cron)**
Run this command to append the daily job to your crontab:

```bash
(crontab -l 2>/dev/null; echo "0 4 * * * /usr/bin/python3 /opt/JellyDiscover/JellyDiscover.py") | crontab -
```

### Method 2: System Service (Advanced)
*Ensure `DAEMON_MODE` is set to `true` in config.json.*

**Windows (NSSM)**
Windows does not run EXEs as services natively. You must use [NSSM](https://nssm.cc/download).
1. Download NSSM and extract `nssm.exe` to your `C:\JellyDiscover` folder.
2. Open Command Prompt as Administrator.
3. Run these commands:
```cmd
cd C:\JellyDiscover
nssm install JellyDiscover "C:\JellyDiscover\JellyDiscover.exe"
nssm set JellyDiscover AppDirectory "C:\JellyDiscover"
nssm start JellyDiscover
```

**Linux (Systemd)**
1. Create the service file:
```bash
sudo nano /etc/systemd/system/jellydiscover.service
```

2. Paste the following configuration:
```ini
[Unit]
Description=JellyDiscover
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/JellyDiscover
ExecStart=/usr/bin/python3 /opt/JellyDiscover/JellyDiscover.py
Restart=always

[Install]
WantedBy=multi-user.target
```

3. Enable and start the service:
```bash
sudo systemctl enable jellydiscover
sudo systemctl start jellydiscover
```

---

## Uninstallation

To remove the application and all generated data:

1.  Run the **DiscoverUninstaller** executable (or script).
2.  Type `UNINSTALL` when prompted.

## License

MIT License.