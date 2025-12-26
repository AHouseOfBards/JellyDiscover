# JellyDiscover

JellyDiscover is a recommendation engine for Jellyfin that generates personalized "Recommended" libraries for each user on your server. It analyzes watch history to create tailored lists of Movies, TV Shows, and Music, and presents them as new Virtual Libraries within the Jellyfin interface.

## Installation

1.  **Run the Installer** (`JellyDiscover_Setup_v2.1.exe`).
2.  **Select Storage Type:**
    * **Local Drives:** Choose this if your media is stored on the same PC (e.g., `C:\`, `D:\`, internal drives).
    * **Network Storage (NAS):** Choose this if your media is on a separate device (e.g., Synology, Unraid, TrueNAS). You will be prompted to enter the Windows credentials required to access these shares.
3.  **Enter Jellyfin Details:** Input your Server URL (e.g., `http://localhost:8096`) and an API Key (Generate this in Jellyfin Dashboard > API Keys).
4.  **Finish:** The installer will set up two background services:
    * `JellyDashboard`: The web interface for settings and manual triggers.
    * `JellyDiscover`: The core engine that runs daily at 04:00.

## File Manifest

Below is an explanation of every file included in the installation directory.

### Core Executables
* **`JellyDiscover.exe`**: The main recommendation engine. It connects to the API, calculates scores, generates `.strm` files in `C:\ProgramData\JellyDiscover`, and registers the Virtual Libraries in Jellyfin.
* **`JellyDashboard.exe`**: A lightweight web server running on port 5000. It provides a GUI to modify settings and a "Run Discovery Now" button to force an immediate update.
* **`DiscoverClean.exe`**: A standalone utility that forcefully removes all Virtual Libraries created by this tool from Jellyfin. Use this if you uninstall the software.

### Configuration
* **`config.json`**: Stores your Jellyfin URL, API Key, Runtime schedule, and Thread count.
    * `DAEMON_MODE`: `true` keeps the service running; `false` runs once and exits.
    * `RECOMMENDATION_COUNT`: Number of items to show per library (Default: 25).
* **`libraries.json`**: Defines the logic for recommendations.
    * `min_community_score`: The minimum points an item needs to be included.
    * `discovery_name`: The prefix for the library names (e.g., "Recommended Movies").

### System Utilities
* **`nssm.exe`**: (Non-Sucking Service Manager) A helper utility used by the installer to register the Python scripts as Windows Services.
* **`unins000.exe`**: The uninstaller.

## How It Works

1.  **Analysis:** The engine fetches the watch history for every user on the server.
2.  **Scoring:** It assigns points to media items based on Genres, Actors, Directors, and Community Rating. Unwatched items with high scores are selected.
3.  **Generation:** It creates a folder structure in `C:\ProgramData\JellyDiscover\<Username>`. Inside, it creates `.strm` (Stream) files. These are tiny text files that point to the real location of your media (e.g., `\\NAS\Movies\Avatar.mkv`). No media files are duplicated.
4.  **Registration:** It uses the Jellyfin API to mount these folders as "Virtual Libraries" visible only to the specific user.

## Troubleshooting

**Libraries appear but are empty:**
This is usually a permissions issue.
1.  Open **Services** (`Win+R` > `services.msc`).
2.  Right-click **JellyDiscover** > **Properties** > **Log On**.
3.  Ensure "This account" is selected and valid Windows credentials are entered. The "Local System" account cannot access network shares.

**"Fatal Error" in Logs:**
Check the log file at `C:\ProgramData\JellyDiscover\JellyDiscover.log`.
* If you see `HTTP 401`, your API Key is incorrect.
* If you see `Connection Refused`, your Jellyfin Server URL is incorrect.

**Manual Run:**
To run the engine manually without waiting for the scheduled time, open `http://localhost:5000` in your browser and click "Run Discovery Now."