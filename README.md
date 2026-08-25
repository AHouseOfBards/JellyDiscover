# JellyDiscover

> ## ⚠️ This version (1.x) is deprecated
>
> JellyDiscover 1.x — the standalone Python engine — is no longer maintained.
> It has been replaced by **JellyDiscover 2.0**, a native Jellyfin plugin that
> installs in seconds and runs inside Jellyfin itself.
>
> ### 👉 [Get JellyDiscover 2.0](https://github.com/AHouseOfBards/JellyDiscover/releases/tag/v2.0.0-beta)
>
> **Source code:** [`v2.0-plugin` branch](https://github.com/AHouseOfBards/JellyDiscover/tree/v2.0-plugin)

---

## What changed?

JellyDiscover 2.0 is a complete ground-up rewrite — not a port of the Python
code. It runs as a native Jellyfin plugin instead of a standalone app, which
eliminates the installer, the Flask dashboard on port 5000, path substitution,
and the Windows service.

| | 1.x (Python, deprecated) | 2.0 (Jellyfin plugin) |
|---|---|---|
| Install | 229 MB installer, admin rights | Paste a repo URL, click install |
| Payload | 3 bundled executables | 2 DLLs, 174 KB |
| Config UI | Flask on `:5000`, **no authentication** | Jellyfin dashboard, admin auth inherited |
| Scheduling | thread comparing `HH:MM` strings | Jellyfin's own task scheduler |
| Item ceiling | first 600 items, unsorted, unpaged | entire catalogue |
| Path substitution | required, and inverted | cannot exist — same process |
| Uninstall | manual library cleanup | one button, plus uninstall hook |

---

## Install JellyDiscover 2.0

1. Dashboard → Plugins → Repositories → add:
   ```
   https://raw.githubusercontent.com/AHouseOfBards/JellyDiscover/v2.0-plugin/manifest.json
   ```
2. Install **JellyDiscover**, then restart Jellyfin.
3. Dashboard → Plugins → JellyDiscover: enable the types you want, then **Generate now**.

Or install manually: download [`jellydiscover_2.0.0.0.zip`](https://github.com/AHouseOfBards/JellyDiscover/releases/download/v2.0.0-beta/jellydiscover_2.0.0.0.zip), unzip into `<jellyfin-config>/plugins/JellyDiscover/`, and restart.

---

## Migrating from 1.x

1. **Uninstall the old version** (Windows: Control Panel; Linux/Docker: stop the service and delete the folder).
2. **Install 2.0** as above.
3. The plugin's config page has a **"Preview what would be removed"** tool that finds and safely removes libraries left behind by 1.x — matching on the exact invisible-character naming scheme, never on words like "Discover" or "Recommended".
4. Old *content folders* on disk are deliberately left alone. The preview shows their paths so you can remove them yourself.

---

<details>
<summary><strong>Legacy 1.x documentation</strong> (click to expand)</summary>

## Features (1.x)
* **Personalized:** Analyzes user history to recommend unwatched content they will actually like.
* **Zero-Copy:** Uses `.strm` files and Symlinks to link to your media. No extra storage space is consumed.
* **Self-Healing:** Automatically refreshes libraries and cleans up stale entries to keep Jellyfin in sync.
* **Universal:** Works on Windows, Linux, and Docker.
* **Dashboard:** A web-based GUI to manage settings, schedules, and path mappings.

## Installation (1.x)

### Option 1: Windows (Installer)
1.  Download `JellyDiscover_Setup_v1.1.0.exe` from [Releases](https://github.com/AHouseOfBards/JellyDiscover/releases/tag/v1.1.3-pre-release).
2.  Run the installer.
3.  Once installed, open `http://localhost:5000` to finish the setup.

### Option 2: Docker (Compose)
1.  Download `JellyDiscover_Docker_v1.1.0.zip` from [Releases](https://github.com/AHouseOfBards/JellyDiscover/releases/tag/v1.1.3-pre-release).
2.  Extract it and edit `docker-compose.yml` volume mounts.
3.  Run `docker-compose up -d --build`.
4.  Open `http://localhost:5000` and configure **Path Substitutions**.

### Option 3: Linux (Manual)
1.  Download `JellyDiscover_Linux_v1.1.0.zip` from [Releases](https://github.com/AHouseOfBards/JellyDiscover/releases/tag/v1.1.3-pre-release).
2.  Extract, install dependencies (`pip3 install -r requirements.txt`), and set up the systemd service.

## Troubleshooting (1.x)

| Error Message | Meaning | Solution |
| :--- | :--- | :--- |
| `HTTP 401 Unauthorized` | The API Key is invalid. | Generate a new API Key in Jellyfin and update the Dashboard. |
| `Connection Refused` | Cannot reach Jellyfin. | Check the Jellyfin URL. Ensure the port (8096) is correct. |
| `FileNotFoundError` | The engine cannot see your media files. | **Windows:** Re-install with correct credentials.<br>**Docker:** Check your Path Substitutions table. |
| `0 items found` | Scoring is too strict. | Lower the `Min Score` in `libraries.json` or check watch history. |
| `Playback Error / Unsupported` | Database out of sync ("Ghost Items"). | Run the **Cleaner Utility**, then **Run Discovery**. |

</details>

---

## Licence

MIT.
