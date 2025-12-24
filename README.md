# JellyDiscover

JellyDiscover is a Python-based engine that generates personalized "Recommended" libraries for every user on your Jellyfin server. 

**Features:**
* **Unique Recommendations:** Every user gets their own "Recommended Movies" library.
* **Genre Bias:** The engine learns what you watch and prioritizes those genres automatically.
* **Privacy Shield:** Ensures User A cannot see User B's recommendations.
* **Portable:** Does not require installation; runs from a single folder.

---

## 🚀 Setup Guide

### 1. Prerequisites
* **Jellyfin Server** (v10.8 or newer)
* **Python 3.10+** (Only if running from source. Not needed if using the .exe)

### 2. Configuration
1.  Open `config.json`.
2.  Set `JELLYFIN_URL` to your server address (e.g., `http://192.168.1.5:8096`).
3.  Set `API_KEY`. (Generate one in Jellyfin Dashboard -> Advanced -> API Keys).
4.  Set `OS_TYPE`:
    * Use `"windows"` for Windows.
    * Use `"linux"` for Linux/Docker.

### 3. Usage
**Windows:**
Double-click `JellyDiscover.exe`. A terminal window will open, show progress, and close when finished.


### Linux Installation
1.  Download and extract the zip file.
2.  Open a terminal in the folder.
3.  Install dependencies:
    `pip3 install -r requirements.txt`
4.  Run the script:
    `python3 JellyDiscover.py`