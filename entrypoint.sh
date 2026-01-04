#!/bin/bash

# 1. SETUP CRON (The Automation)
# Runs engine.py at 04:00 daily.
# Redirects output to Docker logs (PID 1) so users can see "Run Complete" in Portainer.
echo "0 4 * * * cd /app && /usr/local/bin/python3 src/engine.py > /proc/1/fd/1 2>&1" > /etc/cron.d/jelly-cron
chmod 0644 /etc/cron.d/jelly-cron
crontab /etc/cron.d/jelly-cron

# 2. START CRON SERVICE
cron

# 3. START DASHBOARD
echo "Starting JellyDiscover Dashboard..."
exec python3 src/app.py