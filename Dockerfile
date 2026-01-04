FROM python:3.11-slim

WORKDIR /app

# 1. INSTALL CRON (Required for the 4 AM schedule)
RUN apt-get update && apt-get install -y --no-install-recommends \
    iputils-ping \
    cron \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/
# Ensure entrypoint is copied from root or docker folder depending on your structure
COPY docker/entrypoint.sh .

RUN chmod +x entrypoint.sh
RUN mkdir -p /config/JellyDiscover && chmod 777 /config/JellyDiscover

ENV IS_DOCKER="true"
ENV PYTHONUNBUFFERED=1

EXPOSE 5000

ENTRYPOINT ["./entrypoint.sh"]