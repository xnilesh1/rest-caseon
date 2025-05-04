#!/usr/bin/env bash
set -e

# 1. Dump the JSON key into a file
echo "$GOOGLE_APPLICATION_CREDENTIALS_JSON" > /tmp/sa.json

# 2. Fetch the Cloud SQL Auth Proxy binary
curl -fsSL -o cloud_sql_proxy \
  https://dl.google.com/cloudsql/cloud_sql_proxy.linux.amd64
chmod +x cloud_sql_proxy

# 3. Launch the proxy in the background
#    Replace with your Instance Connection Name from GCP (project:region:instance)
./cloud_sql_proxy \
  -instances="$CLOUDSQL_CONNECTION_NAME"=tcp:3306 \
  -credential_file=/tmp/sa.json &

# 4. Give the proxy a moment to start (optional, but safer)
sleep 2

# 5. Finally, start your Python app
#    (Adjust to your entrypoint, e.g. uvicorn or gunicorn)
exec python app.py
