#!/bin/bash

# Print environment variables (except credentials)
echo "Starting proxy-start.sh"
echo "CLOUD_SQL_CONNECTION_NAME: $CLOUD_SQL_CONNECTION_NAME"
echo "USERNAME set: $(if [ -n "$USERNAME" ]; then echo "Yes"; else echo "No"; fi)"
echo "PASSWORD set: $(if [ -n "$PASSWORD" ]; then echo "Yes"; else echo "No"; fi)"
echo "DATABASE set: $(if [ -n "$DATABASE" ]; then echo "Yes"; else echo "No"; fi)"

# 1. Write service account credentials to a file
echo "Writing service account credentials to /tmp/sa.json"
echo "$GOOGLE_APPLICATION_CREDENTIALS_JSON" > /tmp/sa.json
if [ $? -eq 0 ]; then
  echo "Successfully wrote credentials file"
  ls -la /tmp/sa.json
else
  echo "ERROR: Failed to write credentials file"
  exit 1
fi

# 2. Download the proxy binary
echo "Downloading Cloud SQL Auth Proxy..."
curl -o cloud_sql_proxy https://dl.google.com/cloudsql/cloud_sql_proxy.linux.amd64 \
  && chmod +x cloud_sql_proxy

if [ ! -f "cloud_sql_proxy" ]; then
  echo "ERROR: Failed to download Cloud SQL Auth Proxy"
  exit 1
fi

echo "Successfully downloaded Cloud SQL Auth Proxy"
ls -la cloud_sql_proxy

# 3. Start the proxy in background with logging
echo "Starting Cloud SQL Auth Proxy..."
./cloud_sql_proxy \
  -instances="$CLOUD_SQL_CONNECTION_NAME"=tcp:3306 \
  -credential_file=/tmp/sa.json \
  > /tmp/cloud_sql_proxy.log 2>&1 &

PROXY_PID=$!
echo "Cloud SQL Auth Proxy started with PID: $PROXY_PID"

# 4. Wait for proxy to start
echo "Waiting for proxy to initialize..."
sleep 10

# 5. Output proxy status and log
echo "Cloud SQL Auth Proxy process status:"
ps aux | grep cloud_sql_proxy
echo "Cloud SQL Auth Proxy log (first 10 lines):"
head -10 /tmp/cloud_sql_proxy.log || echo "No log file found"

# Check if proxy is listening on port 3306
echo "Checking if proxy is listening on port 3306:"
netstat -tlnp 2>/dev/null | grep 3306 || echo "WARNING: No process is listening on port 3306"

# 6. Make script executable
chmod +x debug.sh

# 7. Start your main application
echo "Starting application..."
exec gunicorn rag:app
