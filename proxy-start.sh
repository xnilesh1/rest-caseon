#!/bin/bash

set -e  # Exit on any error

# Print environment variables (except credentials)
echo "==== Starting proxy-start.sh ===="
echo "CLOUD_SQL_CONNECTION_NAME: $CLOUD_SQL_CONNECTION_NAME"
echo "USERNAME set: $(if [ -n "$USERNAME" ]; then echo "Yes"; else echo "No"; fi)"
echo "PASSWORD set: $(if [ -n "$PASSWORD" ]; then echo "Yes"; else echo "No"; fi)"
echo "DATABASE set: $(if [ -n "$DATABASE" ]; then echo "Yes"; else echo "No"; fi)"

# Check if required environment variables are set
if [ -z "$CLOUD_SQL_CONNECTION_NAME" ]; then
  echo "ERROR: CLOUD_SQL_CONNECTION_NAME is not set"
  exit 1
fi

if [ -z "$GOOGLE_APPLICATION_CREDENTIALS_JSON" ]; then
  echo "ERROR: GOOGLE_APPLICATION_CREDENTIALS_JSON is not set"
  exit 1
fi

# Create tmp directory if it doesn't exist
mkdir -p /tmp

# 1. Write service account credentials to a file using printf
echo "Writing service account credentials to /tmp/sa.json using printf"
printf '%s\n' "$GOOGLE_APPLICATION_CREDENTIALS_JSON" > /tmp/sa.json
ls -la /tmp/sa.json

# Make sure the credentials file is valid JSON
echo "Validating credentials file /tmp/sa.json..."
if ! jq . /tmp/sa.json > /dev/null; then
  echo "ERROR: Credentials file /tmp/sa.json is not valid JSON after writing!"
  echo "File contents:"
  cat /tmp/sa.json
  exit 1
else
  echo "Credentials file /tmp/sa.json is valid JSON."
fi

# 2. Download the proxy binary
echo "Downloading Cloud SQL Auth Proxy..."
curl -s -o cloud_sql_proxy https://storage.googleapis.com/cloud-sql-connectors/cloud-sql-proxy/v2.16.0/cloud-sql-proxy.linux.amd64
chmod +x cloud_sql_proxy
ls -la cloud_sql_proxy

# 3. Test the proxy with connection test
echo "Testing connection to Cloud SQL instance..."
./cloud_sql_proxy --run-connection-test --credentials-file=/tmp/sa.json $CLOUD_SQL_CONNECTION_NAME

# 4. Start the proxy in the background
echo "Starting Cloud SQL Auth Proxy..."
./cloud_sql_proxy \
  --credentials-file=/tmp/sa.json \
  --address=0.0.0.0 \
  --port=3306 \
  --debug-logs \
  $CLOUD_SQL_CONNECTION_NAME > /tmp/cloud_sql_proxy.log 2>&1 &

PROXY_PID=$!
echo "Cloud SQL Auth Proxy started with PID: $PROXY_PID"

# 5. Wait for proxy to initialize
echo "Waiting for proxy to initialize..."
sleep 10

# Check if proxy is still running
if ! ps -p $PROXY_PID > /dev/null; then
  echo "ERROR: Cloud SQL Auth Proxy PID $PROXY_PID died shortly after starting"
  echo "Proxy logs:"
  cat /tmp/cloud_sql_proxy.log
  exit 1
fi

# Check if proxy is listening on port 3306
echo "Checking if proxy is listening on port 3306:"
netstat -tlnp | grep 3306 || { 
  echo "WARNING: No process is listening on port 3306"; 
  echo "Proxy logs:"; 
  cat /tmp/cloud_sql_proxy.log; 
}

# Wait until port 3306 is open or timeout after 30 seconds
echo "Waiting for port 3306 to be available..."
timeout=30
for i in $(seq 1 $timeout); do
  if nc -z localhost 3306; then
    echo "Port 3306 is now available"
    break
  fi
  if [ $i -eq $timeout ]; then
    echo "ERROR: Timed out waiting for port 3306"
    echo "Proxy logs:"
    cat /tmp/cloud_sql_proxy.log
    exit 1
  fi
  echo "Waiting for port 3306... ($i/$timeout)"
  sleep 1
done

# 6. Print the last few lines of the proxy log
echo "Cloud SQL Auth Proxy log (last 20 lines):"
tail -20 /tmp/cloud_sql_proxy.log || echo "No log file found"

# 7. Test MySQL connectivity
echo "Testing MySQL connectivity..."
if command -v mysql &> /dev/null; then
  mysql -u$USERNAME -p$PASSWORD -h127.0.0.1 -P3306 $DATABASE -e "SELECT 1" || echo "MySQL connection test failed"
else
  echo "MySQL client not installed, skipping direct connection test"
fi

# 8. Start the application in the foreground
echo "Starting application..."
exec gunicorn --bind 0.0.0.0:$PORT rag:app
