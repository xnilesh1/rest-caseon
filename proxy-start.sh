#!/bin/bash

# 1. Write service account credentials to a file
echo "$GOOGLE_APPLICATION_CREDENTIALS_JSON" > /tmp/sa.json

# 2. Download the proxy binary
curl -o cloud_sql_proxy https://dl.google.com/cloudsql/cloud_sql_proxy.linux.amd64 \
  && chmod +x cloud_sql_proxy

# 3. Start the proxy in background
./cloud_sql_proxy \
  -instances="$CLOUD_SQL_CONNECTION_NAME"=tcp:3306 \
  -credential_file=/tmp/sa.json \
  &

# 4. Wait for proxy to start
sleep 10

# 5. Output proxy status
ps aux | grep cloud_sql_proxy

# 6. Start your main application
# Replace this with your actual application start command
exec gunicorn rag:app
