#!/bin/bash

SERVER="nl_development@172.16.60.50"
APP_DIR="/opt/api-stt-uploader"
SERVICE="api-stt-uploader.service"

echo "=== Deploying to server ==="

ssh $SERVER << 'EOF'

echo "=== Go to app directory ==="
cd /opt/api-stt-uploader

echo "=== Pull latest code ==="
git pull origin master

echo "=== Restart service ==="
sudo systemctl restart api-stt-uploader.service

echo "=== Deployment finished ==="

EOF

echo "=== Done ==="