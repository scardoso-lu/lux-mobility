#!/bin/sh
# Write runtime env before nginx starts — avoids nginx variable substitution issues
cat > /usr/share/nginx/html/env.js <<EOF
window.ENV = { API_URL: '${API_URL}', TILE_URL: '${TILE_URL}' };
EOF
exec nginx -g "daemon off;"
