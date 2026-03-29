#!/usr/bin/env bash
# =============================================================================
# EV Charging Intelligence API — Production Deployment & HTTPS Setup
# =============================================================================
# This script helps you configure the API for production deployment.
# It is interactive and will explain each step before executing.
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
SSL_DIR="$SCRIPT_DIR/ssl"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m' # No Color

print_header() {
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BOLD}  $1${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
}

print_step() {
    echo -e "\n${GREEN}[STEP]${NC} $1"
}

print_info() {
    echo -e "${YELLOW}[INFO]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_ok() {
    echo -e "${GREEN}[OK]${NC} $1"
}

ask_confirm() {
    local prompt="$1"
    local response
    echo -e -n "${BOLD}$prompt [y/N]: ${NC}"
    read -r response
    [[ "$response" =~ ^[Yy]$ ]]
}

# ─────────────────────────────────────────────────────────────────────────────
# Step 1: Prerequisites Check
# ─────────────────────────────────────────────────────────────────────────────
print_header "Step 1: Checking Prerequisites"

MISSING_REQUIRED=()
MISSING_OPTIONAL=()

echo "Checking required tools..."

# python3
if command -v python3 &>/dev/null; then
    PY_VERSION=$(python3 --version 2>&1)
    print_ok "python3 found: $PY_VERSION"
else
    MISSING_REQUIRED+=("python3")
    print_error "python3 not found"
fi

# openssl
if command -v openssl &>/dev/null; then
    SSL_VERSION=$(openssl version 2>&1)
    print_ok "openssl found: $SSL_VERSION"
else
    MISSING_REQUIRED+=("openssl")
    print_error "openssl not found"
fi

# pip / uvicorn check
if python3 -c "import uvicorn" 2>/dev/null; then
    print_ok "uvicorn is installed"
else
    MISSING_REQUIRED+=("uvicorn (pip install uvicorn[standard])")
    print_error "uvicorn not installed"
fi

echo ""
echo "Checking optional tools..."

# certbot
if command -v certbot &>/dev/null; then
    print_ok "certbot found (for Let's Encrypt production certs)"
else
    MISSING_OPTIONAL+=("certbot")
    print_info "certbot not found — needed only for production Let's Encrypt certs"
fi

# mkcert
if command -v mkcert &>/dev/null; then
    print_ok "mkcert found (for locally-trusted dev certs)"
else
    MISSING_OPTIONAL+=("mkcert")
    print_info "mkcert not found — optional, for locally-trusted dev certs"
fi

# nginx
if command -v nginx &>/dev/null; then
    print_ok "nginx found"
else
    MISSING_OPTIONAL+=("nginx")
    print_info "nginx not found — needed only if using nginx as reverse proxy"
fi

if [[ ${#MISSING_REQUIRED[@]} -gt 0 ]]; then
    echo ""
    print_error "Missing required tools: ${MISSING_REQUIRED[*]}"
    print_error "Please install them before continuing."
    exit 1
fi

if [[ ${#MISSING_OPTIONAL[@]} -gt 0 ]]; then
    echo ""
    print_info "Missing optional tools: ${MISSING_OPTIONAL[*]}"
    print_info "These are not required for local development but may be needed for production."
fi

# ─────────────────────────────────────────────────────────────────────────────
# Step 2: Generate Self-Signed SSL Certificate (Development)
# ─────────────────────────────────────────────────────────────────────────────
print_header "Step 2: SSL Certificate for Development"

echo "This step generates a self-signed SSL certificate for local HTTPS development."
echo "The certificate will be stored in: $SSL_DIR/"
echo ""
echo "Command that will be run:"
echo "  openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem \\"
echo "    -days 365 -nodes -subj '/CN=localhost'"
echo ""
print_info "Self-signed certs trigger browser warnings — this is expected for dev."
print_info "For production, use Let's Encrypt (see instructions at end of script)."

if [[ -f "$SSL_DIR/cert.pem" && -f "$SSL_DIR/key.pem" ]]; then
    echo ""
    print_info "SSL certificates already exist in $SSL_DIR/"
    CERT_EXPIRY=$(openssl x509 -enddate -noout -in "$SSL_DIR/cert.pem" 2>/dev/null || echo "unknown")
    print_info "Current cert expiry: $CERT_EXPIRY"

    if ask_confirm "Regenerate certificates? (existing ones will be overwritten)"; then
        GENERATE_CERT=true
    else
        GENERATE_CERT=false
        print_info "Keeping existing certificates."
    fi
else
    if ask_confirm "Generate self-signed SSL certificate now?"; then
        GENERATE_CERT=true
    else
        GENERATE_CERT=false
        print_info "Skipping certificate generation."
    fi
fi

if [[ "$GENERATE_CERT" == "true" ]]; then
    mkdir -p "$SSL_DIR"
    print_step "Generating RSA 4096-bit self-signed certificate..."

    openssl req -x509 -newkey rsa:4096 \
        -keyout "$SSL_DIR/key.pem" \
        -out "$SSL_DIR/cert.pem" \
        -days 365 -nodes \
        -subj '/CN=localhost'

    chmod 600 "$SSL_DIR/key.pem"
    chmod 644 "$SSL_DIR/cert.pem"

    print_ok "Certificate generated:"
    echo "    Key:  $SSL_DIR/key.pem"
    echo "    Cert: $SSL_DIR/cert.pem"

    # Verify
    openssl x509 -in "$SSL_DIR/cert.pem" -noout -subject -dates 2>/dev/null && \
        print_ok "Certificate verified successfully." || \
        print_error "Certificate verification failed."
fi

# ─────────────────────────────────────────────────────────────────────────────
# Step 3: Production Uvicorn Launch Command
# ─────────────────────────────────────────────────────────────────────────────
print_header "Step 3: Production Uvicorn Configuration"

echo "For production, uvicorn should run with multiple workers behind HTTPS."
echo ""
echo "There are two deployment strategies:"
echo ""
echo "  A) Uvicorn with SSL directly (simpler, no nginx):"
echo "     Best for small deployments or internal APIs."
echo ""
echo "  B) Nginx reverse proxy + uvicorn on localhost (recommended for production):"
echo "     Nginx handles SSL termination, rate limiting, static files."
echo "     Uvicorn listens on 127.0.0.1:8000 (no SSL needed on uvicorn)."

# Create the launch script
LAUNCH_SCRIPT="$SCRIPT_DIR/start_production.sh"

cat > "$LAUNCH_SCRIPT" << 'LAUNCH_EOF'
#!/usr/bin/env bash
# =============================================================================
# EV Charging API — Production Launch Script
# =============================================================================
# Usage:
#   ./start_production.sh [--direct-ssl | --behind-proxy]
#
#   --direct-ssl    : Uvicorn handles SSL directly (port 443, needs root)
#   --behind-proxy  : Uvicorn on localhost:8000, nginx handles SSL (default)
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
SSL_DIR="$SCRIPT_DIR/ssl"

MODE="${1:---behind-proxy}"

cd "$PROJECT_DIR"

# Load environment from .env if present
if [[ -f "$PROJECT_DIR/.env" ]]; then
    set -a
    source "$PROJECT_DIR/.env"
    set +a
fi

case "$MODE" in
    --direct-ssl)
        echo "Starting uvicorn with direct SSL on 0.0.0.0:443 ..."
        echo "NOTE: Port 443 requires root/sudo on most systems."

        if [[ ! -f "$SSL_DIR/cert.pem" || ! -f "$SSL_DIR/key.pem" ]]; then
            echo "ERROR: SSL certificates not found in $SSL_DIR/"
            echo "Run deploy_production.sh first to generate them."
            exit 1
        fi

        exec uvicorn api_server.server:app \
            --host 0.0.0.0 \
            --port 443 \
            --workers 4 \
            --ssl-keyfile "$SSL_DIR/key.pem" \
            --ssl-certfile "$SSL_DIR/cert.pem" \
            --access-log \
            --log-level warning
        ;;

    --behind-proxy)
        echo "Starting uvicorn behind reverse proxy on 127.0.0.1:8000 ..."
        echo "Make sure nginx (or another reverse proxy) is handling SSL on port 443."

        exec uvicorn api_server.server:app \
            --host 127.0.0.1 \
            --port 8000 \
            --workers 4 \
            --proxy-headers \
            --forwarded-allow-ips='127.0.0.1' \
            --access-log \
            --log-level warning
        ;;

    *)
        echo "Usage: $0 [--direct-ssl | --behind-proxy]"
        exit 1
        ;;
esac
LAUNCH_EOF

chmod +x "$LAUNCH_SCRIPT"
print_ok "Created launch script: $LAUNCH_SCRIPT"
echo ""
echo "  Direct SSL:      $LAUNCH_SCRIPT --direct-ssl"
echo "  Behind proxy:    $LAUNCH_SCRIPT --behind-proxy"

# ─────────────────────────────────────────────────────────────────────────────
# Step 4: Systemd Service File
# ─────────────────────────────────────────────────────────────────────────────
print_header "Step 4: Systemd Service File (Linux)"

SYSTEMD_FILE="$SCRIPT_DIR/ev-charging-api.service"

echo "Creating a systemd service template for Linux server deployments."
echo "You will need to copy this to /etc/systemd/system/ and adjust paths."

cat > "$SYSTEMD_FILE" << 'SYSTEMD_EOF'
# =============================================================================
# EV Charging Intelligence API — Systemd Service
# =============================================================================
# Installation:
#   1. Copy to /etc/systemd/system/ev-charging-api.service
#   2. Edit User, WorkingDirectory, and ExecStart paths
#   3. sudo systemctl daemon-reload
#   4. sudo systemctl enable --now ev-charging-api
#
# Logs:  journalctl -u ev-charging-api -f
# =============================================================================

[Unit]
Description=EV Charging Intelligence API (uvicorn)
After=network.target
Wants=network-online.target

[Service]
Type=exec
User=www-data
Group=www-data
WorkingDirectory=/opt/ev-charging-api
EnvironmentFile=/opt/ev-charging-api/.env

ExecStart=/opt/ev-charging-api/venv/bin/uvicorn api_server.server:app \
    --host 127.0.0.1 \
    --port 8000 \
    --workers 4 \
    --proxy-headers \
    --forwarded-allow-ips='127.0.0.1' \
    --access-log \
    --log-level warning

ExecReload=/bin/kill -HUP $MAINPID

Restart=always
RestartSec=5

# Security hardening
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/opt/ev-charging-api/data
PrivateTmp=true
ProtectKernelTunables=true
ProtectControlGroups=true

# Resource limits
LimitNOFILE=65536
LimitNPROC=4096

[Install]
WantedBy=multi-user.target
SYSTEMD_EOF

print_ok "Created systemd service template: $SYSTEMD_FILE"

# ─────────────────────────────────────────────────────────────────────────────
# Step 5: Nginx Reverse Proxy Configuration
# ─────────────────────────────────────────────────────────────────────────────
print_header "Step 5: Nginx Reverse Proxy Configuration"

NGINX_FILE="$SCRIPT_DIR/nginx-ev-api.conf"

echo "Creating an nginx config template with:"
echo "  - SSL termination (TLS 1.2+ with strong ciphers)"
echo "  - Rate limiting (10 req/s, burst 20)"
echo "  - Reverse proxy to uvicorn on 127.0.0.1:8000"
echo "  - Let's Encrypt ACME challenge support"
echo "  - Blocked access to /data/ paths"

cat > "$NGINX_FILE" << 'NGINX_EOF'
# =============================================================================
# EV Charging Intelligence API — Nginx Reverse Proxy Configuration
# =============================================================================
# Installation:
#   1. Copy to /etc/nginx/sites-available/ev-charging-api
#   2. ln -s /etc/nginx/sites-available/ev-charging-api /etc/nginx/sites-enabled/
#   3. Update server_name and SSL cert paths
#   4. sudo nginx -t && sudo systemctl reload nginx
# =============================================================================

# ── Rate limiting zone ──────────────────────────────────────────────────────
# 10 requests per second per IP, with a 10MB shared memory zone
limit_req_zone $binary_remote_addr zone=api_limit:10m rate=10r/s;

# ── Upstream ────────────────────────────────────────────────────────────────
upstream ev_api_backend {
    server 127.0.0.1:8000;
    keepalive 32;
}

# ── Redirect HTTP to HTTPS ──────────────────────────────────────────────────
server {
    listen 80;
    listen [::]:80;
    server_name your-domain.com;

    # Let's Encrypt ACME challenge — must remain accessible over HTTP
    location /.well-known/acme-challenge/ {
        root /var/www/certbot;
        allow all;
    }

    # Redirect everything else to HTTPS
    location / {
        return 301 https://$host$request_uri;
    }
}

# ── Main HTTPS server ──────────────────────────────────────────────────────
server {
    listen 443 ssl http2;
    listen [::]:443 ssl http2;
    server_name your-domain.com;

    # ── SSL certificates ────────────────────────────────────────────────────
    # For Let's Encrypt (production):
    ssl_certificate     /etc/letsencrypt/live/your-domain.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/your-domain.com/privkey.pem;

    # For self-signed (development) — uncomment and comment out the above:
    # ssl_certificate     /path/to/scripts/ssl/cert.pem;
    # ssl_certificate_key /path/to/scripts/ssl/key.pem;

    # ── SSL hardening ───────────────────────────────────────────────────────
    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_ciphers ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256:ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:ECDHE-ECDSA-CHACHA20-POLY1305:ECDHE-RSA-CHACHA20-POLY1305:DHE-RSA-AES128-GCM-SHA256:DHE-RSA-AES256-GCM-SHA384;
    ssl_prefer_server_ciphers off;

    ssl_session_timeout 1d;
    ssl_session_cache shared:SSL:10m;
    ssl_session_tickets off;

    # OCSP stapling (Let's Encrypt only — comment out for self-signed)
    ssl_stapling on;
    ssl_stapling_verify on;
    resolver 1.1.1.1 8.8.8.8 valid=300s;
    resolver_timeout 5s;

    # ── Security headers ────────────────────────────────────────────────────
    add_header Strict-Transport-Security "max-age=63072000; includeSubDomains; preload" always;
    add_header X-Content-Type-Options "nosniff" always;
    add_header X-Frame-Options "DENY" always;
    add_header X-XSS-Protection "1; mode=block" always;
    add_header Referrer-Policy "strict-origin-when-cross-origin" always;

    # ── Block direct access to /data/ paths ─────────────────────────────────
    location ~* ^/data/ {
        deny all;
        return 403;
    }

    # ── Let's Encrypt ACME challenge (HTTPS fallback) ───────────────────────
    location /.well-known/acme-challenge/ {
        root /var/www/certbot;
        allow all;
    }

    # ── API proxy ───────────────────────────────────────────────────────────
    location / {
        # Rate limiting: 10 req/s per IP, allow bursts up to 20
        limit_req zone=api_limit burst=20 nodelay;
        limit_req_status 429;

        proxy_pass http://ev_api_backend;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        # WebSocket support (if needed)
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";

        # Timeouts
        proxy_connect_timeout 60s;
        proxy_send_timeout 60s;
        proxy_read_timeout 60s;
    }

    # ── Logging ─────────────────────────────────────────────────────────────
    access_log /var/log/nginx/ev-api-access.log;
    error_log  /var/log/nginx/ev-api-error.log warn;

    # ── Limits ──────────────────────────────────────────────────────────────
    client_max_body_size 10m;
}
NGINX_EOF

print_ok "Created nginx config template: $NGINX_FILE"

# ─────────────────────────────────────────────────────────────────────────────
# Step 6: Deployment Instructions
# ─────────────────────────────────────────────────────────────────────────────
print_header "Deployment Instructions"

cat << 'INSTRUCTIONS'

┌─────────────────────────────────────────────────────────────────────────────┐
│  DEVELOPMENT (Local HTTPS with self-signed cert)                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  1. The self-signed cert was generated above in scripts/ssl/                │
│                                                                             │
│  2. Run the API with direct SSL:                                            │
│     sudo ./scripts/start_production.sh --direct-ssl                         │
│                                                                             │
│  3. Or run without SSL for plain dev:                                       │
│     uvicorn api_server.server:app --reload --port 8000                      │
│                                                                             │
│  4. For browser-trusted local certs, install mkcert:                        │
│     brew install mkcert    # macOS                                          │
│     mkcert -install                                                         │
│     mkcert -key-file scripts/ssl/key.pem -cert-file scripts/ssl/cert.pem \ │
│       localhost 127.0.0.1 ::1                                               │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│  PRODUCTION (Let's Encrypt + Nginx + Systemd)                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  1. Point your domain's DNS A record to your server IP.                     │
│                                                                             │
│  2. Install certbot and obtain a certificate:                               │
│     sudo apt install certbot python3-certbot-nginx                          │
│     sudo certbot --nginx -d your-domain.com                                 │
│                                                                             │
│  3. Install the nginx config:                                               │
│     sudo cp scripts/nginx-ev-api.conf /etc/nginx/sites-available/          │
│     sudo ln -s /etc/nginx/sites-available/nginx-ev-api.conf \              │
│       /etc/nginx/sites-enabled/                                             │
│     # Edit server_name and SSL paths in the config                          │
│     sudo nginx -t && sudo systemctl reload nginx                            │
│                                                                             │
│  4. Install the systemd service:                                            │
│     sudo cp scripts/ev-charging-api.service /etc/systemd/system/           │
│     # Edit paths and user in the service file                               │
│     sudo systemctl daemon-reload                                            │
│     sudo systemctl enable --now ev-charging-api                             │
│                                                                             │
│  5. Certbot auto-renews via systemd timer. Verify:                          │
│     sudo certbot renew --dry-run                                            │
│                                                                             │
│  6. Set environment variables in /opt/ev-charging-api/.env:                 │
│     ADMIN_TOKEN=<strong-random-token>                                       │
│     ALLOWED_ORIGINS=https://your-domain.com                                 │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│  DOCKER DEPLOYMENT                                                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  1. Create a Dockerfile (if not already present):                           │
│                                                                             │
│     FROM python:3.12-slim                                                   │
│     WORKDIR /app                                                            │
│     COPY requirements.txt .                                                 │
│     RUN pip install --no-cache-dir -r requirements.txt                      │
│     COPY . .                                                                │
│     EXPOSE 8000                                                             │
│     CMD ["uvicorn", "api_server.server:app",                                │
│          "--host", "0.0.0.0", "--port", "8000",                             │
│          "--workers", "4", "--proxy-headers",                               │
│          "--access-log", "--log-level", "warning"]                          │
│                                                                             │
│  2. Build and run:                                                          │
│     docker build -t ev-charging-api .                                       │
│     docker run -d --name ev-api \                                           │
│       -p 8000:8000 \                                                        │
│       --env-file .env \                                                     │
│       --restart unless-stopped \                                            │
│       ev-charging-api                                                       │
│                                                                             │
│  3. Use nginx or a cloud load balancer in front for SSL termination.        │
│                                                                             │
│  4. For docker-compose with nginx + certbot, see:                           │
│     https://github.com/nginx-proxy/acme-companion                           │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘

INSTRUCTIONS

print_ok "Deployment setup complete. Files created:"
echo ""
echo "  scripts/ssl/cert.pem              — Self-signed SSL certificate"
echo "  scripts/ssl/key.pem               — SSL private key"
echo "  scripts/start_production.sh       — Production launch script"
echo "  scripts/ev-charging-api.service   — Systemd service template"
echo "  scripts/nginx-ev-api.conf         — Nginx reverse proxy config"
echo "  scripts/security_checklist.md     — Production security checklist"
echo ""
echo "Review scripts/security_checklist.md before deploying to production."
