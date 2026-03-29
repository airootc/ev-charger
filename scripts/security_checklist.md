# Production Security Checklist — EV Charging Intelligence API

Use this checklist before every production deployment. Items marked `[CRITICAL]` must be addressed before going live.

---

## Environment Variables

- [ ] `[CRITICAL]` Set `ADMIN_TOKEN` to a strong random value (minimum 32 characters):
  ```bash
  ADMIN_TOKEN=$(python3 -c "import secrets; print(secrets.token_urlsafe(48))")
  ```
- [ ] `[CRITICAL]` Set `ALLOWED_ORIGINS` to your actual domain(s) only — never use `*` in production:
  ```
  ALLOWED_ORIGINS=https://your-domain.com,https://www.your-domain.com
  ```
- [ ] Set `LOG_LEVEL=warning` (avoid `debug` or `info` in production — they log sensitive data)
- [ ] Store `.env` file with mode `600` (owner read/write only):
  ```bash
  chmod 600 /opt/ev-charging-api/.env
  ```
- [ ] Never commit `.env` to version control — verify it is in `.gitignore`

---

## Firewall Rules

- [ ] `[CRITICAL]` Allow inbound: **80/tcp** (HTTP redirect), **443/tcp** (HTTPS), **22/tcp** (SSH)
- [ ] `[CRITICAL]` Block inbound port **8000** from external — uvicorn should only be reachable from localhost
- [ ] Deny all other inbound by default
- [ ] Example (ufw):
  ```bash
  sudo ufw default deny incoming
  sudo ufw default allow outgoing
  sudo ufw allow 22/tcp
  sudo ufw allow 80/tcp
  sudo ufw allow 443/tcp
  sudo ufw enable
  ```
- [ ] Example (iptables — block external access to 8000):
  ```bash
  sudo iptables -A INPUT -p tcp --dport 8000 -s 127.0.0.1 -j ACCEPT
  sudo iptables -A INPUT -p tcp --dport 8000 -j DROP
  ```

---

## SSL/TLS Configuration

- [ ] `[CRITICAL]` Use Let's Encrypt (or equivalent CA) certificates in production — not self-signed
- [ ] `[CRITICAL]` Enforce TLS 1.2 minimum (TLS 1.3 preferred). The provided nginx config does this.
- [ ] Enable HSTS header (`Strict-Transport-Security: max-age=63072000; includeSubDomains`)
- [ ] Redirect all HTTP traffic to HTTPS (port 80 -> 301 to 443)
- [ ] Set up automatic certificate renewal:
  ```bash
  sudo certbot renew --dry-run   # verify renewal works
  # certbot installs a systemd timer by default; verify it is active:
  systemctl list-timers | grep certbot
  ```
- [ ] Test your SSL configuration: https://www.ssllabs.com/ssltest/
- [ ] Keep SSL private keys with mode `600`, owned by root or the service user

---

## API Key Management

- [ ] `[CRITICAL]` Generate unique API tokens per client/integration — never share a single token
- [ ] Rotate `ADMIN_TOKEN` at least every 90 days
- [ ] Use environment variables or a secrets manager (AWS Secrets Manager, HashiCorp Vault) — never hardcode tokens in source
- [ ] Log authentication failures but never log the token values themselves
- [ ] Implement token expiration if building a multi-tenant system
- [ ] Revocation plan: know how to invalidate a compromised token immediately (restart with new `ADMIN_TOKEN` as minimum)

---

## Monitoring

- [ ] `[CRITICAL]` Set up uptime monitoring (e.g., UptimeRobot, Pingdom, or a simple curl cron):
  ```bash
  # Minimal health check cron (every 5 min)
  */5 * * * * curl -sf https://your-domain.com/api/v1/stations/geojson > /dev/null || echo "API DOWN" | mail -s "Alert" you@email.com
  ```
- [ ] Monitor system resources (CPU, memory, disk) — `htop`, `node_exporter` + Prometheus, or cloud metrics
- [ ] Set up log aggregation — at minimum, configure log rotation:
  ```bash
  # /etc/logrotate.d/ev-charging-api
  /var/log/nginx/ev-api-*.log {
      daily
      rotate 30
      compress
      delaycompress
      missingok
      notifempty
      postrotate
          systemctl reload nginx > /dev/null 2>&1 || true
      endscript
  }
  ```
- [ ] Monitor for failed login attempts (grep for 401/403 in access logs)
- [ ] Set up alerting for: high error rates (5xx), high latency (>2s p95), disk >80%, cert expiry <14 days

---

## Backup Strategy

- [ ] `[CRITICAL]` Back up the SQLite database (or whatever DB you use) daily:
  ```bash
  # SQLite safe backup (does not corrupt during writes)
  sqlite3 /opt/ev-charging-api/data/stations.db ".backup /backups/stations-$(date +%Y%m%d).db"
  ```
- [ ] Back up `.env` and config files separately from code (they contain secrets)
- [ ] Back up the GeoJSON/data source files
- [ ] Store backups off-server (S3, GCS, rsync to a second machine)
- [ ] Test restore from backup at least once — an untested backup is not a backup
- [ ] Retention policy: keep 7 daily + 4 weekly + 3 monthly backups
- [ ] Example backup cron:
  ```bash
  # /etc/cron.d/ev-api-backup
  0 2 * * * www-data sqlite3 /opt/ev-charging-api/data/stations.db ".backup /backups/daily/stations-$(date +\%Y\%m\%d).db" && find /backups/daily/ -name "*.db" -mtime +7 -delete
  ```

---

## Additional Hardening

- [ ] Run the API as a non-root user (the systemd service template uses `www-data`)
- [ ] Keep Python dependencies updated — check for vulnerabilities:
  ```bash
  pip install pip-audit
  pip-audit
  ```
- [ ] Disable server version headers (uvicorn `--header server:` or strip in nginx)
- [ ] Set `client_max_body_size` in nginx to prevent large payload attacks (set to 10m in provided config)
- [ ] If using Docker, run as non-root user inside the container:
  ```dockerfile
  RUN adduser --disabled-password --no-create-home appuser
  USER appuser
  ```
- [ ] Review CORS settings quarterly — remove any origins that are no longer needed
