#!/usr/bin/env bash
# Generates Infisical's own infrastructure secrets into ./.env (mode 600).
# Idempotent: refuses to overwrite an existing .env, because rotating
# ENCRYPTION_KEY would make already-stored secrets unreadable.
set -euo pipefail
cd "$(dirname "$0")"

if [[ -e .env ]]; then
  echo ".env already exists, leaving it untouched." >&2
  exit 0
fi

umask 077
cat > .env <<EOF
INFISICAL_ENCRYPTION_KEY=$(openssl rand -hex 16)
INFISICAL_AUTH_SECRET=$(openssl rand -base64 32)
INFISICAL_DB_PASSWORD=$(openssl rand -hex 24)
EOF
chmod 600 .env
echo "Generated .env (600). Now run: docker compose up -d"
