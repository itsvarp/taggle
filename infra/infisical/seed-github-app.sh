#!/usr/bin/env bash
# Prompts for the GitHub App credentials (no echo, nothing written to disk or
# shell history) and stores them in the local Infisical project.
#
# Prereqs: Infisical is up, you created an account at http://localhost:8080,
# created a project "taggle", and ran:  infisical login --domain http://localhost:8080
#   then in this repo root: infisical init
set -euo pipefail

ENV_SLUG="${1:-dev}"
PATH_SLUG="/github-app"

ask() { # name, prompt
  local v
  read -rsp "$2: " v; echo >&2
  [[ -n "$v" ]] || { echo "empty value for $1" >&2; exit 1; }
  printf '%s' "$v"
}

CLIENT_ID="$(ask GITHUB_APP_CLIENT_ID 'GitHub App Client ID')"
CLIENT_SECRET="$(ask GITHUB_APP_CLIENT_SECRET 'GitHub App Client Secret')"
APP_ID="$(ask GITHUB_APP_ID 'GitHub App ID (numeric)')"
WEBHOOK_SECRET="$(ask GITHUB_WEBHOOK_SECRET 'Webhook secret (the one you set in GitHub)')"
read -rp "Path to downloaded private key .pem: " PEM_PATH
[[ -r "$PEM_PATH" ]] || { echo "cannot read $PEM_PATH" >&2; exit 1; }

infisical secrets folders create --name "${PATH_SLUG#/}" --path / --env "$ENV_SLUG" \
  --domain http://localhost:8080 >/dev/null 2>&1 || true

infisical secrets set --env "$ENV_SLUG" --path "$PATH_SLUG" --domain http://localhost:8080 \
  "GITHUB_APP_CLIENT_ID=$CLIENT_ID" \
  "GITHUB_APP_CLIENT_SECRET=$CLIENT_SECRET" \
  "GITHUB_APP_ID=$APP_ID" \
  "GITHUB_WEBHOOK_SECRET=$WEBHOOK_SECRET" \
  "GITHUB_APP_PRIVATE_KEY=$(cat "$PEM_PATH")"

echo "Stored in Infisical (env=$ENV_SLUG, path=$PATH_SLUG)."
echo "You can now shred the .pem:  shred -u \"$PEM_PATH\""
