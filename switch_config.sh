#!/bin/bash

# This script switches between different configurations (development, production, testing)
# Usage: ./switch_config.sh [development|production|testing]

set -euo pipefail

if [ -z "$1" ]; then
  echo "Usage: ./switch_config.sh [development|production|testing]"
  exit 1
fi

CONFIG="$1"

case "$CONFIG" in
  development)
    echo "Switching to development configuration..."
    if grep -q "^FLASK_CONFIG=development" .env; then
      echo ".env is already configured for development."
    else
      rm -f .env
      cp .env.development .env
    fi
    ;;
  production)
    echo "Switching to production configuration..."
    if grep -q "^FLASK_CONFIG=production" .env; then
      echo ".env is already configured for production."
    else
      rm -f .env
      cp .env.production .env
    fi
    ;;
  testing)
    echo "Switching to testing configuration..."
    if grep -q "^FLASK_CONFIG=testing" .env; then
      echo ".env is already configured for testing."
    else
      rm -f .env
      cp .env.testing .env
    fi
    ;;
  *)
    echo "Invalid configuration: $CONFIG"
    echo "Usage: ./switch_config.sh [development|production|testing]"
    exit 1
    ;;
esac

echo ".env file updated for $CONFIG configuration."
echo "Please reload the environment variables. You can do this by restarting the application or sourcing the .env file (source .env)."
