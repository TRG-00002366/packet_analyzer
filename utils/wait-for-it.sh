#!/usr/bin/env bash
# wait-for-it.sh: Wait until a host and port are available
# Usage: wait-for-it.sh host:port [-- command args]

set -e

hostport="$1"
shift

host="${hostport%%:*}"
port="${hostport##*:}"

while ! nc -z "$host" "$port"; do
  echo "Waiting for $host:$port..."
  sleep 1
done

echo "$host:$port is available!"

exec "$@"
