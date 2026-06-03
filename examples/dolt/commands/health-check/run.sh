#!/bin/sh
# gc dolt health-check — Parse `gc dolt health --json` for order outcomes.
#
# Reads a health JSON report from stdin, echoes it to stdout for diagnostics,
# and exits nonzero with a concise stderr message for critical data-plane
# failures. This lets the generic order runner record `order.failed` with a
# useful message without making `gc dolt health --json` itself fail before
# programmatic consumers can parse the report.
set -e

report=$(cat)
printf '%s\n' "$report"

json_field() {
  field="$1"
  if command -v jq >/dev/null 2>&1; then
    printf '%s\n' "$report" | jq -r "if $field == null then \"\" else $field end" 2>/dev/null || true
    return
  fi
  key=$(printf '%s' "$field" | sed 's/^\.server\.//')
  printf '%s\n' "$report" \
    | sed -n "/\"server\"[[:space:]]*:/,/}/p" \
    | sed -n "s/.*\"$key\"[[:space:]]*:[[:space:]]*\\([^,}]*\\).*/\\1/p" \
    | head -1 \
    | tr -d ' "'
}

reachable=$(json_field ".server.reachable")
running=$(json_field ".server.running")
pid=$(json_field ".server.pid")
port=$(json_field ".server.port")
latency=$(json_field ".server.latency_ms")
degraded=$(json_field ".server.degraded")

# Probe/pool detail (nested fields resolve only when jq is available;
# the sed fallback yields empty strings and the message degrades to the
# scalar summary). Carrying the classification into the order outcome is
# what distinguishes "pool wedge: every fresh client rejected" from a
# bare "unreachable" when the order.failed event is triaged.
probe_attempts=$(json_field ".server.probe.attempts")
probe_rejected=$(json_field ".server.probe.rejected")
probe_timeouts=$(json_field ".server.probe.timeouts")
probe_last_error=$(json_field ".server.probe.last_error")
pool_saturated=$(json_field ".server.pool.saturated")
pool_active=$(json_field ".server.pool.active_connections")
pool_max=$(json_field ".server.pool.max_connections")

detail=""
if [ -n "$probe_attempts" ] && [ "$probe_attempts" != "0" ]; then
  detail="$detail rejected=${probe_rejected:-0}/$probe_attempts timeouts=${probe_timeouts:-0}"
fi
if [ "$pool_saturated" = "true" ]; then
  if [ -n "$pool_active" ] && [ -n "$pool_max" ] && [ "$pool_max" != "0" ]; then
    detail="$detail pool=${pool_active}/${pool_max} saturated"
  else
    detail="$detail pool saturated (wait-queue rejections observed)"
  fi
fi
[ -n "$probe_last_error" ] && detail="$detail last_error=\"$probe_last_error\""

case "$reachable" in
  true)
    if [ "$degraded" = "true" ]; then
      # Degraded-but-reachable is a warning, not an order failure:
      # failing the 30s health order on partial evidence would flap.
      # The detail still lands in the order log for diagnostics, and
      # the doctor advisory path owns operator notification.
      echo "Dolt server reachable but degraded:${detail:- see report}" >&2
    fi
    exit 0
    ;;
  false)
    echo "Dolt server unreachable: running=${running:-unknown} pid=${pid:-0} port=${port:-unknown} latency_ms=${latency:-0}${detail}" >&2
    exit 1
    ;;
  *)
    echo "Dolt health report missing server.reachable" >&2
    exit 1
    ;;
esac
