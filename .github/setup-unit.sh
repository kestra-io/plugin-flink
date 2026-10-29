#!/bin/bash
set -euo pipefail

# Setup Flink test environment
echo "Setting up Flink test environment..."
compose_file="${1:-docker-compose-ci.yml}"
expected_version="${2:-1.20.1}"

# Start Flink cluster using docker-compose
docker compose -f "$compose_file" up -d

# Wait for Flink cluster to be ready
echo "Waiting for Flink cluster to be ready..."
timeout=60
counter=0

while [ $counter -lt $timeout ]; do
    actual_version=""
    taskmanagers=0
    slots_available=0
    if config=$(curl -fsS http://localhost:8081/config 2>/dev/null); then
        if actual_version=$(printf '%s' "$config" | python3 -c 'import json, sys; print(json.load(sys.stdin).get("flink-version") or "")'); then
            if [ -n "$actual_version" ] && [ "$actual_version" != "$expected_version" ]; then
                echo "Flink version mismatch: expected $expected_version, but REST configuration reports $actual_version." >&2
                docker compose -f "$compose_file" logs
                exit 1
            fi
            if [ "$actual_version" = "$expected_version" ] \
                && overview=$(curl -fsS http://localhost:8081/overview 2>/dev/null); then
                if overview_details=$(printf '%s' "$overview" | python3 -c 'import json, sys; data = json.load(sys.stdin); print(data.get("taskmanagers", 0), data.get("slots-available", 0))'); then
                    read -r taskmanagers slots_available <<< "$overview_details"
                    if [[ "$taskmanagers" =~ ^[0-9]+$ ]] \
                        && [ "$taskmanagers" -gt 0 ] \
                        && [[ "$slots_available" =~ ^[0-9]+$ ]] \
                        && [ "$slots_available" -gt 0 ]; then
                        echo "Flink $expected_version cluster is ready with $taskmanagers registered TaskManager(s) and $slots_available available task slot(s)!"
                        break
                    fi
                fi
            fi
        fi
    fi
    echo "Waiting for Flink $expected_version REST API, TaskManager registration, and an available slot... ($counter/$timeout)"
    sleep 2
    counter=$((counter + 2))
done

if [ $counter -ge $timeout ]; then
    echo "Timeout: Flink cluster did not become ready with version $expected_version, a registered TaskManager, and an available slot (reported version: ${actual_version:-unavailable}, TaskManagers: $taskmanagers, available slots: $slots_available)." >&2
    docker compose -f "$compose_file" logs
    exit 1
fi

echo "Flink cluster started successfully!"
echo "Web UI available at: http://localhost:8081"
echo ""
echo "To run tests with Flink integration:"
echo "  ./gradlew test -Dflink.integration.test=true"
printf '  ./gradlew flinkCompatibilityTest -PflinkCompatibilityVersion=%s -PflinkCompatibilityComposeFile=%s\n' "$expected_version" "$compose_file"
echo ""
echo "To stop the environment:"
printf '  docker compose -f "%s" down\n' "$compose_file"