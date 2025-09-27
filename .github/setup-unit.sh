#!/bin/bash

# Setup Flink test environment
echo "Setting up Flink test environment..."

# Start Flink cluster using docker-compose
docker compose -f docker-compose-ci.yml up -d

# Wait for Flink cluster to be ready
echo "Waiting for Flink cluster to be ready..."
timeout=60
counter=0

while [ $counter -lt $timeout ]; do
    if curl -s -f http://localhost:8081/ > /dev/null 2>&1; then
        echo "Flink cluster is ready!"
        break
    fi
    echo "Waiting for Flink cluster... ($counter/$timeout)"
    sleep 2
    counter=$((counter + 2))
done

if [ $counter -ge $timeout ]; then
    echo "Timeout: Flink cluster failed to start"
    docker-compose -f docker-compose-ci.yml logs
    exit 1
fi

echo "Flink cluster started successfully!"
echo "Web UI available at: http://localhost:8081"
echo ""
echo "To run tests with Flink integration:"
echo "  ./gradlew test -Dflink.integration.test=true"
echo ""
echo "To stop the environment:"
echo "  docker-compose -f docker-compose-ci.yml down"