#!/bin/bash
# Quick Start for StackOverflow2013 Performance Testing

set -e

echo "=========================================="
echo "Quick Start: StackOverflow2013 Testing"
echo "=========================================="
echo ""

# Step 1: Start Astro
echo "Step 1: Starting Astro environment..."
astro dev start

echo ""
echo "Waiting for Airflow to initialize (30 seconds)..."
sleep 30

# Step 2: Get Astro network name
echo ""
echo "Step 2: Getting Astro network name..."
ASTRO_NETWORK=$(docker network ls --format '{{.Name}}' | grep 'stackoverflow.*_airflow' | head -1)

if [ -z "$ASTRO_NETWORK" ]; then
    echo "Error: Could not find Astro network"
    echo "Networks found:"
    docker network ls
    exit 1
fi

echo "Found Astro network: $ASTRO_NETWORK"

# Step 3: Start SQL Server source
echo ""
echo "Step 3: Starting SQL Server source container..."
docker run -d \
  --name stackoverflow-mssql-source \
  --platform linux/amd64 \
  --memory="4g" \
  -e "ACCEPT_EULA=Y" \
  -e "MSSQL_SA_PASSWORD=StackOverflow123!" \
  -e "MSSQL_PID=Developer" \
  -v "$(pwd)/include/stackoverflow":/var/opt/mssql/backup \
  -p 1433:1433 \
  mcr.microsoft.com/mssql/server:2022-latest

# Step 4: Start PostgreSQL target
echo ""
echo "Step 4: Starting PostgreSQL target container..."
docker run -d \
  --name stackoverflow-postgres-target \
  -e "POSTGRES_PASSWORD=StackOverflow123!" \
  -e "POSTGRES_USER=postgres" \
  -e "POSTGRES_DB=stackoverflow_target" \
  -v "$(pwd)/include/bulk_files":/bulk_files \
  -p 5433:5432 \
  postgres:16

# Step 5: Connect to Astro network
echo ""
echo "Step 5: Connecting containers to Astro network..."
docker network connect $ASTRO_NETWORK stackoverflow-mssql-source
docker network connect $ASTRO_NETWORK stackoverflow-postgres-target

# Step 6: Wait for SQL Server to start
echo ""
echo "Step 6: Waiting for SQL Server to start (45 seconds)..."
sleep 45

# Step 7: Create Airflow connections
echo ""
echo "Step 7: Creating Airflow connections..."

astro dev run connections delete stackoverflow_source 2>/dev/null || true
astro dev run connections add stackoverflow_source \
  --conn-type mssql \
  --conn-host stackoverflow-mssql-source \
  --conn-port 1433 \
  --conn-login sa \
  --conn-password "StackOverflow123!" \
  --conn-schema StackOverflow2013

astro dev run connections delete stackoverflow_postgres_target 2>/dev/null || true
astro dev run connections add stackoverflow_postgres_target \
  --conn-type postgres \
  --conn-host stackoverflow-postgres-target \
  --conn-port 5432 \
  --conn-login postgres \
  --conn-password "StackOverflow123!" \
  --conn-schema stackoverflow_target

echo ""
echo "=========================================="
echo "✅ Environment ready!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Run: ./test_stackoverflow2013_performance.sh"
echo "   OR"
echo "2. Follow: STACKOVERFLOW2013_TESTING_GUIDE.md"
echo ""
echo "Airflow UI: http://localhost:8080"
echo "  Username: admin"
echo "  Password: admin"
echo ""
