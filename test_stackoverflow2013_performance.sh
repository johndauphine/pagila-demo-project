#!/bin/bash
# StackOverflow2013 Performance Testing Script
#
# This script runs both DAGs against StackOverflow2013 and records performance metrics

set -e

echo "========================================"
echo "StackOverflow2013 Performance Test"
echo "========================================"
echo ""

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check prerequisites
echo -e "${BLUE}Step 1: Checking prerequisites...${NC}"
if ! command -v docker &> /dev/null; then
    echo "Error: Docker not found"
    exit 1
fi

if ! command -v astro &> /dev/null; then
    echo "Error: Astro CLI not found"
    exit 1
fi

# Check if StackOverflow2013 files exist
echo -e "${BLUE}Step 2: Checking StackOverflow2013 database files...${NC}"
if [ ! -f "include/stackoverflow/StackOverflow2013_1.mdf" ]; then
    echo "Error: StackOverflow2013_1.mdf not found"
    echo "Please extract StackOverflow2013.7z first:"
    echo "  cd include/stackoverflow"
    echo "  7z x StackOverflow2013.7z"
    exit 1
fi

# Check if containers are running
echo -e "${BLUE}Step 3: Checking Docker containers...${NC}"
if ! docker ps | grep -q stackoverflow-mssql-source; then
    echo "Error: SQL Server source container not running"
    echo "Please start it first (see CLAUDE.md for instructions)"
    exit 1
fi

if ! docker ps | grep -q stackoverflow-postgres-target; then
    echo "Error: PostgreSQL target container not running"
    echo "Please start it first (see CLAUDE.md for instructions)"
    exit 1
fi

echo -e "${GREEN}✓ All prerequisites met${NC}"
echo ""

# Attach StackOverflow2013 database
echo -e "${BLUE}Step 4: Attaching StackOverflow2013 database...${NC}"

# Copy database files to container
echo "Copying database files to container..."
docker exec stackoverflow-mssql-source mkdir -p /var/opt/mssql/data

docker cp include/stackoverflow/StackOverflow2013_1.mdf stackoverflow-mssql-source:/var/opt/mssql/data/
docker cp include/stackoverflow/StackOverflow2013_2.ndf stackoverflow-mssql-source:/var/opt/mssql/data/
docker cp include/stackoverflow/StackOverflow2013_3.ndf stackoverflow-mssql-source:/var/opt/mssql/data/
docker cp include/stackoverflow/StackOverflow2013_4.ndf stackoverflow-mssql-source:/var/opt/mssql/data/
docker cp include/stackoverflow/StackOverflow2013_log.ldf stackoverflow-mssql-source:/var/opt/mssql/data/

# Fix permissions
echo "Setting permissions..."
docker exec -u root stackoverflow-mssql-source chown mssql:mssql \
  /var/opt/mssql/data/StackOverflow2013_1.mdf \
  /var/opt/mssql/data/StackOverflow2013_2.ndf \
  /var/opt/mssql/data/StackOverflow2013_3.ndf \
  /var/opt/mssql/data/StackOverflow2013_4.ndf \
  /var/opt/mssql/data/StackOverflow2013_log.ldf

docker exec -u root stackoverflow-mssql-source chmod 660 \
  /var/opt/mssql/data/StackOverflow2013_1.mdf \
  /var/opt/mssql/data/StackOverflow2013_2.ndf \
  /var/opt/mssql/data/StackOverflow2013_3.ndf \
  /var/opt/mssql/data/StackOverflow2013_4.ndf \
  /var/opt/mssql/data/StackOverflow2013_log.ldf

# Drop StackOverflow2010 if it exists
echo "Dropping existing databases..."
docker exec stackoverflow-mssql-source /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P StackOverflow123! -C -Q \
  "IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'StackOverflow2010') DROP DATABASE StackOverflow2010" 2>/dev/null || true

docker exec stackoverflow-mssql-source /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P StackOverflow123! -C -Q \
  "IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'StackOverflow2013') DROP DATABASE StackOverflow2013" 2>/dev/null || true

# Attach StackOverflow2013
echo "Attaching StackOverflow2013..."
docker exec stackoverflow-mssql-source /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P StackOverflow123! -C -Q \
  "CREATE DATABASE StackOverflow2013 ON
   (FILENAME = '/var/opt/mssql/data/StackOverflow2013_1.mdf'),
   (FILENAME = '/var/opt/mssql/data/StackOverflow2013_2.ndf'),
   (FILENAME = '/var/opt/mssql/data/StackOverflow2013_3.ndf'),
   (FILENAME = '/var/opt/mssql/data/StackOverflow2013_4.ndf'),
   (FILENAME = '/var/opt/mssql/data/StackOverflow2013_log.ldf')
   FOR ATTACH"

echo -e "${GREEN}✓ StackOverflow2013 attached successfully${NC}"
echo ""

# Update connection to use StackOverflow2013
echo -e "${BLUE}Step 5: Updating Airflow connection...${NC}"
astro dev run connections delete stackoverflow_source 2>/dev/null || true
astro dev run connections add stackoverflow_source \
  --conn-type mssql \
  --conn-host stackoverflow-mssql-source \
  --conn-port 1433 \
  --conn-login sa \
  --conn-password "StackOverflow123!" \
  --conn-schema StackOverflow2013

echo -e "${GREEN}✓ Connection updated to StackOverflow2013${NC}"
echo ""

# Get row counts
echo -e "${BLUE}Step 6: Getting row counts...${NC}"
docker exec stackoverflow-mssql-source /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P StackOverflow123! -C -Q \
  "USE StackOverflow2013;
   SELECT 'VoteTypes' AS TableName, COUNT(*) AS RowCount FROM VoteTypes
   UNION ALL SELECT 'PostTypes', COUNT(*) FROM PostTypes
   UNION ALL SELECT 'LinkTypes', COUNT(*) FROM LinkTypes
   UNION ALL SELECT 'Users', COUNT(*) FROM Users
   UNION ALL SELECT 'Badges', COUNT(*) FROM Badges
   UNION ALL SELECT 'Posts', COUNT(*) FROM Posts
   UNION ALL SELECT 'PostLinks', COUNT(*) FROM PostLinks
   UNION ALL SELECT 'Comments', COUNT(*) FROM Comments
   UNION ALL SELECT 'Votes', COUNT(*) FROM Votes
   ORDER BY TableName"

echo ""

# Test 1: Streaming DAG
echo "========================================"
echo -e "${YELLOW}Test 1: Streaming DAG Performance${NC}"
echo "========================================"
echo ""

echo "Unpausing streaming DAG..."
astro dev run dags unpause replicate_stackoverflow_to_postgres_parallel

echo "Triggering streaming DAG..."
echo "Start time: $(date '+%Y-%m-%d %H:%M:%S')"
START_TIME=$(date +%s)

astro dev run dags trigger replicate_stackoverflow_to_postgres_parallel

echo ""
echo "DAG triggered. Monitor progress at: http://localhost:8080"
echo ""
echo "Waiting for DAG to complete..."
echo "Press CTRL+C when the DAG completes, then note the end time"
echo ""

# Wait for user input
read -p "Press Enter when the streaming DAG has completed..."

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))
MINUTES=$((DURATION / 60))
SECONDS=$((DURATION % 60))

echo ""
echo -e "${GREEN}Streaming DAG completed${NC}"
echo "Duration: ${MINUTES}m ${SECONDS}s (${DURATION} seconds total)"
echo ""

# Save results
echo "StackOverflow2013 - Streaming DAG: ${MINUTES}m ${SECONDS}s (${DURATION}s)" > performance_results.txt

# Verify row counts in PostgreSQL
echo "Verifying row counts in PostgreSQL..."
docker exec stackoverflow-postgres-target psql -U postgres -d stackoverflow_target -c \
  "SELECT 'Users' AS table_name, COUNT(*) AS row_count FROM \"Users\"
   UNION ALL SELECT 'Posts', COUNT(*) FROM \"Posts\"
   UNION ALL SELECT 'Comments', COUNT(*) FROM \"Comments\"
   UNION ALL SELECT 'Votes', COUNT(*) FROM \"Votes\"
   UNION ALL SELECT 'Badges', COUNT(*) FROM \"Badges\"
   UNION ALL SELECT 'PostLinks', COUNT(*) FROM \"PostLinks\"
   UNION ALL SELECT 'VoteTypes', COUNT(*) FROM \"VoteTypes\"
   UNION ALL SELECT 'PostTypes', COUNT(*) FROM \"PostTypes\"
   UNION ALL SELECT 'LinkTypes', COUNT(*) FROM \"LinkTypes\"
   ORDER BY table_name;"

echo ""

# Test 2: Bulk Parallel DAG
echo "========================================"
echo -e "${YELLOW}Test 2: Bulk Parallel DAG Performance${NC}"
echo "========================================"
echo ""

echo "Unpausing bulk parallel DAG..."
astro dev run dags unpause replicate_stackoverflow_to_postgres_bulk_parallel

echo "Triggering bulk parallel DAG..."
echo "Start time: $(date '+%Y-%m-%d %H:%M:%S')"
START_TIME=$(date +%s)

astro dev run dags trigger replicate_stackoverflow_to_postgres_bulk_parallel

echo ""
echo "DAG triggered. Monitor progress at: http://localhost:8080"
echo ""
echo "Waiting for DAG to complete..."
echo "Press CTRL+C when the DAG completes, then note the end time"
echo ""

# Wait for user input
read -p "Press Enter when the bulk parallel DAG has completed..."

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))
MINUTES=$((DURATION / 60))
SECONDS=$((DURATION % 60))

echo ""
echo -e "${GREEN}Bulk Parallel DAG completed${NC}"
echo "Duration: ${MINUTES}m ${SECONDS}s (${DURATION} seconds total)"
echo ""

# Save results
echo "StackOverflow2013 - Bulk Parallel DAG: ${MINUTES}m ${SECONDS}s (${DURATION}s)" >> performance_results.txt

# Verify row counts in PostgreSQL
echo "Verifying row counts in PostgreSQL..."
docker exec stackoverflow-postgres-target psql -U postgres -d stackoverflow_target -c \
  "SELECT 'Users' AS table_name, COUNT(*) AS row_count FROM \"Users\"
   UNION ALL SELECT 'Posts', COUNT(*) FROM \"Posts\"
   UNION ALL SELECT 'Comments', COUNT(*) FROM \"Comments\"
   UNION ALL SELECT 'Votes', COUNT(*) FROM \"Votes\"
   UNION ALL SELECT 'Badges', COUNT(*) FROM \"Badges\"
   UNION ALL SELECT 'PostLinks', COUNT(*) FROM \"PostLinks\"
   UNION ALL SELECT 'VoteTypes', COUNT(*) FROM \"VoteTypes\"
   UNION ALL SELECT 'PostTypes', COUNT(*) FROM \"PostTypes\"
   UNION ALL SELECT 'LinkTypes', COUNT(*) FROM \"LinkTypes\"
   ORDER BY table_name;"

echo ""
echo "========================================"
echo -e "${GREEN}Performance Test Complete!${NC}"
echo "========================================"
echo ""
cat performance_results.txt
echo ""
echo "Results saved to: performance_results.txt"
echo ""
