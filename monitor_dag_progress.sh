#!/bin/bash
# Monitor DAG progress in real-time

DAG_ID=$1
if [ -z "$DAG_ID" ]; then
    echo "Usage: $0 <dag_id>"
    echo "Example: $0 replicate_stackoverflow_to_postgres_parallel"
    exit 1
fi

echo "Monitoring DAG: $DAG_ID"
echo "Press CTRL+C to stop monitoring"
echo ""

while true; do
    clear
    echo "========================================"
    echo "DAG Progress Monitor - $(date '+%H:%M:%S')"
    echo "========================================"
    echo ""

    # Get latest DAG run status
    astro dev run dags list-runs -d $DAG_ID --limit 1 2>/dev/null | grep -v "Astro managed"

    echo ""
    echo "----------------------------------------"
    echo "Task States:"
    echo "----------------------------------------"

    # Get task states
    astro dev run tasks states-for-dag-run $DAG_ID manual__* 2>/dev/null | tail -20

    sleep 10
done
