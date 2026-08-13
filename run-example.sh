#!/bin/bash
#
# Run one of the hitorro-jvssql examples.
#
# Usage:
#   ./run-example.sh                  # list available examples
#   ./run-example.sh 01               # runs Example01_BasicSelect
#   ./run-example.sh BasicSelect      # runs Example01_BasicSelect (partial name match)
#   ./run-example.sh all              # runs every example in order
#
set -e
cd "$(dirname "$0")"

EX_DIR="src/main/java/com/hitorro/jvssql/examples"
ALL_EXAMPLES=$(ls $EX_DIR | grep '^Example' | grep -v ExampleSupport | sed 's|\.java$||' | sort)

if [ $# -eq 0 ]; then
    echo "Available examples:"
    for e in $ALL_EXAMPLES; do echo "  $e"; done
    echo
    echo "Usage: $0 <name-fragment | all>"
    exit 0
fi

run_one() {
    local ex="$1"
    echo
    echo "═══════════════════════════════════════════════════════════════"
    echo " Running $ex"
    echo "═══════════════════════════════════════════════════════════════"
    mvn -q exec:java -Dexec.mainClass="com.hitorro.jvssql.examples.$ex"
}

if [ "$1" = "all" ]; then
    for e in $ALL_EXAMPLES; do run_one "$e"; done
    exit 0
fi

# Resolve name fragment to a full class name.
MATCH=$(echo "$ALL_EXAMPLES" | grep -i "$1" | head -1)
if [ -z "$MATCH" ]; then
    echo "No example matches '$1'. Available:" >&2
    for e in $ALL_EXAMPLES; do echo "  $e" >&2; done
    exit 1
fi
run_one "$MATCH"
