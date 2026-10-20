#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-11-openjdk-amd64}"
export PATH="$JAVA_HOME/bin:$PATH"

mvn -q -DskipTests compile

OUT_DIR="${TMPDIR:-/tmp}/bfs-demo-out"
rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR"

mvn -q exec:java \
  -Dexec.mainClass=GASBfs \
  -Dexec.args="--file data/sample-edges.tsv --vertex 4 --start 0 --para 1 --iterations 10 --output $OUT_DIR/gas-bfs.csv"

echo "GAS BFS output:"
cat "$OUT_DIR/gas-bfs.csv"

mvn -q exec:java \
  -Dexec.mainClass=BFS \
  -Dexec.args="0 data/sample-edges.tsv $OUT_DIR/gelly-bfs.csv 10 1 4"

echo "Gelly vertex-centric BFS output:"
cat "$OUT_DIR/gelly-bfs.csv"
