#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

if ! command -v mvn >/dev/null 2>&1 || ! test -d /usr/lib/jvm/java-11-openjdk-amd64; then
  sudo DEBIAN_FRONTEND=noninteractive apt-get update -qq
  sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq maven openjdk-11-jdk
fi

export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PATH="$JAVA_HOME/bin:$PATH"

if command -v update-alternatives >/dev/null 2>&1; then
  sudo update-alternatives --set java "$JAVA_HOME/bin/java" 2>/dev/null \
    || sudo update-alternatives --install /usr/bin/java java "$JAVA_HOME/bin/java" 1100
  sudo update-alternatives --set javac "$JAVA_HOME/bin/javac" 2>/dev/null \
    || sudo update-alternatives --install /usr/bin/javac javac "$JAVA_HOME/bin/javac" 1100
fi

mvn -q -DskipTests compile
