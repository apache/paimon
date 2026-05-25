#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Paimon CLI install script.
# Builds the fat jar (if needed) and installs the `paimon` command to a
# directory on PATH (default: /usr/local/bin).
#
# Usage:
#   ./install.sh                          # install to /usr/local/bin
#   ./install.sh --prefix ~/.local        # install to ~/.local/bin
#   ./install.sh --with oss               # bundle OSS plugin
#   ./install.sh --with s3                # bundle S3 plugin
#   ./install.sh --with oss --with s3     # bundle both
#   ./install.sh --no-build               # skip Maven build, assume jar exists

set -euo pipefail

PREFIX="/usr/local"
DO_BUILD=true
PLUGINS=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        --prefix)
            PREFIX="$2"
            shift 2
            ;;
        --with)
            PLUGINS+=("$2")
            shift 2
            ;;
        --no-build)
            DO_BUILD=false
            shift
            ;;
        -h|--help)
            echo "Usage: install.sh [--prefix DIR] [--with PLUGIN]... [--no-build]"
            echo ""
            echo "Options:"
            echo "  --prefix DIR     Installation prefix (default: /usr/local)"
            echo "                   The paimon command is placed in DIR/bin/"
            echo "                   The jar and plugins are placed in DIR/lib/paimon-cli/"
            echo "  --with PLUGIN    Include a filesystem plugin: oss, s3"
            echo "                   Can be specified multiple times."
            echo "  --no-build       Skip Maven build, use existing jar in target/"
            echo ""
            echo "Examples:"
            echo "  ./install.sh --with oss              # install with Aliyun OSS support"
            echo "  ./install.sh --with s3 --with oss    # install with S3 and OSS support"
            exit 0
            ;;
        *)
            echo "Unknown option: $1" >&2
            exit 1
            ;;
    esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
REPO_ROOT="$(dirname "$PROJECT_DIR")"

echo "==> Paimon CLI Installer"
echo "    Project:  $PROJECT_DIR"
echo "    Prefix:   $PREFIX"
if [ ${#PLUGINS[@]} -gt 0 ]; then
    echo "    Plugins:  ${PLUGINS[*]}"
fi
echo ""

# Validate plugin names
for plugin in "${PLUGINS[@]+"${PLUGINS[@]}"}"; do
    case "$plugin" in
        oss|s3) ;;
        *)
            echo "Error: Unknown plugin '$plugin'. Supported: oss, s3" >&2
            exit 1
            ;;
    esac
done

# Build if needed
if [ "$DO_BUILD" = true ]; then
    MODULES="-pl paimon-cli"
    for plugin in "${PLUGINS[@]+"${PLUGINS[@]}"}"; do
        MODULES="$MODULES,paimon-filesystems/paimon-$plugin"
    done
    echo "==> Building ($MODULES)..."
    (cd "$REPO_ROOT" && mvn package $MODULES -am -DskipTests -q)
    echo "    Build complete."
fi

# Locate the fat jar
JAR=$(find "$PROJECT_DIR/target" -name "paimon-cli-*.jar" ! -name "original-*" ! -name "*-sources*" ! -name "*-tests*" | head -1)
if [ -z "$JAR" ]; then
    echo "Error: Cannot find paimon-cli jar in $PROJECT_DIR/target/" >&2
    echo "Run 'mvn package -DskipTests' first or remove --no-build flag." >&2
    exit 1
fi

JAR_NAME="$(basename "$JAR")"
INSTALL_BIN="$PREFIX/bin"
INSTALL_LIB="$PREFIX/lib/paimon-cli"
INSTALL_PLUGINS="$INSTALL_LIB/plugins"

# Create directories
mkdir -p "$INSTALL_BIN" "$INSTALL_LIB" "$INSTALL_PLUGINS"

# Copy main jar
echo "==> Installing $JAR_NAME to $INSTALL_LIB/"
cp "$JAR" "$INSTALL_LIB/$JAR_NAME"

# Copy plugin jars
for plugin in "${PLUGINS[@]+"${PLUGINS[@]}"}"; do
    PLUGIN_JAR=$(find "$REPO_ROOT/paimon-filesystems/paimon-$plugin/target" \
        -name "paimon-$plugin-*.jar" ! -name "original-*" ! -name "*-sources*" ! -name "*-tests*" 2>/dev/null | head -1)
    if [ -z "$PLUGIN_JAR" ]; then
        echo "Warning: Cannot find paimon-$plugin jar. Skipping." >&2
        continue
    fi
    PLUGIN_JAR_NAME="$(basename "$PLUGIN_JAR")"
    echo "==> Installing plugin $PLUGIN_JAR_NAME to $INSTALL_PLUGINS/"
    cp "$PLUGIN_JAR" "$INSTALL_PLUGINS/$PLUGIN_JAR_NAME"
done

# Create launcher script
LAUNCHER="$INSTALL_BIN/paimon"
echo "==> Installing launcher to $LAUNCHER"

cat > "$LAUNCHER" <<'WRAPPER'
#!/usr/bin/env bash
set -euo pipefail

PAIMON_LIB="PLACEHOLDER_LIB"
PAIMON_JAR=$(find "$PAIMON_LIB" -maxdepth 1 -name "paimon-cli-*.jar" | head -1)

if [ -z "$PAIMON_JAR" ]; then
    echo "Error: paimon-cli jar not found in $PAIMON_LIB" >&2
    exit 1
fi

if [ -n "${JAVA_HOME:-}" ]; then
    JAVA="$JAVA_HOME/bin/java"
else
    JAVA="java"
fi

JVM_OPTS="${PAIMON_CLI_OPTS:--Xmx512m}"

# Build classpath: main jar + plugins + hadoop
CLASSPATH="$PAIMON_JAR"

PLUGIN_DIR="$PAIMON_LIB/plugins"
if [ -d "$PLUGIN_DIR" ]; then
    for jar in "$PLUGIN_DIR"/*.jar; do
        [ -f "$jar" ] && CLASSPATH="$CLASSPATH:$jar"
    done
fi

if [ -n "${HADOOP_HOME:-}" ]; then
    HADOOP_CP="$("$HADOOP_HOME/bin/hadoop" classpath 2>/dev/null || true)"
    [ -n "$HADOOP_CP" ] && CLASSPATH="$CLASSPATH:$HADOOP_CP"
fi

exec "$JAVA" $JVM_OPTS -cp "$CLASSPATH" org.apache.paimon.cli.PaimonCli "$@"
WRAPPER

# Replace placeholder with actual lib path
sed -i.bak "s|PLACEHOLDER_LIB|$INSTALL_LIB|g" "$LAUNCHER"
rm -f "$LAUNCHER.bak"
chmod +x "$LAUNCHER"

echo ""
echo "==> Installation complete!"
echo ""
echo "    paimon command: $LAUNCHER"
echo "    jar location:   $INSTALL_LIB/$JAR_NAME"
if [ -d "$INSTALL_PLUGINS" ] && ls "$INSTALL_PLUGINS"/*.jar &>/dev/null; then
    echo "    plugins:        $INSTALL_PLUGINS/"
fi
echo ""

# Check if the install location is on PATH
if ! echo "$PATH" | tr ':' '\n' | grep -qx "$INSTALL_BIN"; then
    echo "    NOTE: $INSTALL_BIN is not on your PATH."
    echo "    Add it with:"
    echo ""
    echo "      export PATH=\"$INSTALL_BIN:\$PATH\""
    echo ""
fi

echo "    Run 'paimon --help' to get started."
