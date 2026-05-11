#!/bin/bash
# Скачивает JDBC-драйверы в папку jars/ перед запуском docker-compose

set -e
mkdir -p jars

PG_JAR="jars/postgresql-42.7.3.jar"
CH_JAR="jars/clickhouse-jdbc-0.6.3-all.jar"

PG_URL="https://jdbc.postgresql.org/download/postgresql-42.7.3.jar"
CH_URL="https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.6.3/clickhouse-jdbc-0.6.3-all.jar"

download() {
    local dst=$1
    local url=$2
    if [ -f "$dst" ]; then
        echo "[SKIP] $dst already exists"
        return
    fi
    echo "Downloading $dst ..."
    if command -v wget &>/dev/null; then
        wget -q --show-progress -O "$dst" "$url"
    elif command -v curl &>/dev/null; then
        curl -L --progress-bar -o "$dst" "$url"
    else
        echo "ERROR: wget or curl required" && exit 1
    fi
    echo "[OK] $dst"
}

download "$PG_JAR" "$PG_URL"
download "$CH_JAR" "$CH_URL"

echo ""
echo "Готово! Можно запускать: docker compose up -d"
