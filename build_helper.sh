#!/bin/bash

# Check if VERSION environment is set
if [ -z "$VERSION" ]; then
  echo "ERROR: VERSION environment variable is not set"
  exit 1
fi

# Check if TWINE_PASSWORD is set
if [ -z "$TWINE_PASSWORD" ]; then
  echo "ERROR: TWINE_PASSWORD environment variable is not set"
  exit 1
fi

rm -rf ./mariadb_binlog_indexer/lib || true
mkdir -p ./mariadb_binlog_indexer/lib
CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -o ./mariadb_binlog_indexer/lib/indexer_linux_amd64
CGO_ENABLED=1 GOOS=linux GOARCH=arm64 CC=aarch64-linux-gnu-gcc go build -o ./mariadb_binlog_indexer/lib/indexer_linux_arm64
chmod +x ./mariadb_binlog_indexer/lib/indexer_linux_amd64
chmod +x ./mariadb_binlog_indexer/lib/indexer_linux_arm64
rm -rf dist
rm -rf build
cat pyproject.toml | sed -i "s/version = \"[^\"]*\"/version = \"$VERSION\"/" pyproject.toml
pip install build twine
python -m build
ls -alh ./dist
twine upload dist/* --non-interactive