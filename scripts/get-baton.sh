#!/usr/bin/env bash

set -euxo pipefail

OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m)
if [ "${ARCH}" = "x86_64" ]; then
  ARCH="amd64"
elif [ "${ARCH}" = "aarch64" ]; then
  ARCH="arm64"
fi

# Use the CLI shipped with the connector's SDK so it can read the same C1Z
# format, including Pebble-backed files.
SDK_VERSION=$(go list -m -f '{{.Version}}' github.com/conductorone/baton-sdk)
BASE_URL="https://github.com/conductorone/baton-sdk/releases/download"

EXTENSION="tar.gz"
if [ "${OS}" = "darwin" ]; then
  EXTENSION="zip"
fi
FILENAME="baton-${SDK_VERSION}-${OS}-${ARCH}.${EXTENSION}"
DOWNLOAD_URL="${BASE_URL}/${SDK_VERSION}/${FILENAME}"

curl -fSL -o "${FILENAME}" "${DOWNLOAD_URL}"
CHECKSUMS="baton_${SDK_VERSION#v}_checksums.txt"
curl -fSL -o "${CHECKSUMS}" "${BASE_URL}/${SDK_VERSION}/${CHECKSUMS}"

if command -v sha256sum >/dev/null 2>&1; then
  CHECKSUM_COMMAND=(sha256sum)
else
  CHECKSUM_COMMAND=(shasum -a 256)
fi
# Check this archive specifically; unrelated files left by earlier installs
# must not let a missing checksum for the selected archive pass verification.
awk -v filename="${FILENAME}" '$2 == filename { print; found = 1 } END { exit !found }' "${CHECKSUMS}" |
  "${CHECKSUM_COMMAND[@]}" --check -

if [ "${EXTENSION}" = "zip" ]; then
  unzip -o "${FILENAME}"
else
  tar xzf "${FILENAME}"
fi
