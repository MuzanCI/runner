#!/usr/bin/env bash

set -o errexit
set -o nounset

STAGING_DIR="${1:-/tmp/image_stage/}"
BASE_DIR="${2:-/tmp/base}"

SCRIPT_DIR="$(cd "$(dirname "$(realpath "${BASH_SOURCE[0]}")")" && pwd)"
OVERLAY_DIR="$SCRIPT_DIR/overlay"

ARCH="arm64"
TAG="15.1-RELEASE"

# 1. Initialize and mount ZFS dataset for base.
zfs create zpool/base
zfs set mountpoint="$BASE_DIR" zpool/base
zfs mount zpool/base

# 2. Download FreeBSD kernel and base.
fetch \
    --output "$WORKING_DIR/base.txz" \
    "https://download.freebsd.org/ftp/releases/$ARCH/$TAG/base.txz"

fetch \
    --output "$WORKING_DIR/kernel.txz" \
    "https://download.freebsd.org/ftp/releases/$ARCH/$TAG/kernel.txz"

# 3. Extract into ZFS dataset for base.
mkdir -p "$BASE_DIR"
tar -xvf "$WORKING_DIR/base.txz" "$BASE_DIR"
tar -xvf "$WORKING_DIR/kernel.txz" "$BASE_DIR"

# 4. Initialize and mount ZFS clone for staging.
zfs snapshot "zpool/base@$TAG"
zfs clone "zpool/base@$TAG" zpool/staging
zfs set mountpoint="$STAGING_DIR" zpool/staging
zfs mount zpool/staging

# 5. Apply overlay edits.
cp -a "$OVERLAY_DIR/." "$STAGING_DIR/"

# 6. Build filesystem image.
makefs -t ufs -s 10G ufs.img "$STAGING_DIR"

# 7. Build partitioned image.
mkimg -s gpt \
  -p efi:="$STAGING_DIR/boot/boot1.efifat" \
  -p freebsd-boot:="$STAGING_DIR/boot/gptboot" \
  -p freebsd-ufs:=ufs.img \
  -o custom_freebsd.raw
