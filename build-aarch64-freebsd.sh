#!/usr/bin/env bash

set -euo pipefail

PATH="$(pwd):$PATH"
FREEBSD_SYSROOT="$HOME/freebsd/aarch64/15.1-RELEASE"
CC_WRAPPER="$(which aarch64-freebsd-cc)"
LINKER_WRAPPER="$(which aarch64-freebsd-linker)"
CMAKE_WRAPPER="$(which aarch64-freebsd-cmake)"

###########
# Bindgen #
###########

# cmake path for bindgen to configure CMake-based projects
export CMAKE=$CMAKE_WRAPPER

# clang path for bindgen to parse C headers
export CLANG_PATH=$CC_WRAPPER

# clang args for bindgen to find FreeBSD sysroot C headers
export BINDGEN_EXTRA_CLANG_ARGS="--sysroot=$FREEBSD_SYSROOT -isystem $FREEBSD_SYSROOT/usr/include"

# cc path for bindgen to compile C sources
export CC=$CC_WRAPPER

######################
# Bindgen: aws-lc-rs #
######################

# Bypass zig cc error with jitterentropy module in aws-lc-sys
# See https://github.com/aws/aws-lc-rs/issues/993#issuecomment-3723739936
export AWS_LC_SYS_NO_JITTER_ENTROPY=1

#################
# rustc / Cargo #
#################

# cargo target dir for caching build artifacts
export CARGO_TARGET_DIR="../target-aarch64-unknown-freebsd"

# cargo linker for linking objects
export CARGO_TARGET_AARCH64_UNKNOWN_FREEBSD_LINKER=$LINKER_WRAPPER

# cargo linker flags for finding FreeBSD sysroot objects
export CARGO_TARGET_AARCH64_UNKNOWN_FREEBSD_RUSTFLAGS="-C link-arg=-L$FREEBSD_SYSROOT/lib -C link-arg=-L$FREEBSD_SYSROOT/usr/lib"

cargo build --target aarch64-unknown-freebsd
cargo test --target aarch64-unknown-freebsd --no-run
