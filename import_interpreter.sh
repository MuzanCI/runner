#!/usr/bin/env bash

set -o errexit
set -o nounset

cargo build -p muzanci-interpreter

mkdir -p ./embed
cp ../target/debug/interpreter ./embed/interpreter
