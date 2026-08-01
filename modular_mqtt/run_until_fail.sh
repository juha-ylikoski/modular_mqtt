#!/usr/bin/env bash
set -u

count=0
while true; do
    count=$((count + 1))
    echo "=== Run #${count} ==="
    RUST_LOG=TRACE cargo test sync_tests::publish::publish_qos1::v3
    status=$?
    if [ "$status" -ne 0 ]; then
        echo "Failed on run #${count} (exit code ${status})"
        exit "$status"
    fi
done
