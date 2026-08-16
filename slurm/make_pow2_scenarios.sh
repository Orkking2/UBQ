#!/usr/bin/env bash
set -euo pipefail

cpus=${1:?CPU count required}

for ((producers = 1; producers < cpus; producers *= 2)); do
    for ((consumers = 1; consumers < cpus; consumers *= 2)); do
        if ((producers + consumers <= cpus)); then
            printf '%dp%dc\n' "$producers" "$consumers"
        fi
    done
done