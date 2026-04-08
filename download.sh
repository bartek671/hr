#!/usr/bin/env bash
set -euo pipefail

BUCKET="s3://hydromancer-reservoir"
DATE="2026-03-22"
DATA_DIR="data/${DATE}"

FILES=(
    "by_dex/hyperliquid/fills/perp/all/date=${DATE}/fills.parquet"
    "by_dex/hyperliquid/candles/1s/date=${DATE}/candles.parquet"
)

# Snapshots use a wildcard — sync the whole prefix
SNAPSHOT_PREFIX="by_dex/hyperliquid/snapshots/perp/date=${DATE}/"

mkdir -p "${DATA_DIR}/fills" "${DATA_DIR}/candles" "${DATA_DIR}/snapshots"

for f in "${FILES[@]}"; do
    dest="${DATA_DIR}/$(basename "$(dirname "$f")")/$(basename "$f")"
    if [[ -f "$dest" ]]; then
        echo "SKIP $dest (exists)"
    else
        echo "DOWNLOADING $f ..."
        aws s3 cp "${BUCKET}/${f}" "$dest" --request-payer requester
    fi
done

# Sync snapshots (multiple parquet files)
echo "SYNCING snapshots..."
aws s3 sync "${BUCKET}/${SNAPSHOT_PREFIX}" "${DATA_DIR}/snapshots/" --request-payer requester
echo "DONE"
