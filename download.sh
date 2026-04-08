#!/usr/bin/env bash
set -euo pipefail

BUCKET="s3://hydromancer-reservoir"
DATE="2026-03-22"
DATA_DIR="data/${DATE}"

declare -A FILES=(
    ["fills/fills.parquet"]="by_dex/hyperliquid/fills/perp/all/date=${DATE}/fills.parquet"
    ["candles/candles.parquet"]="by_dex/hyperliquid/candles/1s/date=${DATE}/candles.parquet"
)

SNAPSHOT_PREFIX="by_dex/hyperliquid/snapshots/perp/date=${DATE}/"

mkdir -p "${DATA_DIR}/fills" "${DATA_DIR}/candles" "${DATA_DIR}/snapshots"

for dest_rel in "${!FILES[@]}"; do
    s3_key="${FILES[$dest_rel]}"
    dest="${DATA_DIR}/${dest_rel}"
    if [[ -f "$dest" ]]; then
        echo "SKIP $dest (exists)"
    else
        echo "DOWNLOADING $s3_key ..."
        aws s3 cp "${BUCKET}/${s3_key}" "$dest" --request-payer requester
    fi
done

# Sync snapshots (multiple parquet files)
echo "SYNCING snapshots..."
aws s3 sync "${BUCKET}/${SNAPSHOT_PREFIX}" "${DATA_DIR}/snapshots/" --request-payer requester
echo "DONE"
