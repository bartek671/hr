#!/usr/bin/env bash
set -euo pipefail

BUCKET="s3://hydromancer-reservoir"
DATE="2026-03-22"
DATA_DIR="data/${DATE}"

declare -A FILES=(
    ["fills/fills.parquet"]="global/fills/raw/date=${DATE}/fills.parquet"
    ["candles/candles.parquet"]="global/candles/1s/date=${DATE}/candles.parquet"
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

# Sync raw Hyperliquid fills
HL_DATE="${DATE//-/}"
mkdir -p "${DATA_DIR}/hl_raw/fills"
echo "SYNCING HL raw fills..."
aws s3 sync "s3://hl-mainnet-node-data/node_fills_by_block/hourly/${HL_DATE}/" "${DATA_DIR}/hl_raw/fills/" --request-payer requester

mkdir -p "${DATA_DIR}/hl_raw/fills_json"
echo "DECOMPRESSING HL fills..."
for f in "${DATA_DIR}/hl_raw/fills/"*.lz4; do
    out="${DATA_DIR}/hl_raw/fills_json/$(basename "${f%.lz4}")"
    if [[ -f "$out" ]]; then
        echo "SKIP $out (exists)"
    else
        unlz4 "$f" "$out"
    fi
done

echo "DONE"
