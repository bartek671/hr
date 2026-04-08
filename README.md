# hr

[Hydromancer Reservoir](https://docs.hydromancer.xyz/reservoir) EDA

## Prerequisites

- [uv](https://docs.astral.sh/uv/)
- [AWS CLI](https://aws.amazon.com/cli/) (configured for requester-pays S3)
- [lz4](https://github.com/lz4/lz4)

## TODO

- [x] Get [Hydromancer Reservoir](https://docs.hydromancer.xyz/reservoir) data
- [x] Fetch raw data from Hyperliquid's S3
- [x] Make fills parquet from HL
- [ ] Make candles from HL
- [ ] Compare

## Notes

- `start_position` precision: Hydromancer uses `Decimal(20, 10)` but some HL values overflow — needs `Decimal(22, 10)` or `TRY_CAST`
- `timestamp` not yet mapped from HL raw data (currently NULL)
- Hydromancer columns `dex`, `asset_class`, `base_symbol`, `quote_symbol` are omitted (derivable from `coin`)
