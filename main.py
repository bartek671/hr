import duckdb


q = """
INSTALL httpfs;
LOAD httpfs;
SET s3_region = 'ap-northeast-1';

-- BTC fills for a day
SELECT timestamp, side, price, size, direction, address
FROM read_parquet('s3://hydromancer-reservoir/by_dex/hyperliquid/fills/perp/all/date=2026-03-22/fills.parquet')
WHERE coin = 'BTC'
ORDER BY timestamp;

-- BTC 1-minute candles (aggregated from 1s)
SELECT time_bucket(INTERVAL '1 minute', timestamp) as minute,
       first(open) as open, max(high) as high,
       min(low) as low, last(close) as close,
       sum(volume) as volume, sum(trade_count) as trades
FROM read_parquet('s3://hydromancer-reservoir/by_dex/hyperliquid/candles/1s/date=2026-03-22/candles.parquet')
WHERE coin = 'BTC'
GROUP BY minute ORDER BY minute;

-- Largest BTC positions
SELECT user, size, notional, entry_price, leverage, leverage_type
FROM read_parquet('s3://hydromancer-reservoir/by_dex/hyperliquid/snapshots/perp/date=2026-03-22/*.parquet')
WHERE market = 'BTC'
ORDER BY abs(size) DESC LIMIT 10;
"""


def main():
    conn = duckdb.connect()
    conn.execute(q)
    print(conn.fetch_df())


if __name__ == "__main__":
    main()
