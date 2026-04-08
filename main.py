import duckdb


setup = """
INSTALL httpfs;
LOAD httpfs;
SET s3_region = 'ap-northeast-1';
SET s3_requester_pays = true;
CREATE OR REPLACE SECRET secret (
    TYPE s3,
    PROVIDER credential_chain
);
"""

# BTC fills for a day
fills = """
SELECT timestamp, side, price, size, direction, address
FROM read_parquet('s3://hydromancer-reservoir/by_dex/hyperliquid/fills/perp/all/date=2026-03-22/fills.parquet')
WHERE coin = 'BTC'
ORDER BY timestamp
LIMIT 10;
"""

# BTC 1-minute candles (aggregated from 1s)
candles = """
SELECT time_bucket(INTERVAL '1 minute', timestamp) as minute,
       first(open) as open, max(high) as high,
       min(low) as low, last(close) as close,
       sum(volume) as volume, sum(trade_count) as trades
FROM read_parquet('s3://hydromancer-reservoir/by_dex/hyperliquid/candles/1s/date=2026-03-22/candles.parquet')
WHERE coin = 'BTC'
GROUP BY minute ORDER BY minute;
"""

# Largest BTC positions
positions = """
SELECT user, size, notional, entry_price, leverage, leverage_type
FROM read_parquet('s3://hydromancer-reservoir/by_dex/hyperliquid/snapshots/perp/date=2026-03-22/*.parquet')
WHERE market = 'BTC'
ORDER BY abs(size) DESC LIMIT 10;
"""


def main():
    conn = duckdb.connect()
    conn.execute(setup)
    for q in [fills, candles, positions]:
        conn.execute(q)
        print(conn.fetch_df())
        print()


if __name__ == "__main__":
    main()
