import duckdb


DATE = "2026-03-22"
DATA_DIR = f"data/{DATE}"

# BTC fills for a day
fills = f"""
SELECT timestamp, side, price, size, direction, address
FROM read_parquet('{DATA_DIR}/fills/fills.parquet')
WHERE coin = 'BTC'
ORDER BY timestamp
LIMIT 10;
"""

# BTC 1-minute candles (aggregated from 1s)
candles = f"""
SELECT time_bucket(INTERVAL '1 minute', timestamp) as minute,
       first(open) as open, max(high) as high,
       min(low) as low, last(close) as close,
       sum(volume) as volume, sum(trade_count) as trades
FROM read_parquet('{DATA_DIR}/candles/candles.parquet')
WHERE coin = 'BTC'
GROUP BY minute ORDER BY minute;
"""

# Largest BTC positions
positions = f"""
SELECT user, size, notional, entry_price, leverage, leverage_type
FROM read_parquet('{DATA_DIR}/snapshots/*.parquet')
WHERE market = 'BTC'
ORDER BY abs(size) DESC LIMIT 10;
"""


def main():
    conn = duckdb.connect()
    for q in [fills, candles, positions]:
        conn.execute(q)
        print(conn.fetch_df())
        print()


if __name__ == "__main__":
    main()
