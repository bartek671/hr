import marimo

__generated_with = "0.22.5"
app = marimo.App(width="medium")


@app.cell
def _():
    import polars as pl

    return (pl,)


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell
def _(mo):
    keys_df = mo.sql(
        f"""
        with
            raw as (
                SELECT
                    *
                FROM
                    read_json("data/2026-03-22/hl_raw/fills_json/*") -- TODO: date var
            ),
            flat as (
                select
                    * EXCLUDE (events),
                    UNNEST(events) as e
                from
                    raw
            )
        SELECT DISTINCT
            UNNEST(JSON_KEYS(e[2])) -- get unique keys
        from
            flat
        """
    )
    return (keys_df,)


@app.cell
def _(keys_df):
    fill_schema = ",\n".join(f'"{r[0]}": "VARCHAR"' for r in keys_df.rows())  # 🙃

    print(fill_schema)
    return


@app.cell
def _(pl):
    hr_schema = dict(pl.read_parquet_schema("data/2026-03-22/fills/fills.parquet"))

    hr_schema
    return (hr_schema,)


@app.cell
def _(hr_schema, pl):
    def make_select(k, v):
        cast = None
        match v:
            case pl.Decimal(precision=p, scale=s):
                cast = f"Decimal({p}, {s})"
            case pl.UInt64:
                cast = "UINT64"
            case pl.String:
                pass
            case pl.Boolean:
                cast = "boolean"
            case pl.Datetime(time_unit=tu, time_zone=tz):
                "dt"
            case ____:  # marimo use _ so ...
                print(f"unhandled {k}:{v}")

        if cast is None:
            return f"{k} as {k}"
        return f"{k}::{cast} as {k}"

    # k needs a fix later
    hr_select = ",\n".join(make_select(k, v) for k, v in hr_schema.items())
    return (hr_select,)


@app.cell
def _(hr_select):
    print(hr_select)
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        SET TimeZone = 'UTC';

        create view hr_fills_from_hl as 
        with
            raw as (
                from
                    read_json("data/2026-03-22/hl_raw/fills_json/*")
            ),
            flat as (
                select
                    * EXCLUDE (events),
                    UNNEST(events) as e
                from
                    raw
            ),
            flat_flat as (
                SELECT
                    * EXCLUDE (e),
                    trim(e[1], '"') as u,
                    -- https://duckdb.org/docs/lts/data/json/json_functions#transforming-json-to-nested-types
                    unnest(
                        json_transform(
                            e[2],
                            '{{
            "dir": "VARCHAR",
            "builder": "VARCHAR",
            "hash": "VARCHAR",
            "deployerFee": "VARCHAR",
            "crossed": "VARCHAR",
            "cloid": "VARCHAR",
            "feeToken": "VARCHAR",
            "sz": "VARCHAR",
            "time": "VARCHAR",
            "startPosition": "VARCHAR",
            "oid": "VARCHAR",
            "builderFee": "VARCHAR",
            "liquidation": {{
            	"liquidatedUser": "VARCHAR",
        	    "markPx": "VARCHAR",
        	    "method": "VARCHAR",
            }},
            "coin": "VARCHAR",
            "px": "VARCHAR",
            "twapId": "VARCHAR",
            "tid": "VARCHAR",
            "fee": "VARCHAR",
            "side": "VARCHAR",
            "closedPnl": "VARCHAR"
            }}'
                        )
                    ) as fill
                from
                    flat
            ),
            hr as (
                from
                    flat_flat
                SELECT
                    coin as coin,
                    -- no need fopr these,  can derive if needed
                    -- dex as dex,
                    -- asset_class as asset_class,
                    -- base_symbol as base_symbol,
                    -- quote_symbol as quote_symbol,
                    px::Decimal(20, 10) as price,
                    sz::Decimal(20, 10) as size,
                    side as side,
                    (to_timestamp(time::INT64 / 1000.0))::TIMESTAMPTZ as timestamp,
                    dir as direction,
                    closedPnl::Decimal(20, 10) as realized_pnl,
                    hash as tx_hash,
                    oid::UINT64 as order_id,
                    tid::UINT64 as trade_id,
                    fee::Decimal(20, 10) as fee,
                    feeToken as fee_token,
                    u as address,
                    crossed::boolean as crossed,
                    startPosition::Decimal(25, 10) as start_position,
                    -- interesting hr uses (20, 10) ,  do they use try_cast then? in #TODO
                    TRY_CAST(startPosition as Decimal(20,10)) as try_sp,
                    cloid as client_order_id,
                    builder as builder,
                    builderFee::Decimal(20, 10) as builder_fee,
                    twapId::UINT64 as twap_id,
                    liquidation.liquidatedUser = u as is_liquidation,
                    liquidation.markPx::Decimal(20, 10) as liquidation_mark_px,
                    liquidation.method as liquidation_method
            )
        from
            hr
        -- HL files are split by ingest time, not block time. We pull DATE/* and
        -- DATE+1/0 (as next_0), then clip back to DATE by block time. Lower
        -- bound also drops DATE-1 late fills sitting in DATE/0. TODO: date var
        where
            timestamp >= '2026-03-22 00:00:00+00'
            and timestamp < '2026-03-23 00:00:00+00';


        select * from hr_fills_from_hl
        """
    )
    return


@app.cell
def _(hr_fills_from_hl, mo):
    _df = mo.sql(
        f"""
        COPY hr_fills_from_hl to 'data/2026-03-22/hr_fills_from_hl.parquet' (
            FORMAT parquet,
            COMPRESSION zstd,
            COMPRESSION_LEVEL 5
        )
        """
    )
    return


if __name__ == "__main__":
    app.run()
