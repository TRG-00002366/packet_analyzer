# dbt Models for GOLD Data Marts

This dbt project reads packet data from the Snowflake SILVER layer and materializes GOLD marts for the batch analytics that were previously implemented in Spark.

## Models

- `intermediate/int_packet_events.sql`: joins `FACT_PACKET_EVENTS` and `DIM_TIME` into a reusable event-level model.
- `marts/gold/gold_hourly_packet_summary.sql`: hourly packet aggregation.
- `marts/gold/gold_top_10_destination_ips.sql`: ranked destination IP volume mart.
- `marts/gold/gold_syn_flood_detection.sql`: 1-minute SYN flood alert mart.
- `marts/gold/gold_protocol_distribution.sql`: protocol-level traffic distribution.

## Run

1. Install dependencies:
   `pip install -r dbt/requirements-dbt.txt`
2. Copy `dbt/profiles.yml.example` to your local dbt profiles directory as `profiles.yml`.
3. Set Snowflake credentials in environment variables.
4. Run from the `dbt` directory:
   `dbt debug`
   `dbt run`
   `dbt test`

## Assumptions

- SILVER tables live in `PACKET.SILVER`.
- dbt uses explicit model schemas, so marts materialize in `GOLD` and intermediate models materialize in `INTERMEDIATE` rather than prefixing them with the target schema.
- The Snowflake stream/task load has already populated `FACT_PACKET_EVENTS` and `DIM_TIME`.
