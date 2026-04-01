with source_talkers as (
    select
        date_trunc('hour', event_time) as hour_start,
        'source' as talker_role,
        src_ip as ip_address,
        count(*) as packet_count,
        count(distinct dst_ip) as distinct_counterpart_ips,
        count(distinct protocol) as distinct_protocols,
        avg(data_length) as avg_data_length,
        max(event_time) as last_seen_at
    from {{ ref('int_packet_events') }}
    where src_ip is not null
    group by 1, 2, 3
), destination_talkers as (
    select
        date_trunc('hour', event_time) as hour_start,
        'destination' as talker_role,
        dst_ip as ip_address,
        count(*) as packet_count,
        count(distinct src_ip) as distinct_counterpart_ips,
        count(distinct protocol) as distinct_protocols,
        avg(data_length) as avg_data_length,
        max(event_time) as last_seen_at
    from {{ ref('int_packet_events') }}
    where dst_ip is not null
    group by 1, 2, 3
), combined_talkers as (
    select * from source_talkers
    union all
    select * from destination_talkers
)
select
    hour_start,
    talker_role,
    ip_address,
    packet_count,
    distinct_counterpart_ips,
    distinct_protocols,
    avg_data_length,
    last_seen_at,
    dense_rank() over (
        partition by hour_start, talker_role
        order by packet_count desc, ip_address
    ) as traffic_rank
from combined_talkers
