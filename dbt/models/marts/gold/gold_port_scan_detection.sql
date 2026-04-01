with per_source_minute as (
    select
        date_trunc('minute', event_time) as minute_window_start,
        dateadd(minute, 1, date_trunc('minute', event_time)) as minute_window_end,
        src_ip,
        count(*) as packet_count,
        count(distinct dst_ip) as distinct_destination_ips,
        count(distinct dest_port) as distinct_destination_ports,
        min(event_time) as first_seen_at,
        max(event_time) as last_seen_at
    from {{ ref('int_packet_events') }}
    where src_ip is not null
    group by 1, 2, 3
)
select
    minute_window_start,
    minute_window_end,
    src_ip,
    packet_count,
    distinct_destination_ips,
    distinct_destination_ports,
    first_seen_at,
    last_seen_at,
    case
        when distinct_destination_ports >= 100 or distinct_destination_ips >= 50 then 'critical'
        when distinct_destination_ports >= 60 or distinct_destination_ips >= 30 then 'high'
        when distinct_destination_ports >= 40 or distinct_destination_ips >= 20 then 'medium'
        else 'low'
    end as severity,
    true as scan_flag
from per_source_minute
where distinct_destination_ports >= 20
   or distinct_destination_ips >= 10
