select
    date_trunc('hour', event_time) as hour_start,
    dst_ip,
    dest_port,
    protocol,
    count(*) as packet_count,
    count(distinct src_ip) as unique_source_count,
    avg(data_length) as avg_data_length,
    sum(case when control_flags = 'SYN' then 1 else 0 end) as syn_packet_count,
    sum(case when control_flags = 'RST' then 1 else 0 end) as rst_packet_count,
    min(event_time) as first_seen_at,
    max(event_time) as last_seen_at,
    case
        when count(*) >= 500 or count(distinct src_ip) >= 100 then 'critical'
        when count(*) >= 250 or count(distinct src_ip) >= 50 then 'high'
        when count(*) >= 100 or count(distinct src_ip) >= 20 then 'medium'
        else 'low'
    end as exposure_severity
from {{ ref('int_packet_events') }}
where dst_ip is not null
group by 1, 2, 3, 4
