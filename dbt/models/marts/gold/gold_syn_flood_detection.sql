select
    date_trunc('minute', event_time) as minute_window_start,
    dateadd(minute, 1, date_trunc('minute', event_time)) as minute_window_end,
    dst_ip,
    count(*) as syn_packet_count,
    count(distinct src_ip) as unique_sources,
    count(*) as total_packets,
    case
        when count(*) >= 1000 or count(distinct src_ip) >= 250 then 'critical'
        when count(*) >= 500 or count(distinct src_ip) >= 100 then 'high'
        when count(*) >= 250 or count(distinct src_ip) >= 50 then 'medium'
        else 'low'
    end as severity
from {{ ref('int_packet_events') }}
where control_flags = 'SYN'
group by 1, 2, 3
having count(*) > 100
order by syn_packet_count desc, minute_window_start desc, dst_ip

