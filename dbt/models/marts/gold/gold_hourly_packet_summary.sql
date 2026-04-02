select
    date_trunc('hour', event_time) as hour_start,
    count(*) as total_packets,
    count(distinct src_ip) as total_src_ip_packets,
    count(distinct dst_ip) as total_dst_ip_packets,
    avg(data_length) as avg_data_length
from {{ ref('int_packet_events') }}
group by hour_start
order by hour_start
