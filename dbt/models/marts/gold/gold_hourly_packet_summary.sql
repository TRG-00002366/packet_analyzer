select
    to_varchar(date_trunc('hour', event_time), 'YYYY-MM-DD HH24') as hour,
    count(*) as total_packets,
    count(distinct src_ip) as total_src_ip_packets,
    count(distinct dst_ip) as total_dst_ip_packets,
    avg(data_length) as avg_data_length
from {{ ref('int_packet_events') }}
group by 1
order by 1
