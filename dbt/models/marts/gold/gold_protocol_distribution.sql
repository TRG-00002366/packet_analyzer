select
    protocol,
    count(*) as packet_count,
    avg(data_length) as avg_size
from {{ ref('int_packet_events') }}
group by 1
order by packet_count desc, protocol
