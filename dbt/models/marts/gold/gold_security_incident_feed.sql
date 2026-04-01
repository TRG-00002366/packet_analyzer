with syn_flood_incidents as (
    select
        md5(
            concat(
                'syn_flood|',
                to_varchar(minute_window_start, 'YYYY-MM-DD HH24:MI:SS'),
                '|',
                coalesce(dst_ip, 'unknown')
            )
        ) as incident_id,
        'syn_flood' as incident_type,
        severity,
        minute_window_end as detected_at,
        minute_window_start as window_start,
        minute_window_end as window_end,
        cast(null as varchar) as src_ip,
        dst_ip,
        cast(null as number) as dest_port,
        'TCP' as protocol,
        syn_packet_count as packet_count,
        unique_sources as unique_source_count,
        cast(null as number) as distinct_destination_ip_count,
        cast(null as number) as distinct_destination_port_count,
        'open' as status
    from {{ ref('gold_syn_flood_detection') }}
), port_scan_incidents as (
    select
        md5(
            concat(
                'port_scan|',
                to_varchar(minute_window_start, 'YYYY-MM-DD HH24:MI:SS'),
                '|',
                coalesce(src_ip, 'unknown')
            )
        ) as incident_id,
        'port_scan' as incident_type,
        severity,
        minute_window_end as detected_at,
        minute_window_start as window_start,
        minute_window_end as window_end,
        src_ip,
        cast(null as varchar) as dst_ip,
        cast(null as number) as dest_port,
        cast(null as varchar) as protocol,
        packet_count,
        cast(null as number) as unique_source_count,
        distinct_destination_ips as distinct_destination_ip_count,
        distinct_destination_ports as distinct_destination_port_count,
        'open' as status
    from {{ ref('gold_port_scan_detection') }}
)
select *
from syn_flood_incidents
union all
select *
from port_scan_incidents
