from kafka import KafkaProducer
import json
import logging
import atexit
from datetime import datetime, timedelta
from pathlib import Path
from faker import Faker
import random
import time

from utils.utils import ones_complement_checksum


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


TOTAL_PACKETS = 50000
AVERAGE_PACKETS_PER_HOUR = 500
TOTAL_HOURS = TOTAL_PACKETS // AVERAGE_PACKETS_PER_HOUR
HOT_DESTINATIONS = [
    "192.80.254.3",
    "211.180.93.32",
    "166.175.88.24",
    "2.178.206.105",
]
COMMON_SOURCE_IPS = [
    "186.2.48.5",
    "72.144.15.90",
    "88.23.141.217",
    "103.44.201.76",
    "145.67.88.210",
]
ATTACK_BLUEPRINTS = [
    {"kind": "port_scan", "position": 0.18, "minute": 12, "count": 72},
    {"kind": "syn_flood", "position": 0.32, "minute": 21, "count": 180, "unique_sources": 120},
    {"kind": "port_scan", "position": 0.55, "minute": 17, "count": 110},
    {"kind": "syn_flood", "position": 0.74, "minute": 34, "count": 280, "unique_sources": 260},
    {"kind": "syn_flood", "position": 0.9, "minute": 9, "count": 160, "unique_sources": 90},
]

class PacketProducer:    
    def __init__(self, bootstrap_servers):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            key_serializer=lambda k: k.encode('utf-8'),
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            enable_idempotence=True,
            retries=5,
            retry_backoff_ms=100,
            linger_ms=10,
            compression_type='lz4'
        )

        # Ensure cleanup on exit
        atexit.register(self.close)

    def send_packet(self, packet):
        """Send an order event to Kafka."""
        key = f"packet-{packet['packet_id']}"

        future = self.producer.send(
            topic='packets',
            key=key,
            value=packet
        )

        future.add_callback(self._on_success)
        future.add_errback(self._on_error)

        return future

    def _on_success(self, metadata):
        logger.debug(
            "Message sent: topic=%s, partition=%s, offset=%s",
            metadata.topic,
            metadata.partition,
            metadata.offset,
        )

    def _on_error(self, exception):
        logger.error("Message failed: %s", exception)

    def close(self):
        """Flush and close the producer."""
        self.producer.flush()
        self.producer.close()
        logger.info("Producer closed")


def hourly_volume_weight(hour_of_day):
    if 0 <= hour_of_day <= 4:
        return 0.35
    if 5 <= hour_of_day <= 6:
        return 0.5
    if 7 <= hour_of_day <= 8:
        return 0.8
    if 9 <= hour_of_day <= 16:
        return 1.4
    if 17 <= hour_of_day <= 19:
        return 1.1
    if 20 <= hour_of_day <= 22:
        return 0.7
    return 0.45


def allocate_counts(total_packets, hour_starts):
    weights = [hourly_volume_weight(hour_start.hour) for hour_start in hour_starts]
    total_weight = sum(weights)
    exact_counts = [(total_packets * weight) / total_weight for weight in weights]
    allocated = [int(count) for count in exact_counts]
    remainder = total_packets - sum(allocated)

    ranked_indexes = sorted(
        range(len(exact_counts)),
        key=lambda index: exact_counts[index] - allocated[index],
        reverse=True,
    )
    for index in ranked_indexes[:remainder]:
        allocated[index] += 1

    return allocated


def build_attack_schedule(total_hours):
    if total_hours <= 0:
        return []

    max_hour_offset = total_hours - 1
    used_offsets = set()
    schedule = []

    for blueprint in ATTACK_BLUEPRINTS:
        hour_offset = min(
            max_hour_offset,
            max(0, int(round(max_hour_offset * blueprint["position"]))),
        )

        while hour_offset in used_offsets and hour_offset < max_hour_offset:
            hour_offset += 1
        while hour_offset in used_offsets and hour_offset > 0:
            hour_offset -= 1
        if hour_offset in used_offsets:
            continue

        attack = dict(blueprint)
        attack["hour_offset"] = hour_offset
        schedule.append(attack)
        used_offsets.add(hour_offset)

    return schedule


def build_attack_windows(start_hour):
    attack_windows = []
    reserved_by_hour = {hour_offset: 0 for hour_offset in range(TOTAL_HOURS)}
    attack_schedule = build_attack_schedule(TOTAL_HOURS)

    for attack in attack_schedule:
        attack_time = start_hour + timedelta(
            hours=attack["hour_offset"],
            minutes=attack["minute"],
        )
        window = dict(attack)
        window["timestamp"] = attack_time
        attack_windows.append(window)
        reserved_by_hour[attack["hour_offset"]] += attack["count"]

    return attack_windows, reserved_by_hour


def build_regular_hourly_plan(start_hour):
    hour_starts = [start_hour + timedelta(hours=offset) for offset in range(TOTAL_HOURS)]
    hourly_targets = allocate_counts(TOTAL_PACKETS, hour_starts)
    attack_windows, reserved_by_hour = build_attack_windows(start_hour)

    plan = []
    for hour_offset, hour_start in enumerate(hour_starts):
        regular_packets = max(hourly_targets[hour_offset] - reserved_by_hour.get(hour_offset, 0), 0)
        plan.append({
            "hour_start": hour_start,
            "regular_packets": regular_packets,
            "target_packets": hourly_targets[hour_offset],
        })

    return plan, attack_windows


def choose_public_ip(faker_instance, preferred=None):
    if preferred:
        return random.choice(preferred)
    return faker_instance.ipv4_public()


def generate_packet(packet_id, faker_instance, blacklist=None, packet_timestamp=None, overrides=None):
    overrides = overrides or {}

    protocol = overrides.get(
        "protocol",
        random.choices(['TCP', 'UDP', 'ICMP'], weights=[0.7, 0.25, 0.05])[0],
    )
    version = overrides.get("version", random.choices([4, 6], weights=[0.95, 0.05])[0])
    data_length = overrides.get(
        "data_length",
        random.choices([16, 32, 64, 100, 128], weights=[0.2, 0.4, 0.2, 0.15, 0.05])[0],
    )


    def pick_ip(ip_version):
        use_blacklist = blacklist and random.random() < 0.05  # 5% chance
        if use_blacklist:
            return random.choice(blacklist)

        use_frequent = random.random() < .1192
        if use_frequent:
            return random.choice(COMMON_SOURCE_IPS + HOT_DESTINATIONS + ["127.0.0.1"])

        return faker_instance.ipv4_public() if ip_version == 4 else faker_instance.ipv6()

    src_ip = overrides.get("src_ip", pick_ip(version))
    dst_ip = overrides.get("dst_ip", pick_ip(version))

    # Compose header fields for checksum (excluding checksum itself)
    header_fields = {
        "packet_id": str(packet_id),
        "version": version,
        "data_length": data_length,
        "protocol": protocol,
        "src_ip": src_ip,
        "dst_ip": dst_ip
    }
    # Serialize header for checksum
    header_str = json.dumps(header_fields, sort_keys=True)
    # Calculate checksum
    checksum = ones_complement_checksum(header_str)
    invalid_checksum = overrides.get("invalid_checksum")
    if invalid_checksum is None:
        invalid_checksum = random.random() < .05
    if invalid_checksum:
        checksum = 5

    # Now include checksum in header for length
    header_fields_with_checksum = dict(header_fields)
    header_fields_with_checksum["checksum"] = checksum
    header_str_with_checksum = json.dumps(header_fields_with_checksum, sort_keys=True)
    ip_header_length = len(header_str_with_checksum)

    src_port = overrides.get(
        "src_port",
        random.choices([
            80, 443, 22, 53, 8080, random.randint(1024, 65535)
        ], weights=[0.2, 0.4, 0.1, 0.1, 0.1, 0.1])[0],
    )
    dest_port = overrides.get(
        "dest_port",
        random.choices([
            80, 443, 22, 53, 8080, random.randint(1024, 65535)
        ], weights=[0.2, 0.4, 0.05, 0.05, 0.1, 0.2])[0],
    )
    control_flags = overrides.get(
        "control_flags",
        random.choices([
            'SYN', 'ACK', 'FIN', 'PSH', 'RST', 'URG', 'NONE'
        ], weights=[0.3, 0.3, 0.1, 0.1, 0.05, 0.05, 0.1])[0] if protocol == 'TCP' else 'NONE',
    )
    window_size = overrides.get(
        "window_size",
        random.choices([0, 1024, 4096, 8192, 65535], weights=[0.1, 0.2, 0.3, 0.2, 0.2])[0],
    )
    data = overrides.get("data", faker_instance.text(max_nb_chars=data_length) if data_length > 0 else '')
    timestamp = int(packet_timestamp.timestamp()) if packet_timestamp else int(time.time())
    return {
        "packet_id": str(packet_id),
        "version": version,
        "ip_header_length": ip_header_length,
        "data_length": data_length,
        "protocol": protocol,
        "checksum": checksum,
        "src_ip": src_ip,
        "dst_ip": dst_ip,
        "src_port": src_port,
        "dest_port": dest_port,
        "control_flags": control_flags,
        "window_size": window_size,
        "data": data,
        "timestamp": timestamp
    }


def generate_regular_packets(start_packet_id, faker_instance, blacklist, hourly_plan):
    packets = []
    packet_id = start_packet_id

    for hour_plan in hourly_plan:
        hour_start = hour_plan["hour_start"]
        for _ in range(hour_plan["regular_packets"]):
            second_offset = random.randint(0, 3599)
            packet_time = hour_start + timedelta(seconds=second_offset)
            packet = generate_packet(
                packet_id,
                faker_instance,
                blacklist=blacklist,
                packet_timestamp=packet_time,
            )
            packets.append(packet)
            packet_id += 1

    return packets, packet_id


def generate_port_scan_packets(start_packet_id, faker_instance, attack_window):
    packets = []
    packet_id = start_packet_id
    src_ip = choose_public_ip(faker_instance, preferred=["45.77.23.19", "91.240.118.14"])
    dst_ip = random.choice(HOT_DESTINATIONS)
    dest_ports = random.sample(range(20, 65000), attack_window["count"])

    for index in range(attack_window["count"]):
        packet_time = attack_window["timestamp"] + timedelta(seconds=index % 60)
        packet = generate_packet(
            packet_id,
            faker_instance,
            blacklist=None,
            packet_timestamp=packet_time,
            overrides={
                "version": 4,
                "protocol": "TCP",
                "src_ip": src_ip,
                "dst_ip": dst_ip,
                "src_port": random.randint(20000, 65000),
                "dest_port": dest_ports[index],
                "control_flags": "ACK",
                "invalid_checksum": False,
                "data_length": random.choice([16, 32, 64]),
            },
        )
        packets.append(packet)
        packet_id += 1

    return packets, packet_id


def generate_syn_flood_packets(start_packet_id, faker_instance, attack_window):
    packets = []
    packet_id = start_packet_id
    dst_ip = random.choice(HOT_DESTINATIONS)
    unique_sources = attack_window.get("unique_sources", attack_window["count"])
    attacker_sources = [faker_instance.ipv4_public() for _ in range(unique_sources)]

    for index in range(attack_window["count"]):
        packet_time = attack_window["timestamp"] + timedelta(seconds=index % 60)
        packet = generate_packet(
            packet_id,
            faker_instance,
            blacklist=None,
            packet_timestamp=packet_time,
            overrides={
                "version": 4,
                "protocol": "TCP",
                "src_ip": attacker_sources[index % unique_sources],
                "dst_ip": dst_ip,
                "src_port": random.randint(15000, 65000),
                "dest_port": random.choice([80, 443, 8080]),
                "control_flags": "SYN",
                "window_size": random.choice([1024, 4096, 8192]),
                "invalid_checksum": False,
                "data_length": random.choice([16, 32]),
            },
        )
        packets.append(packet)
        packet_id += 1

    return packets, packet_id


def generate_attack_packets(start_packet_id, faker_instance, attack_windows):
    packets = []
    packet_id = start_packet_id

    for attack_window in attack_windows:
        if attack_window["kind"] == "port_scan":
            attack_packets, packet_id = generate_port_scan_packets(packet_id, faker_instance, attack_window)
        else:
            attack_packets, packet_id = generate_syn_flood_packets(packet_id, faker_instance, attack_window)
        packets.extend(attack_packets)

    return packets, packet_id


def get_state_file_path():
    return Path("data") / "last_generated_date.txt"


def read_last_generated_date(state_file_path):
    try:
        text = Path(state_file_path).read_text(encoding='utf-8').strip()
        if not text:
            return None
        return datetime.strptime(text, "%Y-%m-%d").date()
    except FileNotFoundError:
        return None
    except Exception as exc:
        logger.warning("Could not read state file %s: %s", state_file_path, exc)
        return None


def write_last_generated_date(state_file_path, date_value):
    path = Path(state_file_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(date_value.strftime("%Y-%m-%d"), encoding='utf-8')


def produce_packets(bootstrap_servers = 'kafka:9092', blacklist_path = 'data/blacklist.txt'):
    producer = PacketProducer(bootstrap_servers)
    # Load blacklist.txt
    try:  
        with open(blacklist_path, 'r', encoding='utf-8') as f:
            blacklist = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        blacklist = None
        logger.warning(
            "Blacklist '%s' could not be found. Potentially malicious packets will not be dropped.",
            blacklist_path,
        )


    faker_instance = Faker()
    Faker.seed(42)
    random.seed(42)

    # Determine starting date based on persistent state file. Each run moves one day forward.
    state_file_path = get_state_file_path()
    last_date = read_last_generated_date(state_file_path)
    if last_date is None:
        # first run starts at yesterday to keep first generated day as today
        last_date = datetime.utcnow().date() - timedelta(days=1)

    next_date = last_date + timedelta(days=1)
    start_hour = datetime(next_date.year, next_date.month, next_date.day)

    logger.info("Starting packet generation for day %s", next_date)

    hourly_plan, attack_windows = build_regular_hourly_plan(start_hour)

    regular_packets, next_packet_id = generate_regular_packets(10000, faker_instance, blacklist, hourly_plan)
    attack_packets, _ = generate_attack_packets(next_packet_id, faker_instance, attack_windows)

    all_packets = regular_packets + attack_packets
    all_packets.sort(key=lambda packet: (packet["timestamp"], int(packet["packet_id"])))

    # Persist with this date (the last generated day), so next run will use next day.
    write_last_generated_date(state_file_path, next_date)


    for index, packet in enumerate(all_packets, start=1):
        producer.send_packet(packet)
        time.sleep(.01)

        if index % 1000 == 0:
            logger.info("Queued %s/%s packets", index, len(all_packets))

    producer.producer.flush()
    logger.info("Finished queuing %s packets", len(all_packets))


produce_packets()
 