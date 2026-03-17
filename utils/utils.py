def ones_complement_checksum(data):
    data = data.encode()
    # Pad data to even
    if len(data) % 2:
        data += b'\x00'
    checksum = 0
    for i in range(0, len(data), 2):
        word = (data[i] << 8) + data[i+1]
        checksum += word
        # Wrap around carry
        checksum = (checksum & 0xFFFF) + (checksum >> 16)
    # One's complement
    checksum = ~checksum & 0xFFFF
    return checksum