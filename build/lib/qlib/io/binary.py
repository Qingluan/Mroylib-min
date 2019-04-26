
def string2bit(string, charset='utf8'):
    """
    translate string to bit list
    """
    s = string.encode(charset)
    for by in s:
        for i in range(8):
           yield  (by >> (7 - i)) & 0x01


def bit2bytes(bits):
    
    count = 0
    b = []
    tmp_b = 0
    for bit in bits:
        count += 1
        tmp_b += bit * (1 << ( 8 - count))
        if count == 8:
            count = 0
            b.append(tmp_b)
            tmp_b = 0
    return bytes(b)