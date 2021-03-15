

def time_to_string(seconds):
    """
    Convert a number of minutes to days, hours, minutes, seconds
    """

    days = hours = minutes = sec = 0
    ret = ""

    if seconds == 0:
        return "0 seconds"

    sec = seconds % 60
    if sec:
        ret = "%d sec" % sec

    if seconds <= 60:
        return ret

    tmp = (seconds - sec) / 60
    min = tmp % 60

    if min > 0:
        ret = "%d min " % min + ret

    if tmp <= 60:
        return ret.strip()

    tmp = tmp / 60
    hour = tmp % 24
    if hour > 0:
        ret = "%d hours " % hour + ret

    if tmp <= 24:
        return ret.strip()

    days = tmp / 24

    return ("%d days " % days + ret).strip()


def bytes_to_string(bytes):
    """
    Returns a human readable string (e.g. 123Mb) for a number of bytes
    """

    bytes = int(bytes)

    knownSizes = [("PB", 1024 * 1024 * 1024 * 1024 * 1024),
                  ("TB", 1024 * 1024 * 1024 * 1024),
                  ("GB", 1024 * 1024 * 1024),
                  ("MB", 1024 * 1024),
                  ("KB", 1024)]

    for (type, size) in knownSizes:
        if bytes / size > 0.5:
            return "%0.1f%s" % (bytes / float(size), type)

    return "%d Bytes" % int(bytes)

if __name__ == "__main__":
    import sys
    print(timeToString(int(sys.argv[1]) * 60))

    print("Bytes:", bytesToString(241591910401230.0000))
