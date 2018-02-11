import os


def record(value):
    """ Returns stringified argument with a prepended record separator and
    a new line (https://tools.ietf.org/html/rfc8142).
    """
    return chr(0x1e) + '\n' + str(value)


def rm_if_exists(filename):
    try:
        os.remove(filename)
    except OSError:
        pass
