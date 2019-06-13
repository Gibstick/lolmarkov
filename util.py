def try_config(config, heading, key):
    """Attempt to extract config[heading][key], with error handling.

    This function wraps config access with a try-catch to print out informative
    error messages and then exit."""
    try:
        section = config[heading]
    except KeyError:
        exit("Missing config section [{}]".format(heading))

    try:
        value = section[key]
    except KeyError:
        exit("Missing config key '{}' under section '[{}]'".format(
            key, heading))

    return value

