import re
from ast import Tuple
from typing import Tuple

from .._logger import Logger

log = Logger("UTIL")
log.log("Logger initialized for util.py.")


def extract_date(date_str: str) -> str | None:
    """
    Extracts a date from a string if it exists.

    This function uses regular expressions to search for a date in the format YYYY-MM-DD within a string.
    If a date is found, it is returned. If no date is found, None is returned.

    Args:
        date_str (str): The string to search for a date within.

    Returns:
        str | None: The date found in the string, or None if no date was found.
    """
    # Matches any digits in form of DDDD-DD-DD or DD-DD-DD
    # where 'D' is a digit.
    search_regex = r"\d{2,4}-\d{2}-\d{2}"
    search_res = re.search(search_regex, date_str)
    return search_res.group() if search_res else None


def extract_ATA(desc_str: str) -> Tuple[int, str] | int:
    """
    Extracts the ATA (Air Transport Association) code and the description from a string if it exists.

    This function uses regular expressions to search for a code in the format DD-DD-DD within a string,
    where 'D' is a digit. If a code is found, the first two digits and the description enclosed in parentheses
    are returned as a tuple. If no code is found, -1 is returned. If an error occurs during the process, -2 is returned.

    Args:
        desc_str (str): The string to search for an ATA code within.

    Returns:
        Tuple[int, str] | int: A tuple containing the ATA code and the description found in the string,
        -1 if no code was found, or -2 if an error occurred.
    """
    # Using a regex pattern to find the part of string that matches the pattern DD-DD-DD
    # where 'D' is a digit.
    try:
        regex = r"\d{2}-\d{2}-\d{2,4}"
        match = re.search(regex, desc_str)

        # Most of the entries have the ATA code as the first two digits of the above pattern.
        if match:
            return (
                int(match.group()[0:2]),
                desc_str[
                    desc_str.find("(") + 1 : desc_str.find(")")
                ],  # Credits: https://stackoverflow.com/a/4894156/22647897
            )
        else:
            return -1
    except Exception as e:
        log.log(f"Error: {e}")
        return -2
