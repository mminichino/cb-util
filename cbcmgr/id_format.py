##
##

import re
from cbcmgr.util import MPValue

incr = MPValue()


def doc_id_format(format_string: str,
                  separator: str = "::",
                  number: int = 1,
                  text: str = "document",
                  keyspace: str = "default",
                  field: str = "key"):
    vector = list(re.search(regex, format_string) for regex in ['%n', '%t', '%k', '%f', '%i', '%s'])

    if vector[0]:
        format_string = format_string.replace('%n', str(number))
    if vector[1]:
        format_string = format_string.replace('%t', text)
    if vector[2]:
        format_string = format_string.replace('%k', keyspace)
    if vector[3]:
        format_string = format_string.replace('%f', field)
    if vector[4]:
        format_string = format_string.replace('%i', incr.next)
    if vector[5]:
        format_string = format_string.replace('%s', separator)

    return re.sub(r'%[a-z]', '', format_string)
