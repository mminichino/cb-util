##
##

import functools


def r_getattr(obj, path):
    def _getattr(o, s):
        return getattr(o, s)
    return functools.reduce(_getattr, [obj] + path.split('.'))


def omit_path(data: dict, keys: list):
    d = data.copy()
    for k in d.keys():
        if k in keys:
            del data[k]
            continue
        if type(d[k]) == dict:
            omit_path(data[k], keys)
        if type(d[k]) == list:
            for elem in d[k]:
                omit_path(elem, keys)
    return data
