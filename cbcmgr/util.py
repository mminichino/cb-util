##
##

import getpass
import functools
import copy
import multiprocessing


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
        if type(d[k]) is dict:
            omit_path(data[k], keys)
        if type(d[k]) is list:
            for elem in d[k]:
                omit_path(elem, keys)
    return data


def copy_path(path: str, data: dict):
    parts = path.split('.')
    if parts[0] in data:
        if parts[0] == parts[-1]:
            return copy.deepcopy(data[parts[0]])
        else:
            return copy_path('.'.join(parts[1:]), data[parts[0]])
    elif len(parts) == 1:
        return {}


def ask_for_password():
    while True:
        pass_answer = getpass.getpass(prompt="Password: ")
        pass_answer = pass_answer.rstrip("\n")

        check_answer = getpass.getpass(prompt="Re-enter password: ")
        check_answer = check_answer.rstrip("\n")
        if pass_answer == check_answer:
            return pass_answer
        else:
            print("[!] Passwords do not match, please try again ...")


def progress_bar(iteration, total, decimals=1, length=100, fill='#', errors=0, ops_per_sec=0.0, end="\r"):
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filled = int(length * iteration // total)
    bar = fill * filled + '-' * (length - filled)
    print(f'\rProgress: |{bar}| {percent}% Complete - Errors: {errors} Ops/s: {ops_per_sec:.1f}', end=end)
    if iteration == total:
        print()


def progress_count(count, finished=False, errors=0, ops_per_sec=0.0, end="\r"):
    print(f'\rProgress: | {count:>20,} Documents | Errors: {errors} Ops/s: {ops_per_sec:.1f}', end=end)
    if finished:
        print()


class MPValue(object):

    def __init__(self, i=0):
        self.count = multiprocessing.Value('i', i)

    def increment(self, i=1):
        with self.count.get_lock():
            self.count.value += i

    def decrement(self, i=1):
        with self.count.get_lock():
            self.count.value -= i

    def reset(self, i=0):
        with self.count.get_lock():
            self.count.value = i

    @property
    def next(self):
        with self.count.get_lock():
            self.count.value += 1
        return self.count.value

    @property
    def value(self):
        return self.count.value
