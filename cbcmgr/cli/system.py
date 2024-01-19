##
##

import logging
import platform
import os
try:
    import resource as res
except ImportError:
    res = None

logger = logging.getLogger('cbcmgr.cli.system')
logger.addHandler(logging.NullHandler())


class SysInfo(object):

    def __init__(self):
        self.os_type = platform.system()

    @staticmethod
    def get_proc_fs(parameter):
        value = None

        path_prefix = '/proc/sys/'
        path_suffix = parameter.replace('.', '/')
        search_path = path_prefix + path_suffix

        try:
            with open(search_path, 'r') as proc_file:
                line = proc_file.read()
                value = line.split()[-1]
            proc_file.close()
        except OSError:
            pass

        return int(value)

    @staticmethod
    def get_mac_sysctl(parameter):
        value = None

        for line in os.popen('sysctl -a'):
            line = line.strip()
            if line.startswith(parameter):
                value = line.split(':')[-1]
                value = value.lstrip()

        return value

    def get_net_buffer(self):
        value = None

        if self.os_type == 'Linux':
            value = self.get_proc_fs('net.ipv4.tcp_wmem')
        elif self.os_type == 'Darwin':
            value = self.get_mac_sysctl('kern.ipc.maxsockbuf')

        if value:
            return int(value)
        else:
            return None

    @staticmethod
    def raise_nofile(nofile=4096):
        if res is None:
            return

        soft, hard = res.getrlimit(res.RLIMIT_NOFILE)

        if soft < nofile:
            soft = nofile

            if hard < soft:
                hard = soft

            logger.debug(f"setting ulimit file descriptors {soft} {hard}")
            try:
                res.setrlimit(res.RLIMIT_NOFILE, (soft, hard))
            except (ValueError, res.error):
                try:
                    hard = soft
                    logger.debug(f"trouble with max limit, retrying with soft,hard {soft},{hard}")
                    res.setrlimit(res.RLIMIT_NOFILE, (soft, hard))
                except Exception as err:
                    logger.error(f"failed to raise descriptor ulimit: {err}")
