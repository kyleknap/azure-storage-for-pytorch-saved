import argparse
import dataclasses
import os
import sys
import time

import psutil
from psutil._common import bytes2human

POLLING_INTERVAL = 1


@dataclasses.dataclass
class MetricRecord:
    timestamp: float
    mem_usage: float
    cpu_percent: float
    bytes_sent: int
    bytes_recv: int

    @classmethod
    def collect(cls, p: psutil.Process):
        # TODO: recursive collect this across grandchildren
        memory_used = p.memory_info().rss
        cpu_percent = p.cpu_percent()
        net = psutil.net_io_counters()
        return cls(
            timestamp=time.time(),
            mem_usage=memory_used,
            cpu_percent=cpu_percent,
            bytes_recv=net.bytes_recv,
            bytes_sent=net.packets_sent,
        )

    def compute_throughput(self, other: "MetricRecord"):
        time_delta = self.timestamp - other.timestamp
        bytes_sent_delta = self.bytes_sent - other.bytes_sent
        bytes_recv_delta = self.bytes_recv - other.bytes_recv
        return bytes_sent_delta/time_delta, bytes_recv_delta/time_delta


def benchmark(parsed_args):
    with psutil.Popen([sys.executable] + parsed_args.script_args) as process_to_measure:
        try:
            return _poll_for_metrics(process_to_measure, parsed_args.human_readable)
        except KeyboardInterrupt:
            _kill_child_processes(os.getpid())
            return 1


def _poll_for_metrics(p, human_readable=False):
    prev_metric = None
    while p.is_running():
        time.sleep(POLLING_INTERVAL)
        try:
            metric = MetricRecord.collect(p)
        except psutil.AccessDenied:
            # If process is closed, we can get access denied. So, exit from loop.
            break
        if prev_metric is not None:
            sent_rate, recv_rate = metric.compute_throughput(prev_metric)
            if human_readable:
                print(f"{metric.timestamp},{bytes2human(metric.mem_usage)},{metric.cpu_percent},{bytes2human(sent_rate)},{bytes2human(recv_rate)}")
            else:
                print(f"{metric.timestamp},{metric.mem_usage},{metric.cpu_percent},{sent_rate},{recv_rate}")
        prev_metric = metric
    return 0


def _kill_child_processes(pid):
    children = psutil.Process(pid).children(recursive=True)
    for child in children:
        child.terminate()

    _, alive = psutil.wait_procs(children, timeout=1)
    for child in alive:
        child.kill()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('script_args', nargs=argparse.REMAINDER)
    parser.add_argument('--human-readable', action='store_true')
    parsed_args = parser.parse_args()
    return benchmark(parsed_args)


if __name__ == '__main__':
    sys.exit(main())
