import collections
import os
import io
import json
import time
import dataclasses
from typing import Optional

import torch
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import blobfile as bf

import azstoragetorch.io

ACCOUNT_NAME = "azstoragetorchdev"
CREDENTIAL = None


@dataclasses.dataclass
class FileOp:
    name: str
    args: list


@dataclasses.dataclass
class ReadOp(FileOp):
    name: str = dataclasses.field(default="read", init=False)


@dataclasses.dataclass
class SeekOp(FileOp):
    name: str = dataclasses.field(default="seek", init=False)


@dataclasses.dataclass
class TellOp(FileOp):
    name: str = dataclasses.field(default="tell", init=False)
    args: list = dataclasses.field(init=False)

    def __post_init__(self):
        self.args = []


@dataclasses.dataclass
class CloseOp(FileOp):
    name: str = dataclasses.field(default="close", init=False)
    args: list = dataclasses.field(init=False)

    def __post_init__(self):
        self.args = []


class FileIOSpy(io.IOBase):
    def __init__(self, underlying_file):
        self.underlying_file = underlying_file
        self.recorded_operations = []
        self.total_file_size = self._get_size()

    def read(self, size=-1):
        self.recorded_operations.append(ReadOp([size]))
        return self.underlying_file.read(size)

    def seekable(self):
        return self.underlying_file.seekable()

    def seek(self, offset, whence=io.SEEK_SET):
        self.recorded_operations.append(SeekOp([offset, whence]))
        return self.underlying_file.seek(offset, whence)

    def tell(self):
        self.recorded_operations.append(TellOp())
        return self.underlying_file.tell()

    def close(self):
        self.recorded_operations.append(CloseOp())
        return self.underlying_file.close()

    def _get_size(self):
        self.underlying_file.seek(0, io.SEEK_END)
        size = self.underlying_file.tell()
        self.underlying_file.seek(0)
        return size


def load_with_spy(filename):
    with open(filename, "rb") as f:
        spy = FileIOSpy(f)
        torch.load(spy, weights_only=True)
    return spy


def load_from_file(filename):
    with open(filename, "rb") as f:
        torch.load(filename, weights_only=True)


def load_from_filename(filename):
    torch.load(filename, weights_only=True)


def load_from_io(io_obj):
    torch.load(io_obj, weights_only=True)


def get_as_byteio(filename):
    with open(filename, "rb") as f:
        return io.BytesIO(f.read())

def get_as_blobio(filename):
    model_uri = f"https://{ACCOUNT_NAME}.blob.core.windows.net/models/{filename}"
    return azstoragetorch.io.BlobIO(model_uri, mode='rb', credential=CREDENTIAL)


def get_as_adlfs(filename):
    from adlfs import AzureBlobFileSystem
    fs = AzureBlobFileSystem(account_name=ACCOUNT_NAME, anon=False)
    return fs.open(f"abfs://models/{filename}", "rb")


def get_as_blobfile(filename):
    os.environ['AZURE_USE_IDENTITY'] = '1'
    return bf.BlobFile(f"az://{ACCOUNT_NAME}/models/{filename}", "rb")


def playback(io_object, playback_json):
    for op in playback_json:
        getattr(io_object, op["name"])(*op["args"])


def get_total_read(spy):
    return sum(
        op.args[0]
        for op in filter_ops(spy, op_type="read")
    )

def get_op_counts(spy, op_type="read"):
    counts = collections.defaultdict(int)
    for op in spy.recorded_operations:
        if op.name == op_type:
            counts[op.args[0]] += 1
    read_amounts = sorted(counts, key=lambda x: x)
    return {k: counts[k] for k in read_amounts}

def is_seeks_increasing(spy):
    seeks = [op.args[0] for op in filter_ops(spy, op_type="seek")]
    return seeks == sorted(seeks)

def filter_ops(spy, op_type="read"):
    return [op for op in spy.recorded_operations if op.name == op_type]

def plot_spy(spy):
    fig = plt.figure(figsize=(24, 12))
    gs = fig.add_gridspec(5, 1)  # Create a 2x3 grid

    ax1 = fig.add_subplot(gs[0, :])
    ax2 = fig.add_subplot(gs[1, :])
    ax5 = fig.add_subplot(gs[2, :])
    ax3 = fig.add_subplot(gs[3, :])
    ax4 = fig.add_subplot(gs[4, :])


    formatter = mticker.ScalarFormatter(useOffset=False, useLocale=True)
    formatter.set_scientific(False)
    ax1.yaxis.set_major_formatter(formatter)
    ax2.yaxis.set_major_formatter(formatter)
    import locale
    locale.setlocale(locale.LC_ALL, '')

    seek_x, seek_y, read_x, read_y = get_seeks_and_reads_points(spy)

    ax1.plot(seek_x, seek_y, '.', color='red')
    for i in range(0, len(read_x) - 1, 2):
        ax1.plot(read_x[i:i + 2], read_y[i:i + 2], linestyle='-', color='blue')

    bar_read_y = []
    for i in range(len(read_y)):
        if i % 2 == 0:
            bar_read_y.append(read_y[i + 1] - read_y[i])
        else:
            bar_read_y.append(0)
    ax2.bar(read_x, bar_read_y, color='orange')

    bar_skipped_y = []
    for i in range(len(read_y)):
        if i == 0 or i % 2 == 1:
            bar_skipped_y.append(0)
        else:
            bar_skipped_y.append(read_y[i] - read_y[i - 1])
    ax5.bar(read_x, bar_skipped_y, color='red')

    read_counts = get_op_counts(spy)
    ax3.bar(range(len(read_counts.keys())), read_counts.values(), color='green')
    ax3.set_xticks(range(len(read_counts.keys())))
    ax3.set_xticklabels(read_counts.keys())

    skipped_amounts = [read["total_skipped"] for read in get_contiguous_reads(spy) if read["total_skipped"]]
    skipped_counts = collections.Counter(skipped_amounts)
    skipped_amounts = sorted(skipped_counts, key=lambda x: x)
    skipped_counts = {k: skipped_counts[k] for k in skipped_amounts}
    ax4.bar(range(len(skipped_counts.keys())), skipped_counts.values(), color='purple')
    ax4.set_xticks(range(len(skipped_counts.keys())))
    ax4.set_xticklabels(skipped_counts.keys())
    left_limit = 0
    for i, skipped_amount in enumerate(skipped_amounts):
        if skipped_amount > 0:
            left_limit = i
            break
    ax4.set_xlim(left=left_limit)

    ax1.set_xlabel('Operation Index')
    ax1.set_ylabel('Position / Size')
    ax1.set_title('Seeks and Reads on File')

    ax2.set_xlabel('Operation Index')
    ax2.set_ylabel('Read Size')
    ax2.set_title('Read Sizes per operation')
    ax2.set_yscale('log')

    ax5.set_xlabel('Operation Index')
    ax5.set_ylabel('Skipped between reads')
    ax5.set_title('Skipped amount between reads')
    ax5.set_yscale('log')

    ax3.set_xlabel('Read Size')
    ax3.set_ylabel('Number of occurrences')
    ax3.set_title('Read size distribution')

    ax4.set_xlabel('Amount skipped between reads')
    ax4.set_ylabel('Number of occurrences')
    ax4.set_title('Skipped amount distribution')


    fig.set_size_inches(24, 12)
    plt.subplots_adjust(hspace=0.5)
    plt.show()

def get_seeks_and_reads_points(spy):
    operation_index = 0
    current_position = 0
    seek_x = []
    seek_y = []
    read_x = []
    read_y = []
    for op in spy.recorded_operations:
        if op.name == "seek":
            seek_x.append(operation_index)
            seek_y.append(op.args[0])
            operation_index += 1
            current_position = op.args[0]
        if op.name == "read":
            read_x.append(operation_index-1)
            read_y.append(current_position)
            current_position += op.args[0]
            read_x.append(operation_index)
            read_y.append(current_position)
            operation_index += 1
    return seek_x, seek_y, read_x, read_y


def display_read_stats(spy):
    stats = get_read_stats(spy)
    print(json.dumps(stats, indent=2))


def get_read_stats(spy):
    stats = {}
    stats["file_size"] = spy.total_file_size
    stats["total_read"] = get_total_read(spy)
    stats["continuous_reads"] = get_contiguous_reads(spy)
    return stats


def record_playback(spy, filename):
    with open(f"{filename}_recorded.json", "w") as f:
        serialized = [dataclasses.asdict(op) for op in spy.recorded_operations]
        json.dump(serialized, f, indent=2)


@dataclasses.dataclass
class ContiguousRead:
    start: int
    end: int
    num_reads: int
    total_skipped: Optional[int] = None

    @property
    def size(self):
        return self.end - self.start


def get_contiguous_reads(spy):
    current_position = 0
    current_contiguous = None
    contiguous_reads = []
    for op in spy.recorded_operations:
        if op.name == "read":
            if current_contiguous is None:
                current_contiguous = ContiguousRead(current_position, current_position + op.args[0], 1)
            if current_contiguous:
                if current_position == current_contiguous.end:
                    current_contiguous.end = current_position + op.args[0]
                    current_contiguous.num_reads += 1
                else:
                    _add_contiguous_read(current_contiguous, contiguous_reads)
                    current_contiguous = ContiguousRead(
                        current_position, current_position + op.args[0], 1, current_position - current_contiguous.end)
            current_position += op.args[0]
        if op.name == "seek":
            current_position = op.args[0]
    if current_contiguous:
        _add_contiguous_read(current_contiguous, contiguous_reads)
    return contiguous_reads

def _add_contiguous_read(current_contiguous, contiguous_reads):
    contiguous_reads.append(
        {
            "start": current_contiguous.start,
            "end": current_contiguous.end,
            "num_reads": current_contiguous.num_reads,
            "size": current_contiguous.size,
            "total_skipped": current_contiguous.total_skipped
        }
    )



def get_elapsed_time(load_fn, *args):
    start = time.time()
    load_fn(*args)
    return time.time() - start

def benchmark_load_from_file(filename):
    print(get_elapsed_time(load_from_file, filename))

def benchmark_load_from_filename(filename):
    print(get_elapsed_time(load_from_filename, filename))

def benchmark_load_from_bytesio(filename):
    io_obj = get_as_byteio(filename)
    print(get_elapsed_time(load_from_io, io_obj))

def benchmark_load_from_blobio(filename):
    io_obj = get_as_blobio(filename)
    print(get_elapsed_time(load_from_io, io_obj))

def benchmark_load_from_adlfs(filename):
    io_obj = get_as_adlfs(filename)
    print(get_elapsed_time(load_from_io, io_obj))

def benchmark_load_from_blobfile(filename):
    io_obj = get_as_blobfile(filename)
    print(get_elapsed_time(load_from_io, io_obj))

def benchmark_playback_from_blobio(filename):
    io_obj = get_as_blobio(filename)
    json_playback = json.load(open(f"{filename}_recorded.json"))
    print(get_elapsed_time(playback, io_obj, json_playback))

def enable_logging():
    import logging
    import sys
    # Create a logger object for the Azure SDK
    logger = logging.getLogger('')
    logger.setLevel(logging.NOTSET)

    # Create a stream handler to output logs to stdout
    handler = logging.StreamHandler(stream=sys.stdout)
    logger.addHandler(handler)


def main():
    pass
    # enable_logging()
    # load_from_file("large_model.pth")
    #spy = load_with_spy("large_model.pth")
    # record_playback(spy, "large_model.pth")
    # display_read_stats(spy)
    #plot_spy(spy)
    #content = get_as_byteio("bert_model.pth")
    # content = get_as_blobio("bert_model.pth")
    # content = get_as_adlfs("bert_model.pth")
    # content = get_as_blobfile("bert_model.pth")
    # start = time.time()
    # print(get_elapsed_time(load_from_filename, "large_model.pth"))
    # print(get_elapsed_time(load_from_io, content))
    # print(get_elapsed_time(content.read))
    # #load_from_file("bert_model.pth")
    # print(time.time() - start)
    # benchmark_load_from_filename("large_model.pth")
    # benchmark_load_from_bytesio("large_model.pth")
    # benchmark_load_from_blobio("large_model.pth")
    # benchmark_load_from_adlfs("large_model.pth")
    # benchmark_load_from_blobfile("large_model.pth")
    benchmark_playback_from_blobio("large_model.pth")


if __name__ == "__main__":
    main()
