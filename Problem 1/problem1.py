# Implement a program that will launch a specified process and periodically (with a provided time
# interval) collect the following data about it:
# - CPU usage (percent);
# - Memory consumption: Working Set and Private Bytes (for Windows systems) or Resident Set Size and Virtual Memory Size
#   (for Linux systems);
# - Number of open handles (for Windows systems) or file descriptors (for Linux systems).
# Data collection should be performed all the time the process is running. Path to the executable file for the process
# and time interval between data collection iterations should be provided by user. Collected data should be stored
# on the disk. Format of stored data should support automated parsing to potentially allow, for example, drawing of
# charts.

import argparse
import pathlib
import sys
import platform
import typing
import psutil
import atexit
from datetime import datetime


def create_parser() -> argparse.ArgumentParser:
    def positive_float(s: str) -> float:
        i = float(s)
        if i <= 0:
            raise argparse.ArgumentTypeError("Time interval must be a positive float!")
        else:
            return i

    def executable_path(s: str) -> pathlib.Path:
        path = pathlib.Path(s)
        if not path.is_file():
            raise argparse.ArgumentTypeError("Path to executable is not valid!")
        else:
            return path.resolve()

    par = argparse.ArgumentParser(description="A simple utility that runs and monitors a process.")
    par.add_argument('-i', '--interval', type=positive_float, default=1, help="Data collection interval in seconds")
    par.add_argument('-l', '--log', type=argparse.FileType('w'), default="run_log.csv", help="Log output filepath")
    par.add_argument('-v', '--verbose', action='store_true', help="Prints Log to console")
    par.add_argument('executable', type=executable_path, help="Path to process executable")

    return par


def run_process(pth: pathlib.Path) -> psutil.Popen:
    try:
        pop = psutil.Popen(pth)
    except Exception as e:
        print(e)
        sys.exit(1)
    else:
        return pop


def collect_data(proc: psutil.Popen, interval: float, windows_platform: bool) -> dict:
    res = dict()
    with p.oneshot():
        res["cpu"] = proc.cpu_percent(interval)  # CPU utilization in percent
        res["memory"] = proc.memory_info()
        if windows_platform:
            res["files"] = proc.num_handles()
        else:
            res["files"] = proc.num_fds()

    return res


def write_header(f: typing.TextIO, windows_platform: bool):
    """Create header of the csv file"""
    f.write(
        f"timestamp,cpu,{'working_set' if windows_platform else 'rss'},{'private_bytes' if windows_platform else 'vms'}"
        f",{'open_handles' if windows_platform else 'file_descriptors'}\n")
    f.flush()


def write_line(f: typing.TextIO, d: dict):
    timestamp = datetime.now().timestamp()
    cpu = d['cpu']
    wss = data['memory'][0]
    rss = data['memory'][1]
    fil = data['files']
    f.write(f"{timestamp},{cpu},{wss},{rss},{fil}\n")
    f.flush()


def stop_process(proc: psutil.Popen):
    print("Stopping executable...")
    proc.kill()


if __name__ == '__main__':
    parser = create_parser()
    args = parser.parse_args()

    if platform.system() == 'Windows':
        win = True
        print("Windows platform detected")
    elif platform.system() == 'Linux':
        win = False
        print("Linux platform detected")
    else:
        win = False
        print(
            f"Platform \'{platform.system()}\' is not supported, defaulting to Linux methods, may not work as expected")

    print(f"Starting executable from path {args.executable}")
    p = run_process(args.executable)

    # Exit handler to kill the started executable
    atexit.register(stop_process, p)

    write_header(args.log, win)

    print('Collecting data...')
    while True:
        try:
            data = collect_data(p, args.interval, win)
        except psutil.NoSuchProcess:
            print(f"Process exited, stopping data collection.")
            sys.exit()
        else:
            if args.verbose:
                print(
                    f"{datetime.now()}: CPU={data['cpu']}, WS/RSS={data['memory'][0]}, PB/VMS={data['memory'][1]}, "
                    f"OH/FD={data['files']}")
            write_line(args.log, data)
