#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
# Copyright (C) 2019, 2020 Brandon M. Pace
#
# This file is part of threadmanager
#
# threadmanager is free software: you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# threadmanager is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with threadmanager.
# If not, see <https://www.gnu.org/licenses/>.


"""This is an extremely basic example"""


import logcontrol
import logging
import threadmanager
import time


logfile = "example.log"
logger = logging.getLogger()
logcontrol.set_log_file(logfile, max_size=64000)
logcontrol.set_level(logging.DEBUG)


def continuous_func(work_time: float):
    """A function that repeats until a stop is requested"""
    return_value: int = 0
    while tm.go:
        log_time("continuous_func - doing work")
        time.sleep(work_time)
        return_value += 1
    log_time("returning from continuous_func() as go is False")
    return return_value


def fibonacci(input_number: int):
    n1 = 0
    n2 = 1
    count = 0
    if input_number <= 0:
        raise ValueError("Please enter a positive integer")
    elif input_number == 1:
        return [n1]
    else:
        return_values = []
        while count < input_number:
            return_values.append(n1)
            nth = n1 + n2
            # update values
            n1 = n2
            n2 = nth
            count += 1


def generate_exception():
    """Intentionally generate an exception"""
    a


def log_time(item: str):
    print(f"{time.time()} - {item}")
    logger.debug(item)


def long_running_func(name: str, line_count: int, chunk_size: int = 2):
    """
    Function that pretends to process text in chunks. (for real-world application the chunk size would likely be much larger)
    """
    number_of_lines_processed: int = 0
    for x in range(line_count):
        if number_of_lines_processed % chunk_size == 0:
            if tm.no_go:
                log_time(f"returning early from long_running_func '{name}' as no_go is True")
                return
            # else:
            #     log_time(f"not returning early from {name}")
        # else:
        #     log_time(f"{name} - doing more work")
        fibonacci(5000)  # simulate work/processing
        number_of_lines_processed += 1
    log_time(f"completed {name} with chunk_size: {chunk_size}")


def print_after(period: float):
    """print hello after given period of seconds"""
    time.sleep(period)
    log_time(f"hello (after {period})")


def main():
    global tm
    # logcontrol.log_to_console()
    threadmanager.enable_statistics()

    tm = threadmanager.ThreadManager("example", monitor_interval=1.0)

    tm.add_idle_callback(log_time, "went idle")
    tm.add_start_callback(log_time, "started")
    tm.add_stop_callback(log_time, "stopped")

    test_pool_name = "testpool"

    test_pool_controller = tm.add_pool(test_pool_name, threadmanager.THREAD, runtime_alert=1)
    # test_pool_controller = tm.add_pool(test_pool_name, threadmanager.FUTURE)
    # test_pool_controller.disable_tag_in_stats()
    # print(threadmanager.statistics.collect_stats_tables())
    log_time("adding threads")

    tm.add(test_pool_name, long_running_func, args=("first function", 1000), kwargs={"chunk_size": 20})

    second_item = tm.add(test_pool_name, continuous_func, args=(.5,), get_ref=True)

    tm.add(test_pool_name, generate_exception, tag="exc")

    time.sleep(1)

    log_time("calling .stop()")
    tm.stop()

    log_time(f"continuous_func result is: {second_item.result()} and it ran for {second_item.total_runtime()} seconds")
    # print(threadmanager.collect_pool_stats_table(specific_names=("testpool", )))
    # print(threadmanager.collect_thread_stats_table(specific_names=("testpool-continuous_func", )))
    # print(threadmanager.collect_stats_tables())
    # threadmanager.log_stats_tables()

    tm.shutdown()

    log_time("exiting")

    print(f"Check the logfile {logfile} for the example log output")


if __name__ == "__main__":
    main()
