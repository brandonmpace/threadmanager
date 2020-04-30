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


import collections
import logging
import prettytable
import statistics
import threading

from typing import DefaultDict, Deque, Dict, List, Optional, Tuple
from .convenience import get_caller
from .log import create_logger


logger = create_logger(__name__)

_config_lock = threading.RLock()

# Whether or not statistics are generally enabled
_enabled = False
# Whether or not collection of statistics is actually allowed (enabled but not running == paused)
_running = False

_pool_stats: Optional[DefaultDict[str, Deque[float]]] = None
_thread_stats: Optional[DefaultDict[str, Deque[float]]] = None

_stat_history_size = 1000


class StatSummary:
    min: Optional[float]
    max: Optional[float]
    mean: Optional[float]

    def __init__(self, name: str, data: Deque[float]):
        self.name = name
        if data:
            self.valid = True
            self.min = min(data)
            self.max = max(data)
            self.mean = statistics.mean(data)
        else:
            self.valid = False
            self.min = None
            self.max = None
            self.mean = None

    @property
    def average(self) -> Optional[float]:
        return self.mean

    def report_line(self) -> str:
        """A quick single-line format for logging or debugging"""
        if self.valid:
            return f"{self.name}: min({self.min}), max({self.max}), average({self.mean})"
        else:
            return f"{self.name}: [no data]"


def collect_pool_stats(specific_names: Tuple[str] = ()) -> List[StatSummary]:
    """Get StatSummary items representing the pool stats"""
    with _config_lock:
        return _collect_stats(_pool_stats, specific_names=specific_names)


def collect_pool_stats_table(prefix: str = "\nPOOL STATS:\n", specific_names: Tuple[str] = ()) -> str:
    """Get string form of a PrettyTable of pool stats"""
    with _config_lock:
        stats = collect_pool_stats(specific_names=specific_names)
        if stats:
            return f"{prefix}{generate_table_string(stats)}"
        else:
            return f"{prefix}[no pool stats available]"


def collect_thread_stats(specific_names: Tuple[str] = ()) -> List[StatSummary]:
    """Get StatSummary items representing the thread stats"""
    with _config_lock:
        return _collect_stats(_thread_stats, specific_names=specific_names)


def collect_thread_stats_table(prefix: str = "\nTHREAD STATS:\n", specific_names: Tuple[str] = ()) -> str:
    """Get string form of a PrettyTable of thread stats"""
    with _config_lock:
        stats = collect_thread_stats(specific_names=specific_names)
        if stats:
            return f"{prefix}{generate_table_string(stats)}"
        else:
            return f"{prefix}[no thread stats available]"


def collect_stats_tables() -> str:
    """Get string form of a PrettyTable of all stats"""
    with _config_lock:
        pool_stats_table = collect_pool_stats_table()
        thread_stats_table = collect_thread_stats_table()
    return pool_stats_table + "\n" + thread_stats_table + "\n"


def disable_statistics():
    """Disable collection of statistics"""
    global _enabled, _running, _pool_stats, _thread_stats
    with _config_lock:
        if _enabled:
            _running = False
            _enabled = False
            logger.debug(f"disabled statistics. Caller: {get_caller()}")
        else:
            logger.error(f"called while statistics are already disabled! Caller: {get_caller()}")


def enable_statistics():
    """Enable statistics and initialize the associated storage items"""
    global _enabled, _running, _pool_stats, _thread_stats
    with _config_lock:
        if _enabled:
            logger.error(f"called while statistics are already enabled! Caller: {get_caller()}")

        _pool_stats = collections.defaultdict(_new_stats_deque)
        _thread_stats = collections.defaultdict(_new_stats_deque)
        logger.debug(f"enabled statistics. Caller: {get_caller()}")
        _enabled = True
        _running = True


def generate_table_string(stats: List[StatSummary]) -> str:
    """Generate string result of PrettyTable representing the stats provided"""
    table = prettytable.PrettyTable(field_names=("name", "min", "max", "average"))
    for item in stats:
        table.add_row((item.name, item.min, item.max, item.mean))
    return table.get_string()


def log_pool_stats_table(level: int = logging.INFO):
    """Generate pool statistics tables and send to python logger"""
    tables_string = collect_pool_stats_table()
    logger.log(level, tables_string)


def log_thread_stats_table(level: int = logging.INFO):
    """Generate thread statistics table and send to python logger"""
    tables_string = collect_thread_stats_table()
    logger.log(level, tables_string)


def log_stats_tables(level: int = logging.INFO):
    """Generate statistics tables and send to python logger"""
    tables_string = collect_stats_tables()
    logger.log(level, tables_string)


def pause_statistics():
    """Leave statistics enabled and preserve collected data, but stop collection of new data"""
    global _enabled, _running
    with _config_lock:
        if statistics_paused():
            logger.error(f"called while statistics are already paused! Caller: {get_caller()}")
        elif _enabled and _running:
            _running = False
            logger.debug(f"paused statistics. Caller: {get_caller()}")
        elif _running:
            raise RuntimeError(f"bad state! Statistics are disabled but supposedly running! Caller: {get_caller()}")
        else:
            raise RuntimeError(f"called while statistics are disabled! Caller: {get_caller()}")


def record_statistics(pool_name: str, thread_name: str, runtime: float):
    """Internal function used to record statistics"""
    with _config_lock:
        if _running:
            _pool_stats[pool_name].append(runtime)
            _thread_stats[thread_name].append(runtime)


def reset_statistics():
    """Public function for resetting statistics"""
    with _config_lock:
        if _enabled:
            disable_statistics()
            _clear_statistics()
            enable_statistics()
        else:
            _clear_statistics()
        logger.debug(f"reset statistics. Caller: {get_caller()}")


def resume_statistics():
    """Continue collection of statistics. To be used after calling pause_statistics()"""
    global _running
    with _config_lock:
        if statistics_paused():
            _running = True
        else:
            raise RuntimeError(f"called while statistics were not in paused state! Caller: {get_caller()}")


def set_history_size(size: int):
    """Set the number of items kept per unique record"""
    global _stat_history_size
    with _config_lock:
        disable_statistics()
        _stat_history_size = size
        _clear_statistics()
        enable_statistics()
        logger.debug(f"statistics history size changed to {size} by caller: {get_caller()}")


def statistics_enabled() -> bool:
    """Check to see if statistics are enabled for threadmanager"""
    return _enabled


def statistics_paused() -> bool:
    """Whether or not statistics are in a paused state"""
    with _config_lock:
        return _enabled and (not _running)


def _clear_statistics():
    """Internal function to release existing statistics data"""
    global _enabled, _running, _pool_stats, _thread_stats
    with _config_lock:
        if _enabled or _running:
            raise RuntimeError(f"called while statistics were enabled! Caller: {get_caller()}")
        if _pool_stats:
            _pool_stats.clear()
            _pool_stats = None
        if _thread_stats:
            _thread_stats.clear()
            _pool_stats = None


def _collect_stats(data_dict: Dict[str, Deque[float]], specific_names: Tuple[str] = ()) -> List[StatSummary]:
    """Internal function to convert data sets into StatSummary items"""
    collected_data = []
    if specific_names:
        for specific_name in specific_names:
            if specific_name in data_dict:
                collected_data.append(StatSummary(specific_name, data_dict[specific_name]))
            else:
                logger.error(f"provided name does not exist in dataset: '{specific_name}'")
    else:
        for name, data in data_dict.items():
            collected_data.append(StatSummary(name, data))
    return collected_data


def _new_stats_deque() -> Deque:
    """Internal function to generate a properly-sized deque"""
    return collections.deque(maxlen=_stat_history_size)
