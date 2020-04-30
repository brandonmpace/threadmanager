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
import threading

from typing import DefaultDict, Deque, Optional
from .convenience import get_caller
from .log import create_logger


logger = create_logger(__name__)
_config_lock = threading.RLock()
_enabled = False
_recorded_stats: Optional[DefaultDict[str, Deque[float]]] = None
_running = False
_stat_history_size = 1000


def disable_statistics():
    global _enabled, _recorded_stats, _running
    with _config_lock:
        _running = False
        _enabled = False
        if _recorded_stats:
            _recorded_stats.clear()
            _recorded_stats = None
        logger.debug(f"disabled statistics. Caller: {get_caller()}")


def enable_statistics():
    global _enabled, _recorded_stats, _running
    with _config_lock:
        _recorded_stats = collections.defaultdict(_new_stats_deque)
        _enabled = True
        logger.debug(f"enabled statistics. Caller: {get_caller()}")
        _running = True


def record_statistics(pool_name: str, thread_name: str, runtime: float):
    # TODO: determine if lock is needed here
    if _running:
        _recorded_stats[pool_name].append(runtime)
        _recorded_stats[thread_name].append(runtime)


def set_history_size(size: int):
    """Set the number of items kept per unique record"""
    global _stat_history_size
    with _config_lock:
        disable_statistics()
        _stat_history_size = size
        enable_statistics()
        logger.debug(f"statistics history size changed to {size} by caller: {get_caller()}")


def statistics_enabled() -> bool:
    """Check to see if statistics are enabled for threadmanager"""
    return _enabled


def _new_stats_deque() -> Deque:
    return collections.deque(maxlen=_stat_history_size)
