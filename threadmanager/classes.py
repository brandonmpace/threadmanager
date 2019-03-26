#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
# Copyright (C) 2019 Brandon M. Pace
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

import concurrent.futures
import queue
import threading
import time

from .constants import *
from .exceptions import *


# TODO: Maybe subclass ThreadPoolExecutor and Future from concurrent.futures to add some basic time tracking
class TimedFuturePool(concurrent.futures.ThreadPoolExecutor):
    def submit(self, fn, *args, **kwargs):
        return super().submit(self, fn, *args, **kwargs)


class ThreadManager(object):
    """A class for managing, organizing and tracking threads"""
    def __init__(self):
        self._rlock = threading.RLock()
        # Queue for sending items to the ThreadLauncher instance
        self._submission_queue = queue.Queue()
        self._thread_launcher = None  # TODO: This will be an on-demand thread that spawns other threads for submitted items.
        self._thread_monitor = None

    def add(self, func, args=(), kwargs=None, get_ref=False):
        """Add a new thread for a callable"""
        pass  # TODO: Accept a pool identifier, check that self._thread_launcher is running, then add to its queue
        # TODO: when get_ref is True, return a reference to the Thread or Future

    def add_pool(self):
        """Add a new organizational pool for separating threads"""
        pass  # TODO: Determine if we should return a pool ID or allow users to specify the same string for .add()
        # TODO: Allow specifying a time as a float, that if threads run longer than it then it will be logged

    def _thread_monitor_is_running(self) -> bool:
        with self._rlock:
            return self._thread_monitor and self._thread_monitor.is_alive()


class ThreadLauncher(threading.Thread):
    """A thread that launches and manages other threads"""
    def __init__(self, master: ThreadManager, input_queue: queue.Queue, out_queue: queue.Queue, group=None, target=None, name=None, args=(), kwargs=None, safe=True):
        """Initialize a thread with added 'safe' boolean parameter. When True, exceptions will be caught."""
        super().__init__(group=group, target=target, name=name, args=args, kwargs=kwargs)
        self._input_queue = input_queue
        self._output_queue = out_queue
        self_master = master
        self._safe = safe

    def run(self):
        while True:
            try:
                thread_request = self._input_queue.get(True, 30)  # TODO: consider adding a function to set the timeout in ThreadManager
                # TODO: Launch the thread with returned info
                # TODO: If the thread_info indicates a return is needed, put the thread/future reference into the output queue
                # Indicate we've completed the request, so .join() can be used on the queue
                self._input_queue.task_done()
            except queue.Empty:
                break


class ThreadPool(object):
    """Internal class that is an abstraction layer for ThreadPoolExecutor and other internal pools"""
    # TODO: This will keep the name/identifier, etc.
    pass


class ThreadRequest(object):
    """Internal class that is used for queueing requests"""
    # TODO: accept a ThreadPool, and whether or not a reference to the thread needs to be set
    # TODO: If a ref is needed, consumer will wait() on a Condition created during init
    pass


class TimedThread(threading.Thread):
    """
    A thread that tracks start and completion times for statistics.
    There are some API similarities to Future objects to simplify other parts of this package.
    """
    def __init__(self, group=None, target=None, name=None, args=(), kwargs=None, safe=True):
        """Initialize a thread with added 'safe' boolean parameter. When True, exceptions will be caught."""
        super().__init__(group=group, target=target, name=name, args=args, kwargs=kwargs)
        self._rlock = threading.RLock()
        self._condition = threading.Condition(self._rlock)
        self._args = args
        self._kwargs = kwargs
        self._exception = None
        self._result = None
        self._safe = safe
        self._state = INITIALIZED
        self._target = target
        self._time_started: float = 0.0
        self._time_completed: float = 0.0

    def done(self):
        with self._condition:
            return self._state == COMPLETED

    def exception(self, timeout: float = None):
        with self._condition:
            if self.done():
                return self._exception
            else:
                self._condition.wait(timeout)

                if self.done():
                    return self._exception
                else:
                    raise WaitTimeout

    def result(self, timeout: float = None):
        with self._condition:
            if self.done():
                if self._exception:
                    raise self._exception
                return self._result
            else:
                self._condition.wait(timeout)

                if self.done():
                    if self._exception:
                        raise self._exception
                    return self._result
                else:
                    raise WaitTimeout

    def run(self):
        with self._condition:
            self._time_started = time.time()
        try:
            result = self._target(*self._args, **self._kwargs)
        except BaseException as E:
            if self._safe:
                with self._condition:
                    self._exception = E
            else:
                raise
        else:
            with self._condition:
                self._result = result
        finally:
            with self._condition:
                self._time_completed = time.time()
                # Release all threads that are waiting on exception or result, etc.
                self._condition.notify_all()

    def total_time(self, timeout: float = None) -> float:
        with self._condition:
            if self.done():
                return self._time_completed - self._time_started
            else:
                self._condition.wait(timeout)

                if self.done():
                    return self._time_completed - self._time_started
                else:
                    raise WaitTimeout
