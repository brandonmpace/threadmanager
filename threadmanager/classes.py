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

from typing import Any, Iterable, Mapping

from .constants import *
from .exceptions import *


# TODO: Maybe subclass ThreadPoolExecutor and Future from concurrent.futures to add some basic time tracking
class TimedFuturePool(concurrent.futures.ThreadPoolExecutor):
    def submit(self, fn, *args, **kwargs):
        return super().submit(self, fn, *args, **kwargs)


class ThreadManager(object):
    """A class for managing, organizing and tracking threads"""
    def __init__(self, safe=True):
        self._rlock = threading.RLock()
        self._launcher_lock = threading.Lock()
        self._running = False
        self._stop_requested = False
        # Queue for sending items to the ThreadLauncher instance
        self._submission_queue = queue.Queue()
        self._thread_launcher = None
        self._thread_monitor = None
        self._run_thread_launcher()

    def add(self, pool_name: str, func, args=(), kwargs=None, get_ref=False):
        """Add a new thread for a callable"""
        with self._rlock:
            if self._stop_requested:
                return
            # Keep the ThreadLauncher from exiting while we create and send the request, in case it times out now
            with self._launcher_lock:
                self._running = True
                # TODO: Get the pool via identifier, check that self._thread_launcher is running, then add to its queue
                thread_request = ThreadRequest(None, func, args, kwargs, get_ref)
                self._submission_queue.put(thread_request)
            self._run_thread_launcher()
        if get_ref:
            return thread_request.get_thread()

    def add_pool(self):
        """Add a new organizational pool for separating threads"""
        pass  # TODO: Determine if we should return a pool ID or allow users to specify the same string for .add()
        # TODO: Allow specifying a time as a float, that if threads run longer than it then it will be logged

    def go(self) -> bool:
        """Can be used in functions running in threads to determine if they should keep going"""
        with self._rlock:
            return not self._stop_requested

    def no_go(self) -> bool:
        """Can be used in functions running in threads to determine if they should stop"""
        with self._rlock:
            return self._stop_requested

    def stop(self):
        """Prevent new threads from being started"""
        # TODO: After becoming idle, be sure to set _stop_requested back to False
        with self._rlock:
            self._running = False
            self._stop_requested = True

    def _run_thread_launcher(self):
        with self._rlock:
            if self._thread_launcher_is_running():
                return
            else:
                self._thread_launcher = ThreadLauncher(self, self._submission_queue, self._launcher_lock)

    def _thread_launcher_is_running(self) -> bool:
        with self._rlock:
            return self._thread_launcher and self._thread_launcher.is_alive()

    def _thread_monitor_is_running(self) -> bool:
        with self._rlock:
            return self._thread_monitor and self._thread_monitor.is_alive()


class ThreadLauncher(threading.Thread):
    """A thread that launches and manages other threads"""
    def __init__(self, master: ThreadManager, input_queue: queue.Queue, pending_lock: threading.Lock, group=None, target=None, name=None, args: Iterable = (), kwargs: Mapping[str, Any] = None, safe=True):
        """Initialize a thread with added 'safe' boolean parameter. When True, exceptions will be caught."""
        super().__init__(group=group, target=target, name=name, args=args, kwargs=kwargs)
        self._input_queue = input_queue
        self._master = master
        self._pending_lock = pending_lock
        self._safe = safe
        self.start()

    def run(self):
        while True:
            try:
                thread_request = self._input_queue.get(True, 30)  # TODO: consider adding a function to set the timeout in ThreadManager
                print(thread_request)
                # TODO: Launch the thread with returned info
                if thread_request.get_ref:
                    thread_request.set_thread('test')
                # Indicate we've completed the request, so .join() can be used on the queue
                self._input_queue.task_done()
            except queue.Empty:
                # blocks if an item is being created to add to our queue
                with self._pending_lock:
                    if self._input_queue.empty():
                        break


class ThreadMonitor(threading.Thread):
    """A thread that monitors other threads and gathers statistics when they are enabled"""
    pass


class ThreadPool(object):
    """Internal class that is an abstraction layer for ThreadPoolExecutor and other internal pools"""
    # TODO: This will keep the name/identifier, whether or not threads in the pool are daemonic etc.
    pass


class ThreadRequest(object):
    """Internal class that is used for queueing requests and returning thread objects when requested"""
    def __init__(self, pool: ThreadPool, func, args: Iterable, kwargs: Mapping[str, Any], get_ref: bool):
        self._rlock = threading.RLock()
        self._condition = threading.Condition(self._rlock)
        self.pool = pool
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.get_ref = get_ref
        self._thread = None

    def get_thread(self, timeout: float = None):
        with self._condition:
            if self._thread:
                return self._thread
            else:
                self._condition.wait(timeout)

                if self._thread:
                    return self._thread
                else:
                    raise WaitTimeout

    def set_thread(self, thread_item):
        """For internal use by ThreadLauncher"""
        with self._condition:
            self._thread = thread_item
            # Inform any waiting threads that the item is ready
            self._condition.notify_all()


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
            self._state = RUNNING
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
                self._state = COMPLETED
                # Release all threads that are waiting on exception or result, etc.
                self._condition.notify_all()

    def running(self):
        with self._condition:
            return self._state == RUNNING

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
