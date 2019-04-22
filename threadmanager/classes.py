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
import warnings

from typing import Any, Callable, Iterable, Mapping, Optional

from .convenience import get_caller, get_func_name
from .constants import *
from .exceptions import *
from .log import create_logger


_logger = create_logger(__name__)


class TimedFuture(concurrent.futures.Future):
    def __init__(self, func_name: str, runtime_alert: float):
        super().__init__()
        self._func_name = func_name
        self._runtime_alert = runtime_alert
        self._time_started: float = 0.0
        self._time_completed: float = 0.0

    def set_exception(self, exception):
        with self._condition:
            self._set_time_completed()
            _logger.exception("Future ended with an exception!")
            super().set_exception(exception)

    def set_result(self, result):
        with self._condition:
            self._set_time_completed()
            super().set_result(result)

    def set_running_or_notify_cancel(self) -> bool:
        with self._condition:
            self._time_started = time.time()
            if super().set_running_or_notify_cancel() is False:
                # Future was cancelled
                self._set_time_completed()
                return False
            else:
                return True

    def total_runtime(self, timeout: Optional[float] = None) -> float:
        with self._condition:
            if self.done():
                return self._time_completed - self._time_started
            else:
                self._condition.wait(timeout)

                if self.done():
                    return self._time_completed - self._time_started
                else:
                    raise concurrent.futures.TimeoutError()

    def _set_time_completed(self):
        self._time_completed = time.time()
        # TODO: log runtime statistics for self._func_name if enabled


class TimedFutureThreadPool(concurrent.futures.ThreadPoolExecutor):
    def __init__(self, *args, runtime_alert: float = 0.0, **kwargs):
        super().__init__(*args, **kwargs)
        self.runtime_alert = runtime_alert

    def submit(self, fn, *args, **kwargs):
        # Over-ridden to use TimedFuture instead
        with self._shutdown_lock:
            if self._shutdown:
                raise RuntimeError('cannot schedule new futures after shutdown')

            f = TimedFuture(get_func_name(fn), self.runtime_alert)
            w = concurrent.futures.thread._WorkItem(f, fn, args, kwargs)

            self._work_queue.put(w)
            self._adjust_thread_count()
            return f


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

    def cancel(self) -> bool:
        """
        Attempt to cancel the run of the thread.
        :return: bool True if successful
        """
        with self._condition:
            if self._state in NOCANCELSTATES:
                return False
            else:
                self._state = CANCELLED
                return True

    def cancelled(self) -> bool:
        with self._condition:
            return self._state == CANCELLED

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
            if self._state == INITIALIZED:
                self._state = RUNNING
                self._time_started = time.time()
            elif self._state == CANCELLED:
                return
            else:
                raise BadStateError("Thread is not in expected state in run method")
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

    def total_runtime(self, timeout: float = None) -> float:
        with self._condition:
            if self.done():
                return self._time_completed - self._time_started
            else:
                self._condition.wait(timeout)

                if self.done():
                    return self._time_completed - self._time_started
                else:
                    raise WaitTimeout


class ThreadLauncher(threading.Thread):
    """A thread that launches and manages other threads"""
    def __init__(
            self, master: 'ThreadManager', input_queue: queue.Queue, pending_lock: threading.Lock, timeout: float,
            group=None, target=None, name=None, args: Iterable = (), kwargs: Mapping[str, Any] = None, safe=True
    ):
        """Initialize a thread with added 'safe' boolean parameter. When True, exceptions will be caught."""
        super().__init__(group=group, target=target, name=name, args=args, kwargs=kwargs)
        self._input_queue = input_queue
        self._master = master
        self._pending_lock = pending_lock
        self._safe = safe
        self._timeout = timeout
        self.start()

    def run(self):
        while True:
            try:
                thread_request: Optional[ThreadRequest] = self._input_queue.get(True, self._timeout)

                if thread_request is None:
                    # shutdown was requested
                    self._input_queue.task_done()
                    return

                thread_request.submit()

                # Indicate we've completed the request, so .join() can be used on the queue
                self._input_queue.task_done()

            except queue.Empty:  # happens when the timeout is reached
                # the below lock blocks if an item is being created to add to our queue
                with self._pending_lock:
                    if self._input_queue.empty():
                        break

            except BaseException:
                if self._safe:
                    _logger.exception("TimedThread ended with exception!")
                else:
                    raise


class ThreadManager(object):
    """A class for managing, organizing and tracking threads"""
    def __init__(self, safe=True, thread_launcher_timeout: float = 30):
        self._rlock = threading.RLock()
        self._launcher_lock = threading.Lock()
        self._pools = {}
        self._safe = safe
        self._running = False
        self._shutdown = False
        self._stop_requested = False
        # Queue for sending items to the ThreadLauncher instance
        self._submission_queue = queue.Queue()
        self._thread_launcher: ThreadLauncher = None
        self._thread_launcher_runs = 0
        self._thread_launcher_timeout = thread_launcher_timeout
        self._thread_monitor: ThreadMonitor = None
        self._run_thread_launcher()

    def add(self, pool_name: str, func: Callable, args: Iterable = (), kwargs: Optional[Mapping[str, Any]] = None, get_ref: bool = False, timeout: Optional[float] = None):
        """
        Submit the callable to the requested pool
        :param pool_name: str
        :param func: Callable
        :param args: Iterable (optional, typically a tuple) Single argument needs a trailing comma. e.g. (5,)
        :param kwargs: Mapping (optional, typically a dict)
        :param get_ref: bool whether or not to return a reference to the Future
        :param timeout: float time to wait on getting the thread reference
        :return:
        """
        with self._rlock:
            if self._shutdown:
                raise RuntimeError("add called after ThreadManager shutdown method was used")

            if pool_name not in self._pools:
                raise ValueError(f"Pool does not exist with pool_name: {pool_name}")

            caller = get_caller()
            func_name = func.__qualname__ if hasattr(func, '__qualname__') else '<unknown function>'
            pool: ThreadPoolWrapper = self._pools[pool_name]

            if self._stop_requested and pool.obey_stop:
                msg = f"ThreadManager::add - NOT going to run {func_name} submitted by {caller} as stop was requested"
                if self._safe:
                    _logger.info(msg)
                    return
                else:
                    # TODO: Decide if I want to just raise an exception instead. Using a warning allows for simple
                    #  handling with warning control (see warnings module docs) and allows for cleaner code in the
                    #  calling function. I used a warning because there may be cases where someone wants to track down
                    #  an issue by setting safe to False, but wouldn't want this case to halt their program..
                    warnings.warn(msg, StopNotificationWarning)
                    return

            # Keep the ThreadLauncher from exiting while we create and send the request, in case it times out now
            with self._launcher_lock:
                if pool.state_updates_enabled and self._running is False:
                    _logger.debug("ThreadManager::add - setting running to True")
                    self._running = True
                thread_request = ThreadRequest(pool, func, args, kwargs, get_ref)
                self._submission_queue.put(thread_request)
            self._run_thread_launcher()
        if get_ref:
            return thread_request.get_thread(timeout=timeout)

    def add_pool(self, name: str, pool_type: str = THREAD, runtime_alert: float = 0, worker_count: Optional[int] = None) -> 'ThreadPoolController':
        """Add a new organizational pool for separating threads"""
        with self._rlock:
            if name in self._pools:
                raise ValueError(f"Pool already exists with name of {name}")
            new_pool = ThreadPoolWrapper(name, pool_type, runtime_alert, worker_count)
            self._pools[name] = new_pool
            return ThreadPoolController(new_pool)

    # TODO: I removed the lock usage from go and no_go because it can keep threads from exiting when shutdown is called.
    #  If lock is found to be necessary, make a second lock either for these or for the shutdown to avoid that issue.
    @property
    def go(self) -> bool:
        """Can be used in functions running in threads to determine if they should keep going"""
        return self._stop_requested is False

    @property
    def idle(self):
        """Can be used to identify when there are/aren't submitted threads still running"""
        return self._running is False

    @property
    def no_go(self) -> bool:
        """Can be used in functions running in threads to determine if they should stop"""
        return self._stop_requested

    def shutdown(self, wait: bool = True):
        # TODO: finish this.. shutdown all pools, which should join threads, etc.
        #  Set a flag or state that shutdown was called and block new adds..
        #  maybe allow cancel_all param or similar to cancel pending items
        with self._rlock:
            self._shutdown = True
            if self._running:
                self.stop()
            if self._thread_launcher_is_running():
                self._submission_queue.put(None)  # Signal to ThreadLauncher that we're shutting down.
                if wait:
                    self._submission_queue.join()
            for pool_item in self._pools.values():
                pool_item.shutdown(wait=wait)

    def stop(self) -> bool:
        """
        Prevent new threads from being started and allow thread using go and no_go to be aware when they check
        :return: bool False if already stopped (in that case a RuntimeError is raised if safe was False at creation)
        """
        # TODO: After becoming idle, be sure to set _stop_requested back to False
        with self._rlock:
            if self._running:
                self._running = False
                self._stop_requested = True
                return True
            elif self._safe:
                return False
            else:
                raise RuntimeError("stop called while already stopped!")

    def _run_thread_launcher(self):
        with self._rlock:
            if self._thread_launcher_is_running():
                return
            else:
                _name = f"threadlauncher{self._thread_launcher_runs}"
                self._thread_launcher = ThreadLauncher(self, self._submission_queue, self._launcher_lock, self._thread_launcher_timeout, name=_name, safe=self._safe)
                self._thread_launcher_runs += 1

    def _thread_launcher_is_running(self) -> bool:
        with self._rlock:
            return self._thread_launcher and self._thread_launcher.is_alive()

    def _thread_monitor_is_running(self) -> bool:
        with self._rlock:
            return self._thread_monitor and self._thread_monitor.is_alive()


class ThreadMonitor(threading.Thread):
    """A thread that monitors other threads and gathers statistics when they are enabled"""
    pass


class ThreadPool(object):
    """Internal class for grouping threads"""
    def __init__(self):
        # TODO: instantiate a queue, rlock, etc. Determine if there should be a single ThreadMonitor for all pools,
        #  or if each pool should have a ThreadMonitor handling the queue and gathering statistics..
        pass

    def submit(self, func, *args, **kwargs):
        pass

    def shutdown(self):
        pass


class ThreadPoolController(object):
    """Class allowing clients to control some attributes of ThreadPoolWrapper"""
    def __init__(self, pool: 'ThreadPoolWrapper'):
        self._pool = pool

    def disable_state_updates(self):
        """Prevent this pool from affecting ThreadManger's state (e.g. idle attribute)"""
        self._pool.state_updates_enabled = False

    def enable_state_updates(self):
        """Allow this pool to affect ThreadManger's state (e.g. idle attribute)"""
        self._pool.state_updates_enabled = True

    def ignore_stop(self):
        """Allow submission to this pool after stop was requested of ThreadManager"""
        self._pool.obey_stop = False

    def obey_stop(self):
        """Block submission to this pool after stop was requested of ThreadManager"""
        self._pool.obey_stop = True


class ThreadPoolWrapper(object):
    """Internal class that is an abstraction layer for ThreadPoolExecutor and internal pool types"""
    _supported_types = (FUTURE, THREAD)

    def __init__(self, name: str, pool_type: str, runtime_alert: float, worker_count: Optional[int] = None):
        """
        Initializes a ThreadPoolWrapper
        :param name: str name of the pool. This should be alphanumeric
        :param pool_type: str type of pool, options defined in constants
        :param runtime_alert: float amount of seconds. When threads run longer than this, it will be logged.
        :param worker_count: int max number of threads allowed to run at the same time
        """
        if name.isalnum() is False:
            raise ValueError("Pool name value contains unsupported characters. Hint: Use an alphanumeric string")
        if pool_type not in self._supported_types:
            raise ValueError(f"Unrecognized pool type: {pool_type}")

        self._name = name
        self._rlock = threading.RLock()
        self._runtime_alert = runtime_alert
        self._type = pool_type

        # Whether or not submissions are allowed in ThreadManager after stop was requested
        self.obey_stop = False

        # Whether or not ThreadManager will update its state when submitting to this pool
        self.state_updates_enabled = True

        if pool_type == FUTURE:
            self._pool = TimedFutureThreadPool(runtime_alert=runtime_alert, max_workers=worker_count, thread_name_prefix=name)
        elif pool_type == THREAD:
            raise NotImplementedError("TODO...")  # TODO: Create and use internal pool

    def shutdown(self, wait: bool = True):
        self._pool.shutdown(wait=wait)

    def submit(self, func: Callable, *args, **kwargs):
        return self._pool.submit(func, *args, **kwargs)


class ThreadRequest(object):
    """Internal class that is used for queueing requests and returning thread objects when requested"""
    def __init__(self, pool: ThreadPoolWrapper, func: Callable, args: Iterable, kwargs: Mapping[str, Any], get_ref: bool):
        self._rlock = threading.RLock()
        self._condition = threading.Condition(self._rlock)
        self.pool: ThreadPoolWrapper = pool
        self.func = func
        self.args = args
        self.kwargs = kwargs if kwargs else {}
        self.get_ref = get_ref
        self._thread = None

    def get_thread(self, timeout: float = None):
        if self.get_ref is False:
            raise ValueError("Cannot use get_thread when get_ref was not specified as True!")
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
        """For internal use by submit method"""
        with self._condition:
            self._thread = thread_item
            # Inform any waiting threads that the item is ready
            self._condition.notify_all()

    def submit(self):
        thread_obj = self.pool.submit(self.func, *self.args, **self.kwargs)
        if self.get_ref:
            self.set_thread(thread_obj)
