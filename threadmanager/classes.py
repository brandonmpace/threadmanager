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

from typing import Any, Callable, Dict, Iterable, Mapping, Optional

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

    @property
    def func_name(self):
        return self._func_name

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

    @property
    def time_started(self):
        with self._condition:
            return self._time_started

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
        with self._condition:
            self._time_completed = time.time()
        # TODO: log runtime statistics for self._func_name if enabled


class TimedFutureThreadPool(concurrent.futures.ThreadPoolExecutor):
    def __init__(self, master: 'ThreadPoolWrapper', *args, runtime_alert: float = 0.0, **kwargs):
        super().__init__(*args, **kwargs)
        self._active_thread_count = 0
        self._master = master
        self.runtime_alert = runtime_alert

    def submit(self, fn, *args, **kwargs):
        # Over-ridden to use TimedFuture instead
        with self._shutdown_lock:
            if self._shutdown:
                raise RuntimeError('cannot schedule new futures after shutdown')

            f = TimedFuture(get_func_name(fn), self.runtime_alert)
            w = concurrent.futures.thread._WorkItem(f, fn, args, kwargs)

            f.add_done_callback(self._master.discard_thread)

            self._master.track_thread(f)
            self._work_queue.put(w)
            self._adjust_thread_count()
            return f


class TimedThread(threading.Thread):
    """
    A thread that tracks start and completion times for statistics.
    There are some API similarities to Future objects to simplify other parts of this package.
    """
    def __init__(self, master: 'ThreadPoolWrapper' = None, pool: 'ThreadPool' = None, group=None, target=None, name=None, args=(), kwargs=None, safe=True):
        """Initialize a thread with added 'safe' boolean parameter. When True, exceptions will be caught."""
        super().__init__(group=group, target=target, name=name, args=args, kwargs=kwargs)
        self._rlock = threading.RLock()
        self._condition = threading.Condition(self._rlock)
        self._args = args
        self._kwargs = kwargs
        self._exception = None
        self._func_name = get_func_name(target)
        self._master = master
        self._pool = pool
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
                self._time_started = self._time_completed = time.time()
                self._notify_master()
                return True

    def cancelled(self) -> bool:
        with self._condition:
            return self._state == CANCELLED

    def done(self):
        with self._condition:
            return self._state in (CANCELLED, COMPLETED)

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

    @property
    def func_name(self):
        return self._func_name

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
                self._condition.notify_all()
                return  # master was notified in cancel method and times were stored.
            else:
                raise BadStateError("TimedThread is not in expected state in run method")
        try:
            result = self._target(*self._args, **self._kwargs)
        except BaseException as E:
            _logger.exception("TimedThread ended with an exception!")
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
            self._notify_master()

    def running(self):
        with self._condition:
            return self._state == RUNNING

    @property
    def time_started(self):
        with self._condition:
            return self._time_started

    def total_runtime(self, timeout: float = None) -> float:
        """
        Get the total time in seconds that it took to complete the threaded function.
        This will block until that completion or until a timeout, if one is specified.
        :param timeout: float for seconds to wait for result
        :return: float number of seconds
        """
        with self._condition:
            if self.done():
                return self._time_completed - self._time_started
            else:
                self._condition.wait(timeout)

                if self.done():
                    return self._time_completed - self._time_started
                else:
                    raise WaitTimeout

    def _notify_master(self):
        """
        Notify the master ThreadPoolWrapper that we've either completed or have been cancelled.
        This should only be called from .run() as it means the thread was actually started.
        """
        if self._master:
            self._master.discard_thread(self)
            self._pool.queue_check()


class ThreadLauncher(threading.Thread):
    """A thread that launches and manages other threads"""
    _count = 0
    _rlock = threading.RLock()

    def __init__(
            self, master: 'ThreadManager', input_queue: queue.Queue, pending_lock: threading.Lock, timeout: float,
            group=None, target=None, args: Iterable = (), kwargs: Mapping[str, Any] = None, safe=True
    ):
        """Initialize a thread with added 'safe' boolean parameter. When True, exceptions will be caught."""
        with self._rlock:
            name = f"threadlauncher{self._count}"
            self._count += 1
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
                    _logger.debug(f"{self.name} - kept alive by last-second submission")

            except BaseException:
                if self._safe:
                    _logger.exception(f"{self.name} - Exception in ThreadRequest submission!")
                else:
                    raise


class ThreadManager(object):
    """A class for managing, organizing and tracking threads"""
    def __init__(self, safe=True, thread_launcher_timeout: float = 30):
        self._rlock = threading.RLock()
        self._launcher_lock = threading.Lock()
        self._pool_idle_event = threading.Event()
        self._pool_lock = threading.RLock()
        self._pools: Dict[str, ThreadPoolWrapper] = {}
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
        self._run_thread_monitor()

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
            func_name = get_func_name(func)
            pool: ThreadPoolWrapper = self._pools[pool_name]

            if self._stop_requested:
                if pool.obey_stop:
                    msg = f"ThreadManager - NOT going to run {func_name} submitted by {caller} as stop was requested"
                    if self._safe:
                        _logger.info(msg)
                        return
                    else:
                        # TODO: Decide if I want to just raise an exception instead. Using a warning allows for simple
                        #  handling with warning control (see warnings module docs) and allows for cleaner code in the
                        #  calling function. I used a warning because there may be cases where someone wants to track
                        #  down an issue by setting safe to False, but wouldn't want this case to halt their program..
                        warnings.warn(msg, StopNotificationWarning)
                        return
                else:
                    msg = f"ThreadManager - stop was requested, running {func_name} submitted by {caller} because pool {pool_name} does not obey stop"
                    _logger.debug(msg)

            # Keep the ThreadLauncher from exiting while we create and send the request, in case it times out now
            with self._launcher_lock:
                if pool.state_updates_enabled:
                    if self._running is False:
                        _logger.info("ThreadManager - setting running to True")
                        self._running = True
                else:
                    _logger.debug(f"ThreadManager - not changing state because pool {pool_name} has this disabled")
                _logger.debug(f"ThreadManager - submitting request to run function {func_name} requested by {caller}")
                thread_request = ThreadRequest(pool, func, args, kwargs, get_ref)
                self._submission_queue.put(thread_request)
            self._run_thread_launcher()
        if get_ref:
            return thread_request.get_thread(timeout=timeout)

    def add_pool(self, name: str, pool_type: str = THREAD, runtime_alert: float = 0, worker_count: Optional[int] = None) -> 'ThreadPoolController':
        """Add a new organizational pool for separating threads"""
        with self._rlock:
            if self._shutdown:
                raise RuntimeError("add_pool called after ThreadManager shutdown method was used")
            if name in self._pools:
                raise ValueError(f"Pool already exists with name of {name}")
            new_pool = ThreadPoolWrapper(name, pool_type, runtime_alert, self._pool_idle_event, worker_count, safe=self._safe)
            with self._pool_lock:
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
            if self._thread_monitor_is_running():
                self._pool_idle_event.set()
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
                _logger.info(f"ThreadManager - stop requested, stop flag set. Caller: {get_caller()}")
                self._cancel_all()
                return True
            elif self._safe:
                _logger.warn(f"ThreadManager - stop requested when already stopped! Caller: {get_caller()}")
                return False
            else:
                raise RuntimeError("stop called while already stopped! Hint: Check .idle first")

    def _cancel_all(self):
        """Cancel pending threads for pools that obey stop"""
        with self._rlock:
            for thread_pool in self._pools.values():
                if thread_pool.obey_stop:
                    thread_pool.cancel_all()

    def _monitor_pools(self):
        """Internal function for use by ThreadMonitor thread"""
        while True:
            with self._pool_lock:
                for pool_name, thread_pool in self._pools.items():
                    if thread_pool.idle():
                        continue
                    # TODO: Add a method in ThreadPoolWrapper that checks if any threads have gone past the max runtime.
                    #  Use it here and log the pool name and name of thread func with _logger.info()
                    # TODO: If there are pools that should update our state and are not idle, update ._running as needed
                    #  If all relevant pools are idle, make sure ._running is False
                    # TODO: When there are threads running, log the count with _logger.info()
            if self._pool_idle_event.wait(1):
                # Woken up by a state-affecting ThreadPoolWrapper becoming idle or by shutdown
                if self._shutdown:
                    break
                self._pool_idle_event.clear()

    def _run_thread_launcher(self):
        with self._rlock:
            if self._thread_launcher_is_running():
                return
            else:
                self._thread_launcher = ThreadLauncher(
                    self, self._submission_queue, self._launcher_lock, self._thread_launcher_timeout, safe=self._safe
                )
                _logger.info(f"ThreadManager - started new ThreadLauncher instance as {self._thread_launcher.name}")
                self._thread_launcher_runs += 1

    def _run_thread_monitor(self):
        with self._rlock:
            if self._thread_monitor_is_running():
                return
            else:
                self._thread_monitor = ThreadMonitor(target=self._monitor_pools)
                _logger.info(f"ThreadManager - started new ThreadMonitor instance as {self._thread_monitor.name}")

    def _thread_launcher_is_running(self) -> bool:
        with self._rlock:
            return self._thread_launcher and self._thread_launcher.is_alive()

    def _thread_monitor_is_running(self) -> bool:
        with self._rlock:
            return self._thread_monitor and self._thread_monitor.is_alive()


class ThreadMonitor(threading.Thread):
    """A thread that monitors other threads and gathers statistics when they are enabled"""
    _count = 0
    _rlock = threading.RLock()

    def __init__(self, group=None, target=None, args: Iterable = (), kwargs: Mapping[str, Any] = None):
        with self._rlock:
            name = f"threadmonitor{self._count}"
            self._count += 1
        super().__init__(group=group, target=target, name=name, args=args, kwargs=kwargs)
        self.daemon = True
        self.start()


class ThreadPool(object):
    """Internal class for grouping threads"""
    _thread_class = TimedThread

    def __init__(
            self, master: 'ThreadPoolWrapper', name: str, max_running: Optional[int] = None, runtime_alert: float = 0.0,
            safe: bool = True
    ):
        self._master = master
        self._max_running = max_running
        self._name = name
        self._pending_queue = queue.Queue()
        self._rlock = threading.RLock()
        self._runtime_alert = runtime_alert
        self._safe = safe

    def queue_check(self):
        """If there is a limit for concurrent threads, start the next one if needed"""
        if self._max_running and (self._master.active_count() < self._max_running):
            try:
                next_thread = self._pending_queue.get_nowait()
            except queue.Empty():
                return

            next_thread.start()

    def submit(self, func, *args, **kwargs):
        thread_name = f"{self._name}-{get_func_name(func)}"
        new_thread = TimedThread(
            master=self._master, pool=self, target=func, name=thread_name, args=args, kwargs=kwargs, safe=self._safe
        )

        self._master.track_thread(new_thread)

        if self._max_running and (self._master.active_count() >= self._max_running):
            self._pending_queue.put(new_thread)  # Too many threads already running, queue this one.
        else:
            new_thread.start()

        return new_thread

    def shutdown(self, wait: bool = True):
        # TODO: block new submissions, empty the queue and join all the currently running threads.
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

    def __init__(self, name: str, pool_type: str, runtime_alert: float, idle_event: threading.Event, worker_count: Optional[int] = None, safe: bool = True):
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

        self._active_threads = set()
        self._idle_event = idle_event
        self._name = name
        self._pool: [TimedFutureThreadPool, ThreadPool] = None
        self._rlock = threading.RLock()
        self._runtime_alert = runtime_alert
        self._safe = safe
        self._type = pool_type

        # Whether or not submissions are allowed in ThreadManager after stop was requested
        self.obey_stop = True

        # Whether or not ThreadManager will update its state when submitting to this pool
        self.state_updates_enabled = True

        if pool_type == FUTURE:
            self._pool = TimedFutureThreadPool(self, runtime_alert=runtime_alert, max_workers=worker_count, thread_name_prefix=name)
        elif pool_type == THREAD:
            self._pool = ThreadPool(self, name, max_running=worker_count, runtime_alert=runtime_alert, safe=self._safe)

    def active_count(self) -> int:
        """Number of threads that are running or will run soon"""
        with self._rlock:
            return len(self._active_threads)

    def cancel_all(self):
        """Attempt to cancel all threads"""
        with self._rlock:
            for thread_obj in self._active_threads:
                thread_obj.cancel()

    def discard_thread(self, thread_obj: [TimedFuture, TimedThread]):
        """Internal method used in tracking active threads"""
        with self._rlock:
            self._active_threads.discard(thread_obj)
            if self.idle() and self.state_updates_enabled:
                self._idle_event.set()

    def idle(self) -> bool:
        with self._rlock:
            return not self._active_threads

    def shutdown(self, wait: bool = True):
        self._pool.shutdown(wait=wait)

    def submit(self, func: Callable, *args, **kwargs):
        return self._pool.submit(func, *args, **kwargs)

    def track_thread(self, thread_obj: [TimedFuture, TimedThread]):
        """Internal method used in tracking active threads"""
        with self._rlock:
            self._active_threads.add(thread_obj)


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
