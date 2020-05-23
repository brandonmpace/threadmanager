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
import concurrent.futures
import queue
import threading
import time
import warnings

from typing import Any, Callable, Dict, Iterable, Mapping, Optional, Set, Union

from .convenience import get_caller, get_func_name, pluralize, thread_func_tag, thread_nametag
from .constants import *
from .exceptions import *
from .log import create_logger
from . import statistics


logger = create_logger(__name__)


class Callback(object):
    def __init__(self, removal_cb: Callable, callback_type: str, func: Callable, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs
        self._func = func
        self._removal_cb = removal_cb
        self._type = callback_type
        self._warning_block_time = DEFAULT_CALLBACK_EXCESSIVE_BLOCK_TIME
        logger.debug(f"adding {self._type} callback for {get_func_name(self._func)}")

    def __repr__(self):
        return "%s(%r, %r)" % (self.__class__.__name__, self._type, self._func)

    def remove(self):
        logger.debug(f"removing {self._type} callback for {get_func_name(self._func)}")
        self._removal_cb(self)

    def run(self):
        start_time = time.perf_counter()
        try:
            self._func(*self._args, **self._kwargs)
        except Exception:
            logger.exception(f"Exception in {self._type} callback! func: {get_func_name(self._func)}")
        total_time = time.perf_counter() - start_time
        if total_time >= self._warning_block_time:
            logger.warning(
                f"callback for {get_func_name(self._func)} took longer than {self._warning_block_time} seconds"
            )

    def set_warning_block_time(self, seconds: float):
        """Override the amount of time at which excessive blocking by this callback is logged"""
        if isinstance(seconds, float):
            if seconds <= 0:
                raise ValueError(f"seconds must be greater than 0, got {seconds}")
            elif seconds > MAX_CALLBACK_EXCESSIVE_BLOCK_TIME:
                raise ValueError(f"seconds can not be greater than {MAX_CALLBACK_EXCESSIVE_BLOCK_TIME}, got {seconds}")
            else:
                self._warning_block_time = seconds
        else:
            raise ValueError(f"Expected float, got '{type(seconds)}'")

    @property
    def type(self):
        return self._type


class TimedFuture(concurrent.futures.Future):
    def __init__(self, func_name: str, runtime_alert: float, tag: str = ""):
        super().__init__()
        self._func_name = func_name
        self._runtime_alert = runtime_alert
        self._tag = tag
        self._time_started: float = 0.0
        self._time_completed: float = 0.0

    def current_runtime(self) -> float:
        """Return the time since start. Returns total runtime if already finished running."""
        if self._state == concurrent.futures._base.PENDING:
            raise RuntimeError("current_runtime called before TimedThread started")
        elif self.done():
            return self.total_runtime()
        else:
            return time.time() - self._time_started

    @property
    def func_name(self):
        return self._func_name
    name = func_name

    def set_exception(self, exception):
        with self._condition:
            self._set_time_completed()
            logger.exception(f"TimedFuture ({thread_func_tag(self)}) ended with an exception!")
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
    def tag(self):
        return self._tag

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
        self._master = master
        self.runtime_alert = runtime_alert

    def submit(self, tag: str, fn, *args, **kwargs):
        # Over-ridden to use TimedFuture instead
        with self._shutdown_lock:
            if self._shutdown:
                raise RuntimeError("cannot schedule new futures after shutdown")

            f = TimedFuture(get_func_name(fn), self.runtime_alert, tag=tag)
            w = concurrent.futures.thread._WorkItem(f, fn, args, kwargs)

            f.add_done_callback(self._master.discard_thread)

            self._master.track_thread(f)
            self._work_queue.put(w)
            self._adjust_thread_count()
            return f

    @property
    def worker_count(self):
        return self._max_workers

    @worker_count.setter
    def worker_count(self, value: int):
        raise NotImplementedError("On-the-fly worker count adjustment has not been implemented for Future-based pools")


class TimedThread(threading.Thread):
    """
    A thread that tracks start and completion times for statistics.
    There are some API similarities to Future objects to simplify other parts of this package.
    """
    def __init__(self, master: 'ThreadPoolWrapper' = None, pool: 'ThreadPool' = None, group=None, tag: str = "", target=None, name=None, args=(), kwargs=None, safe=True):
        """Initialize a thread with added 'safe' boolean parameter. When True, exceptions will be caught."""
        super().__init__(group=group, target=target, name=name, args=args, kwargs=kwargs)
        self._rlock = threading.RLock()
        self._condition = threading.Condition(self._rlock)
        self._args = args
        self._kwargs = (kwargs if kwargs else {})
        self._exception = None
        self._func_name = get_func_name(target)
        self._master = master
        self._pool = pool
        self._result = None
        self._safe = safe
        self._state = INITIALIZED
        self._tag = tag
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
                self._notify_master(cancelled=True)
                return True

    def cancelled(self) -> bool:
        with self._condition:
            return self._state == CANCELLED

    def current_runtime(self) -> float:
        """Return the time since start. Returns total runtime if already finished running."""
        if self._state == INITIALIZED:
            raise RuntimeError("current_runtime called before TimedThread started")
        elif self.done():
            return self.total_runtime()
        else:
            return time.time() - self._time_started

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
            logger.exception(f"TimedThread ({thread_func_tag(self)}) ended with an exception!")
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
    def tag(self):
        return self._tag

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

    def _notify_master(self, cancelled: bool = False):
        """
        Notify the master ThreadPoolWrapper that we've either completed or have been cancelled.
        This should only be called from .run() as it means the thread was actually started.
        """
        if self._master:
            self._master.discard_thread(self)
            self._pool.queue_check(cancelled=cancelled)


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
            type(self)._count += 1
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
                    logger.debug(f"{self.name} - kept alive by last-second submission")

            except BaseException:
                if self._safe:
                    logger.exception(f"{self.name} - Exception in ThreadRequest submission!")
                else:
                    raise


class ThreadManager(object):
    _instances: Dict[str, 'ThreadManager'] = {}
    _supported_callback_types = (IDLE, START, STOP)

    """A class for managing, organizing and tracking threads"""
    def __init__(self, name: str, safe=True, thread_launcher_timeout: float = 30.0, monitor_interval: float = 10.0):
        """
        Initialize a ThreadManager instance
        :param name: str name that can be used to retrieve the instance from other parts of the program
        :param safe: bool used for debugging issues within threadmanager
        :param thread_launcher_timeout: float number of seconds to keep a ThreadLauncher alive with no new requests
        :param monitor_interval: float number of seconds to wait between forced check of running thread count and more.
        """
        self._callbacks: Dict[str, Set[Callback]] = {cb_type: set() for cb_type in self._supported_callback_types}

        # thread locking items
        self._callback_lock = threading.RLock()
        self._rlock = threading.RLock()
        self._launcher_lock = threading.Lock()
        self._pool_idle_event = threading.Event()
        self._pool_lock = threading.RLock()

        if monitor_interval < MIN_POOL_MONITOR_INTERVAL:
            raise ValueError(f"monitor_interval must be at least {MIN_POOL_MONITOR_INTERVAL}")
        self._monitor_interval = monitor_interval
        self._name = name
        self._pools: Dict[str, ThreadPoolWrapper] = {}
        self._safe = safe
        self._running = False
        self._shutdown = False
        self._stop_requested = False

        # Queue for sending items to the ThreadLauncher instance
        self._submission_queue = queue.Queue()
        self._thread_launcher: Optional[ThreadLauncher] = None
        self._thread_launcher_runs: int = 0
        if thread_launcher_timeout < MIN_THREAD_LAUNCHER_TIMEOUT:
            raise ValueError(f"thread_launcher_timeout must be at least {MIN_THREAD_LAUNCHER_TIMEOUT}")
        self._thread_launcher_timeout = thread_launcher_timeout
        self._thread_monitor: Optional[ThreadMonitor] = None

        if name in self._instances:
            raise ValueError(f"ThreadManager already exists with name: {name}")
        self._instances[name] = self
        self._run_thread_launcher()
        self._run_thread_monitor()

    def __repr__(self):
        return "%s(%r, %r, %r)" % (self.__class__.__name__, self._name, self._safe, self._thread_launcher_timeout)

    def add(self, pool_name: str, func: Callable, args: Iterable = (), kwargs: Optional[Mapping[str, Any]] = None, get_ref: bool = False, timeout: Optional[float] = None, tag: str = ""):
        """
        Submit the callable to the requested pool
        :param pool_name: str
        :param func: Callable
        :param args: Iterable (optional, typically a tuple) Single argument needs a trailing comma. e.g. (5,)
        :param kwargs: Mapping (optional, typically a dict)
        :param get_ref: bool whether or not to return a reference to the Thread/Future
        :param timeout: float time to wait on getting the thread reference
        :param tag: str alphanumeric (- and _ allowed) string to append to thread name for easier tracking
        :return: Optional[Union[TimedThread, TimedFuture]]
        """
        with self._rlock:
            if self._shutdown:
                raise RuntimeError("add called after ThreadManager shutdown method was used")

            if tag and tag.replace("-", "").replace("_", "").isalnum() is False:
                raise ValueError(
                    f"tag value of '{tag}' contains unsupported characters. Hint: Use an alphanumeric string"
                )
            if pool_name not in self._pools:
                raise ValueError(f"Pool does not exist with pool_name: {pool_name}")

            caller = get_caller()
            func_name = get_func_name(func)
            pool: ThreadPoolWrapper = self._pools[pool_name]

            if self._stop_requested:
                if pool.obey_stop:
                    msg = f"ThreadManager - NOT going to run {func_name} submitted by {caller} as stop was requested"
                    if self._safe:
                        logger.info(msg)
                        return
                    else:
                        warnings.warn(msg, StopNotificationWarning)
                        return
                else:
                    msg = f"ThreadManager - stop was requested, running {func_name} submitted by {caller} because pool {pool_name} does not obey stop"
                    logger.debug(msg)

            # Keep the ThreadLauncher from exiting while we create and send the request, in case it times out now
            with self._launcher_lock:
                logger.debug(f"ThreadManager - submitting request to run function {func_name} requested by {caller}")
                thread_request = ThreadRequest(pool, func, args, kwargs, get_ref, tag)
                self._submission_queue.put(thread_request)
            self._run_thread_launcher()
        if get_ref:
            return thread_request.get_thread(timeout=timeout)

    def add_idle_callback(self, func: Callable, *args, **kwargs) -> Callback:
        """Add a callback to run when all state-affecting pools go idle and ThreadManager state changes to idle"""
        return self._add_callback(IDLE, func, *args, **kwargs)

    def add_start_callback(self, func: Callable, *args, **kwargs) -> Callback:
        """Add a callback to run when any state-affecting pool causes ThreadManager state to no longer be idle"""
        return self._add_callback(START, func, *args, **kwargs)

    def add_stop_callback(self, func: Callable, *args, **kwargs) -> Callback:
        """Add a callback to run when ThreadManager.stop() is called (also called during shutdown)"""
        return self._add_callback(STOP, func, *args, **kwargs)

    def add_pool(self, name: str, pool_type: str = THREAD, runtime_alert: float = 0, worker_count: int = DEFAULT_WORKER_COUNT) -> 'ThreadPoolController':
        """Add a new organizational pool for separating threads"""
        with self._rlock:
            if self._shutdown:
                raise RuntimeError("add_pool called after ThreadManager shutdown method was used")
            if name in self._pools:
                raise ValueError(f"Pool already exists with name of {name}")

            new_pool = ThreadPoolWrapper(self, name, pool_type, runtime_alert, self._pool_idle_event, worker_count=worker_count, safe=self._safe)

            with self._pool_lock:
                self._pools[name] = new_pool

            return ThreadPoolController(new_pool)

    @classmethod
    def get_instance(cls, name: str) -> 'ThreadManager':
        """Get an existing ThreadManager instance"""
        if name in cls._instances:
            return cls._instances[name]
        else:
            raise ValueError(f"ThreadManager instance does not exist with name: {name}")

    @property
    def go(self) -> bool:
        """Can be used in functions running in threads to determine if they should keep going"""
        return self._stop_requested is False

    @property
    def idle(self):
        """Can be used to identify when there are/aren't submitted threads still running"""
        return self._running is False

    @property
    def name(self):
        return self._name

    @property
    def no_go(self) -> bool:
        """Can be used in functions running in threads to determine if they should stop"""
        return self._stop_requested

    def shutdown(self, wait: bool = True):
        # TODO: maybe allow cancel_all param or similar to cancel pending items in pools
        with self._rlock:
            if self._shutdown:
                if self._safe:
                    logger.warning(f"ThreadManager - shutdown called more than once! Caller: {get_caller()}")
                    return
                else:
                    raise RuntimeError(f"ThreadManager - shutdown called more than once! Caller: {get_caller()}")
            logger.info(f"ThreadManager - shutdown requested. Caller: {get_caller()}")
            if self._running:
                logger.info("ThreadManager - still running, calling stop function")
                self.stop()
            self._shutdown = True
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
        with self._rlock:
            if self._shutdown:
                msg = f"ThreadManager - stop called after shutdown! Caller: {get_caller()}"
                if self._safe:
                    logger.warning(msg)
                    return False
                else:
                    raise RuntimeError(msg)
            if self._running:
                self._stop_requested = True
                logger.info(f"ThreadManager - stop requested, stop flag set. Caller: {get_caller()}")
                self._cancel_all()
                self._run_callback_thread(STOP)
                return True
            elif self._safe:
                logger.warning(f"ThreadManager - stop requested when already stopped! Caller: {get_caller()}")
                return False
            else:
                raise RuntimeError("stop called while already stopped! Hint: Check .idle first")

    def _add_callback(self, callback_type: str, func: Callable, *args, **kwargs) -> Callback:
        self._validate_callback_type(callback_type)

        callback = Callback(self._remove_callback, callback_type, func, *args, **kwargs)

        with self._callback_lock:
            self._callbacks[callback_type].add(callback)

        return callback

    def _cancel_all(self):
        """Cancel pending threads for pools that obey stop"""
        with self._rlock:
            for thread_pool in self._pools.values():
                if thread_pool.obey_stop:
                    thread_pool.cancel_all()

    def _monitor_pools(self):
        """Internal function for use by ThreadMonitor thread"""
        while True:
            busy_pool = False
            with self._pool_lock:
                active_thread_count = 0
                for pool_name, thread_pool in self._pools.items():
                    if thread_pool.idle():
                        continue
                    elif thread_pool.state_updates_enabled and (busy_pool is False):
                        busy_pool = True
                        logger.debug(f"ThreadManager - pool {pool_name} is not idle")

                    active_thread_count += thread_pool.active_count()
                    thread_pool.runtime_check()

                if active_thread_count:
                    logger.info(
                        f"ThreadManager - {active_thread_count} active thread{pluralize(active_thread_count)} counted during this loop"
                    )

            with self._rlock:
                if busy_pool:
                    if self._running is False:
                        logger.debug("ThreadManager - there is a busy pool, setting idle to False")
                        self._set_running(True)
                else:
                    if self._stop_requested:
                        logger.info("ThreadManager - stop completed, removing flag")
                        self._stop_requested = False
                    if self._running:
                        logger.debug("ThreadManager - all state-affecting pools are idle, setting idle to True")
                        self._set_running(False)

            # Go back to waiting on an idle event:
            if self._pool_idle_event.wait(self._monitor_interval):
                # Woken up by a state-affecting ThreadPoolWrapper becoming idle or by shutdown
                if self._shutdown:
                    break
                self._pool_idle_event.clear()

    def _remove_callback(self, callback: Callback):
        """Removes a callback. To trigger this, call .remove() on the Callback object."""
        self._validate_callback_type(callback.type)
        self._callbacks[callback.type].discard(callback)

    def _run_callback_thread(self, callback_type: str) -> TimedThread:
        self._validate_callback_type(callback_type)

        logger.debug(f"starting thread to run {callback_type} callbacks")
        thread_obj = TimedThread(target=self._run_callbacks, name=f"callbacks-{callback_type}", args=(callback_type,))
        thread_obj.start()

        return thread_obj

    def _run_callbacks(self, callback_type: str):
        with self._callback_lock:
            logger.debug(f"about to run {callback_type} callbacks")

            for callback_item in self._callbacks[callback_type]:
                callback_item.run()

            logger.debug(f"finished running {callback_type} callbacks")

    def _run_thread_launcher(self):
        with self._rlock:
            if self._thread_launcher_is_running():
                return
            else:
                self._thread_launcher = ThreadLauncher(
                    self, self._submission_queue, self._launcher_lock, self._thread_launcher_timeout, safe=self._safe
                )
                logger.info(f"ThreadManager - started new ThreadLauncher instance as {self._thread_launcher.name}")
                self._thread_launcher_runs += 1

    def _run_thread_monitor(self):
        with self._rlock:
            if self._thread_monitor_is_running():
                return
            else:
                self._thread_monitor = ThreadMonitor(target=self._monitor_pools)
        logger.info(f"ThreadManager - started new ThreadMonitor instance as {self._thread_monitor.name}")

    def _set_running(self, value: bool) -> bool:
        """:return: bool True if state was changed"""
        with self._rlock:
            if self._running == value:
                msg = f"ThreadManager - attempt to set _running to the existing value! Caller: {get_caller()}"
                if self._safe:
                    logger.warning(msg)
                    return False
                else:
                    raise RuntimeError(msg)
            else:
                self._running = value

            if value:
                self._run_callback_thread(START)
            else:
                self._run_callback_thread(IDLE)

    def _thread_launcher_is_running(self) -> bool:
        with self._rlock:
            return self._thread_launcher and self._thread_launcher.is_alive()

    def _thread_monitor_is_running(self) -> bool:
        with self._rlock:
            return self._thread_monitor and self._thread_monitor.is_alive()

    def _validate_callback_type(self, callback_type: str):
        if callback_type not in self._supported_callback_types:
            raise ValueError(
                f"callback_type of {callback_type} is not in supported types: {self._supported_callback_types}"
            )

        if callback_type not in self._callbacks:
            raise ValueError(f"callback type {callback_type} missing in ThreadManager instance!")


class ThreadMonitor(threading.Thread):
    """A thread that monitors thread pools - only used by ThreadManager"""
    _count = 0
    _rlock = threading.RLock()

    def __init__(self, group=None, target=None, args: Iterable = (), kwargs: Mapping[str, Any] = None):
        with self._rlock:
            name = f"threadmonitor{self._count}"
            type(self)._count += 1
        super().__init__(group=group, target=target, name=name, args=args, kwargs=kwargs)
        self.daemon = True
        self.start()


class ThreadPool(object):
    """Internal class for grouping threads"""
    _rlock = threading.RLock()

    def __init__(
            self, master: 'ThreadPoolWrapper', name: str, max_running: int = DEFAULT_WORKER_COUNT, runtime_alert: float = 0.0,
            safe: bool = True
    ):
        self._master = master
        self._max_running = max_running
        self._name = name
        self._pending_queue: collections.deque[TimedThread] = collections.deque()
        self._rlock = threading.RLock()
        self._runtime_alert = runtime_alert
        self._safe = safe
        self._shutdown = False

    def __repr__(self):
        return "%s(%r, %r, %r)" % (self.__class__.__name__, self._name, self._max_running, self._runtime_alert)

    def cancel_pending_threads(self):
        """Cancel all threads that are waiting to run"""
        with self._rlock:
            self._empty_pending_queue()

    def queue_check(self, cancelled: bool = False):
        """If there is a limit for concurrent threads, start the next one if needed"""
        with self._rlock:
            if self._shutdown:
                if cancelled:
                    logger.debug(
                        f"ThreadPool ({self._name}) - the calling thread was cancelled - NOT emptying pending queue"
                    )
                    return
                logger.info(f"ThreadPool ({self._name}) - not starting next thread as shutdown was requested")
                self._empty_pending_queue()
                return
            elif self._master.master.no_go and self._master.obey_stop:
                if cancelled:
                    logger.debug(f"ThreadPool ({self._name}) - the calling thread was cancelled")
                    return
                logger.info(
                    f"ThreadPool ({self._name}) - not starting next thread as stop was requested and obey_stop is True"
                )
                return

            if self._max_running and (self._master.active_count() < self._max_running):
                try:
                    next_thread = self._pending_queue.popleft()

                    while next_thread and next_thread.cancelled():
                        next_thread = self._pending_queue.popleft()

                    if next_thread:
                        if next_thread.running() or next_thread.done():
                            logger.critical(
                                f"ThreadPool ({self._name}) - thread has already been started: {thread_nametag(next_thread)}"
                            )
                        else:
                            self._master.track_thread(next_thread)
                            next_thread.start()
                    else:
                        logger.debug(f"ThreadPool ({self._name}) - empty result from queue")
                except IndexError:
                    return
            elif self._max_running == 0:  # can happen if user changed to 0, will run ALL pending threads NOW
                try:
                    next_thread = self._pending_queue.popleft()
                    while next_thread:
                        self._master.track_thread(next_thread)
                        next_thread.start()
                        next_thread = self._pending_queue.popleft()
                except IndexError:
                    return

    def submit(self, tag: str, func, *args, **kwargs):
        thread_name = f"{self._name}-{get_func_name(func)}"
        if self._shutdown:
            logger.warning(f"ThreadPool ({self._name}) - not running thread {thread_name} due to shutdown request")
            if self._safe:
                return None
            else:
                raise RuntimeError(f"ThreadPool ({self._name}) - thread {thread_name} submitted after shutdown!")
        new_thread = TimedThread(
            master=self._master, pool=self, tag=tag, target=func, name=thread_name, args=args, kwargs=kwargs, safe=self._safe
        )

        with self._rlock:
            if self._max_running and (self._master.active_count() >= self._max_running):
                self._pending_queue.append(new_thread)  # Too many threads already running, queue this one.
            else:
                self._master.track_thread(new_thread)
                new_thread.start()

        return new_thread

    def shutdown(self, wait: bool = True):
        # TODO: maybe join all the currently running threads.
        self._shutdown = True
        self._empty_pending_queue()

    @property
    def worker_count(self):
        return self._max_running

    @worker_count.setter
    def worker_count(self, value: int):
        if isinstance(value, int) is False:
            raise ValueError(f"New worker count for ThreadPool must be int. Got value {value} with type {type(value)}")
        elif value < 0:
            raise ValueError("New worker count for ThreadPool must be 0 or higher")
        self._max_running = value
        logger.info(f"ThreadPool {self._name} worker count adjusted to {value}")

    def _empty_pending_queue(self):
        with self._rlock:
            if self._pending_queue:
                logger.info(f"ThreadPool ({self._name}) - removing all pending threads from the queue")
                try:
                    next_thread = self._pending_queue.popleft()
                    while next_thread:
                        next_thread.cancel()
                        next_thread = self._pending_queue.popleft()
                except IndexError:
                    return


class ThreadPoolController(object):
    """Class allowing clients to control some attributes of ThreadPoolWrapper"""
    def __init__(self, pool: 'ThreadPoolWrapper'):
        self._pool = pool

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self._pool)

    def disable_state_updates(self):
        """Prevent this pool from affecting ThreadManger's state (e.g. idle attribute)"""
        self._pool.state_updates_enabled = False

    def enable_state_updates(self):
        """Allow this pool to affect ThreadManger's state (e.g. idle attribute)"""
        self._pool.state_updates_enabled = True

    def disable_statistics(self):
        self._pool.statistics_enabled = False

    def enable_statistics(self):
        self._pool.statistics_enabled = True

    def disable_tag_in_stats(self):
        """Prevent this pool from using thread tag in statistics"""
        self._pool.use_tag_in_stats = False

    def enable_tag_in_stats(self):
        """Allow this pool to use thread tag in statistics"""
        self._pool.use_tag_in_stats = True

    def ignore_stop(self):
        """Allow submission to this pool after stop was requested of ThreadManager"""
        self._pool.obey_stop = False

    def obey_stop(self):
        """Block submission to this pool after stop was requested of ThreadManager"""
        self._pool.obey_stop = True

    def set_worker_count(self, value: int):
        """Change the maximum number of worker threads for the pool"""
        self._pool.set_worker_count(value)

    @property
    def statistics_enabled(self) -> bool:
        return self._pool.statistics_enabled

    @statistics_enabled.setter
    def statistics_enabled(self, value: bool):
        self._pool.statistics_enabled = value

    @property
    def use_tag_in_stats(self) -> bool:
        return self._pool.use_tag_in_stats

    @use_tag_in_stats.setter
    def use_tag_in_stats(self, value: bool):
        self._pool.use_tag_in_stats = value

    @property
    def worker_count(self) -> int:
        return self._pool.worker_count

    @worker_count.setter
    def worker_count(self, value: int):
        self._pool.set_worker_count(value)


class ThreadPoolWrapper(object):
    """Internal class that is an abstraction layer for ThreadPoolExecutor and internal pool types"""
    _supported_types = (FUTURE, THREAD)

    def __init__(self, master: ThreadManager, name: str, pool_type: str, runtime_alert: float, idle_event: threading.Event, worker_count: int = DEFAULT_WORKER_COUNT, safe: bool = True):
        """
        Initializes a ThreadPoolWrapper
        :param name: str name of the pool. This should be alphanumeric
        :param pool_type: str type of pool, options defined in constants
        :param runtime_alert: float amount of seconds. When threads run longer than this, it will be logged.
        :param worker_count: int max number of threads allowed to run at the same time
        """
        if name.isalnum() is False:
            raise ValueError(
                f"Pool name value of '{name}' contains unsupported characters. Hint: Use an alphanumeric string"
            )

        if runtime_alert < 0:
            raise ValueError(f"runtime_alert must be greater than or equal to 0, got {runtime_alert}")

        self.use_tag_in_stats = True
        self._active_threads: Set[TimedThread, TimedFuture] = set()
        self._idle_event = idle_event
        self._name = name
        self._master = master
        self._pool: [TimedFutureThreadPool, ThreadPool] = None
        self._rlock = threading.RLock()
        self._runtime_alert = runtime_alert
        self._safe = safe
        self._type = pool_type

        # Whether or not submissions are allowed in ThreadManager after stop was requested
        self._obey_stop = True

        # Whether or not ThreadManager will update its state when submitting to this pool
        self._state_updates_enabled = True

        # Whether or not statistics reporting will be performed for this pool (only when global statistics are enabled)
        self._statistics_enabled = True

        if pool_type == FUTURE:
            self._pool = TimedFutureThreadPool(self, runtime_alert=runtime_alert, max_workers=worker_count, thread_name_prefix=name)
        elif pool_type == THREAD:
            self._pool = ThreadPool(self, name, max_running=worker_count, runtime_alert=runtime_alert, safe=self._safe)
        elif pool_type in self._supported_types:
            raise NotImplementedError(f"It seems that pool type of {pool_type} is not yet implemented")
        else:
            raise ValueError(f"Unrecognized pool type: {pool_type}")

    def __repr__(self):
        return "%s(%r, %r)" % (self.__class__.__name__, self._name, self._type)

    def active_count(self) -> int:
        """Number of threads that are running"""
        with self._rlock:
            if self._type == FUTURE:
                return len([item.running() for item in self._active_threads])
            else:
                return len(self._active_threads)

    def cancel_all(self):
        """Attempt to cancel all threads"""
        with self._rlock:
            logger.debug(f"ThreadPoolWrapper ({self._name}) - attempting to cancel all threads")
            if self._type == THREAD:
                self._pool.cancel_pending_threads()
            for thread_obj in self._active_threads.copy():
                thread_obj.cancel()

    def discard_thread(self, thread_item: [TimedFuture, TimedThread]):
        """Internal method used in tracking active threads"""
        thread_name = thread_nametag(thread_item)
        logger.debug(f"ThreadPoolWrapper ({self._name}) - thread completed - name: {thread_name}")
        with self._rlock:
            if thread_item not in self._active_threads:
                return
            self._active_threads.discard(thread_item)
            if self._statistics_enabled:
                if self.use_tag_in_stats:
                    statistics.record_statistics(self._name, thread_name, thread_item.total_runtime())
                else:
                    statistics.record_statistics(self._name, thread_item.name, thread_item.total_runtime())
            if self.idle():
                self._wake_thread_monitor()

    def idle(self) -> bool:
        """True if the pool has no actively running threads"""
        with self._rlock:
            return not self._active_threads

    @property
    def master(self) -> ThreadManager:
        """Get the ThreadManager instance managing this pool"""
        return self._master

    @property
    def name(self) -> str:
        """Get the name assigned to this pool"""
        return self._name

    @property
    def obey_stop(self) -> bool:
        """Whether or not submissions are allowed in ThreadManager after stop was requested"""
        return self._obey_stop

    @obey_stop.setter
    def obey_stop(self, value: bool):
        self._obey_stop = value
        logger.debug(f"Pool {self._name} configured to {'obey' if value else 'ignore'} stop commands")

    def runtime_check(self) -> bool:
        """
        Check if there are threads that have run longer than the pool's runtime_alert, if set.
        :return: bool True if runtime_alert is > 0 and there is an active thread that ran longer than specified there.
        """
        if self._runtime_alert:
            alert = False
            with self._rlock:
                for thread_item in self._active_threads:
                    if thread_item.running():
                        current_runtime = thread_item.current_runtime()
                        if current_runtime > self._runtime_alert:
                            logger.warning(
                                f"ThreadPoolWrapper ({self._name}) - thread {thread_nametag(thread_item)} has been running longer than alert time of {self._runtime_alert}. Runtime: {current_runtime}"
                            )
                            alert = True
                return alert
        else:
            return False

    def set_worker_count(self, value: int):
        self._pool.worker_count = value

    def shutdown(self, wait: bool = True):
        self._pool.shutdown(wait=wait)

    @property
    def state_updates_enabled(self) -> bool:
        """Whether or not ThreadManager will update its state when submitting to this pool"""
        return self._state_updates_enabled

    @state_updates_enabled.setter
    def state_updates_enabled(self, value: bool):
        self._state_updates_enabled = value
        logger.debug(f"Pool {self._name} configured to {'' if value else 'not '}affect ThreadManager state")

    @property
    def statistics_enabled(self):
        return self._statistics_enabled

    @statistics_enabled.setter
    def statistics_enabled(self, value: bool):
        self._statistics_enabled = value
        logger.debug(f"Pool {self._name} configured to {'' if value else 'not '}report statistics")

    def submit(self, tag: str, func: Callable, *args, **kwargs) -> Optional[Union[TimedThread, TimedFuture]]:
        """Add a function to be ran in a thread by the pool"""
        ret_val = self._pool.submit(tag, func, *args, **kwargs)
        self._wake_thread_monitor()
        return ret_val

    def track_thread(self, thread_obj: [TimedFuture, TimedThread]):
        """Internal method used in tracking active threads"""
        with self._rlock:
            logger.debug(
                f"ThreadPoolWrapper ({self._name}) - thread added - name: {thread_nametag(thread_obj)}"
            )
            self._active_threads.add(thread_obj)

    @property
    def worker_count(self):
        return self._pool.worker_count

    def _wake_thread_monitor(self):
        """Used to make sure the ThreadMonitor runs another check loop now (to keep state updated properly)"""
        if self.state_updates_enabled:
            if self._idle_event.is_set():
                return
            else:
                self._idle_event.set()


class ThreadRequest(object):
    """Internal class that is used for queueing requests and returning thread objects when requested"""
    def __init__(self, pool: ThreadPoolWrapper, func: Callable, args: Iterable, kwargs: Mapping[str, Any], get_ref: bool, tag: str):
        self._rlock = threading.RLock()
        self._condition = threading.Condition(self._rlock)
        self.pool: ThreadPoolWrapper = pool
        self.func = func
        self.args = args
        self.kwargs = kwargs if kwargs else {}
        self.get_ref = get_ref
        self.tag = tag
        self._thread = None

    def __repr__(self):
        return "%s(%r, %r, %r)" % (self.__class__.__name__, self.pool, self.func, self.tag)

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
        thread_obj = self.pool.submit(self.tag, self.func, *self.args, **self.kwargs)
        if self.get_ref:
            self.set_thread(thread_obj)
