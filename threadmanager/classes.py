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

import datetime
import queue
import threading


# TODO: Maybe subclass ThreadPoolExecutor and Future from concurrent.futures to add time tracking


class ThreadManager(object):
    """A class for managing, organizing and tracking threads"""
    def __init__(self):
        self._rlock = threading.RLock()
        self._queue = queue.Queue()
        self._threadlauncher = None

    def add(self, func, args=(), kwargs=None):
        """Add a new thread for a callable"""
        pass

    def add_pool(self):
        """Add a new organizational pool for separating threads"""
        pass


class ThreadLauncher(threading.Thread):
    """A thread that launches and manages other threads"""
    def __init__(self, in_queue: queue.Queue, group=None, target=None, name=None, args=(), kwargs=None, safe=True):
        """Initialize a thread with added 'safe' boolean parameter. When True, exceptions will be caught."""
        super().__init__(group=group, target=target, name=name, args=args, kwargs=kwargs)
        self._queue = in_queue
        self.safe = safe

    def run(self):
        while True:
            try:
                self._queue.get(True, 30)  # TODO: consider adding a function to set the timeout in ThreadManager
            except queue.Empty:
                break


class TimedThread(threading.Thread):
    """A thread that tracks start and completion times using the datetime module"""
    def __init__(self, group=None, target=None, name=None, args=(), kwargs=None, safe=True):
        """Initialize a thread with added 'safe' boolean parameter. When True, exceptions will be caught."""
        super().__init__(group=group, target=target, name=name, args=args, kwargs=kwargs)
        self.args = args
        self.kwargs = kwargs
        self.exception = None
        self.safe = safe
        self.time_started: datetime.datetime = None
        self.time_completed: datetime.datetime = None

    def run(self):
        self.time_started = datetime.datetime.now()
        try:
            super().run()
        except Exception as E:
            if self.safe:
                self.exception = E
            else:
                raise
        finally:
            self.time_completed = datetime.datetime.now()

    @property
    def time_total(self) -> datetime.timedelta:
        if self.is_alive() or (self.time_started is None) or (self.time_completed is None):
            raise RuntimeError('This must only be used after .start() is called and the thread has completed')
        return self.time_completed - self.time_started
