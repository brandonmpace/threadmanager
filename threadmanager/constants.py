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


# Callback types
IDLE = 'IDLE'
START = 'START'
STOP = 'STOP'

DEFAULT_CALLBACK_EXCESSIVE_BLOCK_TIME = 0.5
MAX_CALLBACK_EXCESSIVE_BLOCK_TIME = 10.0

MIN_POOL_MONITOR_INTERVAL = 1.0

MIN_THREAD_LAUNCHER_TIMEOUT = 5.0

DEFAULT_WORKER_COUNT = 5

# States for TimedThread
CANCELLED = 'CANCELLED'
COMPLETED = 'COMPLETED'
INITIALIZED = 'INITIALIZED'
RUNNING = 'RUNNING'

# States that .cancel() returns False for
NOCANCELSTATES = (COMPLETED, RUNNING)

# Types for ThreadPoolWrapper
FUTURE = 'FUTURE'
THREAD = 'THREAD'
