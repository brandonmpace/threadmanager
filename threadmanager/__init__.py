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

"""
The threadmanager package provides a way to manage and organize threads as well as keep track of state.
It was originally created to encapsulate such functionality for use with GUIs to avoid blocking their main loop.
"""


__author__ = "Brandon M. Pace"
__copyright__ = "Copyright 2019, Brandon M. Pace"
__license__ = "GNU LGPL 3+"
__maintainer__ = "Brandon M. Pace"
__status__ = "Development"  # not Production yet as extra features are being implemented
__version__ = "0.0.1"


from .log import log_to_console, set_log_level
from .classes import ThreadManager
from .constants import FUTURE, THREAD
from .convenience import get_caller, get_func_name
from .exceptions import *
from .statistics import disable_statistics, enable_statistics, statistics_enabled
