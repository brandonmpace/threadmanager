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


"""This module contains (typically) small convenience functions. (usually for de-duplication purposes)"""


import inspect

from typing import Callable


def get_caller(filepath: bool = False) -> str:
    """Return the name of the caller's caller. If filepath is True, also print the full filepath and line number"""
    current_frame = inspect.currentframe()
    if current_frame:
        caller_code = current_frame.f_back.f_back.f_code
        if filepath:
            caller = f"{caller_code.co_name} at '{caller_code.co_filename}', line {caller_code.co_firstlineno}"
        else:
            caller = caller_code.co_name
        del current_frame, caller_code
        return caller
    else:
        return "<unknown>"


def get_func_name(func: Callable) -> str:
    return func.__qualname__ if hasattr(func, '__qualname__') else '<unknown function>'
