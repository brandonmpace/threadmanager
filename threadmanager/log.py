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


import logging
import threading


from .convenience import get_caller


_logger = None
_loggers = set()
_rlock = threading.RLock()


if not _logger:
    _logger = logging.getLogger(__name__)
    _loggers.add(_logger)


def create_logger(name: str) -> logging.Logger:
    """For internal use in this package. Add a logger for a module."""
    new_logger = logging.getLogger(name)
    with _rlock:
        _loggers.add(new_logger)
    return new_logger


def log_to_console():
    """
    Allows clients to enable printing log items to the console for only the loggers in this package.
    Another option is to enable this for root logger. e.g. logging.getLogger().addHandler(logging.StreamHandler())
    """
    handler = logging.StreamHandler()
    with _rlock:
        for logger in _loggers:
            logger.addHandler(handler)
        _logger.debug(f"logging to console enabled for threadmanager. Caller: {get_caller()}")


def set_log_level(level: int):
    """Allows clients to set log level for all of the loggers used in the package"""
    with _rlock:
        for logger in _loggers:
            logger.setLevel(level)
        _logger.debug(f"threadmanager log level set to {logging.getLevelName(level)} by {get_caller()}")
