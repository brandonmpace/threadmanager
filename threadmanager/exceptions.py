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

# For Warning items, see https://docs.python.org/3/library/warnings.html
# and https://docs.python.org/3/library/logging.html#logging.captureWarnings


class GeneralError(Exception):
    """Base Exception class for threadmanager"""
    pass


class BadStateError(GeneralError):
    """State is not as expected"""
    pass


class StopNotificationWarning(UserWarning):
    """A submission was made, but not accepted as the machine was stopping"""
    pass


class WaitTimeout(GeneralError):
    """A .wait() timeout was hit and data was still not ready"""
    pass
