#!/usr/bin/env python3
# thoth-advise-reporter
# Copyright(C) 2020 Francesco Murdaca
#
# This program is free software: you can redistribute it and / or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.


"""This is advise reporter for Thoth adviser justifications."""

from thoth.common import __version__ as __common__version__
from thoth.common import init_logging
from thoth.messaging import __version__ as __messaging__version__
from thoth.lab import __version__ as __lab__version__

__version__ = "0.2.1"
__service_version__ = f"{__version__}+\
    messaging.{__messaging__version__}.\
        common.{__common__version__}.\
            lab.{__lab__version__}"


# Init logging here when gunicorn import this application.
init_logging()
