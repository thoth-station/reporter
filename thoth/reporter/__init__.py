#!/usr/bin/env python3
# thoth-reporter
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


"""This is reporter for Thoth components reports analysis."""

from thoth.common import __version__ as __common__version__
from thoth.messaging import __version__ as __messaging__version__
from thoth.report_processing import __version__ as __report_processing__version__
from thoth.python import __version__ as __python__version__

__version__ = "0.11.0"
__service_version__ = (
    f"{__version__}+"
    f"messaging.{__messaging__version__}."
    f"common.{__common__version__}.report-processing.{__report_processing__version__}."
    f"python.{__python__version__}"
)
