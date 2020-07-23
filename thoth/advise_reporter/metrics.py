#!/usr/bin/env python3
# thoth-adviser-reporter
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


"""This is Thoth adviser-reporter consumer metrics."""


from thoth.advise_reporter import __service_version__

from prometheus_client import Gauge, Counter


# add the application version info metric
advise_reporter_info = Gauge("advise_reporter_consumer_info", "Adviser reporter Version Info", labelnames=["version"])
advise_reporter_info.labels(version=__service_version__).inc()

# Metrics for Kafka
in_progress = Gauge("investigators_in_progress", "Total number of investigation messages currently being processed.")
exceptions = Counter("investigator_exceptions", "Number of investigation messages which failed to be processed.")
success = Counter("investigators_processed", "Number of investigation messages which were successfully processed.")

# Advise justifications
advise_justification_type_number = Gauge(
    "thoth_advise_justification_type_number",
    "Number of advise justification provided per justification type.",
    ["advise_message", "thoth_environment"],
)
