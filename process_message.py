#!/usr/bin/env python3
# thoth-package-update
# Copyright(C) 2020 Kevin Postlethwait
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

"""Process message for update_consumer."""

import logging
import os

from thoth.common import init_logging, OpenShift
from prometheus_client import CollectorRegistry, Gauge, Counter, Summary, push_to_gateway

init_logging()

_LOGGER = logging.getLogger("thoth.package_update")

prometheus_registry = CollectorRegistry()

_THOTH_METRICS_PUSHGATEWAY_URL = (
    os.getenv("PROMETHEUS_PUSHGATEWAY_URL") or "pushgateway-dh-prod-monitoring.cloud.datahub.psi.redhat.com:80"
)

_METRIC_ADVISE_TYPE = Gauge(
    "thoth_advise_type_number",
    "Number of thamos advise provided per type.",
    ["advise_message", "advise_type"],
    registry=prometheus_registry,
)

REQUEST_TIME = Summary("request_processing_seconds", "Time spent processing request")


@REQUEST_TIME.time()
def process_advise_justification(advise_justification):
    """Process a advise justification message from advise-reporter producer."""
    _METRIC_ADVISE_TYPE.labels(advise_justification["message"]).set(advise_justification["count"])

    if _THOTH_METRICS_PUSHGATEWAY_URL:
        try:
            _LOGGER.info(f"Submitting metrics to Prometheus pushgateway {_THOTH_METRICS_PUSHGATEWAY_URL}")
            push_to_gateway(_THOTH_METRICS_PUSHGATEWAY_URL, job="advise-type-analysis", registry=prometheus_registry)
        except Exception as e:
            _LOGGER.warning(f"An error occurred pushing the metrics: {str(e)}")
