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

"""This is run to retrieve adviser justifications."""

import logging
import os

from typing import Dict, Any
from thoth.lab import adviser
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

_LOGGER = logging.getLogger("thoth.advise_reporter")

prometheus_registry = CollectorRegistry()

_METRIC_ADVISE_TYPE = Gauge(
    "thoth_advise_message_number", "Number of thamos advise provided per message.", ["advise_message"], registry=prometheus_registry
)

_THOTH_METRICS_PUSHGATEWAY_URL = os.getenv("PROMETHEUS_PUSHGATEWAY_URL") or "pushgateway-dh-prod-monitoring.cloud.datahub.psi.redhat.com:80"


def retrieve_adviser_reports_justifications(adviser_version: str):
    """Retrieve adviser reports justifications."""
    adviser_dataframe = adviser.aggregate_adviser_results(
        adviser_version=adviser_version,
        limit_results=False
    )
    final_dataframe = adviser.create_final_dataframe(adviser_dataframe=adviser_dataframe)

    advise_justifications = {}

    for index, row in final_dataframe[["jm_hash_id_encoded", "message", "type"]].iterrows():
        encoded_id = row["jm_hash_id_encoded"]
        if encoded_id not in advise_justifications.keys():
            advise_justifications[encoded_id] = {
                "jm_hash_id_encoded": f"type-{encoded_id}",
                "message": row["message"],
                "type": row["type"],
                "count": final_dataframe["jm_hash_id_encoded"].value_counts()[encoded_id]
            }

    return advise_justifications


def send_metrics_to_pushgateway(advise_justification: Dict[str, Any]):
    """Send metrics to Pushgateway."""
    _METRIC_ADVISE_TYPE.labels(advise_message=advise_justification.message).set(advise_justification.count)
    _LOGGER.info("advise_message_number(%r)=%r", advise_justification.message, advise_justification.count)

    if _THOTH_METRICS_PUSHGATEWAY_URL:
        try:
            _LOGGER.info(
                f"Submitting metrics to Prometheus pushgateway {_THOTH_METRICS_PUSHGATEWAY_URL}"
            )
            push_to_gateway(
                _THOTH_METRICS_PUSHGATEWAY_URL,
                job="advise-error-analysis",
                registry=prometheus_registry,
            )
        except Exception as e:
            _LOGGER.info(f"An error occurred pushing the metrics: {str(e)}")


def expose_metrics(advise_justification: Dict[str, Any]):
    """Retrieve adviser reports justifications."""
    _METRIC_ADVISE_TYPE.labels(advise_message=advise_justification.message).set(advise_justification.count)
    _LOGGER.info("advise_message_number(%r)=%r", advise_justification.message, advise_justification.count)
