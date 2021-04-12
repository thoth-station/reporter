#!/usr/bin/env python3
# adviser-reporter
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

"""This is run periodically to provide metrics regarding Advise provided to Thoth Users."""

import logging
import os
import datetime

import pandas as pd

from typing import Dict, Any, List

import thoth.messaging.producer as producer
from thoth.messaging import AdviseJustificationMessage

from thoth.report_processing.components.adviser import Adviser
from thoth.advise_reporter.utils import save_results_to_ceph
from thoth.advise_reporter.processed_results import _retrieve_processed_justifications_dataframe
from thoth.advise_reporter.processed_results import _post_process_total_justifications
from thoth.advise_reporter.processed_results import _retrieve_processed_integration_info_dataframe
from thoth.advise_reporter.processed_results import _post_process_total_integration_info

from thoth.advise_reporter import __service_version__
from thoth.common import init_logging

from thoth.storages import GraphDatabase

from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

_LOGGER = logging.getLogger("thoth.advise_reporter")
_LOGGER.info("Thoth advise reporter producer v%s", __service_version__)

COMPONENT_NAME = "advise_reporter"

_THOTH_DEPLOYMENT_NAME = os.getenv("THOTH_DEPLOYMENT_NAME")

_SEND_MESSAGES = bool(int(os.getenv("THOTH_ADVISE_REPORTER_SEND_KAFKA_MESSAGES", 0)))
_STORE_ON_CEPH = bool(int(os.getenv("THOTH_ADVISE_REPORTER_STORE_ON_CEPH", 1)))
_SEND_METRICS = bool(int(os.getenv("THOTH_ADVISE_REPORTER_SEND_METRICS", 1)))

if _SEND_MESSAGES:
    p = producer.create_producer()

_THOTH_METRICS_PUSHGATEWAY_URL = os.getenv("PROMETHEUS_PUSHGATEWAY_URL")

EVALUATION_METRICS_DAYS = int(os.getenv("THOTH_EVALUATION_METRICS_NUMBER_DAYS", 1))
LIMIT_RESULTS = bool(int(os.getenv("THOTH_LIMIT_RESULTS", 0)))
MAX_IDS = int(os.getenv("THOTH_MAX_IDS", 100))

_LOGGER.info(f"THOTH_EVALUATION_METRICS_NUMBER_DAYS set to {EVALUATION_METRICS_DAYS}.")

prometheus_registry = CollectorRegistry()

thoth_adviser_reporter_info = Gauge(
    "advise_reporter_info",
    "Thoth Adviser Reporter information",
    ["version"],
    registry=prometheus_registry,
)
thoth_adviser_reporter_info.labels(__service_version__).inc()

if _THOTH_METRICS_PUSHGATEWAY_URL:
    _METRIC_DATABASE_SCHEMA_SCRIPT = Gauge(
        "thoth_database_schema_revision_script",
        "Thoth database schema revision from script",
        ["component", "revision", "env"],
        registry=prometheus_registry,
    )

    _METRIC_DATABASE_SCHEMA_SCRIPT.labels(
        "advise-reporter", GraphDatabase().get_script_alembic_version_head(), _THOTH_DEPLOYMENT_NAME
    ).inc()


def main():
    """Run advise-reporter to produce message."""
    init_logging()

    total_justifications: List[Dict[str, Any]] = []
    total_integration_info: List[Dict[str, Any]] = []

    total_processed_daframes: List[pd.DataFrame] = {}

    for i in range(0, EVALUATION_METRICS_DAYS):
        start_date = datetime.date.today() - datetime.timedelta(days=i)
        end_date = datetime.date.today()
        _LOGGER.info(f"Date considered: {start_date}")

        daily_processed_daframes: List[pd.DataFrame] = {}

        adviser_files = Adviser.aggregate_adviser_results(start_date=start_date)

        dataframes = Adviser.create_adviser_dataframes(adviser_files=adviser_files)

        daily_justifications = _retrieve_processed_justifications_dataframe(date_=end_date, dataframes=dataframes)
        daily_processed_daframes["adviser-justifications"] = pd.DataFrame(daily_justifications)

        daily_integration_info = _retrieve_processed_integration_info_dataframe(date_=end_date, dataframes=dataframes)
        daily_processed_daframes["adviser-integration-info"] = pd.DataFrame(daily_integration_info)

        if _STORE_ON_CEPH:
            for result_class, processed_df in daily_processed_daframes.items():
                save_results_to_ceph(processed_df=processed_df, result_class=result_class, date_filter=end_date)

        total_justifications += daily_justifications
        total_integration_info += daily_integration_info

    if total_justifications:

        result_class = "total-adviser-justifications"

        justification_tdf = pd.DataFrame(total_justifications)

        total_advise_justifications = _post_process_total_justifications(
            start_date=start_date, result_class=result_class, justification_tdf=justification_tdf
        )

        total_processed_daframes[result_class] = pd.DataFrame(total_advise_justifications)

    if total_integration_info:

        result_class = "total-adviser-users-info"
        user_info_tdf = pd.DataFrame(total_integration_info)

        total_advise_integration_info = _post_process_total_integration_info(
            start_date=start_date, result_class=result_class, user_info_tdf=user_info_tdf
        )

        total_processed_daframes[result_class] = pd.DataFrame(total_advise_integration_info)

    if _STORE_ON_CEPH:
        for result_class, processed_df in total_processed_daframes.items():
            save_results_to_ceph(processed_df=processed_df, date_filter=start_date)

    if _SEND_METRICS:
        try:
            _LOGGER.debug(
                "Submitting metrics to Prometheus pushgateway %r",
                _THOTH_METRICS_PUSHGATEWAY_URL,
            )
            push_to_gateway(
                _THOTH_METRICS_PUSHGATEWAY_URL,
                job="advise-reporter",
                registry=prometheus_registry,
            )
        except Exception as exc:
            _LOGGER.exception("An error occurred pushing the metrics: %s", str(exc))

    if not _SEND_MESSAGES or EVALUATION_METRICS_DAYS > 1:
        return

    for advise_justification in total_justifications:
        message = advise_justification["message"]
        count = advise_justification["count"]
        justification_type = advise_justification["type"]
        adviser_version = advise_justification["adviser_version"]

        try:
            producer.publish_to_topic(
                p,
                AdviseJustificationMessage(),
                AdviseJustificationMessage.MessageContents(
                    message=message,
                    count=int(count),
                    justification_type=justification_type,
                    adviser_version=adviser_version,
                    component_name=COMPONENT_NAME,
                    service_version=__service_version__,
                ),
            )
            _LOGGER.debug(
                "Adviser justification message:\n%r\nJustification type:\n%r\nCount:\n%r\n",
                message,
                justification_type,
                count,
            )
        except Exception as identifier:
            _LOGGER.exception("Failed to publish with the following error message: %r", identifier)


if __name__ == "__main__":
    main()
