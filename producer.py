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
from thoth.messaging.advise_justification import MessageContents as AdviseJustificationContents

from thoth.report_processing.components.adviser import Adviser
from thoth.advise_reporter.utils import save_results_to_ceph
from thoth.advise_reporter.processed_results import retrieve_processed_justifications_dataframe
from thoth.advise_reporter.processed_results import retrieve_processed_statistics_dataframe
from thoth.advise_reporter.processed_results import retrieve_processed_inputs_info_dataframe

from thoth.advise_reporter import __service_version__

from thoth.storages import GraphDatabase

from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

_LOGGER = logging.getLogger("thoth.advise_reporter")
_LOGGER.info("Thoth advise reporter producer v%s", __service_version__)

COMPONENT_NAME = "advise_reporter"

_THOTH_DEPLOYMENT_NAME = os.getenv("THOTH_DEPLOYMENT_NAME")

_SEND_MESSAGES = bool(int(os.getenv("THOTH_ADVISE_REPORTER_SEND_KAFKA_MESSAGES", 0)))
_STORE_ON_CEPH = bool(int(os.getenv("THOTH_ADVISE_REPORTER_STORE_ON_CEPH", 1)))
_STORE_ON_PUBLIC_CEPH = bool(int(os.getenv("THOTH_ADVISE_REPORTER_STORE_ON_PUBLIC_CEPH", 0)))

_SEND_METRICS = bool(int(os.getenv("THOTH_ADVISE_REPORTER_SEND_METRICS", 1)))

if _SEND_MESSAGES:
    p = producer.create_producer()

_THOTH_METRICS_PUSHGATEWAY_URL = os.getenv("PROMETHEUS_PUSHGATEWAY_URL")

_DEBUG_LEVEL = bool(int(os.getenv("DEBUG_LEVEL", 0)))

if _DEBUG_LEVEL:
    logging.basicConfig(level=logging.DEBUG)
else:
    logging.basicConfig(level=logging.INFO)

LIMIT_RESULTS = bool(int(os.getenv("THOTH_LIMIT_RESULTS", 0)))
MAX_IDS = int(os.getenv("THOTH_MAX_IDS", 100))

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

TODAY = datetime.date.today()

START_DATE = os.getenv("ADVISE_REPORTER_START_DATE", str(TODAY))
END_DATE = os.getenv("ADVISE_REPORTER_END_DATE", str(TODAY))


def main():
    """Run advise-reporter to produce message."""
    if not _SEND_MESSAGES:
        _LOGGER.info("No messages are sent. THOTH_ADVISE_REPORTER_SEND_KAFKA_MESSAGES is set to 0")

    total_justifications: List[Dict[str, Any]] = []
    try:
        datetime.datetime.strptime(START_DATE, "%Y-%m-%d")
    except ValueError as err:
        _LOGGER.error(f"ADVISE_REPORTER_START_DATE uses incorrect format: {err}")

    s_date = START_DATE.split("-")
    start_date = datetime.date(year=int(s_date[0]), month=int(s_date[1]), day=int(s_date[2]))

    try:
        datetime.datetime.strptime(END_DATE, "%Y-%m-%d")
    except ValueError as err:
        _LOGGER.error(f"ADVISE_REPORTER_END_DATE uses incorrect format: {err}")

    e_date = END_DATE.split("-")
    end_date = datetime.date(year=int(e_date[0]), month=int(e_date[1]), day=int(e_date[2]))

    _LOGGER.info(f"Start Date considered: {start_date}")
    _LOGGER.info(f"End Date considered (excluded): {end_date}")

    delta = datetime.timedelta(days=1)

    if start_date == TODAY + delta:
        _LOGGER.info(f"start date ({start_date}) cannot be in the future. Today is: {TODAY}.")
        start_date = TODAY
        _LOGGER.info(f"new start date is: {start_date}.")

    if end_date > TODAY + delta:
        _LOGGER.info(f"end date ({end_date}) cannot be in the future. Today is: {TODAY}.")
        end_date = TODAY
        _LOGGER.info(f"new end date is: {end_date}.")

    if end_date < start_date:
        _LOGGER.error(f"Cannot analyze adviser data: end date ({end_date}) < start_date ({start_date}).")
        return

    if end_date == start_date:
        if start_date == TODAY:
            _LOGGER.info(f"end date ({end_date}) == start_date ({start_date}) == today ({TODAY}).")
            start_date = start_date - delta
            _LOGGER.info(f"new start date is: {start_date}.")
        else:
            _LOGGER.info(f"end date ({end_date}) == start_date ({start_date}).")
            end_date = end_date + datetime.timedelta(days=1)
            _LOGGER.info(f"new end date (excluded) is: {end_date}.")

    _LOGGER.info(f"Initial start date: {start_date}")
    _LOGGER.info(f"Initial end date (excluded): {end_date}")

    current_initial_date = start_date

    while current_initial_date < end_date:

        current_end_date = current_initial_date + delta

        _LOGGER.info(f"Analyzing for start date: {current_initial_date}")
        _LOGGER.info(f"Analyzing for end date (excluded): {current_end_date}")

        daily_processed_daframes: List[pd.DataFrame] = {}

        adviser_files = Adviser.aggregate_adviser_results(start_date=current_initial_date, end_date=current_end_date)

        if not adviser_files:
            _LOGGER.info("No adviser files identifed!")
            current_initial_date += delta
            continue

        dataframes = Adviser.create_adviser_dataframes(adviser_files=adviser_files)

        daily_justifications = retrieve_processed_justifications_dataframe(
            date_=current_initial_date, dataframes=dataframes
        )
        daily_processed_daframes["adviser_justifications"] = pd.DataFrame(daily_justifications)

        if not daily_processed_daframes["adviser_justifications"].empty and not _STORE_ON_CEPH:
            _LOGGER.info(
                "Adviser justifications:"
                f'\n{daily_processed_daframes["adviser_justifications"].to_csv(header=False, sep="`", index=False)}'
            )

        daily_statistics = retrieve_processed_statistics_dataframe(date_=current_initial_date, dataframes=dataframes)
        daily_processed_daframes["adviser_statistics"] = pd.DataFrame(daily_statistics)

        if not daily_processed_daframes["adviser_statistics"].empty and not _STORE_ON_CEPH:
            _LOGGER.info(
                "Adviser statistics success rate:"
                f'\n{daily_processed_daframes["adviser_statistics"].to_csv(header=False, sep="`", index=False)}'
            )

        daily_inputs_info = retrieve_processed_inputs_info_dataframe(date_=start_date, dataframes=dataframes)
        daily_processed_daframes["adviser_integration_info"] = pd.DataFrame(daily_inputs_info["integration_info"])
        daily_processed_daframes["adviser_recommendation_info"] = pd.DataFrame(daily_inputs_info["recommendation_info"])
        daily_processed_daframes["adviser_solver_info"] = pd.DataFrame(daily_inputs_info["solver_info"])
        daily_processed_daframes["adviser_base_image_info"] = pd.DataFrame(daily_inputs_info["base_image_info"])
        daily_processed_daframes["adviser_hardware_info"] = pd.DataFrame(daily_inputs_info["hardware_info"])

        if not daily_processed_daframes["adviser_integration_info"].empty and not _STORE_ON_CEPH:
            _LOGGER.info(
                "Adviser integration info stats:"
                f'\n{daily_processed_daframes["adviser_integration_info"].to_csv(header=False, sep="`", index=False)}'
            )

        if not daily_processed_daframes["adviser_recommendation_info"].empty and not _STORE_ON_CEPH:
            _LOGGER.info(
                "Adviser recomendation info stats:"
                f'\n{daily_processed_daframes["adviser_recommendation_info"].to_csv(header=False, sep="`", index=False)}'
            )

        if not daily_processed_daframes["adviser_solver_info"].empty and not _STORE_ON_CEPH:
            _LOGGER.info(
                "Adviser solver info stats:"
                f'\n{daily_processed_daframes["adviser_solver_info"].to_csv(header=False, sep="`", index=False)}'
            )

        if not daily_processed_daframes["adviser_base_image_info"].empty and not _STORE_ON_CEPH:
            _LOGGER.info(
                "Adviser base image info stats:"
                f'\n{daily_processed_daframes["adviser_base_image_info"].to_csv(header=False, sep="`", index=False)}'
            )

        if not daily_processed_daframes["adviser_hardware_info"].empty and not _STORE_ON_CEPH:
            _LOGGER.info(
                "Adviser hardware info stats:"
                f'\n{daily_processed_daframes["adviser_hardware_info"].to_csv(header=False, sep="`", index=False)}'
            )

        if _STORE_ON_CEPH:
            for result_class, processed_df in daily_processed_daframes.items():
                save_results_to_ceph(
                    processed_df=processed_df,
                    result_class=result_class,
                    date_filter=current_initial_date,
                    store_to_public_ceph=_STORE_ON_PUBLIC_CEPH,
                )

        total_justifications += daily_justifications
        current_initial_date += delta

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

    if not _SEND_MESSAGES:
        return

    for advise_justification in total_justifications:
        message = advise_justification["message"]
        count = advise_justification["count"]
        justification_type = advise_justification["type"]
        adviser_version = advise_justification["adviser_version"]

        try:
            producer.publish_to_topic(
                p,
                AdviseJustificationContents(),
                AdviseJustificationContents.MessageContents(
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
