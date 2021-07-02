#!/usr/bin/env python3
# thoth-reporter
# Copyright(C) 2020, 2021 Francesco Murdaca
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

"""This is run periodically to provide metrics regarding Thoth services to Thoth contributors."""

import logging
import os
import datetime

from typing import Dict, Any, List

import thoth.messaging.producer as producer
from thoth.messaging import advise_justification_message
from thoth.messaging.advise_justification import MessageContents as AdviseJustificationContents

from thoth.reporter.processing import evaluate_requests_statistics, explore_adviser_files

from thoth.reporter import __service_version__

from thoth.storages import GraphDatabase
from thoth.storages.advisers import AdvisersResultsStore
from thoth.storages.analyses import AnalysisResultsStore
from thoth.storages.provenance import ProvenanceResultsStore


from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

_LOGGER = logging.getLogger("thoth.thoth_reporter")
_LOGGER.info("Thoth thoth reporter producer v%s", __service_version__)

COMPONENT_NAME = "thoth_reporter"

_THOTH_DEPLOYMENT_NAME = os.getenv("THOTH_DEPLOYMENT_NAME")

_SEND_MESSAGES = bool(int(os.getenv("THOTH_REPORTER_SEND_KAFKA_MESSAGES", 0)))
STORE_ON_CEPH = bool(int(os.getenv("THOTH_REPORTER_STORE_ON_CEPH", 1)))
STORE_ON_PUBLIC_CEPH = bool(int(os.getenv("THOTH_REPORTER_STORE_ON_PUBLIC_CEPH", 0)))

_SEND_METRICS = bool(int(os.getenv("THOTH_REPORTER_SEND_METRICS", 1)))

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

# Define metrics

thoth_reporter_info = Gauge(
    "thoth_reporter_info",
    "Thoth Reporter information",
    ["version"],
    registry=prometheus_registry,
)
thoth_reporter_info.labels(__service_version__).inc()

thoth_reporter_requests_gauge = Gauge(
    "thoth_reporter_requests_gauge",
    "Thoth Reporter requests created per component",
    ["component", "env"],
    registry=prometheus_registry,
)

thoth_reporter_reports_gauge = Gauge(
    "thoth_reporter_reports_gauge",
    "Thoth Reporter reports created per component",
    ["component", "env"],
    registry=prometheus_registry,
)

thoth_reporter_failed_adviser_justifications_gauge = Gauge(
    "thoth_reporter_failed_adviser_justifications_gauge",
    "Thoth Reporter percentage of failed adviser justifications",
    ["adviser_version", "justification", "env"],
    registry=prometheus_registry,
)

thoth_reporter_failed_adviser_gauge = Gauge(
    "thoth_reporter_failed_adviser_gauge",
    "Thoth Reporter percentage of failed adviser",
    ["adviser_version", "env"],
    registry=prometheus_registry,
)

thoth_reporter_success_adviser_gauge = Gauge(
    "thoth_reporter_success_adviser_gauge",
    "Thoth Reporter percentage of successfull adviser",
    ["adviser_version", "env"],
    registry=prometheus_registry,
)

if _THOTH_METRICS_PUSHGATEWAY_URL:
    _METRIC_DATABASE_SCHEMA_SCRIPT = Gauge(
        "thoth_database_schema_revision_script",
        "Thoth database schema revision from script",
        ["component", "revision", "env"],
        registry=prometheus_registry,
    )

    _METRIC_DATABASE_SCHEMA_SCRIPT.labels(
        "thoth-reporter", GraphDatabase().get_script_alembic_version_head(), _THOTH_DEPLOYMENT_NAME
    ).inc()

ADVISER_STORE = AdvisersResultsStore()
ADVISER_STORE.connect()

PACKAGE_EXTRACT_STORE = AnalysisResultsStore()
PACKAGE_EXTRACT_STORE.connect()

PROVENANCE_STORE = ProvenanceResultsStore()
PROVENANCE_STORE.connect()

RESULTS_STORES = {
    "adviser": ADVISER_STORE,
    "package-extract": PACKAGE_EXTRACT_STORE,
    "provenance-checker": PROVENANCE_STORE,
}

TODAY = datetime.date.today()

START_DATE = os.getenv("THOTH_REPORTER_START_DATE", str(TODAY))
END_DATE = os.getenv("THOTH_REPORTER_END_DATE", str(TODAY))


def main():
    """Run thoth-reporter to provide information on status of services to Thoth contributors."""
    if not _SEND_MESSAGES:
        _LOGGER.info("No messages are sent. THOTH_REPORTER_SEND_KAFKA_MESSAGES is set to 0")

    try:
        datetime.datetime.strptime(START_DATE, "%Y-%m-%d")
    except ValueError as err:
        _LOGGER.error(f"THOTH_REPORTER_START_DATE uses incorrect format: {err}")

    s_date = START_DATE.split("-")
    start_date = datetime.date(year=int(s_date[0]), month=int(s_date[1]), day=int(s_date[2]))

    try:
        datetime.datetime.strptime(END_DATE, "%Y-%m-%d")
    except ValueError as err:
        _LOGGER.error(f"THOTH_REPORTER_END_DATE uses incorrect format: {err}")

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

    total_justifications: List[Dict[str, Any]] = []

    current_initial_date = start_date

    while current_initial_date < end_date:

        current_end_date = current_initial_date + delta

        _LOGGER.info(f"Analyzing for start date: {current_initial_date}")
        _LOGGER.info(f"Analyzing for end date (excluded): {current_end_date}")

        stats = evaluate_requests_statistics(
            current_initial_date=current_initial_date,
            current_end_date=current_end_date,
            results_store=RESULTS_STORES,
            store_on_ceph=STORE_ON_CEPH,
            store_on_public_bucket=STORE_ON_PUBLIC_CEPH,
        )

        # Assign metrics for pushgateway
        for stats_analysis in stats:

            thoth_reporter_requests_gauge.labels(stats_analysis["component"], _THOTH_DEPLOYMENT_NAME).set(
                stats_analysis["requests"]
            )

            thoth_reporter_reports_gauge.labels(stats_analysis["component"], _THOTH_DEPLOYMENT_NAME).set(
                stats_analysis["documents"]
            )

        daily_processed_dataframes = explore_adviser_files(
            current_initial_date=current_initial_date,
            current_end_date=current_end_date,
            total_justifications=total_justifications,
            store_on_ceph=STORE_ON_CEPH,
            store_on_public_bucket=STORE_ON_PUBLIC_CEPH,
        )

        justification_to_send = []
        statistics_to_send = []

        if not daily_processed_dataframes:
            current_initial_date += delta
            continue

        # Advise justifications
        total_js: Dict[str, Any] = {}
        daily_justifications_df = daily_processed_dataframes["adviser_justifications"]

        if not daily_justifications_df.empty:
            for message in daily_justifications_df["message"].unique():
                for adviser_version in daily_justifications_df["adviser_version"].unique():
                    subset_df = daily_justifications_df[
                        (daily_justifications_df["message"] == message)
                        & (daily_justifications_df["adviser_version"] == adviser_version)
                    ]

                    if subset_df.shape[0] < 1:
                        continue

                    counts = subset_df["count"].values[0]

                    message_type = subset_df["type"].values[0]

                    if message_type != "ERROR":
                        continue

                    if adviser_version not in total_js:
                        total_js[adviser_version] = {}

                        total_js[adviser_version][message] = counts
                    else:
                        if message not in total_js[adviser_version]:
                            total_js[adviser_version][message] = counts
                        else:
                            total_js[adviser_version][message] += counts

            for adviser_version, justifications_info in total_js.items():

                total_errors = 0
                for _, errors_counts in justifications_info.items():
                    total_errors += errors_counts

                for justification, counts in justifications_info.items():

                    if not counts:
                        total = "0"
                        percentage = 0
                    else:
                        total = "+" + "{}".format(int(counts))
                        percentage = counts / total_errors

                    justification_to_send.append(
                        {
                            "adviser_version": adviser_version,
                            "justification": justification,
                            "total": total,
                            "percentage": abs(round(percentage * 100, 3)),
                        },
                    )
        else:
            _LOGGER.warning("No adviser justification identified.")

        # Advise statistics
        total_statistics: Dict[str, Any] = {}

        adviser_statistics = daily_processed_dataframes["adviser_statistics"]

        if not adviser_statistics.empty:
            for adviser_version in adviser_statistics["adviser_version"].unique():
                subset_df = adviser_statistics[adviser_statistics["adviser_version"] == adviser_version]

                s_counts = 0
                f_counts = 0

                if not subset_df.empty:
                    s_counts = subset_df[subset_df["adviser_version"] == adviser_version]["success"].values[0]
                    f_counts = subset_df[subset_df["adviser_version"] == adviser_version]["failure"].values[0]

                if adviser_version not in total_statistics:
                    total_statistics[adviser_version] = {}

                    total_statistics[adviser_version]["success"] = s_counts
                    total_statistics[adviser_version]["failure"] = f_counts
                else:
                    total_statistics[adviser_version]["success"] += s_counts
                    total_statistics[adviser_version]["failure"] += f_counts

            for adviser_version, statistics_info in total_statistics.items():

                total = statistics_info["success"] + statistics_info["failure"]

                success_p = statistics_info["success"] / total
                failure_p = statistics_info["failure"] / total

                statistics_to_send.append(
                    {
                        "adviser_version": adviser_version,
                        "success_p": abs(round(success_p * 100, 3)),
                        "failure_p": abs(round(failure_p * 100, 3)),
                    },
                )
        else:
            _LOGGER.warning("No adviser statistics identified.")

        current_initial_date += delta

    if _SEND_METRICS:

        if justification_to_send:
            for js in justification_to_send:

                thoth_reporter_failed_adviser_justifications_gauge.labels(
                    adviser_version=js["adviser_version"], justification=js["justification"], env=_THOTH_DEPLOYMENT_NAME
                ).set(js["percentage"])

        if statistics_to_send:
            for a_stats in statistics_to_send:

                thoth_reporter_failed_adviser_gauge.labels(
                    adviser_version=a_stats["adviser_version"], env=_THOTH_DEPLOYMENT_NAME
                ).set(a_stats["failure_p"])

                thoth_reporter_success_adviser_gauge.labels(
                    adviser_version=a_stats["adviser_version"], env=_THOTH_DEPLOYMENT_NAME
                ).set(a_stats["success_p"])

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
                advise_justification_message,
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

    p.flush()


if __name__ == "__main__":
    main()
