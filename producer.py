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

from thoth.messaging import AdviseJustificationMessage
import thoth.messaging.producer as producer
from thoth.report_processing.components.adviser import Adviser
from thoth.advise_reporter.advise_reporter import save_results_to_ceph
from thoth.advise_reporter import __service_version__
from thoth.common import init_logging
from thoth.python import Source

_LOGGER = logging.getLogger("thoth.advise_reporter")
_LOGGER.info("Thoth advise reporter producer v%s", __service_version__)

p = producer.create_producer()

ADVISER_VERSION = os.getenv("THOTH_ADVISER_VERSION", None)
_LOGGER.info(f"THOTH_ADVISER_VERSION set to {ADVISER_VERSION}.")
NUMBER_RELEASES = int(os.getenv("THOTH_NUMBER_RELEASES", 2))
ONLY_STORE = bool(int(os.getenv("THOTH_ADVISE_REPORTER_ONLY_STORE", 0)))
COMPONENT_NAME = "advise_reporter"
EVALUATION_METRICS_DAYS = int(os.getenv("THOTH_EVALUATION_METRICS_NUMBER_DAYS", 1))
LIMIT_RESULTS = bool(int(os.getenv("THOTH_LIMIT_RESULTS", 0)))
MAX_IDS = int(os.getenv("THOTH_MAX_IDS", 100))
_LOGGER.info(f"THOTH_EVALUATION_METRICS_NUMBER_DAYS set to {EVALUATION_METRICS_DAYS}.")


def main():
    """Run advise-reporter to produce message."""
    init_logging()

    adviser_versions = []

    adviser_versions.append(ADVISER_VERSION)

    number_releases = 1

    if NUMBER_RELEASES:
        number_releases = NUMBER_RELEASES

    if not ADVISER_VERSION:
        package_name = "thoth-adviser"
        index_url = "https://pypi.org/simple"
        source = Source(index_url)
        # Consider only last two releases by default
        adviser_versions = [str(v) for v in source.get_sorted_package_versions(package_name)][:number_releases]

    total_justifications: List[Dict[str, Any]] = []

    for i in range(0, EVALUATION_METRICS_DAYS):
        initial_date = datetime.datetime.utcnow() - datetime.timedelta(days=i)
        _LOGGER.info(f"Date considered: {initial_date.strftime('%d-%m-%Y')}")

        adviser_files = Adviser.aggregate_adviser_results(
            initial_date=initial_date.strftime("%d-%m-%Y"), limit_results=LIMIT_RESULTS, max_ids=MAX_IDS
        )

        justifications_collected: List[Dict[str, Any]] = []

        for adviser_version in adviser_versions:
            justifications_collected = Adviser.create_adviser_dataframe(
                adviser_version=adviser_version,
                adviser_files=adviser_files,
                justifications_collected=justifications_collected,
            )

        adviser_dataframe = Adviser._create_adviser_dataframe(justifications_collected)

        if not adviser_dataframe.empty:

            adviser_dataframe["date_"] = [
                pd.to_datetime(str(v_date)).strftime("%Y-%m-%d") for v_date in adviser_dataframe["date"].values
            ]

            advise_justifications: List[Dict[str, Any]] = []

            for adviser_version in adviser_versions:
                for unique_message in adviser_dataframe["message"].unique():
                    subset_df = adviser_dataframe[
                        (adviser_dataframe["message"] == unique_message)
                        & (adviser_dataframe["date_"] == str(initial_date.strftime("%Y-%m-%d")))
                        & (adviser_dataframe["analyzer_version"] == adviser_version)
                    ]

                    if not subset_df.empty:
                        counts = subset_df.shape[0]

                        types = [t for t in subset_df["type"].unique()]

                        if len(types) > 1:
                            _LOGGER.warning("type assigned to same message is different %s", types)

                        advise_justifications.append(
                            {
                                "date": initial_date.strftime("%Y-%m-%d"),
                                "message": unique_message,
                                "count": counts,
                                "type": types[0],
                                "adviser_version": adviser_version,
                            }
                        )

            if not advise_justifications:
                _LOGGER.info(
                    f"No adviser justifications found in date: {initial_date.strftime('%Y-%m-%d')}"
                    f" for adviser versions: {adviser_versions}"
                )

            total_justifications += advise_justifications

            advise_justification_df = pd.DataFrame(advise_justifications)

            save_results_to_ceph(advise_justification_df=advise_justification_df, date_filter=initial_date)

        else:
            _LOGGER.warning(f"No adviser documents identified on {initial_date.strftime('%d-%m-%Y')}")

    if ONLY_STORE or EVALUATION_METRICS_DAYS > 1:
        return

    for advise_justification in total_justifications:
        message = advise_justification["message"]
        count = advise_justification["count"]
        justification_type = advise_justification["type"]
        adviser_version = advise_justification["adviser_version"]

        try:
            producer.publish_to_topic(p, AdviseJustificationMessage(), AdviseJustificationMessage.MessageContents(
                message=message,
                count=int(count),
                justification_type=justification_type,
                adviser_version=adviser_version,
                component_name=COMPONENT_NAME,
                service_version=__service_version__,
            ))
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
