#!/usr/bin/env python3
# thoth-advise-reporter
# Copyright(C) 2021 Francesco Murdaca
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

"""Methods to retrieve processed results from adviser reports."""

import logging
import datetime

from typing import Dict, Any, List

from thoth.common.enums import ThothAdviserIntegrationEnum
from thoth.advise_reporter.utils import parse_justification, retrieve_thoth_sli_from_ceph

import pandas as pd


_LOGGER = logging.getLogger(__name__)


def _retrieve_processed_justifications_dataframe(
    date_: datetime.datetime,
    dataframes: Dict[str, pd.DataFrame],
) -> List[Dict[str, Any]]:
    adviser_justifications_dataframe = dataframes["justifications"]

    advise_justifications: List[Dict[str, Any]] = []

    if not adviser_justifications_dataframe.empty:

        for adviser_version in adviser_justifications_dataframe["analyzer_version"].unique():
            for unique_message in adviser_justifications_dataframe["message"].unique():
                subset_df = adviser_justifications_dataframe[
                    (adviser_justifications_dataframe["message"] == unique_message)
                    & (adviser_justifications_dataframe["date_"] == str(date_.strftime("%Y-%m-%d")))
                    & (adviser_justifications_dataframe["analyzer_version"] == adviser_version)
                ]

                if not subset_df.empty:
                    counts = subset_df.shape[0]

                    types = [t for t in subset_df["type"].unique()]

                    if len(types) > 1:
                        _LOGGER.warning("type assigned to same message is different %s", types)

                    advise_justifications.append(
                        {
                            "date": date_.strftime("%Y-%m-%d"),
                            "message": parse_justification(unique_message),
                            "count": counts,
                            "type": types[0],
                            "adviser_version": adviser_version,
                        }
                    )

        if not advise_justifications:
            _LOGGER.info(f"No adviser justifications found in date: {date_.strftime('%Y-%m-%d')}")

    else:
        _LOGGER.warning(f"No adviser justifications identified on {date_.strftime('%d-%m-%Y')}")

    return advise_justifications


def _post_process_total_justifications(
    start_date: datetime.date, result_class: str, justification_tdf: pd.DataFrame
) -> List[Dict[str, Any]]:
    total_advise_justifications = []

    file_path = f"{result_class}/{result_class}-{start_date - datetime.timedelta(days=1)}.csv"
    stored_justifications_df = retrieve_thoth_sli_from_ceph(
        file_path=file_path, columns=["adviser_version", "message", "count"]
    )

    for message in justification_tdf["message"].unique():
        for adviser_version in justification_tdf["adviser_version"].unique():
            subset_df = justification_tdf[
                (justification_tdf["message"] == message) & (justification_tdf["adviser_version"] == adviser_version)
            ]
            counts = subset_df["count"].sum()

            additions = 0
            if not stored_justifications_df.empty:
                additions = stored_justifications_df[
                    (stored_justifications_df["message"] == message)
                    & (stored_justifications_df["adviser_version"] == adviser_version)
                ]["count"]

            if counts:
                total_advise_justifications.append(
                    {
                        "adviser_version": adviser_version,
                        "message": message,
                        "count": counts + additions,
                    }
                )

    return total_advise_justifications


def _retrieve_processed_integration_info_dataframe(
    date_: datetime.datetime,
    dataframes: Dict[str, pd.DataFrame],
) -> List[Dict[str, Any]]:
    adviser_integration_info_dataframe = dataframes["integration_info"]

    integration_info: List[Dict[str, Any]] = []

    if not adviser_integration_info_dataframe.empty:

        for advise_integration in ThothAdviserIntegrationEnum._member_names_:  # type: ignore
            subset_df = adviser_integration_info_dataframe[
                (adviser_integration_info_dataframe["source_type"] == advise_integration)
                & (adviser_integration_info_dataframe["date_"] == str(date_.strftime("%Y-%m-%d")))
            ]

            counts = 0

            if not subset_df.empty:
                counts = subset_df.shape[0]

            integration_info.append(
                {
                    "date": date_.strftime("%Y-%m-%d"),
                    "source_type": advise_integration,
                    "count": counts,
                }
            )

        if not integration_info:
            _LOGGER.info(f"No adviser integration info found in date: {date_.strftime('%Y-%m-%d')}")

    else:
        _LOGGER.warning(f"No adviser integration info identified on {date_.strftime('%d-%m-%Y')}")

    return integration_info


def _post_process_total_integration_info(
    start_date: datetime.date, result_class: str, user_info_tdf: pd.DataFrame
) -> List[Dict[str, Any]]:

    total_advise_integration_info = []

    file_path = f"{result_class}/{result_class}-{start_date - datetime.timedelta(days=1)}.csv"
    stored_integration_info_df = retrieve_thoth_sli_from_ceph(file_path=file_path, columns=["source_type", "count"])

    for source_type in user_info_tdf["source_type"].unique():
        subset_df = user_info_tdf[user_info_tdf["source_type"] == source_type]
        counts = subset_df["count"].sum()

        additions = 0
        if not stored_integration_info_df.empty:
            additions = stored_integration_info_df[stored_integration_info_df["source_type"] == source_type]["count"]

        total_advise_integration_info.append(
            {
                "source_type": source_type,
                "count": counts + additions,
            }
        )

    return total_advise_integration_info
