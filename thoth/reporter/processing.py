#!/usr/bin/env python3
# thoth-reporter
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

"""Methods to process results from components reports."""

import logging
import datetime

from typing import Dict, Any, List

from thoth.report_processing.components.adviser import Adviser
from thoth.common.enums import ThothAdviserIntegrationEnum
from thoth.storages.graph.enums import RecommendationTypeEnum
from thoth.reporter.utils import parse_justification
from thoth.reporter.utils import save_results_to_ceph

import pandas as pd


_LOGGER = logging.getLogger(__name__)


def explore_adviser_files(
    current_initial_date: datetime.date,
    current_end_date: datetime.date,
    total_justifications: List[Dict[str, Any]],
    store_on_ceph: bool = False,
    store_on_public_bucket: bool = False,
):
    """Explore adviser files to gather info for contributors."""
    daily_processed_dataframes: Dict[str, pd.DataFrame] = {}

    adviser_files = Adviser.aggregate_adviser_results(start_date=current_initial_date, end_date=current_end_date)

    if not adviser_files:
        _LOGGER.info("No adviser files identifed!")
        return total_justifications

    dataframes = Adviser.create_adviser_dataframes(adviser_files=adviser_files)

    daily_justifications = retrieve_processed_justifications_dataframe(
        date_=current_initial_date, dataframes=dataframes
    )
    daily_processed_dataframes["adviser_justifications"] = pd.DataFrame(daily_justifications)

    if not daily_processed_dataframes["adviser_justifications"].empty and not store_on_ceph:
        _LOGGER.info(
            "Adviser justifications:"
            f'\n{daily_processed_dataframes["adviser_justifications"].to_csv(header=False, sep="`", index=False)}'
        )

    daily_statistics = retrieve_processed_statistics_dataframe(date_=current_initial_date, dataframes=dataframes)
    daily_processed_dataframes["adviser_statistics"] = pd.DataFrame(daily_statistics)

    if not daily_processed_dataframes["adviser_statistics"].empty and not store_on_ceph:
        _LOGGER.info(
            "Adviser statistics success rate:"
            f'\n{daily_processed_dataframes["adviser_statistics"].to_csv(header=False, sep="`", index=False)}'
        )

    daily_inputs_info = retrieve_processed_inputs_info_dataframe(date_=current_initial_date, dataframes=dataframes)
    daily_processed_dataframes["adviser_integration_info"] = pd.DataFrame(daily_inputs_info["integration_info"])
    daily_processed_dataframes["adviser_recommendation_info"] = pd.DataFrame(daily_inputs_info["recommendation_info"])
    daily_processed_dataframes["adviser_solver_info"] = pd.DataFrame(daily_inputs_info["solver_info"])
    daily_processed_dataframes["adviser_base_image_info"] = pd.DataFrame(daily_inputs_info["base_image_info"])
    daily_processed_dataframes["adviser_hardware_info"] = pd.DataFrame(daily_inputs_info["hardware_info"])

    if not daily_processed_dataframes["adviser_integration_info"].empty and not store_on_ceph:
        _LOGGER.info(
            "Adviser integration info stats:"
            f'\n{daily_processed_dataframes["adviser_integration_info"].to_csv(header=False, sep="`", index=False)}'
        )

    if not daily_processed_dataframes["adviser_recommendation_info"].empty and not store_on_ceph:
        _LOGGER.info(
            "Adviser recomendation info stats:"
            f'\n{daily_processed_dataframes["adviser_recommendation_info"].to_csv(header=False, sep="`", index=False)}'
        )

    if not daily_processed_dataframes["adviser_solver_info"].empty and not store_on_ceph:
        _LOGGER.info(
            "Adviser solver info stats:"
            f'\n{daily_processed_dataframes["adviser_solver_info"].to_csv(header=False, sep="`", index=False)}'
        )

    if not daily_processed_dataframes["adviser_base_image_info"].empty and not store_on_ceph:
        _LOGGER.info(
            "Adviser base image info stats:"
            f'\n{daily_processed_dataframes["adviser_base_image_info"].to_csv(header=False, sep="`", index=False)}'
        )

    if not daily_processed_dataframes["adviser_hardware_info"].empty and not store_on_ceph:
        _LOGGER.info(
            "Adviser hardware info stats:"
            f'\n{daily_processed_dataframes["adviser_hardware_info"].to_csv(header=False, sep="`", index=False)}'
        )

    if store_on_ceph:
        for result_class, processed_df in daily_processed_dataframes.items():
            save_results_to_ceph(
                processed_df=processed_df,
                result_class=result_class,
                date_filter=current_initial_date,
                store_to_public_ceph=store_on_public_bucket,
            )

    total_justifications += daily_justifications

    return total_justifications


def retrieve_processed_justifications_dataframe(
    date_: datetime.date,
    dataframes: Dict[str, pd.DataFrame],
) -> List[Dict[str, Any]]:
    """Process adviser reports justifications (if any)."""
    adviser_justifications_dataframe = dataframes["justifications"]

    advise_justifications: List[Dict[str, Any]] = []

    if not adviser_justifications_dataframe.empty:

        for adviser_version in adviser_justifications_dataframe["analyzer_version"].unique():
            for unique_message in adviser_justifications_dataframe["message"].unique():
                subset_df = adviser_justifications_dataframe[
                    (adviser_justifications_dataframe["message"] == unique_message)
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


def retrieve_processed_statistics_dataframe(
    date_: datetime.date,
    dataframes: Dict[str, pd.DataFrame],
) -> List[Dict[str, Any]]:
    """Process adviser reports statistics."""
    advise_statistics_dataframe = dataframes["statistics"]

    advise_statistics: List[Dict[str, Any]] = []

    if not advise_statistics_dataframe.empty:

        for adviser_version in advise_statistics_dataframe["adviser_version"].unique():
            subset_df = advise_statistics_dataframe[(advise_statistics_dataframe["adviser_version"] == adviser_version)]

            advise_statistics.append(
                {
                    "adviser_version": adviser_version,
                    "success": subset_df["success"].values[0],
                    "failure": subset_df["failure"].values[0],
                }
            )

        if not advise_statistics:
            _LOGGER.info(f"No adviser statistics found in date: {date_.strftime('%Y-%m-%d')}")

    else:
        _LOGGER.warning(f"No adviser statistics identified on {date_.strftime('%d-%m-%Y')}")

    return advise_statistics


def retrieve_processed_inputs_info_dataframe(
    date_: datetime.date,
    dataframes: Dict[str, pd.DataFrame],
) -> Dict[str, List[Dict[str, Any]]]:
    """Process adviser inputs."""
    adviser_inputs_info_dataframe = dataframes["inputs_info"]

    integration_info: List[Dict[str, Any]] = []
    recommendation_type_info: List[Dict[str, Any]] = []
    solver_info: List[Dict[str, Any]] = []
    base_image_info: List[Dict[str, Any]] = []
    hardware_info: List[Dict[str, Any]] = []

    if not adviser_inputs_info_dataframe.empty:

        for advise_integration in ThothAdviserIntegrationEnum._member_names_:  # type: ignore
            subset_df = adviser_inputs_info_dataframe[
                (adviser_inputs_info_dataframe["source_type"] == advise_integration)
            ]

            counts = 0

            if not subset_df.empty:
                counts = subset_df.shape[0]

            integration_info.append(
                {
                    "date": str(date_),
                    "integration": advise_integration,
                    "count": counts,
                }
            )

        if not integration_info:
            _LOGGER.info(f"No adviser integration info found in date: {date_.strftime('%Y-%m-%d')}")

        for recommendation_type in RecommendationTypeEnum._member_names_:  # type: ignore
            subset_df = adviser_inputs_info_dataframe[
                (adviser_inputs_info_dataframe["recommendation_type"] == recommendation_type)
            ]

            counts = 0

            if not subset_df.empty:
                counts = subset_df.shape[0]

            recommendation_type_info.append(
                {
                    "date": str(date_),
                    "recommendation_type": recommendation_type,
                    "count": counts,
                }
            )

        if not recommendation_type_info:
            _LOGGER.info(f"No adviser recommendation_type info found in date: {date_.strftime('%Y-%m-%d')}")

        for solver in adviser_inputs_info_dataframe["solver"].unique():
            subset_df = adviser_inputs_info_dataframe[(adviser_inputs_info_dataframe["solver"] == solver)]

            counts = 0

            if not subset_df.empty:
                counts = subset_df.shape[0]

            solver_info.append(
                {
                    "date": str(date_),
                    "solver": solver,
                    "count": counts,
                }
            )

        if not solver_info:
            _LOGGER.info(f"No adviser solver info found in date: {date_.strftime('%Y-%m-%d')}")

        for base_image in adviser_inputs_info_dataframe["base_image"].unique():
            subset_df = adviser_inputs_info_dataframe[
                (adviser_inputs_info_dataframe["base_image"] == base_image)
                & (adviser_inputs_info_dataframe["cpu_model"] != "None")
            ]

            counts = 0

            if not subset_df.empty:
                counts = subset_df.shape[0]

            if base_image:
                base_image_info.append(
                    {
                        "date": str(date_),
                        "base_image": base_image,
                        "count": counts,
                    }
                )

        if not base_image_info:
            _LOGGER.info(f"No adviser base_image info found in date: {date_.strftime('%Y-%m-%d')}")

        for cpu_model in adviser_inputs_info_dataframe["cpu_model"].unique():
            subset_df = adviser_inputs_info_dataframe[
                (adviser_inputs_info_dataframe["cpu_model"] == cpu_model)
                & (adviser_inputs_info_dataframe["cpu_model"] != "None")
            ]

            counts = 0
            cpu_family = ""

            if not subset_df.empty:
                counts = subset_df.shape[0]
                cpu_family = subset_df["cpu_family"].values[0]

            if cpu_model and cpu_family:
                hardware_info.append(
                    {
                        "date": str(date_),
                        "cpu_model": cpu_model,
                        "cpu_family": cpu_family,
                        "count": counts,
                    }
                )

        if not hardware_info:
            _LOGGER.info(f"No adviser hardware info found in date: {date_.strftime('%Y-%m-%d')}")

    else:
        _LOGGER.warning(f"No adviser inputs info identified on {date_.strftime('%d-%m-%Y')}")

    return {
        "integration_info": integration_info,
        "recommendation_info": recommendation_type_info,
        "solver_info": solver_info,
        "base_image_info": base_image_info,
        "hardware_info": hardware_info,
    }


def evaluate_requests_statistics(
    current_initial_date: datetime.date,
    current_end_date: datetime.date,
    results_store: Dict[str, Any],
    store_on_ceph: bool = False,
    store_on_public_bucket: bool = False,
) -> List[Dict[str, Any]]:
    """Evaluate requests statistics (requests - reports created)."""
    stats: List[Dict[str, Any]] = []
    for component, result_store in results_store.items():
        stats.append(
            {
                "date": current_initial_date,
                "component": component,
                "requests": result_store.get_document_count(
                    start_date=current_initial_date, end_date=current_end_date, only_requests=True
                ),
                "documents": result_store.get_document_count(
                    start_date=current_initial_date, end_date=current_end_date
                ),
            }
        )

    processed_df = pd.DataFrame(stats)

    if not processed_df.empty and not store_on_ceph:
        _LOGGER.info("components requests stats:" f'\n{processed_df.to_csv(header=False, sep="`", index=False)}')

    if store_on_ceph:
        save_results_to_ceph(
            processed_df=processed_df,
            result_class="requests_analysis",
            date_filter=current_initial_date,
            store_to_public_ceph=store_on_public_bucket,
        )

    return stats
