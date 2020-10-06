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
import datetime

import pandas as pd
from typing import Dict, Any, List

from thoth.report_processing.components import Adviser

_LOGGER = logging.getLogger(__name__)

DEPLOYMENT_NAME = os.environ["THOTH_DEPLOYMENT_NAME"]
ENVIRONMENT = os.environ["THOTH_ENVIRONMENT"]
CEPH_BUCKET_PREFIX = os.environ["THOTH_CEPH_BUCKET_PREFIX"]
PUBLIC_CEPH_BUCKET = os.environ["THOTH_PUBLIC_CEPH_BUCKET"]

IS_STORING = bool(int(os.getenv("THOTH_IS_STORING", 1)))


def parse_summary_dataframe(
    advise_justifications: List[Dict[str, Any]],
    summary_dataframe: pd.DataFrame,
    date_filter: datetime.datetime,
    justification_type: str,
    adviser_version: str,
) -> List[Dict[str, Any]]:
    """Parse final dataframe to produce messages depending on the justification type.

    :param summary_dataframe: DataFrame as returned by `Adviser.create_summary_dataframe` method.
    """
    justification_dataframe = summary_dataframe[summary_dataframe["type"] == justification_type]

    if not justification_dataframe.empty:
        adviser_heatmap_df = Adviser.create_adviser_results_dataframe_heatmap(
            adviser_type_dataframe=justification_dataframe, number_days=1
        )
        all_dates = [date for date in adviser_heatmap_df.columns.values]
        last_date = all_dates[-1]

        if not adviser_heatmap_df.empty and last_date > date_filter:
            _LOGGER.info(f"New adviser runs identified for date: {date_filter}")

            selected_date = None
            for considered_date in all_dates[::-1]:
                # Only one column should exist because intervals of 1 are created
                # using `create_adviser_results_dataframe_heatmap`
                date_difference = considered_date - date_filter
                if 0 < date_difference.total_seconds() < datetime.timedelta(days=1).total_seconds():
                    selected_date = considered_date
                    subset_adviser_results = adviser_heatmap_df[[selected_date]]
                    _LOGGER.info(f"Date identified in column: {selected_date}")
                    break

            if not selected_date:
                return advise_justifications

            for index, row in subset_adviser_results[[selected_date]].iterrows():
                message = index
                if message not in [m["message"] for m in advise_justifications]:
                    advise_justification = {
                            "date": selected_date.strftime("%Y-%m-%d"),
                            "message": message,
                            "count": row[selected_date],
                            "type": justification_type,
                            "adviser_version": adviser_version,
                        }
                    if all(value != None for value in advise_justification.values()):
                        advise_justifications.append(advise_justification)
                else:
                    if adviser_version not in [m["adviser_version"] for m in advise_justifications]:
                        advise_justification = {
                                "date": selected_date.strftime("%Y-%m-%d"),
                                "message": message,
                                "count": row[selected_date],
                                "type": justification_type,
                                "adviser_version": adviser_version,
                            }
                        if all(value != None for value in advise_justification.values()):
                            advise_justifications.append(advise_justification)

    return advise_justifications


def save_results_to_ceph(advise_justification_df: pd.DataFrame, date_filter: datetime.datetime):
    """Save results on Ceph."""
    if advise_justification_df.empty:
        return advise_justification_df

    if IS_STORING:
        _LOGGER.info("Storing to Ceph...")
        _store_to_ceph(advise_justification_df=advise_justification_df, date_filter=date_filter)


def _store_to_ceph(advise_justification_df: pd.DataFrame, date_filter: datetime.datetime) -> None:
    """Store results to Ceph for visualization."""
    ceph_sli = Adviser.connect_to_ceph(
        ceph_bucket_prefix=CEPH_BUCKET_PREFIX, processed_data_name="thoth-sli-metrics", environment=ENVIRONMENT
    )

    public_ceph_sli = Adviser.connect_to_ceph(
        ceph_bucket_prefix=CEPH_BUCKET_PREFIX,
        processed_data_name="thoth-sli-metrics",
        environment=ENVIRONMENT,
        bucket=PUBLIC_CEPH_BUCKET,
    )

    result_class = "adviser-justifications"
    ceph_path = f"{result_class}/{result_class}-{date_filter.strftime('%Y-%m-%d')}.csv"

    _LOGGER.info(f"Results to be stored on Ceph...{advise_justification_df}")

    csv: str = advise_justification_df.to_csv(header=False, sep="`", index=False)

    try:
        Adviser.store_csv_from_dataframe(
            csv_from_df=csv, ceph_sli=ceph_sli, file_name=result_class, ceph_path=ceph_path
        )
    except Exception as e_ceph:
        _LOGGER.exception(f"Could not store metrics on Thoth bucket on Ceph...{e_ceph}")
        pass

    _LOGGER.info(f"Successfully stored in Thoth bucket on Ceph...{ceph_path}")

    try:
        Adviser.store_csv_from_dataframe(
            csv_from_df=csv, ceph_sli=public_ceph_sli, file_name=result_class, ceph_path=ceph_path, is_public=True
        )
    except Exception as e_ceph:
        _LOGGER.exception(f"Could not store metrics on Public bucket on Ceph...{e_ceph}")
        pass

    _LOGGER.info(f"Successfully stored in Public bucket on Ceph...{ceph_path}")
