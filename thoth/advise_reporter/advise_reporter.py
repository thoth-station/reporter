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
from thoth.messaging import MessageBase
from thoth.advise_reporter import metrics

_LOGGER = logging.getLogger(__name__)

DEPLOYMENT_NAME = os.environ["THOTH_DEPLOYMENT_NAME"]
ENVIRONMENT = os.environ["THOTH_ENVIRONMENT"]
CEPH_BUCKET_PREFIX = os.environ["THOTH_CEPH_BUCKET_PREFIX"]
PUBLIC_CEPH_BUCKET = os.environ["THOTH_PUBLIC_CEPH_BUCKET"]
LIMIT_RESULTS = bool(int(os.getenv("THOTH_LIMIT_RESULTS", 1)))
MAX_IDS = int(os.getenv("THOTH_MAX_IDS"))
IS_STORING = bool(int(os.getenv("THOTH_IS_STORING", 0)))



def produce_adviser_reports_justifications_dataframe(adviser_version: str) -> pd.DataFrame:
    """Produce adviser reports justifications dataframe."""
    adviser_files = Adviser.aggregate_adviser_results(is_local=False, limit_results=LIMIT_RESULTS, max_ids=MAX_IDS)
    adviser_dataframe = Adviser.create_adviser_dataframe(adviser_version=adviser_version, adviser_files=adviser_files)
    final_dataframe = Adviser.create_summary_dataframe(adviser_dataframe=adviser_dataframe)

    return final_dataframe


def parse_adviser_dataframe(
    final_dataframe: pd.DataFrame, date_filter: datetime.datetime, justification_type: str
) -> List[Dict[str, Any]]:
    """Parse final dataframe to produce messages depending on the justification type."""
    justification_dataframe = final_dataframe[final_dataframe["type"] == justification_type]

    advise_justifications: List[Dict[str, Any]] = []

    if not justification_dataframe.empty:
        adviser_heatmap_df = Adviser.create_adviser_results_dataframe_heatmap(
            adviser_type_dataframe=justification_dataframe, number_days=1
        )
        last_date = [column for column in adviser_heatmap_df.columns][-1]

        if not adviser_heatmap_df.empty and last_date > date_filter:
            subset_adviser_results = adviser_heatmap_df[[last_date]]

            for index, row in subset_adviser_results[[last_date]].iterrows():
                message = index
                if message not in [m["message"] for m in advise_justifications] and row[last_date] > 0:
                    advise_justifications.append(
                        {"date": last_date.strftime("%Y-%m-%d"), "message": message, "count": row[last_date]}
                    )

            advise_justification_df = pd.DataFrame(advise_justifications)

            if IS_STORING:
                _LOGGER("Storing to Ceph...")
                store_to_ceph(
                    advise_justification_df=advise_justification_df,
                    date_filter=date_filter,
                    justification_type=justification_type,
                )

    return advise_justifications


def store_to_ceph(
    advise_justification_df: pd.DataFrame, date_filter: datetime.datetime, justification_type: str
) -> None:
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

    result_class = f"adviser-justificaiton-{justification_type}"
    ceph_path = f"{result_class}/{result_class}-{date_filter}.csv"

    csv: str = advise_justification_df.to_csv(header=False)

    try:
        Adviser.store_csv_from_dataframe(
            csv_from_df=csv, ceph_sli=ceph_sli, file_name=result_class, ceph_path=ceph_path,
        )
    except Exception as e_ceph:
        _LOGGER.exception(f"Could not store metrics on Thoth bucket on Ceph...{e_ceph}")
        pass

    try:
        Adviser.store_csv_from_dataframe(
            csv_from_df=csv, ceph_sli=public_ceph_sli, file_name=result_class, ceph_path=ceph_path, is_public=True
        )
    except Exception as e_ceph:
        _LOGGER.exception(f"Could not store metrics on Public bucket on Ceph...{e_ceph}")
        pass


@metrics.exceptions.count_exceptions()
@metrics.in_progress.track_inprogress()
def expose_metrics(advise_justification: MessageBase):
    """Retrieve adviser reports justifications."""
    metrics.advise_justification_type_number.labels(
        advise_message=advise_justification.message,
        justification_type=advise_justification.justification_type,
        thoth_environment=DEPLOYMENT_NAME,
    ).inc(advise_justification.count)
    _LOGGER.info(
        "advise_justification_type_number(%r, %r)=%r",
        advise_justification.message,
        advise_justification.justification_type,
        advise_justification.count,
    )

    metrics.success.inc()
