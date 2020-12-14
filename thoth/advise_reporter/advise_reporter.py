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

from thoth.report_processing.components import Adviser

_LOGGER = logging.getLogger(__name__)

ENVIRONMENT = os.environ["THOTH_DEPLOYMENT_NAME"].split("-")[1]

CEPH_BUCKET_PREFIX = os.environ["THOTH_CEPH_BUCKET_PREFIX"]
PUBLIC_CEPH_BUCKET = os.environ["THOTH_PUBLIC_CEPH_BUCKET"]

IS_STORING = bool(int(os.getenv("THOTH_IS_STORING", 1)))


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
