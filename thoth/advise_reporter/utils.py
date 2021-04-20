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

"""Utils for advise reporter."""

import logging
import os

from typing import Union
from datetime import date

import pandas as pd

from thoth.report_processing.components import Adviser

_LOGGER = logging.getLogger(__name__)

ENVIRONMENT = os.environ["THOTH_DEPLOYMENT_NAME"].split("-")[1]

CEPH_BUCKET_PREFIX = os.environ["THOTH_CEPH_BUCKET_PREFIX"]
PUBLIC_CEPH_BUCKET = os.environ["THOTH_PUBLIC_CEPH_BUCKET"]


def save_results_to_ceph(
    processed_df: pd.DataFrame,
    result_class: str,
    date_filter: Union[date, str] = None,
    store_to_public_ceph: bool = False,
):
    """Save results on Ceph."""
    if processed_df.empty:
        return processed_df

    _LOGGER.info("Storing to Ceph...")

    return _store_to_ceph(
        processed_df=processed_df,
        result_class=result_class,
        date_filter=date_filter,
        store_to_public_ceph=store_to_public_ceph,
    )


def _store_to_ceph(
    processed_df: pd.DataFrame,
    result_class: str,
    date_filter: Union[date, str] = None,
    store_to_public_ceph: bool = False,
) -> None:
    """Store results to Ceph for visualization."""
    ceph_sli = Adviser.connect_to_ceph(
        ceph_bucket_prefix=CEPH_BUCKET_PREFIX, processed_data_name="thoth-sli-metrics", environment=ENVIRONMENT
    )

    if date_filter:
        ceph_path = f"{result_class}/{result_class}-{date_filter}.csv"
    else:
        ceph_path = f"{result_class}/{result_class}.csv"

    _LOGGER.info(f"Results to be stored on Ceph...\n{processed_df}")

    csv: str = processed_df.to_csv(header=False, sep="`", index=False)

    try:
        Adviser.store_csv_from_dataframe(
            csv_from_df=csv, ceph_sli=ceph_sli, file_name=result_class, ceph_path=ceph_path
        )
    except Exception as e_ceph:
        _LOGGER.exception(f"Could not store metrics on Thoth bucket on Ceph...{e_ceph}")
        pass

    if store_to_public_ceph:
        public_ceph_sli = Adviser.connect_to_ceph(
            ceph_bucket_prefix=CEPH_BUCKET_PREFIX,
            processed_data_name="thoth-sli-metrics",
            environment=ENVIRONMENT,
            bucket=PUBLIC_CEPH_BUCKET,
        )

        try:
            Adviser.store_csv_from_dataframe(
                csv_from_df=csv, ceph_sli=public_ceph_sli, file_name=result_class, ceph_path=ceph_path, is_public=True
            )
        except Exception as e_ceph:
            _LOGGER.exception(f"Could not store metrics on Public bucket on Ceph...{e_ceph}")
            pass


def parse_justification(justification: str) -> str:
    """Parse adviser justification."""
    if "https://thoth-station.ninja/j/" not in justification:
        return justification
    return "https://thoth-station.ninja/j/" + justification.split("https://thoth-station.ninja/j/")[1]
