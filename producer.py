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

from thoth.messaging import MessageBase, AdviseJustificationMessage
from thoth.advise_reporter.advise_reporter import produce_adviser_reports_justifications_dataframe
from thoth.advise_reporter.advise_reporter import parse_adviser_dataframe
from thoth.advise_reporter import __service_version__

app = MessageBase().app

ADVISER_VERSION = os.getenv("ADVISER_VERSION")

COMPONENT_NAME = "advise_reporter"

_LOGGER = logging.getLogger(__name__)


@app.command()
async def main():
    """Run advise-reporter to produce message."""
    advise_justification = AdviseJustificationMessage()

    date = datetime.datetime.utcnow() - datetime.timedelta(days=1)

    final_dataframe = produce_adviser_reports_justifications_dataframe(adviser_version=ADVISER_VERSION)

    for justification_type in ["INFO", "ERROR", "WARNING"]:
        advise_justifications = parse_adviser_dataframe(
            final_dataframe=final_dataframe, date_filter=date, justification_type=justification_type
        )

        if not advise_justifications:
            _LOGGER.info(f"No adviser justifications of type {justification_type} found in date: {date}")

        for advise_justification in advise_justifications:
            message = advise_justification["message"]
            justification_type = justification_type
            count = advise_justification["count"]
            try:
                await advise_justification.publish_to_topic(
                    advise_justification.MessageContents(
                        message=message,
                        count=count,
                        component_name=COMPONENT_NAME,
                        service_version=__service_version__,
                    )
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
    app.main()
