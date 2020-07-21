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

from thoth.messaging import MessageBase, AdviseJustificationMessage
from thoth.advise_reporter.advise_reporter import retrieve_adviser_reports_justifications

app = MessageBase.app

ADVISER_VERSION = os.getenv("ADVISER_VERSION")

_LOGGER = logging.getLogger(__name__)


@app.command()
async def main():
    """Run advise-reporter to produce message."""
    advise_justification = AdviseJustificationMessage()

    advise_justifications = retrieve_adviser_reports_justifications(adviser_version=ADVISER_VERSION)

    for advise_justification_info in advise_justifications.values():
        message = advise_justification_info["message"]
        count = int(advise_justification_info["count"])
        try:
            await advise_justification.publish_to_topic(
                advise_justification.MessageContents(message=message, count=count)
            )
            _LOGGER.debug("Adviser justification message:\n%r\nCount:\n%r\n", message, count)
        except Exception as identifier:
            _LOGGER.exception("Failed to publish with the following error message: %r", identifier)


if __name__ == "__main__":
    app.main()
