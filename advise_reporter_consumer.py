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

"""Consume messages produced by advise-reporter.py faust app."""

import logging

from thoth.common import init_logging
from thoth.messaging import MessageBase, AdviseJustificationMessage
from prometheus_client import start_http_server, Gauge

init_logging()

_LOGGER = logging.getLogger("thoth.advise_reporter")

app = MessageBase.app

_METRIC_ADVISE_TYPE = Gauge(
    "thoth_advise_type_number",
    "Number of thamos advise provided per type.",
    ["advise_message", "advise_type"],
)

advise_justification_topic = AdviseJustificationMessage().topic


@app.agent(advise_justification_topic)
async def consume_hash_mismatch(advise_justification):
    """Loop when a hash mismatch message is received."""
    _METRIC_ADVISE_TYPE.labels(advise_justification["message"]).set(advise_justification["count"])

if __name__ == "__main__":
    start_http_server(8000)
    app.main()
