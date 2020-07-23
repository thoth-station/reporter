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

import os
import logging

from thoth.advise_reporter import __service_version__

from thoth.messaging import MessageBase, AdviseJustificationMessage

from thoth.advise_reporter.advise_reporter import expose_metrics

from aiohttp import web
from prometheus_client import generate_latest

# set up logging
DEBUG_LEVEL = bool(int(os.getenv("DEBUG_LEVEL", 0)))

if DEBUG_LEVEL:
    logging.basicConfig(level=logging.DEBUG)
else:
    logging.basicConfig(level=logging.INFO)

_LOGGER = logging.getLogger(__name__)
_LOGGER.info("Thoth Advise Reporter consumer v%s", __service_version__)

# initialize the application
app = MessageBase.app
advise_justification_topic = AdviseJustificationMessage().topic


@app.page("/metrics")
async def get_metrics(self, request):
    """Serve the metrics from the consumer registry."""
    return web.Response(text=generate_latest().decode("utf-8"))


@app.page("/_health")
async def get_health(self, request):
    """Serve a readiness/liveness probe endpoint."""
    data = {"status": "ready", "version": __service_version__}
    return web.json_response(data)


@app.agent(advise_justification_topic)
async def consume_advise_justification(advise_justifications):
    """Loop when an advise justification message is received."""
    async for advise_justification in advise_justifications:
        expose_metrics(advise_justification=advise_justification)


if __name__ == "__main__":
    app.main()
