#!/usr/bin/env python3
# thoth-storages
# Copyright(C) 2020 Kevin Postlethwait, Francesco Murdaca
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

import asyncio
import logging
import faust
import os
import ssl

from thoth.lab import adviser

from messages.advise_justification import AdviseJustificationMessage

_LOGGER = logging.getLogger("thoth.advise_reporter")

_KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
_KAFKA_CAFILE = os.getenv("KAFKA_CAFILE", "ca.crt")
KAFKA_CLIENT_ID = os.getenv("KAFKA_CLIENT_ID", "thoth-messaging")
KAFKA_PROTOCOL = os.getenv("KAFKA_PROTOCOL", "SSL")
KAFKA_TOPIC_RETENTION_TIME_SECONDS = 60 * 60 * 24 * 45
ssl_context = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH, cafile=_KAFKA_CAFILE)
app = faust.App("thoth-messaging", broker=_KAFKA_BOOTSTRAP_SERVERS, ssl_context=ssl_context, web_enabled=False)

namespace = os.getenv("THOTH_NAMESPACE")


@app.command()
async def main():
    """Run advise-reporter."""
    advise_justification = AdviseJustificationMessage()

    adviser_dataframe = adviser.aggregate_adviser_results(adviser_version="0.7.3", limit_results=True)
    final_dataframe = adviser.create_final_dataframe(adviser_dataframe=adviser_dataframe)

    advise_justifications = {}

    for index, row in final_dataframe[["jm_hash_id_encoded", "message", "type"]].iterrows():
        encoded_id = row["jm_hash_id_encoded"]
        if encoded_id not in histogram_data.keys():
            advise_justifications[encoded_id] = {
                "jm_hash_id_encoded": f"type-{encoded_id}",
                "message": row["message"],
                "type": row["type"],
                "count": plot_df["jm_hash_id_encoded"].value_counts()[encoded_id]
            }

    for advise_justification_info in advise_justifications.values():
        message = advise_justification_info["message"]
        count = advise_justification_info["count"]
        try:
            await advise_justification.publish_to_topic(
                advise_justification.MessageContents(
                    message=message,
                    count=count,
                )
            )
            _LOGGER.debug("Adviser justification message:\n%r\nCount:\n%r\n", message, count)
        except Exception as identifier:
            _LOGGER.exception("Failed to publish with the following error message: %r", identifier.msg)


if __name__ == "__main__":
    app.main()
