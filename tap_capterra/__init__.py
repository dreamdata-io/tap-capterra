#!/usr/bin/env python3
import json
import singer
from singer import utils
from tap_capterra.capterra import Capterra
from typing import Optional, Dict

REQUIRED_CONFIG_KEYS = [
    "start_date",
    "api_key",
]
LOGGER = singer.get_logger()
STREAMS = ["clicks"]


def sync(config: Dict, state: Optional[Dict] = None):
    capterra = Capterra(config)
    LOGGER.info(f"syncing clicks")
    capterra.stream(tap_stream_id="clicks", state=state)


@utils.handle_top_exception(LOGGER)
def main():

    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    sync(args.config, args.state)


if __name__ == "__main__":
    main()
