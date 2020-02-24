#!/usr/bin/env python3
import os
import json
import singer
from singer import utils, metadata, Catalog, CatalogEntry, Schema
import sys
from tap_capterra.capterra import Capterra

REQUIRED_CONFIG_KEYS = ["start_date", "username", "password"]
LOGGER = singer.get_logger()
STREAMS = {
    "clicks": {
        "key_properties": ["date_of_report"],
        "valid_replication_keys": ["date_of_report"],
    }
}


def get_abs_path(filepath):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), filepath)


def load_schema():
    path = get_abs_path("schemas/clicks.json")
    return utils.load_json(path)


def discover():
    schema = load_schema()
    streams = []

    for tap_stream_id, props in STREAMS.items():
        key_properties = props.get("key_properties")
        mdata = metadata.get_standard_metadata(
            schema=schema,
            key_properties=key_properties,
            valid_replication_keys=STREAMS.get("valid_replication_keys"),
        )
        streams.append(
            CatalogEntry(
                stream=tap_stream_id,
                tap_stream_id=tap_stream_id,
                key_properties=key_properties,
                schema=Schema.from_dict(schema),
                metadata=mdata,
            )
        )
    return Catalog(streams)


def do_discover():
    catalog = discover()
    catalog_dict = catalog.to_dict()
    json.dump(catalog_dict, sys.stdout, indent="  ", sort_keys=True)


def sync(catalog, config, state=None):
    for catalog_entry in catalog.streams:
        # Loop over streams in catalog
        capterra = Capterra(catalog_entry, config)
        capterra.stream(state)
    return


@utils.handle_top_exception(LOGGER)
def main():

    args = utils.parse_args([])

    if args.discover:
        do_discover()
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()

        sync(
            catalog, args.config, args.state,
        )


if __name__ == "__main__":
    main()
