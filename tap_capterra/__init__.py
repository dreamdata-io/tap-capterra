#!/usr/bin/env python3
import os
import json
import singer
from singer import utils, metadata

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

    selected_stream_ids = get_selected_streams(catalog)

    # Loop over streams in catalog
    for stream in catalog["streams"]:
        stream_id = stream["tap_stream_id"]
        stream_schema = stream["schema"]
        if stream_id in selected_stream_ids:
            # TODO: sync code for stream goes here...
            LOGGER.info("Syncing stream:" + stream_id)
    return


@utils.handle_top_exception(LOGGER)
def main():

    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    if args.discover:
        do_discover()
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()

        sync(args.config, args.state, catalog)


if __name__ == "__main__":
    main()
