import sys
import logging

from airbyte_cdk.entrypoint import launch

from source.source_kraken.source_kraken import SourceKraken


def run() -> None:
    # Configure logging to display debug messages
    logging.basicConfig(level=logging.DEBUG)
    source = SourceKraken()
    launch(source, sys.argv[1:])
