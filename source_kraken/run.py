import sys
import logging
from source_kraken import SourceKraken
from airbyte_cdk.entrypoint import launch

def run():
    # Configure logging to display debug messages
    logging.basicConfig(level=logging.DEBUG)
    source = SourceKraken()
    launch(source, sys.argv[1:])
