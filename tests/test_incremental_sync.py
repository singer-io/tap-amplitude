
from pprint import pprint

import unittest
import tap_amplitude
import os
import pdb

from singer import get_logger, metadata
from utils import get_test_connection, ensure_test_table


LOGGER = get_logger()


# class TestClearState(unittest.TestCase):

    # Test loading bookmark.
    # 

