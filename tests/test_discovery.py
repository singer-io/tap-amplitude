
"""Test tap discovery mode and metadata."""
from base import AmplitudeBaseTest
from tap_tester.base_suite_tests.discovery_test import DiscoveryTest


class AmplitudeDiscoveryTest(DiscoveryTest, AmplitudeBaseTest):
  @staticmethod
  def name():
    return "tap_tester_amplitude_discovery_test"

  def streams_to_test(self):
    return self.expected_stream_names()
