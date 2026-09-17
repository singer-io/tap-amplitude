from base import AmplitudeBaseTest
from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest


class AmplitudeAllFieldsTest(AllFieldsTest, AmplitudeBaseTest):
    @staticmethod
    def name():
        return "tap_tester_amplitude_all_fields_test"

    def streams_to_test(self):
        return self.expected_stream_names()
